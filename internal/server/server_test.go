package server

import (
	"context"
	"flag"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/toyamagu-2021/distributed-services-with-go/api/v1"
	"github.com/toyamagu-2021/distributed-services-with-go/internal/auth"
	"github.com/toyamagu-2021/distributed-services-with-go/internal/config"
	"github.com/toyamagu-2021/distributed-services-with-go/internal/server/log"
	"github.com/toyamagu-2021/distributed-services-with-go/internal/trace"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging.")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume a stream succeeds":                  testProduceConsumeStream,
		"consume past lg boundary fails":                     testConsumePastBoundary,
		"unauthorized fails":                                 testUnAuthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()

			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		*sdktrace.TracerProvider,
		[]grpc.DialOption,
	) {

		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
		})
		require.NoError(t, err)
		// Setup Client
		tlsCreds := credentials.NewTLS(tlsConfig)

		// Without TLS
		// clientOptions := []grpc.DialOption{grpc.WithInsecure()}

		// Trace
		tp, err := trace.NewTraceProviderWithStdoutTrace(os.Stderr)
		require.NoError(t, err)

		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(tlsCreds),
			grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
			grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()),
		}

		conn, err := grpc.Dial(
			l.Addr().String(),
			opts...,
		)
		require.NoError(t, err)

		client := api.NewLogClient(conn)
		return conn, client, tp, opts
	}

	var rootConn *grpc.ClientConn
	var rootTp *sdktrace.TracerProvider
	rootConn, rootClient, rootTp, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)

	var nobodyConn *grpc.ClientConn
	var clientTp *sdktrace.TracerProvider
	nobodyConn, nobodyClient, clientTp, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	// Setup Server
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)

	cfg = &Config{CommitLog: clog, Authorizer: authorizer}
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)
	go func() {
		server.Serve(l)
	}()
	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		clog.Remove()
		rootTp.Shutdown(context.Background())
		clientTp.Shutdown(context.Background())
	}
}

func testProduceConsume(
	t *testing.T, client,
	_ api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "testProduceConsume")
	defer span.End()
	want := &api.Record{
		Value: []byte("hello world"),
	}
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(
	t *testing.T,
	client, _ api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "testConsumePastBoundary")
	defer span.End()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(
	t *testing.T,
	client, _ api.LogClient,
	config *Config) {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "testProduceConsumeStream")
	defer span.End()
	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)
		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}
	}
	{
		stream, err := client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{Offset: 0},
		)
		require.NoError(t, err)
		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.True(t, proto.Equal(res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			}))
		}
	}
}

func testUnAuthorized(
	t *testing.T,
	_,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "testUnAuthorized")
	defer span.End()
	produce, err := client.Produce(ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		},
	)
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}
