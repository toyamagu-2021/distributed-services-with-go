package server

import (
	"log"
	"os"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	api "github.com/toyamagu-2021/distributed-services-with-go/api/v1"

	"github.com/toyamagu-2021/distributed-services-with-go/internal/trace"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

var tracer = otel.Tracer("github.com/toyamagu-2021/distributed-service-with-go")

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error) {
	_, span := tracer.Start(ctx, "produce")
	defer span.End()

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	_, span := tracer.Start(ctx, "consume")
	defer span.End()

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	// Logging by Zap
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64(
					"grpc.time_ns",
					duration.Nanoseconds(),
				)
			},
		),
	}

	// Trace and Metrics by OpenTelemetry
	tp, err := trace.NewTraceProviderWithStdoutTrace(os.Stderr)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shuttting donw tracer provier: %v", err)
		}
	}()

	// Create gRPC server
	opts = append(
		opts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				otelgrpc.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
				grpc_auth.StreamServerInterceptor(authenticate),
			)),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				otelgrpc.UnaryServerInterceptor(),
				grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
				grpc_auth.UnaryServerInterceptor(authenticate),
			)),
	)
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}
	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}
	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)
	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
