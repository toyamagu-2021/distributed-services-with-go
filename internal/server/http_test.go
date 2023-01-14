package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func Test_httpServer_handleProduce(t *testing.T) {
	type fields struct {
		Log *Log
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		want   ProduceResponse
	}{
		{
			"Produce",
			fields{
				NewLog(),
			},
			args{httptest.NewRecorder(), newProduceRequest("/", ProduceRequest{Record{[]byte("test log"), 0}})},
			ProduceResponse{0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &httpServer{
				Log: tt.fields.Log,
			}
			s.handleProduce(tt.args.w, tt.args.r)
			var res ProduceResponse
			_ = json.NewDecoder(tt.args.w.Body).Decode(&res)
			if !reflect.DeepEqual(res, tt.want) {
				t.Errorf("handleProduce = %v, want %v", res, tt.want)
				return
			}
		})
	}
}

func Test_httpServer_handleConsume(t *testing.T) {
	type fields struct {
		Log *Log
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}

	recs := []Record{
		{[]byte("test log1"), 0},
		{[]byte("test log2"), 1},
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		want   ConsumeResponse
	}{
		{
			"Consume offset 0",
			fields{
				&Log{records: recs},
			},
			args{httptest.NewRecorder(), newConsumeRequest("/", ConsumeRequest{0})},
			ConsumeResponse{recs[0]},
		},
		{
			"Consume offset 1",
			fields{
				&Log{records: recs},
			},
			args{httptest.NewRecorder(), newConsumeRequest("/", ConsumeRequest{1})},
			ConsumeResponse{recs[1]},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &httpServer{
				Log: tt.fields.Log,
			}
			s.handleConsume(tt.args.w, tt.args.r)
			var res ConsumeResponse
			_ = json.NewDecoder(tt.args.w.Body).Decode(&res)
			if !reflect.DeepEqual(res, tt.want) {
				t.Errorf("handleProduce = %v, want %v", res, tt.want)
				return
			}
		})
	}
}

func newProduceRequest(route string, pr ProduceRequest) *http.Request {
	var body bytes.Buffer
	json.NewEncoder(&body).Encode(pr)
	req, _ := http.NewRequest(http.MethodPost, route, &body)
	return req
}

func newConsumeRequest(route string, cr ConsumeRequest) *http.Request {
	var body bytes.Buffer
	json.NewEncoder(&body).Encode(cr)
	req, _ := http.NewRequest(http.MethodGet, route, &body)
	return req
}

func Test_NewHttpServer(t *testing.T) {
	srv := NewHTTPServer(":8080")
	rec1 := Record{[]byte("test log1"), 0}
	rec2 := Record{[]byte("test log2"), 0}
	prec1 := httptest.NewRecorder()
	prec2 := httptest.NewRecorder()
	crec1 := httptest.NewRecorder()
	crec2 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(prec1, newProduceRequest("/", ProduceRequest{rec1}))
	srv.Handler.ServeHTTP(prec2, newProduceRequest("/", ProduceRequest{rec2}))
	srv.Handler.ServeHTTP(crec1, newConsumeRequest("/", ConsumeRequest{0}))
	srv.Handler.ServeHTTP(crec2, newConsumeRequest("/", ConsumeRequest{1}))

	var cres ConsumeResponse
	_ = json.NewDecoder(crec1.Body).Decode(&cres)
	if !reflect.DeepEqual(cres.Record, rec1) {
		t.Errorf("NewHTTPServer = %v, want %v", cres.Record, rec1)
		return
	}

	_ = json.NewDecoder(crec2.Body).Decode(&cres)
	rec2_offset := Record{rec2.Value, 1}
	if !reflect.DeepEqual(cres.Record, rec2_offset) {
		t.Errorf("NewHTTPServer = %v, want %v", cres.Record, rec2_offset)
		return
	}
}
