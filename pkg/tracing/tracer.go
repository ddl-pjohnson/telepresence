package tracing

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dhttp"
	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/rpc/v2/common"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type TraceConfig struct {
	ProcessID   int64
	ProcessName string
}

const (
	DaemonPort = 17666
	UserdPort  = 17667
)

// TODO: Make this an actual config thing
func TracingPort(ctx context.Context) (int, bool) {
	if port, ok := os.LookupEnv("TELEPRESENCE_GRPC_TRACE_PORT"); ok {
		portInt, err := strconv.Atoi(port)
		if err != nil {
			dlog.Warnf(ctx, "Unable to turn port into number: %v", err)
			return 0, false
		}
		return portInt, true
	}
	return 0, false
}

func setupTracer(ctx context.Context, cfg TraceConfig, client otlptrace.Client) (*tracesdk.TracerProvider, error) {
	exp, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, err
	}
	tp := tracesdk.NewTracerProvider(
		// Always be sure to batch in production.
		tracesdk.WithBatcher(exp),
		tracesdk.WithSampler(tracesdk.AlwaysSample()),
		// Record information about this application in a Resource.
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(cfg.ProcessName),
			attribute.Int64("ID", cfg.ProcessID),
		)),
	)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	otel.SetTracerProvider(tp)
	return tp, nil

}

type TraceServer struct {
	common.UnimplementedTracingServerServer
	shim *otlpShim
	port int
	tp   *tracesdk.TracerProvider
}

func NewTraceServer(ctx context.Context, port int, cfg TraceConfig) (*TraceServer, error) {
	client := &otlpShim{}
	tp, err := setupTracer(ctx, cfg, client)
	if err != nil {
		return nil, err
	}

	return &TraceServer{
		port: port,
		tp:   tp,
		shim: client,
	}, nil
}

func (ts *TraceServer) Shutdown(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	if err := ts.tp.Shutdown(ctx); err != nil {
		dlog.Error(ctx, "error shutting down tracer: ", err)
	}
	otel.SetTracerProvider(trace.NewNoopTracerProvider())
}

func (ts *TraceServer) serveGrpc(ctx context.Context) error {
	opts := []grpc.ServerOption{}
	grpcHandler := grpc.NewServer(opts...)
	sc := &dhttp.ServerConfig{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
				grpcHandler.ServeHTTP(w, r)
			} else {
				w.WriteHeader(404) // Nothing doing pal
			}
		}),
	}

	common.RegisterTracingServerServer(grpcHandler, ts)

	return sc.ListenAndServe(ctx, fmt.Sprintf("0.0.0.0:%d", ts.port))
}

func (ts *TraceServer) DumpTraces(ctx context.Context, _ *emptypb.Empty) (*common.Trace, error) {
	b, err := ts.shim.dumpTraces(ctx)
	if err != nil {
		return nil, err
	}
	return &common.Trace{
		TraceData: b,
	}, nil
}

func (ts *TraceServer) Run(ctx context.Context) error {
	wg := dgroup.NewGroup(ctx, dgroup.GroupConfig{})
	wg.Go("grpc", ts.serveGrpc)
	// wg.Go("trace-gc", ts.pruneTraces)
	return wg.Wait()
}
