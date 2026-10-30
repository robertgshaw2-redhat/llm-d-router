/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/llm-d/llm-d-router/pkg/cacheledger"
	"github.com/llm-d/llm-d-router/pkg/cacheledger/api/gen"
	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/version"
)

const (
	storeMemory = "memory"
	storeRedis  = "redis"
)

func main() {
	grpcAddr := pflag.String("grpc-addr", ":9500", "address the CacheLedger gRPC service listens on")
	metricsPort := pflag.Int("metrics-port", 9090, "port for the Prometheus /metrics endpoint. Non-positive disables the endpoint.")
	storeKind := pflag.String("store", storeMemory, "where ledger entries live: memory (one replica, lost on restart) or redis (shared by replicas)")
	redisURL := pflag.String("redis-url", "", "Redis URL for --store=redis, for example redis://redis:6379/0")
	defaultTTL := pflag.Duration("default-ttl", 5*time.Minute, "retention for a request that names no TTL")
	maxTTL := pflag.Duration("max-ttl", time.Hour, "upper bound on a requested retention")
	maxBlocks := pflag.Int("max-blocks", 65536, "maximum block hashes in one request")
	sweepInterval := pflag.Duration("sweep-interval", 30*time.Second, "how often in-process entries past their expiry are dropped")
	localCacheTTL := pflag.Duration("local-cache-ttl", time.Minute, "with --store=redis, how long this replica keeps entries it recorded, answering hits on them without Redis. Zero sends every lookup to Redis.")
	flushInterval := pflag.Duration("flush-interval", 200*time.Millisecond, "with --local-cache-ttl, how often TTL refreshes of locally matched entries are written to Redis. Other replicas see a refresh up to this late.")

	logOpts := logutil.NewOptions()
	logOpts.AddFlags(pflag.CommandLine)
	pflag.Parse()

	logutil.InitSetupLogging("llm-d-cache-ledger")
	log := ctrl.Log.WithName("cache-ledger")
	log.Info("cache-ledger build", "commit-sha", version.CommitSHA, "build-ref", version.BuildRef)

	if *defaultTTL <= 0 || *maxTTL < *defaultTTL {
		log.Error(nil, "--default-ttl must be positive and no larger than --max-ttl", "default-ttl", *defaultTTL, "max-ttl", *maxTTL)
		os.Exit(1)
	}
	if *maxBlocks <= 0 {
		log.Error(nil, "--max-blocks must be positive", "max-blocks", *maxBlocks)
		os.Exit(1)
	}

	cfg := cacheledger.ServerConfig{DefaultTTL: *defaultTTL, MaxTTL: *maxTTL, MaxBlocks: *maxBlocks}
	if err := run(options{
		grpcAddr:      *grpcAddr,
		metricsPort:   *metricsPort,
		storeKind:     *storeKind,
		redisURL:      *redisURL,
		sweepInterval: *sweepInterval,
		localCacheTTL: *localCacheTTL,
		flushInterval: *flushInterval,
		server:        cfg,
	}); err != nil {
		log.Error(err, "cache-ledger failed")
		os.Exit(1)
	}
}

type options struct {
	grpcAddr      string
	metricsPort   int
	storeKind     string
	redisURL      string
	sweepInterval time.Duration
	localCacheTTL time.Duration
	flushInterval time.Duration
	server        cacheledger.ServerConfig
}

func run(opts options) error {
	log := ctrl.Log.WithName("cache-ledger")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	var store cacheledger.Store
	var cached *cacheledger.CachedStore
	switch opts.storeKind {
	case storeMemory:
		mem := cacheledger.NewMemoryStore()
		g.Go(func() error {
			mem.Run(gctx, opts.sweepInterval)
			return nil
		})
		store = mem
	case storeRedis:
		rs, err := cacheledger.NewRedisStore(opts.redisURL)
		if err != nil {
			return err
		}
		defer func() { _ = rs.Close() }()
		if err := rs.Ping(ctx); err != nil {
			return fmt.Errorf("redis at %s is unreachable: %w", opts.redisURL, err)
		}
		store = rs
		if opts.localCacheTTL > 0 {
			cached = cacheledger.NewCachedStore(rs, opts.localCacheTTL, reg)
			g.Go(func() error {
				cached.Run(gctx, log, opts.flushInterval, opts.sweepInterval)
				return nil
			})
			store = cached
		}
	default:
		return fmt.Errorf("unknown --store %q", opts.storeKind)
	}

	srv := grpc.NewServer()
	gen.RegisterCacheLedgerServer(srv, cacheledger.NewServer(store, opts.server, reg))
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)

	lis, err := net.Listen("tcp", opts.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", opts.grpcAddr, err)
	}
	g.Go(func() error {
		return srv.Serve(lis)
	})
	g.Go(func() error {
		<-gctx.Done()
		healthSrv.Shutdown()
		srv.GracefulStop()
		return nil
	})
	if opts.metricsPort > 0 {
		g.Go(func() error {
			return serveMetrics(gctx, opts.metricsPort, reg)
		})
	}

	log.Info("serving", "grpc-addr", opts.grpcAddr, "store", opts.storeKind, "local-cache-ttl", opts.localCacheTTL,
		"default-ttl", opts.server.DefaultTTL, "max-ttl", opts.server.MaxTTL)
	err = g.Wait()
	// The gRPC server has stopped, so nothing queues behind this flush.
	if cached != nil {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cached.Flush(flushCtx, log)
		cancel()
	}
	return err
}

func serveMetrics(ctx context.Context, port int, reg *prometheus.Registry) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true}))
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("metrics server: %w", err)
	}
	return nil
}
