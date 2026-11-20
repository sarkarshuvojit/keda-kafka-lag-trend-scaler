package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/sarkarshuvojit/keda-kafka-lag-trend-scaler/scaler/pkg/config"
	pb "github.com/sarkarshuvojit/keda-kafka-lag-trend-scaler/scaler/pkg/externalscaler"
	"github.com/sarkarshuvojit/keda-kafka-lag-trend-scaler/scaler/pkg/kafka"
	"github.com/sarkarshuvojit/keda-kafka-lag-trend-scaler/scaler/pkg/lag"
	"github.com/sarkarshuvojit/keda-kafka-lag-trend-scaler/scaler/pkg/metrics"
	"github.com/sarkarshuvojit/keda-kafka-lag-trend-scaler/scaler/pkg/scraper"
	"github.com/sarkarshuvojit/keda-kafka-lag-trend-scaler/scaler/pkg/server"
)

func main() {
	cfg, err := config.ParseFromEnv()
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	log.Printf("Starting Kafka lag trend scaler")
	log.Printf("  Brokers:            %s", cfg.BootstrapServers)
	log.Printf("  Topic:              %s", cfg.Topic)
	log.Printf("  Consumer Group:     %s", cfg.ConsumerGroup)
	log.Printf("  Sampling Interval:  %s", cfg.SamplingInterval)
	log.Printf("  Trend Window:       %s", cfg.TrendWindow)
	log.Printf("  Max Drain (SLO):    %s", cfg.MaxDrain)
	log.Printf("  Min Lag:            %d", cfg.MinLag)
	log.Printf("  Headroom:           %.2f", cfg.Headroom)
	if cfg.PerConsumerRate > 0 {
		log.Printf("  Per-Consumer Rate:  %.2f msgs/s (fixed)", cfg.PerConsumerRate)
	} else {
		log.Printf("  Per-Consumer Rate:  learned (smoothing %.2f)", cfg.CapacitySmoothing)
	}
	log.Printf("  Consumers:          min %d, max %d (0 = partition count)", cfg.MinConsumers, cfg.MaxConsumers)
	log.Printf("  Min Samples:        %d", cfg.MinSamples)

	fetcher := kafka.NewLagFetcher(cfg.BootstrapServers, cfg.Topic, cfg.ConsumerGroup)
	window := lag.NewSlidingWindow(cfg.WindowDuration())
	scr := scraper.New(fetcher, window, cfg.SamplingInterval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start background scraper
	go scr.Run(ctx)

	// Start metrics server
	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "9090"
	}
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metrics.Handler())
	metricsServer := &http.Server{Addr: ":" + metricsPort, Handler: metricsMux}
	go func() {
		log.Printf("Metrics server listening on :%s", metricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	// Start gRPC server
	port := os.Getenv("GRPC_PORT")
	if port == "" {
		port = "50051"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterExternalScalerServer(grpcServer, server.New(window, cfg))

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		log.Println("Received shutdown signal, stopping...")
		cancel()
		grpcServer.GracefulStop()
		metricsServer.Close()
	}()

	log.Printf("gRPC server listening on :%s", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
