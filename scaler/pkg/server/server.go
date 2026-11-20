package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sarkarshuvojit/keda-persistent-kafka-lag-scaler/scaler/pkg/config"
	pb "github.com/sarkarshuvojit/keda-persistent-kafka-lag-scaler/scaler/pkg/externalscaler"
	"github.com/sarkarshuvojit/keda-persistent-kafka-lag-scaler/scaler/pkg/lag"
	"github.com/sarkarshuvojit/keda-persistent-kafka-lag-scaler/scaler/pkg/metrics"
)

// MetricName is the external metric reported to KEDA. Its value is the
// desired consumer count; with TargetSize 1, HPA's desired replicas equal it.
const MetricName = "kafka_desired_consumers"

// ErrNoData is returned by GetMetrics before the first scrape lands, so HPA
// keeps the current replica count instead of acting on a made-up number.
var ErrNoData = fmt.Errorf("no lag data collected yet")

type ExternalScalerServer struct {
	pb.UnimplementedExternalScalerServer
	window *lag.SlidingWindow
	config *config.ScalerConfig

	mu    sync.Mutex
	state lag.State
}

func New(window *lag.SlidingWindow, cfg *config.ScalerConfig) *ExternalScalerServer {
	return &ExternalScalerServer{
		window: window,
		config: cfg,
	}
}

func (s *ExternalScalerServer) IsActive(ctx context.Context, ref *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	r := s.evaluate()
	log.Printf("IsActive: active=%v %s", r.Active, describe(r))
	return &pb.IsActiveResponse{Result: r.Active}, nil
}

func (s *ExternalScalerServer) StreamIsActive(ref *pb.ScaledObjectRef, stream pb.ExternalScaler_StreamIsActiveServer) error {
	ticker := time.NewTicker(s.config.SamplingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			r := s.evaluate()
			log.Printf("StreamIsActive: active=%v %s", r.Active, describe(r))
			if err := stream.Send(&pb.IsActiveResponse{Result: r.Active}); err != nil {
				return fmt.Errorf("error sending stream: %w", err)
			}
		}
	}
}

func (s *ExternalScalerServer) GetMetricSpec(ctx context.Context, ref *pb.ScaledObjectRef) (*pb.GetMetricSpecResponse, error) {
	return &pb.GetMetricSpecResponse{
		MetricSpecs: []*pb.MetricSpec{{MetricName: MetricName, TargetSize: 1}},
	}, nil
}

func (s *ExternalScalerServer) GetMetrics(ctx context.Context, req *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	if s.window.Len() == 0 {
		return nil, ErrNoData
	}
	r := s.evaluate()
	log.Printf("GetMetrics: desired=%d %s", r.Desired, describe(r))
	return &pb.GetMetricsResponse{
		MetricValues: []*pb.MetricValue{{MetricName: MetricName, MetricValue: r.Desired}},
	}, nil
}

func (s *ExternalScalerServer) evaluate() lag.TrendResult {
	batches := s.window.Snapshot()

	s.mu.Lock()
	r := lag.EvaluateTrend(batches, s.config.TrendParams(), s.state)
	s.state = r.State
	s.mu.Unlock()

	metrics.UpdateTrend(s.config.Topic, r)
	return r
}

func describe(r lag.TrendResult) string {
	if r.WarmingUp {
		return fmt.Sprintf("warmingUp lag=%d consumers=%d desired=%d", r.Lag, r.Consumers, r.Desired)
	}
	return fmt.Sprintf("stalled=%v lag=%d slope=%.1f/s produce=%.1f/s consume=%.1f/s drainETA=%.0fs capacity=%.1f/s consumers=%d desired=%d",
		r.Stalled, r.Lag, r.LagSlope, r.ProduceRate, r.ConsumeRate, r.DrainETA, r.State.Rate, r.Consumers, r.Desired)
}
