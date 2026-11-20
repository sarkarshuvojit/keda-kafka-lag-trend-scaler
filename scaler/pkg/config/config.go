package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/sarkarshuvojit/keda-kafka-lag-trend-scaler/scaler/pkg/lag"
)

type ScalerConfig struct {
	BootstrapServers string
	Topic            string
	ConsumerGroup    string
	SamplingInterval time.Duration

	// MinLag: lag below this is ignored (never stalled, scaler inactive).
	MinLag int64
	// MaxDrain is the SLO: the backlog must be projected to drain within this.
	MaxDrain time.Duration
	// TrendWindow is how much history the slope and rate fits use.
	TrendWindow time.Duration
	// Headroom multiplies the throughput needed before converting to replicas.
	Headroom float64
	// PerConsumerRate (msgs/sec), when > 0, replaces the learned capacity.
	PerConsumerRate float64
	// CapacitySmoothing is the EWMA alpha for learned capacity, in (0, 1].
	CapacitySmoothing float64
	// MinConsumers / MaxConsumers clamp the desired count. MaxConsumers 0 =
	// partition count.
	MinConsumers int
	MaxConsumers int
	// MinSamples is the minimum number of scrapes before a decision is made.
	MinSamples int
}

// TrendParams returns the evaluator parameters for this config.
func (c *ScalerConfig) TrendParams() lag.TrendParams {
	return lag.TrendParams{
		MinLag:            c.MinLag,
		MaxDrain:          c.MaxDrain,
		TrendWindow:       c.TrendWindow,
		Headroom:          c.Headroom,
		PerConsumerRate:   c.PerConsumerRate,
		CapacitySmoothing: c.CapacitySmoothing,
		MinConsumers:      c.MinConsumers,
		MaxConsumers:      c.MaxConsumers,
		MinSamples:        c.MinSamples,
	}
}

// WindowDuration is how long the sliding window keeps batches: the trend
// window plus a couple of scrapes of slack.
func (c *ScalerConfig) WindowDuration() time.Duration {
	return c.TrendWindow + 2*c.SamplingInterval
}

func ParseFromMetadata(metadata map[string]string) (*ScalerConfig, error) {
	cfg := &ScalerConfig{
		BootstrapServers: getMetadataOrEnv(metadata, "bootstrapServers", "KAFKA_BROKERS", "localhost:9092"),
		Topic:            getMetadataOrEnv(metadata, "topic", "KAFKA_TOPIC", ""),
		ConsumerGroup:    getMetadataOrEnv(metadata, "consumerGroup", "KAFKA_GROUP_ID", ""),
	}

	if cfg.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if cfg.ConsumerGroup == "" {
		return nil, fmt.Errorf("consumerGroup is required")
	}

	p := parser{metadata: metadata}
	cfg.SamplingInterval = p.seconds("samplingInterval", "SAMPLING_INTERVAL", 10)
	cfg.MinLag = p.int64("minLag", "MIN_LAG", 100)
	cfg.MaxDrain = p.seconds("maxDrainSeconds", "MAX_DRAIN_SECONDS", 300)
	cfg.TrendWindow = p.seconds("trendWindowSeconds", "TREND_WINDOW_SECONDS", 120)
	cfg.Headroom = p.float("headroom", "HEADROOM", 1.2)
	cfg.PerConsumerRate = p.float("perConsumerRate", "PER_CONSUMER_RATE", 0)
	cfg.CapacitySmoothing = p.float("capacitySmoothing", "CAPACITY_SMOOTHING", 0.3)
	cfg.MinConsumers = int(p.int64("minConsumers", "MIN_CONSUMERS", 1))
	cfg.MaxConsumers = int(p.int64("maxConsumers", "MAX_CONSUMERS", 0))
	cfg.MinSamples = int(p.int64("minSamples", "MIN_SAMPLES", 6))
	if p.err != nil {
		return nil, p.err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func ParseFromEnv() (*ScalerConfig, error) {
	return ParseFromMetadata(nil)
}

func (c *ScalerConfig) validate() error {
	switch {
	case c.SamplingInterval <= 0:
		return fmt.Errorf("samplingInterval must be > 0")
	case c.MinLag < 0:
		return fmt.Errorf("minLag must be >= 0")
	case c.MaxDrain <= 0:
		return fmt.Errorf("maxDrainSeconds must be > 0")
	case c.TrendWindow < 2*c.SamplingInterval:
		return fmt.Errorf("trendWindowSeconds (%s) must be at least 2x samplingInterval (%s)", c.TrendWindow, c.SamplingInterval)
	case c.Headroom <= 0:
		return fmt.Errorf("headroom must be > 0")
	case c.PerConsumerRate < 0:
		return fmt.Errorf("perConsumerRate must be >= 0")
	case c.CapacitySmoothing <= 0 || c.CapacitySmoothing > 1:
		return fmt.Errorf("capacitySmoothing must be in (0, 1]")
	case c.MinConsumers < 0:
		return fmt.Errorf("minConsumers must be >= 0")
	case c.MaxConsumers < 0:
		return fmt.Errorf("maxConsumers must be >= 0")
	case c.MaxConsumers > 0 && c.MaxConsumers < c.MinConsumers:
		return fmt.Errorf("maxConsumers (%d) must be >= minConsumers (%d)", c.MaxConsumers, c.MinConsumers)
	case c.MinSamples < 2:
		return fmt.Errorf("minSamples must be >= 2")
	}
	return nil
}

// parser reads a value from metadata, then env, then a default, and keeps the
// first parse error.
type parser struct {
	metadata map[string]string
	err      error
}

func (p *parser) raw(key, envKey string) (string, string, bool) {
	if v, ok := p.metadata[key]; ok {
		return v, key, true
	}
	if v := os.Getenv(envKey); v != "" {
		return v, envKey, true
	}
	return "", "", false
}

func (p *parser) int64(key, envKey string, def int64) int64 {
	v, name, ok := p.raw(key, envKey)
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("invalid %s: %w", name, err)
	}
	return n
}

func (p *parser) float(key, envKey string, def float64) float64 {
	v, name, ok := p.raw(key, envKey)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("invalid %s: %w", name, err)
	}
	return f
}

func (p *parser) seconds(key, envKey string, def int64) time.Duration {
	return time.Duration(p.int64(key, envKey, def)) * time.Second
}

func getMetadataOrEnv(metadata map[string]string, key, envKey, defaultVal string) string {
	if v, ok := metadata[key]; ok && v != "" {
		return v
	}
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	return defaultVal
}
