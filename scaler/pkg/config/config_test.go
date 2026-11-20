package config

import (
	"strings"
	"testing"
	"time"
)

// clearEnv blanks every env var the parser reads, so tests don't pick up
// whatever the shell or CI job has set (e.g. KAFKA_BROKERS for integration
// tests). An empty value counts as unset.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"KAFKA_BROKERS", "KAFKA_TOPIC", "KAFKA_GROUP_ID", "SAMPLING_INTERVAL",
		"MIN_LAG", "MAX_DRAIN_SECONDS", "TREND_WINDOW_SECONDS", "HEADROOM",
		"PER_CONSUMER_RATE", "CAPACITY_SMOOTHING", "MIN_CONSUMERS",
		"MAX_CONSUMERS", "MIN_SAMPLES",
	} {
		t.Setenv(k, "")
	}
}

func base() map[string]string {
	return map[string]string{"topic": "my-topic", "consumerGroup": "my-group"}
}

func with(kv ...string) map[string]string {
	m := base()
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

func TestParseFromMetadata_Defaults(t *testing.T) {
	clearEnv(t)
	cfg, err := ParseFromMetadata(base())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	checks := []struct {
		name      string
		got, want any
	}{
		{"bootstrapServers", cfg.BootstrapServers, "localhost:9092"},
		{"samplingInterval", cfg.SamplingInterval, 10 * time.Second},
		{"minLag", cfg.MinLag, int64(100)},
		{"maxDrain", cfg.MaxDrain, 300 * time.Second},
		{"trendWindow", cfg.TrendWindow, 120 * time.Second},
		{"headroom", cfg.Headroom, 1.2},
		{"perConsumerRate", cfg.PerConsumerRate, 0.0},
		{"capacitySmoothing", cfg.CapacitySmoothing, 0.3},
		{"minConsumers", cfg.MinConsumers, 1},
		{"maxConsumers", cfg.MaxConsumers, 0},
		{"minSamples", cfg.MinSamples, 6},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if cfg.WindowDuration() != 140*time.Second {
		t.Errorf("window duration = %s, want 2m20s", cfg.WindowDuration())
	}
}

func TestParseFromMetadata_AllFields(t *testing.T) {
	clearEnv(t)
	cfg, err := ParseFromMetadata(map[string]string{
		"bootstrapServers":   "broker1:9092,broker2:9092",
		"topic":              "orders",
		"consumerGroup":      "order-processor",
		"samplingInterval":   "5",
		"minLag":             "50",
		"maxDrainSeconds":    "600",
		"trendWindowSeconds": "180",
		"headroom":           "1.5",
		"perConsumerRate":    "42.5",
		"capacitySmoothing":  "0.5",
		"minConsumers":       "2",
		"maxConsumers":       "12",
		"minSamples":         "10",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.BootstrapServers != "broker1:9092,broker2:9092" || cfg.Topic != "orders" || cfg.ConsumerGroup != "order-processor" {
		t.Errorf("connection fields wrong: %+v", cfg)
	}
	p := cfg.TrendParams()
	if cfg.SamplingInterval != 5*time.Second ||
		p.MinLag != 50 ||
		p.MaxDrain != 600*time.Second ||
		p.TrendWindow != 180*time.Second ||
		p.Headroom != 1.5 ||
		p.PerConsumerRate != 42.5 ||
		p.CapacitySmoothing != 0.5 ||
		p.MinConsumers != 2 ||
		p.MaxConsumers != 12 ||
		p.MinSamples != 10 {
		t.Errorf("unexpected params: %+v", p)
	}
}

func TestParseFromMetadata_MissingTopic(t *testing.T) {
	clearEnv(t)
	_, err := ParseFromMetadata(map[string]string{"consumerGroup": "my-group"})
	if err == nil || err.Error() != "topic is required" {
		t.Fatalf("expected 'topic is required', got %v", err)
	}
}

func TestParseFromMetadata_MissingConsumerGroup(t *testing.T) {
	clearEnv(t)
	_, err := ParseFromMetadata(map[string]string{"topic": "my-topic"})
	if err == nil || err.Error() != "consumerGroup is required" {
		t.Fatalf("expected 'consumerGroup is required', got %v", err)
	}
}

func TestParseFromMetadata_InvalidNumbers(t *testing.T) {
	clearEnv(t)
	for _, key := range []string{
		"samplingInterval", "minLag", "maxDrainSeconds", "trendWindowSeconds",
		"headroom", "perConsumerRate", "capacitySmoothing", "minConsumers",
		"maxConsumers", "minSamples",
	} {
		t.Run(key, func(t *testing.T) {
			_, err := ParseFromMetadata(with(key, "not-a-number"))
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("expected parse error naming %s, got %v", key, err)
			}
		})
	}
}

func TestParseFromMetadata_Validation(t *testing.T) {
	clearEnv(t)
	cases := map[string][]string{
		"zero sampling":            {"samplingInterval", "0"},
		"negative minLag":          {"minLag", "-1"},
		"zero maxDrain":            {"maxDrainSeconds", "0"},
		"window too short":         {"trendWindowSeconds", "15"},
		"zero headroom":            {"headroom", "0"},
		"negative perConsumerRate": {"perConsumerRate", "-3"},
		"smoothing zero":           {"capacitySmoothing", "0"},
		"smoothing above one":      {"capacitySmoothing", "1.5"},
		"negative minConsumers":    {"minConsumers", "-1"},
		"max below min":            {"minConsumers", "5", "maxConsumers", "3"},
		"minSamples too low":       {"minSamples", "1"},
	}
	for name, kv := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseFromMetadata(with(kv...)); err == nil {
				t.Fatalf("expected validation error for %v", kv)
			}
		})
	}
}

func TestParseFromMetadata_EnvVarFallback(t *testing.T) {
	clearEnv(t)
	t.Setenv("KAFKA_BROKERS", "env-broker:9092")
	t.Setenv("KAFKA_TOPIC", "env-topic")
	t.Setenv("KAFKA_GROUP_ID", "env-group")
	t.Setenv("SAMPLING_INTERVAL", "15")
	t.Setenv("MIN_LAG", "250")
	t.Setenv("MAX_DRAIN_SECONDS", "90")
	t.Setenv("TREND_WINDOW_SECONDS", "60")
	t.Setenv("HEADROOM", "1.1")
	t.Setenv("PER_CONSUMER_RATE", "7")
	t.Setenv("CAPACITY_SMOOTHING", "0.9")
	t.Setenv("MIN_CONSUMERS", "0")
	t.Setenv("MAX_CONSUMERS", "8")
	t.Setenv("MIN_SAMPLES", "3")

	cfg, err := ParseFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.BootstrapServers != "env-broker:9092" || cfg.Topic != "env-topic" || cfg.ConsumerGroup != "env-group" {
		t.Errorf("connection fields wrong: %+v", cfg)
	}
	if cfg.SamplingInterval != 15*time.Second ||
		cfg.MinLag != 250 ||
		cfg.MaxDrain != 90*time.Second ||
		cfg.TrendWindow != 60*time.Second ||
		cfg.Headroom != 1.1 ||
		cfg.PerConsumerRate != 7 ||
		cfg.CapacitySmoothing != 0.9 ||
		cfg.MinConsumers != 0 ||
		cfg.MaxConsumers != 8 ||
		cfg.MinSamples != 3 {
		t.Errorf("env values not applied: %+v", cfg)
	}
}

func TestParseFromMetadata_MetadataOverridesEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("KAFKA_BROKERS", "env-broker:9092")
	t.Setenv("MAX_DRAIN_SECONDS", "999")

	cfg, err := ParseFromMetadata(with("bootstrapServers", "meta-broker:9092", "maxDrainSeconds", "100"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.BootstrapServers != "meta-broker:9092" {
		t.Errorf("bootstrapServers = %s, want metadata value", cfg.BootstrapServers)
	}
	if cfg.MaxDrain != 100*time.Second {
		t.Errorf("maxDrain = %s, want metadata value 100s", cfg.MaxDrain)
	}
}

func TestParseFromMetadata_InvalidEnvVar(t *testing.T) {
	clearEnv(t)
	t.Setenv("KAFKA_TOPIC", "env-topic")
	t.Setenv("KAFKA_GROUP_ID", "env-group")
	t.Setenv("MAX_DRAIN_SECONDS", "soon")

	_, err := ParseFromEnv()
	if err == nil || !strings.Contains(err.Error(), "MAX_DRAIN_SECONDS") {
		t.Fatalf("expected error naming MAX_DRAIN_SECONDS, got %v", err)
	}
}
