package config

import (
	"os"
	"strconv"
)

type Config struct {
	StreamBackend   string
	NATSURL         string
	KafkaBrokers    string
	Topic           string
	ConsumerGroup   string
	BatchSize       int
	BufferSizeBytes int
}

func Load() *Config {
	return &Config{
		StreamBackend:   getEnv("STREAM_BACKEND", "nats"),
		NATSURL:         getEnv("NATS_URL", "nats://localhost:4222"),
		KafkaBrokers:    getEnv("KAFKA_BROKERS", "localhost:9092"),
		Topic:           getEnv("TOPIC", "arrowflow.test"),
		ConsumerGroup:   getEnv("CONSUMER_GROUP", "arrowflow-group"),
		BatchSize:       getEnvInt("BATCH_SIZE", 1000),
		BufferSizeBytes: getEnvInt("BUFFER_SIZE_BYTES", 1024*1024),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}
