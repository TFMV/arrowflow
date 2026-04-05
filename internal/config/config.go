package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	StreamBackend         string
	NATSURL               string
	StreamName            string
	KafkaBrokers          string
	Topic                 string
	ConsumerGroup         string
	ConsumerStartAtNew    bool
	ConsumerAckWait       time.Duration
	ConsumerMaxAckPending int
	ConsumerPullWorkers   int
	ConsumerPullBatchSize int
	ConsumerFetchMaxWait  time.Duration
	BatchSize             int
	BufferSizeBytes       int
	StreamReplicas        int
	StreamMaxBytes        int64
	StreamStorage         string
}

func Load() *Config {
	return &Config{
		StreamBackend:         getEnv("STREAM_BACKEND", "nats"),
		NATSURL:               getEnv("NATS_URL", "nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224"),
		StreamName:            getEnv("STREAM_NAME", "ARROWFLOW"),
		KafkaBrokers:          getEnv("KAFKA_BROKERS", "localhost:9092"),
		Topic:                 getEnv("TOPIC", "arrowflow.test"),
		ConsumerGroup:         getEnv("CONSUMER_GROUP", "arrowflow-group"),
		ConsumerStartAtNew:    getEnvBool("CONSUMER_START_AT_NEW", false),
		ConsumerAckWait:       getEnvDuration("CONSUMER_ACK_WAIT", 30*time.Second),
		ConsumerMaxAckPending: getEnvInt("CONSUMER_MAX_ACK_PENDING", 4096),
		ConsumerPullWorkers:   getEnvInt("CONSUMER_PULL_WORKERS", 4),
		ConsumerPullBatchSize: getEnvInt("CONSUMER_PULL_BATCH_SIZE", 256),
		ConsumerFetchMaxWait:  getEnvDuration("CONSUMER_FETCH_MAX_WAIT", 250*time.Millisecond),
		BatchSize:             getEnvInt("BATCH_SIZE", 1000),
		BufferSizeBytes:       getEnvInt("BUFFER_SIZE_BYTES", 1024*1024),
		StreamReplicas:        getEnvInt("STREAM_REPLICAS", 3),
		StreamMaxBytes:        getEnvInt64("STREAM_MAX_BYTES", 1024*1024*1024),
		StreamStorage:         getEnv("STREAM_STORAGE", "file"),
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

func getEnvInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
