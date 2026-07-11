package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type BaseConfig struct {
	ServiceName       string
	GRPCPort          int
	HTTPPort          int
	LogLevel          string
	TenantServiceAddr string
	NATSUrl           string
	OTELEndpoint      string
	Environment       string
}

func LoadBase(serviceName string) (BaseConfig, error) {
	return BaseConfig{
		ServiceName:       serviceName,
		GRPCPort:          mustInt("OBSP_GRPC_PORT", 50051),
		HTTPPort:          mustInt("OBSP_HTTP_PORT", 8080),
		LogLevel:          getEnv("OBSP_LOG_LEVEL", "info"),
		TenantServiceAddr: getEnv("OBSP_TENANT_ADDR", "tenant:50051"),
		NATSUrl:           getEnv("OBSP_NATS_URL", "nats://nats:4222"),
		OTELEndpoint:      getEnv("OBSP_OTEL_ENDPOINT", "otel-collector:4317"),
		Environment:       getEnv("OBSP_ENV", "development"),
	}, nil
}

func (c BaseConfig) GRPCAddr() string {
	return fmt.Sprintf("0.0.0.0:%d", c.GRPCPort)
}

func (c BaseConfig) HTTPAddr() string {
	return fmt.Sprintf("0.0.0.0:%d", c.HTTPPort)
}

func (c BaseConfig) IsProd() bool {
	return c.Environment == "production"
}

func MustDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	duration, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return duration
}

func getEnv(key string, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}

func mustInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}

	vToInt, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}

	return vToInt
}
