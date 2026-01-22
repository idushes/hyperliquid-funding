package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	NatsURL           string
	HyperliquidAPIURL string
	FundingKVBucket   string
	Environment       string
	Port              string
}

func LoadConfig() *Config {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	return &Config{
		NatsURL:           getEnv("NATS_URL", "nats://localhost:4222"),
		HyperliquidAPIURL: getEnv("HYPERLIQUID_API_URL", "https://api.hyperliquid.xyz/info"),
		FundingKVBucket:   getEnv("FUNDING_KV_BUCKET", "funding_symbols"),
		Environment:       getEnv("APP_ENV", "local"),
		Port:              getEnv("PORT", "8080"),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
