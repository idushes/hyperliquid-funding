package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ─── Types ───────────────────────────────────────────────────────────────────

type config struct {
	NatsURL           string
	HyperliquidAPIURL string
	FundingKVBucket   string
	Environment       string
}

type fundingRate struct {
	Rate        float64 `json:"rate"`
	IntervalSec int64   `json:"interval_sec"`
	FundingTime int64   `json:"funding_time"`
	Ts          int64   `json:"ts"`
}

type predictedFunding struct {
	Rate        float64 `json:"rate"`
	FundingTime int64   `json:"funding_time"`
	Ts          int64   `json:"ts"`
}

type fundingRateStreamData struct {
	Exchange string      `json:"exchange"`
	Symbol   string      `json:"symbol"`
	Data     fundingRate `json:"data"`
}

type predictedFundingStreamData struct {
	Exchange string           `json:"exchange"`
	Symbol   string           `json:"symbol"`
	Data     predictedFunding `json:"data"`
}

type hyperliquidInfoRequest struct {
	Type string `json:"type"`
}

type universeItem struct {
	Name string `json:"name"`
}

type assetCtxItem struct {
	Funding string `json:"funding"`
}

type natsClient struct {
	nc *nats.Conn
	js jetstream.JetStream
	kv jetstream.KeyValue
}

// ─── Config ──────────────────────────────────────────────────────────────────

func loadConfig() *config {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	return &config{
		NatsURL:           getEnv("NATS_URL", "nats://localhost:4222"),
		HyperliquidAPIURL: getEnv("HYPERLIQUID_API_URL", "https://api.hyperliquid.xyz/info"),
		FundingKVBucket:   getEnv("FUNDING_KV_BUCKET", "funding_symbols"),
		Environment:       getEnv("APP_ENV", "local"),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// ─── NATS Transport ──────────────────────────────────────────────────────────

func connectNats(url string) (*natsClient, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to init jetstream: %w", err)
	}

	return &natsClient{
		nc: nc,
		js: js,
	}, nil
}

func (c *natsClient) close() {
	if c.nc != nil {
		c.nc.Close()
	}
}

func (c *natsClient) initKV(bucketName string) error {
	ctx := context.Background()
	kv, err := c.js.KeyValue(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to bind to KV bucket %s: %w", bucketName, err)
	}
	c.kv = kv
	return nil
}

func (c *natsClient) getAllowedSymbols() (map[string]bool, error) {
	if c.kv == nil {
		return nil, fmt.Errorf("kv not initialized")
	}

	ctx := context.Background()
	keys, err := c.kv.Keys(ctx)
	if err != nil {
		if err == jetstream.ErrBucketNotFound {
			return nil, fmt.Errorf("bucket not found")
		}
		return map[string]bool{}, nil
	}

	allowed := make(map[string]bool)
	for _, key := range keys {
		allowed[key] = true
	}
	return allowed, nil
}

func (c *natsClient) publishFundingRate(exchange, symbol string, data interface{}) error {
	subject := fmt.Sprintf("funding.%s.%s.rate", exchange, symbol)
	return c.publishJSON(subject, data)
}

func (c *natsClient) publishPredictedFunding(exchange, symbol string, data interface{}) error {
	subject := fmt.Sprintf("funding.%s.%s.predicted", exchange, symbol)
	return c.publishJSON(subject, data)
}

func (c *natsClient) publishJSON(subject string, data interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	ctx := context.Background()
	_, err = c.js.Publish(ctx, subject, payload)
	if err != nil {
		log.Printf("Error publishing to %s: %v", subject, err)
		return err
	}
	return nil
}

// ─── Business Logic ──────────────────────────────────────────────────────────

func parseMetaAndAssetCtxs(raw []byte) ([]universeItem, []assetCtxItem, error) {
	var rawArr []json.RawMessage
	if err := json.Unmarshal(raw, &rawArr); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal top-level array: %w", err)
	}

	if len(rawArr) < 2 {
		return nil, nil, fmt.Errorf("unexpected response length: %d", len(rawArr))
	}

	var universeWrapper struct {
		Universe []universeItem `json:"universe"`
	}
	if err := json.Unmarshal(rawArr[0], &universeWrapper); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal universe: %w", err)
	}

	var assetCtxs []assetCtxItem
	if err := json.Unmarshal(rawArr[1], &assetCtxs); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal asset contexts: %w", err)
	}

	return universeWrapper.Universe, assetCtxs, nil
}

func processFundingRates(
	universe []universeItem,
	assetCtxs []assetCtxItem,
	allowedSymbols map[string]bool,
) ([]fundingRateStreamData, error) {

	if len(universe) != len(assetCtxs) {
		return nil, fmt.Errorf("universe length (%d) does not match assetCtxs length (%d)", len(universe), len(assetCtxs))
	}

	var results []fundingRateStreamData
	now := time.Now().Unix()

	for i, u := range universe {
		candidateSymbol := u.Name + "USDT"

		if !allowedSymbols[candidateSymbol] {
			continue
		}

		fStr := assetCtxs[i].Funding
		rate, err := strconv.ParseFloat(fStr, 64)
		if err != nil {
			continue
		}

		fundingInterval := int64(28800)
		fundingTime := (now/fundingInterval)*fundingInterval + fundingInterval

		payload := fundingRateStreamData{
			Exchange: "hyperliquid",
			Symbol:   candidateSymbol,
			Data: fundingRate{
				Rate:        rate,
				IntervalSec: fundingInterval,
				FundingTime: fundingTime,
				Ts:          now,
			},
		}
		results = append(results, payload)
	}

	return results, nil
}

func processPredictedFunding(raw []byte, allowedSymbols map[string]bool) ([]predictedFundingStreamData, error) {
	var rawData [][]interface{}
	if err := json.Unmarshal(raw, &rawData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal predicted fundings: %w", err)
	}

	var results []predictedFundingStreamData
	now := time.Now().Unix()

	for _, item := range rawData {
		if len(item) < 2 {
			continue
		}
		name, ok := item[0].(string)
		if !ok {
			continue
		}

		candidateSymbol := name + "USDT"
		if !allowedSymbols[candidateSymbol] {
			continue
		}

		predictionsRaw, ok := item[1].([]interface{})
		if !ok || len(predictionsRaw) == 0 {
			continue
		}

		for _, predItemRaw := range predictionsRaw {
			predPair, ok := predItemRaw.([]interface{})
			if !ok || len(predPair) < 2 {
				continue
			}

			exchName, ok := predPair[0].(string)
			if !ok || exchName != "HlPerp" {
				continue
			}

			details, ok := predPair[1].(map[string]interface{})
			if !ok {
				continue
			}

			var rate float64
			var err error

			if rStr, ok := details["fundingRate"].(string); ok {
				rate, err = strconv.ParseFloat(rStr, 64)
				if err != nil {
					continue
				}
			}

			var ft int64
			if nft, ok := details["nextFundingTime"].(float64); ok {
				ft = int64(nft)
				if ft > 1000000000000 {
					ft = ft / 1000
				}
			}

			results = append(results, predictedFundingStreamData{
				Exchange: "hyperliquid",
				Symbol:   candidateSymbol,
				Data: predictedFunding{
					Rate:        rate,
					FundingTime: ft,
					Ts:          now,
				},
			})

			break
		}
	}

	return results, nil
}

// ─── HTTP Helper ─────────────────────────────────────────────────────────────

func fetchAPI(url string, body interface{}) ([]byte, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("api returned status: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func getKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	cfg := loadConfig()

	log.Printf("Starting Funding Job. Env: %s, NATS: %s, API: %s", cfg.Environment, cfg.NatsURL, cfg.HyperliquidAPIURL)

	log.Println("Connecting to NATS...")
	nc, err := connectNats(cfg.NatsURL)
	if err != nil {
		log.Fatalf("CRITICAL: Failed to connect to NATS: %v", err)
	}
	defer nc.close()
	log.Println("NATS connected.")

	log.Printf("Initializing KV bucket: %s", cfg.FundingKVBucket)
	if err := nc.initKV(cfg.FundingKVBucket); err != nil {
		log.Fatalf("CRITICAL: Failed to init KV bucket: %v", err)
	}

	log.Println("Job started. Fetching Hyperliquid data...")

	if err := runCycle(nc, cfg); err != nil {
		log.Printf("ERROR: Job cycle failed: %v", err)
		os.Exit(1)
	}

	log.Println("Job completed successfully.")
}

func runCycle(nc *natsClient, cfg *config) error {
	// 1. Refresh Allowed Symbols
	allowed, err := nc.getAllowedSymbols()
	if err != nil {
		return fmt.Errorf("error fetching allowed symbols from KV: %w", err)
	}
	if len(allowed) == 0 {
		log.Println("WARNING: No allowed symbols found in KV. Exiting cycle without work.")
		return nil
	}
	log.Printf("Found %d allowed symbols in KV: %v", len(allowed), getKeys(allowed))

	// 2. Fetch & Process Meta (Current Funding)
	log.Println("Fetching 'metaAndAssetCtxs' from Hyperliquid API...")
	metaRaw, err := fetchAPI(cfg.HyperliquidAPIURL, hyperliquidInfoRequest{Type: "metaAndAssetCtxs"})
	if err != nil {
		log.Printf("ERROR: Failed to fetch meta: %v", err)
	} else {
		log.Printf("Meta response received. Size: %d bytes", len(metaRaw))
		universe, assetCtxs, err := parseMetaAndAssetCtxs(metaRaw)
		if err != nil {
			log.Printf("ERROR: Failed to parse meta: %v", err)
		} else {
			fundingPayloads, err := processFundingRates(universe, assetCtxs, allowed)
			if err != nil {
				log.Printf("ERROR: Failed to process funding rates: %v", err)
			} else {
				log.Printf("Processed %d funding rates. Publishing...", len(fundingPayloads))
				successCount := 0
				for _, p := range fundingPayloads {
					if err := nc.publishFundingRate(p.Exchange, p.Symbol, p.Data); err != nil {
						log.Printf("ERROR: Failed to publish rate for %s: %v", p.Symbol, err)
					} else {
						successCount++
					}
				}
				log.Printf("Successfully published %d/%d funding rates.", successCount, len(fundingPayloads))
			}
		}
	}

	// 3. Fetch & Process Predictions
	log.Println("Fetching 'predictedFundings' from Hyperliquid API...")
	predRaw, err := fetchAPI(cfg.HyperliquidAPIURL, hyperliquidInfoRequest{Type: "predictedFundings"})
	if err != nil {
		log.Printf("ERROR: Failed to fetch predictions: %v", err)
	} else {
		log.Printf("Predicted response received. Size: %d bytes", len(predRaw))
		predPayloads, err := processPredictedFunding(predRaw, allowed)
		if err != nil {
			log.Printf("ERROR: Failed to process predictions: %v", err)
		} else {
			log.Printf("Processed %d predicted fundings. Publishing...", len(predPayloads))
			successCount := 0
			for _, p := range predPayloads {
				if err := nc.publishPredictedFunding(p.Exchange, p.Symbol, p.Data); err != nil {
					log.Printf("ERROR: Failed to publish prediction for %s: %v", p.Symbol, err)
				} else {
					successCount++
				}
			}
			log.Printf("Successfully published %d/%d predicted fundings.", successCount, len(predPayloads))
		}
	}

	return nil
}
