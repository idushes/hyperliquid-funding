package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hyperliquid-funding/internal/config"
	"hyperliquid-funding/internal/logic"
	"hyperliquid-funding/internal/models"
	"hyperliquid-funding/internal/transport"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	cfg := config.LoadConfig()

	log.Printf("Starting Funding Job. Env: %s, NATS: %s, API: %s", cfg.Environment, cfg.NatsURL, cfg.HyperliquidAPIURL)

	// 1. Init NATS
	log.Println("Connecting to NATS...")
	natsClient, err := transport.ConnectNats(cfg.NatsURL)
	if err != nil {
		log.Fatalf("CRITICAL: Failed to connect to NATS: %v", err)
	}
	defer natsClient.Close()
	log.Println("NATS connected.")

	log.Printf("Initializing KV bucket: %s", cfg.FundingKVBucket)
	if err := natsClient.InitKV(cfg.FundingKVBucket); err != nil {
		log.Fatalf("CRITICAL: Failed to init KV bucket: %v", err)
	}

	log.Println("Job started. Fetching Hyperliquid data...")

	// 2. Run Single Cycle
	err = runCycle(natsClient, cfg)
	if err != nil {
		log.Printf("ERROR: Job cycle failed: %v", err)
		os.Exit(1)
	}

	log.Println("Job completed successfully.")
}

func runCycle(nc *transport.NatsClient, cfg *config.Config) error {
	// 1. Refresh Allowed Symbols (Dynamic updates)
	allowed, err := nc.GetAllowedSymbols()
	if err != nil {
		return fmt.Errorf("error fetching allowed symbols from KV: %w", err)
	}
	if len(allowed) == 0 {
		log.Println("WARNING: No allowed symbols found in KV. Exiting cycle without work.")
		return nil
	}
	log.Printf("Found %d allowed symbols in KV: %v", len(allowed), getKeys(allowed))

	// 2. Fetch Data
	// Fetch Meta (Current Funding)
	log.Println("Fetching 'metaAndAssetCtxs' from Hyperliquid API...")
	metaRaw, err := fetchAPI(cfg.HyperliquidAPIURL, models.HyperliquidInfoRequest{Type: "metaAndAssetCtxs"})
	if err != nil {
		log.Printf("ERROR: Failed to fetch meta: %v", err)
	} else {
		log.Printf("Meta response received. Size: %d bytes", len(metaRaw))
		// Process Meta
		universe, assetCtxs, err := logic.ParseMetaAndAssetCtxs(metaRaw)
		if err != nil {
			log.Printf("ERROR: Failed to parse meta: %v", err)
			log.Printf("DEBUG: Raw meta snippet: %s", string(metaRaw[:min(len(metaRaw), 200)]))
		} else {
			fundingPayloads, err := logic.ProcessFundingRates(universe, assetCtxs, allowed)
			if err != nil {
				log.Printf("ERROR: Failed to process funding rates: %v", err)
			} else {
				log.Printf("Processed %d funding rates. Publishing...", len(fundingPayloads))
				successCount := 0
				for _, p := range fundingPayloads {
					if err := nc.PublishFundingRate(p.Exchange, p.Symbol, p.Data); err != nil {
						log.Printf("ERROR: Failed to publish rate for %s: %v", p.Symbol, err)
					} else {
						successCount++
					}
				}
				log.Printf("Successfully published %d/%d funding rates.", successCount, len(fundingPayloads))
			}
		}
	}

	// Fetch Predictions
	log.Println("Fetching 'predictedFundings' from Hyperliquid API...")
	predRaw, err := fetchAPI(cfg.HyperliquidAPIURL, models.HyperliquidInfoRequest{Type: "predictedFundings"})
	if err != nil {
		log.Printf("ERROR: Failed to fetch predictions: %v", err)
	} else {
		log.Printf("Predicted response received. Size: %d bytes", len(predRaw))
		// Process Predictions
		predPayloads, err := logic.ProcessPredictedFunding(predRaw, allowed)
		if err != nil {
			log.Printf("ERROR: Failed to process predictions: %v", err)
			log.Printf("DEBUG: Raw pred snippet: %s", string(predRaw[:min(len(predRaw), 200)]))
		} else {
			log.Printf("Processed %d predicted fundings. Publishing...", len(predPayloads))
			successCount := 0
			for _, p := range predPayloads {
				if err := nc.PublishPredictedFunding(p.Exchange, p.Symbol, p.Data); err != nil {
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

func getKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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
