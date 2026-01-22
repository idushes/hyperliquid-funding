package logic

import (
	"hyperliquid-funding/internal/models"
	"testing"
)

func TestProcessFundingRates(t *testing.T) {
	// Mock inputs
	universe := []models.UniverseItem{
		{Name: "BTC"},
		{Name: "ETH"},
		{Name: "SOL"},
	}
	assetCtxs := []models.AssetCtxItem{
		{Funding: "0.0001"},
		{Funding: "0.0002"},
		{Funding: "0.0003"},
	}
	allowedSymbols := map[string]bool{
		"BTCUSDT": true,
		// ETHUSDT not allowed
		"SOLUSDT": true,
	}

	results, err := ProcessFundingRates(universe, assetCtxs, allowedSymbols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	// Verify BTC Data
	btc := results[0]
	if btc.Symbol != "BTCUSDT" {
		t.Errorf("expected BTCUSDT, got %s", btc.Symbol)
	}
	if btc.Data.Rate != 0.0001 {
		t.Errorf("expected 0.0001, got %f", btc.Data.Rate)
	}
	if btc.Data.IntervalSec != 28800 {
		t.Errorf("expected 28800 interval, got %d", btc.Data.IntervalSec)
	}

	// Verify SOL Data
	sol := results[1]
	if sol.Symbol != "SOLUSDT" {
		t.Errorf("expected SOLUSDT, got %s", sol.Symbol)
	}
	if sol.Data.Rate != 0.0003 {
		t.Errorf("expected 0.0003, got %f", sol.Data.Rate)
	}
}

func TestProcessPredictedFunding(t *testing.T) {
	// Mock JSON response matching Real API structure:
	// [ ["Symbol", [ ["Exchange", {"fundingRate": "...", "nextFundingTime": 123...}], ... ]] ]
	rawJSON := `[
		["BTC", [
			["BinPerp", {"fundingRate": "0.0001", "nextFundingTime": 1700000000000}],
			["HlPerp", {"fundingRate": "0.00015", "nextFundingTime": 1700000000000}]
		]],
		["ETH", [
			["HlPerp", {"fundingRate": "0.00025", "nextFundingTime": 1700000000000}]
		]],
		["SOL", [
			["HlPerp", {"fundingRate": "0.00035", "nextFundingTime": 1700000000000}]
		]]
	]`

	allowedSymbols := map[string]bool{
		"BTCUSDT": true,
		"SOLUSDT": true,
	}

	results, err := ProcessPredictedFunding([]byte(rawJSON), allowedSymbols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	// Verify BTC
	btc := results[0]
	if btc.Symbol != "BTCUSDT" {
		t.Errorf("expected BTCUSDT, got %s", btc.Symbol)
	}
	// We expect 0.00015 (HlPerp), not 0.0001 (BinPerp)
	if btc.Data.Rate != 0.00015 {
		t.Errorf("expected 0.00015, got %f", btc.Data.Rate)
	}
	if btc.Data.FundingTime != 1700000000 {
		t.Errorf("expected 1700000000, got %d", btc.Data.FundingTime)
	}
}

func TestParseMetaAndAssetCtxs(t *testing.T) {
	rawJSON := `[
		{"universe": [{"name": "BTC"}, {"name": "ETH"}]},
		[{"funding": "0.01"}, {"funding": "0.02"}]
	]`

	u, ctx, err := ParseMetaAndAssetCtxs([]byte(rawJSON))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(u) != 2 {
		t.Errorf("expected 2 universe items, got %d", len(u))
	}
	if len(ctx) != 2 {
		t.Errorf("expected 2 asset contexts, got %d", len(ctx))
	}
	if u[0].Name != "BTC" {
		t.Errorf("expected BTC, got %s", u[0].Name)
	}
	if ctx[1].Funding != "0.02" {
		t.Errorf("expected 0.02, got %s", ctx[1].Funding)
	}
}
