package logic

import (
	"encoding/json"
	"fmt"
	"hyperliquid-funding/internal/models"
	"strconv"
	"time"
)

// ParseMetaAndAssetCtxs parses the raw response from "metaAndAssetCtxs" type request.
func ParseMetaAndAssetCtxs(raw []byte) ([]models.UniverseItem, []models.AssetCtxItem, error) {
	// The response is an array: [UniverseStruct, AssetCtxsArray]
	// But actually based on some versions it might be separate.
	// Assuming standard: [ { "universe": [...] }, [ { "funding": "..." }, ... ] ]

	var rawArr []json.RawMessage
	if err := json.Unmarshal(raw, &rawArr); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal top-level array: %w", err)
	}

	if len(rawArr) < 2 {
		return nil, nil, fmt.Errorf("unexpected response length: %d", len(rawArr))
	}

	// Parse first element: Universe Wrapper
	var universeWrapper struct {
		Universe []models.UniverseItem `json:"universe"`
	}
	if err := json.Unmarshal(rawArr[0], &universeWrapper); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal universe: %w", err)
	}

	// Parse second element: Asset Contexts
	var assetCtxs []models.AssetCtxItem
	if err := json.Unmarshal(rawArr[1], &assetCtxs); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal asset contexts: %w", err)
	}

	return universeWrapper.Universe, assetCtxs, nil
}

// ProcessFundingRates transforms API data into FundingRate models for allowed symbols.
func ProcessFundingRates(
	universe []models.UniverseItem,
	assetCtxs []models.AssetCtxItem,
	allowedSymbols map[string]bool,
) ([]models.FundingRateStreamData, error) {

	if len(universe) != len(assetCtxs) {
		return nil, fmt.Errorf("universe length (%d) does not match assetCtxs length (%d)", len(universe), len(assetCtxs))
	}

	var results []models.FundingRateStreamData
	now := time.Now().Unix()

	for i, u := range universe {
		// Actually user said: "BTCUSDT" in KV.
		// Hyperliquid universe usually has "BTC", "ETH".
		// We should match strictly or append "USDT".
		// KV has "BTCUSDT".
		// Let's try appending USDT if not present, or checking both.

		// Logic: if "BTC" in universe, check if "BTCUSDT" is in allowedSymbols.
		candidateSymbol := u.Name + "USDT"

		if disallowed := !allowedSymbols[candidateSymbol]; disallowed {
			continue
		}

		fStr := assetCtxs[i].Funding
		rate, err := strconv.ParseFloat(fStr, 64)
		if err != nil {
			continue // skip invalid numbers
		}

		// Hyperliquid rates are often hourly or 8-hourly.
		// User output example: interval_sec: 28800 (8 hours).
		// We hardcode 28800 for now as standard funding interval.

		// Funding Time: usually next funding time.
		// We can calculate or hardcode based on current time buckets (every 8h).
		// current_ts / 28800 * 28800 + 28800

		fundingInterval := int64(28800)
		fundingTime := (now/fundingInterval)*fundingInterval + fundingInterval

		payload := models.FundingRateStreamData{
			Exchange: "hyperliquid",
			Symbol:   candidateSymbol, // "BTCUSDT"
			Data: models.FundingRate{
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

// ProcessPredictedFunding transforms API data into PredictedFunding models.
// rawResponse is expected to be the JSON from "predictedFundings".
// Format: [[symbol, [[ts, rate], ...]], ...]
func ProcessPredictedFunding(raw []byte, allowedSymbols map[string]bool) ([]models.PredictedFundingStreamData, error) {
	var rawData [][]interface{}
	if err := json.Unmarshal(raw, &rawData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal predicted fundings: %w", err)
	}

	var results []models.PredictedFundingStreamData
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

		// predictionsRaw is a list of [ExchangeName, {Details}]
		// We want to find "HlPerp" for Hyperliquid
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

			// Extract fields
			// "fundingRate": "..."
			// "nextFundingTime": 17... (float64 in json unmarshal interface{})

			var rate float64
			var err error

			if rStr, ok := details["fundingRate"].(string); ok {
				rate, err = strconv.ParseFloat(rStr, 64)
				if err != nil {
					continue
				}
			}

			var fundingTime int64
			if nft, ok := details["nextFundingTime"].(float64); ok {
				fundingTime = int64(nft) // timestamps are often ms in these APIs, but checking example: 1730028800 is seconds (10 digits).
				// Real API example: 1769068800000 (13 digits -> ms).
				// We need to divide by 1000 if it is ms.
				// Let's assume ms based on length.
				if fundingTime > 1000000000000 {
					fundingTime = fundingTime / 1000
				}
			}

			results = append(results, models.PredictedFundingStreamData{
				Exchange: "hyperliquid",
				Symbol:   candidateSymbol,
				Data: models.PredictedFunding{
					Rate:        rate,
					FundingTime: fundingTime,
					Ts:          now,
				},
			})

			// Found HlPerp, break inner loop
			break
		}
	}

	return results, nil
}
