package models

type FundingRate struct {
	Rate        float64 `json:"rate"`
	IntervalSec int64   `json:"interval_sec"`
	FundingTime int64   `json:"funding_time"`
	Ts          int64   `json:"ts"`
}

type PredictedFunding struct {
	Rate        float64 `json:"rate"`
	FundingTime int64   `json:"funding_time"`
	Ts          int64   `json:"ts"`
}

type FundingRateStreamData struct {
	Exchange string      `json:"exchange"`
	Symbol   string      `json:"symbol"`
	Data     FundingRate `json:"data"`
}

type PredictedFundingStreamData struct {
	Exchange string           `json:"exchange"`
	Symbol   string           `json:"symbol"`
	Data     PredictedFunding `json:"data"`
}

// REST Request for Hyperliquid Info API
type HyperliquidInfoRequest struct {
	Type string `json:"type"`
}

// --- API Response Structures ---

// UniverseItem represents a single asset in the universe list
type UniverseItem struct {
	Name string `json:"name"`
}

// AssetCtxItem represents the market state for an asset, including funding
type AssetCtxItem struct {
	Funding string `json:"funding"` // API returns string number
}

// MetaResponse corresponds to the structure of "metaAndAssetCtxs"
// We will interpret it as []json.RawMessage in logic because it's a heterogenous array

// PredictedFundingResponseItem represents [symbol, [[rate, ts], ...]]
// But actually API returns objects inside the list? Or arrays?
// Docs say: array of [string, array of [string, string]] usually?
// Let's assume the logic layer will handle the precise unmarshalling of the predicted funding structure
// using interface{} decoding to be safe, or we define:
type PredictedFundingRaw struct {
	Name      string     `json:"name"`
	Predicted [][]string `json:"predicted"` // unknown exact key, often just array in array
}

// Note: "predictedFundings" endpoint usually returns `[[symbol, [[timestamp, fundingRate], ...]], ...]`
// so: `[]interface{}` where item[0] is symbol string, item[1] is array of predictions.
