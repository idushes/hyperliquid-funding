# Hyperliquid Funding Rate Microservice

This service polls the Hyperliquid API for funding rates and predicted funding rates, publishing them to NATS JetStream.

## Usage

### 1. Configuration

The service is configured via environment variables. You can use a `.env` file for local development.

| Variable              | Description                               | Default                            |
| --------------------- | ----------------------------------------- | ---------------------------------- |
| `NATS_URL`            | NATS Server URL                           | `nats://localhost:4222`            |
| `HYPERLIQUID_API_URL` | Hyperliquid Info API URL                  | `https://api.hyperliquid.xyz/info` |
| `FUNDING_KV_BUCKET`   | NATS KV Bucket containing allowed symbols | `funding_symbols`                  |

### 2. Running

The service is designed to run as a **Kubernetes Job** or **CronJob** (e.g., every minute).

**Build and Run:**

```bash
# Build
go build -o hyperliquid-funding-job ./cmd/funding-job/main.go

# Run (Executes one cycle and exits)
./hyperliquid-funding-job
```

## Data Formats

The service publishes data to NATS JetStream subjects.

### Funding Rate

**Subject:** `funding.hyperliquid.<symbol>.rate`  
(e.g., `funding.hyperliquid.BTCUSDT.rate`)

```json
{
  "exchange": "hyperliquid",
  "symbol": "BTCUSDT",
  "data": {
    "rate": 0.0001,
    "interval_sec": 28800,
    "funding_time": 1730028800,
    "ts": 1730000000
  }
}
```

### Predicted Funding

**Subject:** `funding.hyperliquid.<symbol>.predicted`  
(e.g., `funding.hyperliquid.BTCUSDT.predicted`)

```json
{
  "exchange": "hyperliquid",
  "symbol": "BTCUSDT",
  "data": {
    "rate": 0.00012,
    "funding_time": 1730028800,
    "ts": 1730001000
  }
}
```
