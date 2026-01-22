package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type NatsClient struct {
	nc *nats.Conn
	js jetstream.JetStream
	kv jetstream.KeyValue
}

func ConnectNats(url string) (*NatsClient, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to init jetstream: %w", err)
	}

	return &NatsClient{
		nc: nc,
		js: js,
	}, nil
}

func (c *NatsClient) Close() {
	if c.nc != nil {
		c.nc.Close()
	}
}

// InitKV binds to the existing KeyValue bucket
func (c *NatsClient) InitKV(bucketName string) error {
	ctx := context.Background()
	kv, err := c.js.KeyValue(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to bind to KV bucket %s: %w", bucketName, err)
	}
	c.kv = kv
	return nil
}

// GetAllowedSymbols retrieves all keys from the KV bucket as allowed symbols
func (c *NatsClient) GetAllowedSymbols() (map[string]bool, error) {
	if c.kv == nil {
		return nil, fmt.Errorf("kv not initialized")
	}

	ctx := context.Background()
	keys, err := c.kv.Keys(ctx)
	if err != nil {
		// If no keys found, nats might return error or empty.
		if err == jetstream.ErrBucketNotFound {
			return nil, fmt.Errorf("bucket not found")
		}
		// In some NATS versions, if no keys, Keys() might return error.
		// We treat it as empty list.
		return map[string]bool{}, nil
	}

	allowed := make(map[string]bool)
	for _, key := range keys {
		allowed[key] = true
	}
	return allowed, nil
}

// PublishFundingRate publishes the funding rate payload to "funding.<exchange>.<symbol>.rate"
func (c *NatsClient) PublishFundingRate(exchange, symbol string, data interface{}) error {
	subject := fmt.Sprintf("funding.%s.%s.rate", exchange, symbol)
	return c.publishJSON(subject, data)
}

// PublishPredictedFunding publishes prediction payload to "funding.<exchange>.<symbol>.predicted"
func (c *NatsClient) PublishPredictedFunding(exchange, symbol string, data interface{}) error {
	subject := fmt.Sprintf("funding.%s.%s.predicted", exchange, symbol)
	return c.publishJSON(subject, data)
}

func (c *NatsClient) publishJSON(subject string, data interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	ctx := context.Background()
	// Using PublishAsync for higher throughput, or Publish for reliability.
	// User didn't specify, but for "saving to stream", synchronous Publish is safer to ensure receipt.
	_, err = c.js.Publish(ctx, subject, payload)
	if err != nil {
		log.Printf("Error publishing to %s: %v", subject, err)
		return err
	}
	return nil
}
