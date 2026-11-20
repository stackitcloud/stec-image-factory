// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package valkey implements the build queue on top of Valkey (or Redis).
//
// Build requests go to a stream read by a consumer group: a request stays pending until
// it is acknowledged. A worker keeps its pending requests alive with a heartbeat, a request
// without a heartbeat for the recovery timeout is claimed (stolen) by another worker.
// Build results are broadcast via Pub/Sub. The worker lock is a key with a TTL.
package valkey

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/siderolabs/image-factory/internal/queue"
)

const (
	group        = "workers"
	payloadField = "request"
	// pollInterval bounds the time a worker blocks waiting for new requests, before it looks for abandoned ones.
	pollInterval = 2 * time.Second
	retryDelay   = 2 * time.Second
	opTimeout    = 5 * time.Second
)

var (
	unlockScript  = redis.NewScript(`if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) end return 0`)
	refreshScript = redis.NewScript(`if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) end return 0`)
)

// Options configures the Valkey broker.
type Options struct {
	// URL is the URL of the Valkey server, valkey://user:password@host:port/db (valkeys:// for TLS).
	URL string
	// RequestQueue is the key of the build request stream.
	RequestQueue string
	// ResultExchange is the name of the build result Pub/Sub channel.
	ResultExchange string
	// RecoveryTimeout is the time without a heartbeat after which a request is taken over by another worker.
	RecoveryTimeout time.Duration
}

// Broker is a queue.Broker backed by Valkey.
type Broker struct {
	logger   *zap.Logger
	client   *redis.Client
	consumer string
	options  Options
}

// New creates a Valkey broker.
func New(logger *zap.Logger, options Options) (*Broker, error) {
	url := options.URL

	switch {
	case strings.HasPrefix(url, "valkey://"):
		url = "redis://" + strings.TrimPrefix(url, "valkey://")
	case strings.HasPrefix(url, "valkeys://"):
		url = "rediss://" + strings.TrimPrefix(url, "valkeys://")
	}

	clientOptions, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("invalid queue URL: %w", err)
	}

	if options.RequestQueue == "" || options.ResultExchange == "" {
		return nil, errors.New("request queue and result exchange names are required")
	}

	if options.RecoveryTimeout <= 0 {
		options.RecoveryTimeout = time.Minute
	}

	hostname, _ := os.Hostname() //nolint:errcheck

	return &Broker{
		logger:   logger.With(zap.String("component", "valkey")),
		client:   redis.NewClient(clientOptions),
		consumer: hostname + "-" + randomID(),
		options:  options,
	}, nil
}

func randomID() string {
	var id [8]byte

	rand.Read(id[:]) //nolint:errcheck // never fails

	return hex.EncodeToString(id[:])
}

func (b *Broker) ensureGroup(ctx context.Context) error {
	err := b.client.XGroupCreateMkStream(ctx, b.options.RequestQueue, group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("error creating consumer group: %w", err)
	}

	return nil
}

// keepAlive runs fn periodically, several times per recovery timeout, until stop is called.
func (b *Broker) keepAlive(fn func(ctx context.Context) error) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(b.options.RecoveryTimeout / 4)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := fn(ctx); err != nil && ctx.Err() == nil {
					b.logger.Warn("heartbeat failed", zap.Error(err))
				}
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

// PublishRequest implements queue.Broker.
func (b *Broker) PublishRequest(ctx context.Context, request queue.BuildRequest) error {
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}

	if err = b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: b.options.RequestQueue,
		Values: map[string]any{payloadField: body},
	}).Err(); err != nil {
		return fmt.Errorf("error publishing request: %w", err)
	}

	return nil
}

// ConsumeRequests implements queue.Broker.
func (b *Broker) ConsumeRequests(ctx context.Context, concurrency int, handler func(ctx context.Context, request queue.BuildRequest) error) error {
	var wg sync.WaitGroup

	defer wg.Wait()

	slots := make(chan struct{}, concurrency)
	initialized := false

	for {
		select {
		case <-ctx.Done():
			return nil
		case slots <- struct{}{}:
		}

		var (
			message *redis.XMessage
			err     error
		)

		if !initialized {
			if err = b.ensureGroup(ctx); err == nil {
				b.removeStaleConsumers(ctx)

				initialized = true
			}
		}

		if err == nil {
			message, err = b.next(ctx)
		}

		if message == nil {
			<-slots

			if err != nil && ctx.Err() == nil {
				b.logger.Warn("consumer interrupted, retrying", zap.Error(err))

				// the stream might be gone, e.g. after a restart of a server without persistence
				initialized = false

				select {
				case <-ctx.Done():
				case <-time.After(retryDelay):
				}
			}

			continue
		}

		wg.Go(func() {
			defer func() { <-slots }()

			b.handleRequest(ctx, *message, handler)
		})
	}
}

// next returns a request abandoned by another worker, or else a new one.
func (b *Broker) next(ctx context.Context) (*redis.XMessage, error) {
	for start := "0-0"; ; {
		messages, cursor, err := b.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   b.options.RequestQueue,
			Group:    group,
			Consumer: b.consumer,
			MinIdle:  b.options.RecoveryTimeout,
			Start:    start,
			Count:    1,
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("error claiming abandoned requests: %w", err)
		}

		if len(messages) > 0 {
			b.logger.Info("took over an abandoned build request", zap.String("stream_id", messages[0].ID))

			return &messages[0], nil
		}

		if cursor == "0-0" {
			break
		}

		start = cursor
	}

	streams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: b.consumer,
		Streams:  []string{b.options.RequestQueue, ">"},
		Count:    1,
		Block:    pollInterval,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}

		return nil, fmt.Errorf("error reading requests: %w", err)
	}

	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return nil, nil
	}

	return &streams[0].Messages[0], nil
}

func (b *Broker) handleRequest(ctx context.Context, message redis.XMessage, handler func(ctx context.Context, request queue.BuildRequest) error) {
	var request queue.BuildRequest

	payload, _ := message.Values[payloadField].(string) //nolint:errcheck

	if err := json.Unmarshal([]byte(payload), &request); err != nil {
		b.logger.Error("dropping malformed build request", zap.Error(err))
		b.finish(ctx, message.ID, true)

		return
	}

	stop := b.keepAlive(func(ctx context.Context) error {
		return b.client.XClaimJustID(ctx, &redis.XClaimArgs{
			Stream:   b.options.RequestQueue,
			Group:    group,
			Consumer: b.consumer,
			Messages: []string{message.ID},
		}).Err()
	})

	err := handler(ctx, request)

	stop()

	if err != nil && ctx.Err() == nil {
		b.logger.Warn("build request will be redelivered", zap.String("queue_request_id", request.ID), zap.Error(err))

		// don't spin if the failure is persistent
		select {
		case <-ctx.Done():
		case <-time.After(retryDelay):
		}
	}

	b.finish(ctx, message.ID, err == nil)
}

// finish acknowledges a request, or hands it over to the next worker looking for abandoned requests.
func (b *Broker) finish(ctx context.Context, id string, ack bool) {
	// the worker might be shutting down
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
	defer cancel()

	var err error

	if ack {
		pipe := b.client.TxPipeline()
		pipe.XAck(ctx, b.options.RequestQueue, group, id)
		pipe.XDel(ctx, b.options.RequestQueue, id)

		_, err = pipe.Exec(ctx)
	} else {
		err = b.client.Do(ctx, "XCLAIM", b.options.RequestQueue, group, b.consumer, 0, id,
			"IDLE", b.options.RecoveryTimeout.Milliseconds(), "JUSTID").Err()
	}

	// if this fails, the request is redelivered after the recovery timeout
	if err != nil {
		b.logger.Warn("failed to finish build request", zap.String("stream_id", id), zap.Bool("ack", ack), zap.Error(err))
	}
}

// removeStaleConsumers forgets the workers which are gone and left nothing behind.
func (b *Broker) removeStaleConsumers(ctx context.Context) {
	consumers, err := b.client.XInfoConsumers(ctx, b.options.RequestQueue, group).Result()
	if err != nil {
		return
	}

	for _, consumer := range consumers {
		if consumer.Pending == 0 && consumer.Idle > 10*b.options.RecoveryTimeout {
			b.client.XGroupDelConsumer(ctx, b.options.RequestQueue, group, consumer.Name)
		}
	}
}

// PublishResult implements queue.Broker.
func (b *Broker) PublishResult(ctx context.Context, result queue.BuildResult) error {
	body, err := json.Marshal(result)
	if err != nil {
		return err
	}

	if err = b.client.Publish(ctx, b.options.ResultExchange, body).Err(); err != nil {
		return fmt.Errorf("error publishing result: %w", err)
	}

	return nil
}

// SubscribeResults implements queue.Broker.
func (b *Broker) SubscribeResults(ctx context.Context, handler func(result queue.BuildResult)) error {
	// the subscription is re-established by the client if the connection is lost
	pubsub := b.client.Subscribe(ctx, b.options.ResultExchange)
	defer pubsub.Close() //nolint:errcheck

	messages := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return nil
		case message, ok := <-messages:
			if !ok {
				return errors.New("result subscription closed")
			}

			var result queue.BuildResult

			if err := json.Unmarshal([]byte(message.Payload), &result); err != nil {
				b.logger.Error("dropping malformed build result", zap.Error(err))

				continue
			}

			handler(result)
		}
	}
}

// TryLock implements queue.Broker.
//
// The lock expires before a request of its holder can be taken over, so that
// the worker taking over is not locked out.
func (b *Broker) TryLock(ctx context.Context, key string) (func(), bool, error) {
	name := b.options.RequestQueue + ".lock." + key
	token := randomID()
	ttl := b.options.RecoveryTimeout / 2

	if err := b.client.SetArgs(ctx, name, token, redis.SetArgs{Mode: "NX", TTL: ttl}).Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("error taking lock: %w", err)
	}

	stop := b.keepAlive(func(ctx context.Context) error {
		return refreshScript.Run(ctx, b.client, []string{name}, token, ttl.Milliseconds()).Err()
	})

	return func() {
		stop()

		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		defer cancel()

		// if this fails, the lock expires
		unlockScript.Run(ctx, b.client, []string{name}, token) //nolint:errcheck
	}, true, nil
}

// Stats implements queue.Broker.
func (b *Broker) Stats(ctx context.Context) (queue.Stats, error) {
	if err := b.ensureGroup(ctx); err != nil {
		return queue.Stats{}, err
	}

	pipe := b.client.Pipeline()
	length := pipe.XLen(ctx, b.options.RequestQueue)
	pending := pipe.XPending(ctx, b.options.RequestQueue, group)
	consumers := pipe.XInfoConsumers(ctx, b.options.RequestQueue, group)

	if _, err := pipe.Exec(ctx); err != nil {
		return queue.Stats{}, fmt.Errorf("error getting queue stats: %w", err)
	}

	// acknowledged requests are deleted, what is left is either pending or waiting
	stats := queue.Stats{Ready: max(int(length.Val()-pending.Val().Count), 0)}

	for _, consumer := range consumers.Val() {
		if consumer.Idle < b.options.RecoveryTimeout {
			stats.Consumers++
		}
	}

	return stats, nil
}

// Close implements queue.Broker.
func (b *Broker) Close() error {
	return b.client.Close()
}

var _ queue.Broker = (*Broker)(nil)
