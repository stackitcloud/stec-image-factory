// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package rabbitmq implements the build queue on top of RabbitMQ.
//
// Build requests go to a durable work queue shared by all workers. Build results go to
// a fanout exchange, each frontend receives them on a private queue. The worker lock is
// an exclusive queue: RabbitMQ lets only one connection declare it, and removes it once
// that connection is gone.
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/siderolabs/image-factory/internal/queue"
)

const (
	reconnectDelay = 2 * time.Second
	redeliverDelay = 2 * time.Second
	heartbeat      = 10 * time.Second
)

// Options configures the RabbitMQ broker.
type Options struct {
	// URL is the AMQP URL of the RabbitMQ server, amqp://user:password@host:port/vhost.
	URL string
	// RequestQueue is the name of the build request queue.
	RequestQueue string
	// ResultExchange is the name of the build result exchange.
	ResultExchange string
}

// Broker is a queue.Broker backed by RabbitMQ.
//
// The connection is established on first use, and re-established if it is lost.
type Broker struct {
	logger *zap.Logger
	conn   *amqp.Connection
	pubCh  *amqp.Channel
	locks  map[string]struct{}

	options Options

	connMu  sync.Mutex
	pubMu   sync.Mutex
	locksMu sync.Mutex
	closed  bool
}

// New creates a RabbitMQ broker.
func New(logger *zap.Logger, options Options) (*Broker, error) {
	if _, err := amqp.ParseURI(options.URL); err != nil {
		return nil, fmt.Errorf("invalid queue URL: %w", err)
	}

	if options.RequestQueue == "" || options.ResultExchange == "" {
		return nil, errors.New("request queue and result exchange names are required")
	}

	return &Broker{
		logger:  logger.With(zap.String("component", "rabbitmq")),
		options: options,
		locks:   map[string]struct{}{},
	}, nil
}

func (b *Broker) connection() (*amqp.Connection, error) {
	b.connMu.Lock()
	defer b.connMu.Unlock()

	if b.closed {
		return nil, amqp.ErrClosed
	}

	if b.conn != nil && !b.conn.IsClosed() {
		return b.conn, nil
	}

	hostname, _ := os.Hostname() //nolint:errcheck

	conn, err := amqp.DialConfig(b.options.URL, amqp.Config{
		Heartbeat:  heartbeat,
		Properties: amqp.Table{"connection_name": "image-factory@" + hostname},
	})
	if err != nil {
		return nil, fmt.Errorf("error connecting to RabbitMQ: %w", err)
	}

	b.logger.Info("connected to RabbitMQ")

	b.conn = conn

	return conn, nil
}

// channel opens a channel and makes sure the request queue and the result exchange exist.
func (b *Broker) channel() (*amqp.Channel, error) {
	conn, err := b.connection()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("error opening channel: %w", err)
	}

	if _, err = b.declareRequestQueue(ch); err != nil {
		ch.Close() //nolint:errcheck

		return nil, err
	}

	if err = ch.ExchangeDeclare(b.options.ResultExchange, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		ch.Close() //nolint:errcheck

		return nil, fmt.Errorf("error declaring result exchange: %w", err)
	}

	return ch, nil
}

func (b *Broker) declareRequestQueue(ch *amqp.Channel) (amqp.Queue, error) {
	q, err := ch.QueueDeclare(b.options.RequestQueue, true, false, false, false, nil)
	if err != nil {
		return q, fmt.Errorf("error declaring request queue: %w", err)
	}

	return q, nil
}

func (b *Broker) publish(ctx context.Context, exchange, routingKey string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	b.pubMu.Lock()
	defer b.pubMu.Unlock()

	// retry once on a fresh channel, the connection might have been lost since the last publish
	for attempt := range 2 {
		if err = b.publishOnce(ctx, exchange, routingKey, body); err == nil {
			return nil
		}

		if b.pubCh != nil {
			b.pubCh.Close() //nolint:errcheck
			b.pubCh = nil
		}

		if ctx.Err() != nil {
			break
		}

		b.logger.Warn("publish failed", zap.Int("attempt", attempt+1), zap.Error(err))
	}

	return err
}

func (b *Broker) publishOnce(ctx context.Context, exchange, routingKey string, body []byte) error {
	if b.pubCh == nil || b.pubCh.IsClosed() {
		ch, err := b.channel()
		if err != nil {
			return err
		}

		if err = ch.Confirm(false); err != nil {
			ch.Close() //nolint:errcheck

			return fmt.Errorf("error enabling publisher confirms: %w", err)
		}

		b.pubCh = ch
	}

	confirmation, err := b.pubCh.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("error publishing: %w", err)
	}

	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("error waiting for publish confirmation: %w", err)
	}

	if !acked {
		return errors.New("publish was rejected by RabbitMQ")
	}

	return nil
}

// PublishRequest implements queue.Broker.
func (b *Broker) PublishRequest(ctx context.Context, request queue.BuildRequest) error {
	// the default exchange routes to the queue named by the routing key
	return b.publish(ctx, "", b.options.RequestQueue, request)
}

// PublishResult implements queue.Broker.
func (b *Broker) PublishResult(ctx context.Context, result queue.BuildResult) error {
	return b.publish(ctx, b.options.ResultExchange, "", result)
}

// retry runs a consumer session until the context is canceled, starting a new one whenever it breaks.
func (b *Broker) retry(ctx context.Context, what string, session func(ctx context.Context) error) error {
	for {
		err := session(ctx)

		if ctx.Err() != nil {
			return nil //nolint:nilerr // regular shutdown
		}

		if errors.Is(err, amqp.ErrClosed) && b.isClosed() {
			return err
		}

		b.logger.Warn("consumer interrupted, retrying", zap.String("consumer", what), zap.Error(err))

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(reconnectDelay):
		}
	}
}

func (b *Broker) isClosed() bool {
	b.connMu.Lock()
	defer b.connMu.Unlock()

	return b.closed
}

// ConsumeRequests implements queue.Broker.
func (b *Broker) ConsumeRequests(ctx context.Context, concurrency int, handler func(ctx context.Context, request queue.BuildRequest) error) error {
	return b.retry(ctx, "requests", func(ctx context.Context) error {
		ch, err := b.channel()
		if err != nil {
			return err
		}

		defer ch.Close() //nolint:errcheck

		// the prefetch limit is the concurrency limit: RabbitMQ holds back further
		// requests (for the other workers) until one of ours is acknowledged
		if err = ch.Qos(concurrency, 0, false); err != nil {
			return fmt.Errorf("error setting prefetch: %w", err)
		}

		deliveries, err := ch.Consume(b.options.RequestQueue, "", false, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("error consuming requests: %w", err)
		}

		// handlers are canceled when the session ends: their requests are redelivered anyway
		ctx, cancel := context.WithCancel(ctx)

		var wg sync.WaitGroup

		defer wg.Wait()
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return nil
			case delivery, ok := <-deliveries:
				if !ok {
					return errors.New("request channel closed")
				}

				wg.Go(func() { b.handleRequest(ctx, delivery, handler) })
			}
		}
	})
}

func (b *Broker) handleRequest(ctx context.Context, delivery amqp.Delivery, handler func(ctx context.Context, request queue.BuildRequest) error) {
	var request queue.BuildRequest

	if err := json.Unmarshal(delivery.Body, &request); err != nil {
		b.logger.Error("dropping malformed build request", zap.Error(err))

		delivery.Reject(false) //nolint:errcheck

		return
	}

	err := handler(ctx, request)
	if err == nil {
		if err = delivery.Ack(false); err != nil {
			b.logger.Warn("failed to acknowledge build request", zap.String("queue_request_id", request.ID), zap.Error(err))
		}

		return
	}

	if ctx.Err() == nil {
		b.logger.Warn("build request will be redelivered", zap.String("queue_request_id", request.ID), zap.Error(err))

		// don't spin if the failure is persistent
		select {
		case <-ctx.Done():
		case <-time.After(redeliverDelay):
		}
	}

	delivery.Nack(false, true) //nolint:errcheck
}

// SubscribeResults implements queue.Broker.
func (b *Broker) SubscribeResults(ctx context.Context, handler func(result queue.BuildResult)) error {
	return b.retry(ctx, "results", func(ctx context.Context) error {
		ch, err := b.channel()
		if err != nil {
			return err
		}

		defer ch.Close() //nolint:errcheck

		q, err := ch.QueueDeclare("", false, true, true, false, nil)
		if err != nil {
			return fmt.Errorf("error declaring result queue: %w", err)
		}

		if err = ch.QueueBind(q.Name, "", b.options.ResultExchange, false, nil); err != nil {
			return fmt.Errorf("error binding result queue: %w", err)
		}

		deliveries, err := ch.Consume(q.Name, "", true, true, false, false, nil)
		if err != nil {
			return fmt.Errorf("error consuming results: %w", err)
		}

		for {
			select {
			case <-ctx.Done():
				return nil
			case delivery, ok := <-deliveries:
				if !ok {
					return errors.New("result channel closed")
				}

				var result queue.BuildResult

				if err = json.Unmarshal(delivery.Body, &result); err != nil {
					b.logger.Error("dropping malformed build result", zap.Error(err))

					continue
				}

				handler(result)
			}
		}
	})
}

// TryLock implements queue.Broker.
func (b *Broker) TryLock(_ context.Context, key string) (func(), bool, error) {
	// an exclusive queue is exclusive to the connection, so guard against ourselves first
	b.locksMu.Lock()

	if _, held := b.locks[key]; held {
		b.locksMu.Unlock()

		return nil, false, nil
	}

	b.locks[key] = struct{}{}
	b.locksMu.Unlock()

	release := func() {
		b.locksMu.Lock()
		delete(b.locks, key)
		b.locksMu.Unlock()
	}

	conn, err := b.connection()
	if err != nil {
		release()

		return nil, false, err
	}

	ch, err := conn.Channel()
	if err != nil {
		release()

		return nil, false, fmt.Errorf("error opening channel: %w", err)
	}

	name := b.options.RequestQueue + ".lock." + key

	if _, err = ch.QueueDeclare(name, false, false, true, false, nil); err != nil {
		release()
		ch.Close() //nolint:errcheck

		var amqpErr *amqp.Error

		if errors.As(err, &amqpErr) && amqpErr.Code == amqp.ResourceLocked {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("error taking lock: %w", err)
	}

	return func() {
		// if this fails the connection is gone, and the queue with it
		ch.QueueDelete(name, false, false, false) //nolint:errcheck
		ch.Close()                                //nolint:errcheck

		release()
	}, true, nil
}

// Stats implements queue.Broker.
func (b *Broker) Stats(context.Context) (queue.Stats, error) {
	conn, err := b.connection()
	if err != nil {
		return queue.Stats{}, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return queue.Stats{}, fmt.Errorf("error opening channel: %w", err)
	}

	defer ch.Close() //nolint:errcheck

	q, err := b.declareRequestQueue(ch)
	if err != nil {
		return queue.Stats{}, err
	}

	return queue.Stats{Ready: q.Messages, Consumers: q.Consumers}, nil
}

// Close implements queue.Broker.
func (b *Broker) Close() error {
	b.connMu.Lock()
	defer b.connMu.Unlock()

	b.closed = true

	if b.conn == nil || b.conn.IsClosed() {
		return nil
	}

	return b.conn.Close()
}

var _ queue.Broker = (*Broker)(nil)
