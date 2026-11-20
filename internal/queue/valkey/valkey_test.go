// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package valkey_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/siderolabs/image-factory/internal/queue"
	"github.com/siderolabs/image-factory/internal/queue/valkey"
)

const recoveryTimeout = 400 * time.Millisecond

// serverURL returns the server given by IMAGE_FACTORY_TEST_VALKEY_URL, or else an in-process one.
//
//	docker run --rm -p 6379:6379 valkey/valkey:8
//	IMAGE_FACTORY_TEST_VALKEY_URL=valkey://localhost:6379/ go test ./internal/queue/valkey/
func serverURL(t *testing.T) string {
	t.Helper()

	if url := os.Getenv("IMAGE_FACTORY_TEST_VALKEY_URL"); url != "" {
		return url
	}

	server := miniredis.RunT(t)

	// miniredis expires keys only when told so
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		const tick = 10 * time.Millisecond

		for ctx.Err() == nil {
			time.Sleep(tick)
			server.FastForward(tick)
		}
	}()

	return "valkey://" + server.Addr()
}

type env struct {
	url  string
	name string
}

func newEnv(t *testing.T) env {
	t.Helper()

	return env{url: serverURL(t), name: fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())}
}

// broker creates a broker with a connection pool of its own, as a replica has.
func (e env) broker(t *testing.T) *valkey.Broker {
	t.Helper()

	broker, err := valkey.New(zaptest.NewLogger(t), valkey.Options{
		URL:             e.url,
		RequestQueue:    e.name + ".requests",
		ResultExchange:  e.name + ".results",
		RecoveryTimeout: recoveryTimeout,
	})
	require.NoError(t, err)

	t.Cleanup(func() { broker.Close() }) //nolint:errcheck

	return broker
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	_, err := valkey.New(zaptest.NewLogger(t), valkey.Options{URL: "amqp://rabbitmq", RequestQueue: "q", ResultExchange: "x"})
	require.ErrorContains(t, err, "invalid queue URL")

	_, err = valkey.New(zaptest.NewLogger(t), valkey.Options{URL: "valkey://valkey:6379"})
	require.ErrorContains(t, err, "names are required")
}

func TestRequestsAreSpreadAcrossWorkers(t *testing.T) {
	t.Parallel()

	const (
		workers  = 4
		requests = 40
	)

	e := newEnv(t)
	frontend := e.broker(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		handled = map[string]int{}
		busy    [workers]atomic.Int32
	)

	for i := range requests {
		require.NoError(t, frontend.PublishRequest(ctx, queue.BuildRequest{ID: fmt.Sprint(i), EnqueuedAt: time.Now()}))
	}

	stats, err := frontend.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, requests, stats.Ready)

	perWorker := map[int]int{}

	for worker := range workers {
		broker := e.broker(t)

		wg.Go(func() {
			assert.NoError(t, broker.ConsumeRequests(ctx, 1, func(_ context.Context, request queue.BuildRequest) error {
				assert.EqualValues(t, 1, busy[worker].Add(1), "concurrency limit exceeded")
				defer busy[worker].Add(-1)

				time.Sleep(20 * time.Millisecond)

				mu.Lock()
				handled[request.ID]++
				perWorker[worker]++
				mu.Unlock()

				return nil
			}))
		})
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(handled) == requests
	}, 30*time.Second, 20*time.Millisecond)

	cancel()
	wg.Wait()

	for id, count := range handled {
		assert.Equal(t, 1, count, "request %s", id)
	}

	assert.Len(t, perWorker, workers, "every worker should get a share: %v", perWorker)

	stats, err = frontend.Stats(t.Context())
	require.NoError(t, err)
	assert.Zero(t, stats.Ready)
}

func TestStatsCountWorkers(t *testing.T) {
	t.Parallel()

	if os.Getenv("IMAGE_FACTORY_TEST_VALKEY_URL") == "" {
		t.Skip("miniredis does not track idle consumers")
	}

	e := newEnv(t)

	ctx, cancel := context.WithCancel(t.Context())

	var wg sync.WaitGroup

	for range 3 {
		broker := e.broker(t)

		wg.Go(func() {
			assert.NoError(t, broker.ConsumeRequests(ctx, 1, func(context.Context, queue.BuildRequest) error { return nil }))
		})
	}

	frontend := e.broker(t)

	workers := func() int {
		stats, err := frontend.Stats(t.Context())
		assert.NoError(t, err)

		return stats.Consumers
	}

	require.Eventually(t, func() bool { return workers() == 3 }, 10*time.Second, 50*time.Millisecond)

	cancel()
	wg.Wait()

	require.Eventually(t, func() bool { return workers() == 0 }, 10*time.Second, 50*time.Millisecond)
}

func TestFailedRequestIsRedelivered(t *testing.T) {
	t.Parallel()

	broker := newEnv(t).broker(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	require.NoError(t, broker.PublishRequest(ctx, queue.BuildRequest{ID: "flaky"}))

	var attempts atomic.Int32

	require.NoError(t, broker.ConsumeRequests(ctx, 1, func(context.Context, queue.BuildRequest) error {
		if attempts.Add(1) == 1 {
			return errors.New("try again")
		}

		cancel()

		return nil
	}))

	assert.EqualValues(t, 2, attempts.Load())
}

func TestRequestOfStoppedWorkerIsHandedOver(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	stopped := e.broker(t)

	require.NoError(t, stopped.PublishRequest(t.Context(), queue.BuildRequest{ID: "handover"}))

	ctx, cancel := context.WithCancel(t.Context())

	// the worker shuts down in the middle of the build
	require.NoError(t, stopped.ConsumeRequests(ctx, 1, func(ctx context.Context, _ queue.BuildRequest) error {
		cancel()

		return ctx.Err()
	}))

	ctx, cancel = context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	start := time.Now()

	require.NoError(t, e.broker(t).ConsumeRequests(ctx, 1, func(context.Context, queue.BuildRequest) error {
		cancel()

		return nil
	}))

	assert.Less(t, time.Since(start), recoveryTimeout, "the request should be available at once")
}

func TestRequestOfLostWorkerIsStolen(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	lost := e.broker(t)

	require.NoError(t, lost.PublishRequest(t.Context(), queue.BuildRequest{ID: "orphan"}))

	lostCtx, stopLost := context.WithCancel(t.Context())
	started := make(chan struct{})
	lostDone := make(chan struct{})

	go func() {
		defer close(lostDone)

		lost.ConsumeRequests(lostCtx, 1, func(ctx context.Context, _ queue.BuildRequest) error { //nolint:errcheck
			unlock, acquired, err := lost.TryLock(ctx, "orphan")
			assert.NoError(t, err)
			assert.True(t, acquired)

			defer unlock()

			close(started)
			<-ctx.Done()

			return ctx.Err()
		})
	}()

	<-started

	// the worker dies in the middle of the build: no heartbeats, no handover
	require.NoError(t, lost.Close())

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	thief := e.broker(t)

	var got queue.BuildRequest

	require.NoError(t, thief.ConsumeRequests(ctx, 1, func(ctx context.Context, request queue.BuildRequest) error {
		// the lock of the lost worker has expired by now
		unlock, acquired, err := thief.TryLock(ctx, request.ID)
		assert.NoError(t, err)

		if assert.True(t, acquired) {
			unlock()
		}

		got = request

		cancel()

		return nil
	}))

	assert.Equal(t, "orphan", got.ID)

	stopLost()
	<-lostDone
}

func TestLongBuildIsNotStolen(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	frontend := e.broker(t)

	require.NoError(t, frontend.PublishRequest(t.Context(), queue.BuildRequest{ID: "slow"}))

	ctx, cancel := context.WithCancel(t.Context())

	var (
		wg       sync.WaitGroup
		attempts atomic.Int32
	)

	for range 2 {
		broker := e.broker(t)

		wg.Go(func() {
			assert.NoError(t, broker.ConsumeRequests(ctx, 1, func(ctx context.Context, request queue.BuildRequest) error {
				attempts.Add(1)

				unlock, acquired, err := broker.TryLock(ctx, request.ID)
				assert.NoError(t, err)
				assert.True(t, acquired)

				time.Sleep(4 * recoveryTimeout)

				// the lock is still held
				_, acquired, err = frontend.TryLock(ctx, request.ID)
				assert.NoError(t, err)
				assert.False(t, acquired)

				unlock()

				return nil
			}))
		})
	}

	require.Eventually(t, func() bool { return attempts.Load() > 0 }, 10*time.Second, 10*time.Millisecond)
	time.Sleep(6 * recoveryTimeout)

	cancel()
	wg.Wait()

	assert.EqualValues(t, 1, attempts.Load())
}

func TestLock(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	first := e.broker(t)
	second := e.broker(t)

	unlock, acquired, err := first.TryLock(t.Context(), "key")
	require.NoError(t, err)
	require.True(t, acquired)

	_, acquired, err = second.TryLock(t.Context(), "key")
	require.NoError(t, err)
	require.False(t, acquired)

	_, acquired, err = first.TryLock(t.Context(), "key")
	require.NoError(t, err)
	require.False(t, acquired)

	// other keys are not affected
	unlockOther, acquired, err := second.TryLock(t.Context(), "other")
	require.NoError(t, err)
	require.True(t, acquired)

	unlockOther()
	unlock()

	unlock, acquired, err = second.TryLock(t.Context(), "key")
	require.NoError(t, err)
	require.True(t, acquired)

	unlock()
}

func TestResultsAreBroadcast(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	worker := e.broker(t)

	ctx, cancel := context.WithCancel(t.Context())

	const frontends = 3

	var wg sync.WaitGroup

	received := make(chan queue.BuildResult, frontends)

	for range frontends {
		frontend := e.broker(t)

		wg.Go(func() {
			assert.NoError(t, frontend.SubscribeResults(ctx, func(result queue.BuildResult) { received <- result }))
		})
	}

	// there is no way to observe the subscriptions, so publish until everybody got one
	require.Eventually(t, func() bool {
		for len(received) > 0 {
			<-received
		}

		if err := worker.PublishResult(ctx, queue.BuildResult{ProfileHash: "hash", Size: 42, Duration: time.Minute}); err != nil {
			return false
		}

		time.Sleep(200 * time.Millisecond)

		return len(received) == frontends
	}, 20*time.Second, 10*time.Millisecond)

	assert.Equal(t, queue.BuildResult{ProfileHash: "hash", Size: 42, Duration: time.Minute}, <-received)

	cancel()
	wg.Wait()
}
