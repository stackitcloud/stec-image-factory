// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package rabbitmq_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/siderolabs/image-factory/internal/queue"
	"github.com/siderolabs/image-factory/internal/queue/rabbitmq"
)

// newBroker connects to the RabbitMQ server given by IMAGE_FACTORY_TEST_RABBITMQ_URL, e.g.
//
//	docker run --rm -p 5672:5672 rabbitmq:4
//	IMAGE_FACTORY_TEST_RABBITMQ_URL=amqp://guest:guest@localhost:5672/ go test ./internal/queue/rabbitmq/
//
// Each broker has a connection of its own, as a replica has.
func newBroker(t *testing.T, name string) *rabbitmq.Broker {
	t.Helper()

	url := os.Getenv("IMAGE_FACTORY_TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("IMAGE_FACTORY_TEST_RABBITMQ_URL is not set")
	}

	broker, err := rabbitmq.New(zaptest.NewLogger(t), rabbitmq.Options{
		URL:            url,
		RequestQueue:   name + ".requests",
		ResultExchange: name + ".results",
	})
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, broker.Close()) })

	return broker
}

func uniqueName(t *testing.T) string {
	t.Helper()

	return fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	_, err := rabbitmq.New(zaptest.NewLogger(t), rabbitmq.Options{URL: "http://rabbitmq", RequestQueue: "q", ResultExchange: "x"})
	require.ErrorContains(t, err, "invalid queue URL")

	_, err = rabbitmq.New(zaptest.NewLogger(t), rabbitmq.Options{URL: "amqp://rabbitmq"})
	require.ErrorContains(t, err, "names are required")
}

func TestRequestsAreSpreadAcrossWorkers(t *testing.T) {
	t.Parallel()

	const (
		workers  = 4
		requests = 40
	)

	name := uniqueName(t)
	frontend := newBroker(t, name)

	ctx, cancel := context.WithCancel(t.Context())

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		handled = map[string]int{}
		busy    [workers]atomic.Int32
	)

	for worker := range workers {
		broker := newBroker(t, name)

		wg.Go(func() {
			assert.NoError(t, broker.ConsumeRequests(ctx, 1, func(_ context.Context, request queue.BuildRequest) error {
				assert.EqualValues(t, 1, busy[worker].Add(1), "concurrency limit exceeded")
				defer busy[worker].Add(-1)

				time.Sleep(20 * time.Millisecond)

				mu.Lock()
				handled[request.ID]++
				mu.Unlock()

				return broker.PublishResult(ctx, queue.BuildResult{RequestID: request.ID, WorkerID: fmt.Sprint(worker)})
			}))
		})
	}

	results := make(chan queue.BuildResult, requests)

	wg.Go(func() {
		assert.NoError(t, frontend.SubscribeResults(ctx, func(result queue.BuildResult) { results <- result }))
	})

	// wait for the subscription, results published before it are not delivered
	require.Eventually(t, func() bool {
		stats, err := frontend.Stats(ctx)

		return err == nil && stats.Consumers == workers
	}, 10*time.Second, 50*time.Millisecond)
	time.Sleep(500 * time.Millisecond)

	for i := range requests {
		require.NoError(t, frontend.PublishRequest(ctx, queue.BuildRequest{ID: fmt.Sprint(i), EnqueuedAt: time.Now()}))
	}

	perWorker := map[string]int{}

	for range requests {
		select {
		case result := <-results:
			perWorker[result.WorkerID]++
		case <-time.After(30 * time.Second):
			require.FailNow(t, "timed out waiting for results")
		}
	}

	cancel()
	wg.Wait()

	assert.Len(t, handled, requests)

	for id, count := range handled {
		assert.Equal(t, 1, count, "request %s", id)
	}

	assert.Len(t, perWorker, workers, "every worker should get a share: %v", perWorker)
}

func TestFailedRequestIsRedelivered(t *testing.T) {
	t.Parallel()

	name := uniqueName(t)
	broker := newBroker(t, name)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	require.NoError(t, broker.PublishRequest(ctx, queue.BuildRequest{ID: "flaky"}))

	var attempts atomic.Int32

	done := make(chan struct{})

	go func() {
		defer close(done)

		assert.NoError(t, broker.ConsumeRequests(ctx, 1, func(context.Context, queue.BuildRequest) error {
			if attempts.Add(1) == 1 {
				return errors.New("try again")
			}

			cancel()

			return nil
		}))
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "timed out")
	}

	assert.EqualValues(t, 2, attempts.Load())
}

func TestRequestOfLostWorkerIsRedelivered(t *testing.T) {
	t.Parallel()

	name := uniqueName(t)
	frontend := newBroker(t, name)
	lost := newBroker(t, name)

	require.NoError(t, frontend.PublishRequest(t.Context(), queue.BuildRequest{ID: "orphan"}))

	started := make(chan struct{})

	go lost.ConsumeRequests(t.Context(), 1, func(ctx context.Context, _ queue.BuildRequest) error { //nolint:errcheck
		close(started)
		<-ctx.Done()

		return ctx.Err()
	})

	<-started

	// the worker dies in the middle of the build
	require.NoError(t, lost.Close())

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var got queue.BuildRequest

	require.NoError(t, newBroker(t, name).ConsumeRequests(ctx, 1, func(_ context.Context, request queue.BuildRequest) error {
		got = request

		cancel()

		return nil
	}))

	assert.Equal(t, "orphan", got.ID)
}

func TestLock(t *testing.T) {
	t.Parallel()

	name := uniqueName(t)
	first := newBroker(t, name)
	second := newBroker(t, name)

	unlock, acquired, err := first.TryLock(t.Context(), "key")
	require.NoError(t, err)
	require.True(t, acquired)

	// held by another connection
	_, acquired, err = second.TryLock(t.Context(), "key")
	require.NoError(t, err)
	require.False(t, acquired)

	// held by the same connection
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

func TestLockIsReleasedWhenHolderGoesAway(t *testing.T) {
	t.Parallel()

	name := uniqueName(t)
	holder := newBroker(t, name)
	other := newBroker(t, name)

	_, acquired, err := holder.TryLock(t.Context(), "key")
	require.NoError(t, err)
	require.True(t, acquired)

	require.NoError(t, holder.Close())

	require.Eventually(t, func() bool {
		unlock, acquired, err := other.TryLock(t.Context(), "key")
		if err != nil || !acquired {
			return false
		}

		unlock()

		return true
	}, 10*time.Second, 100*time.Millisecond)
}

func TestResultsAreBroadcast(t *testing.T) {
	t.Parallel()

	name := uniqueName(t)
	worker := newBroker(t, name)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const frontends = 3

	received := make(chan queue.BuildResult, frontends)

	for range frontends {
		frontend := newBroker(t, name)

		go frontend.SubscribeResults(ctx, func(result queue.BuildResult) { received <- result }) //nolint:errcheck
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
}
