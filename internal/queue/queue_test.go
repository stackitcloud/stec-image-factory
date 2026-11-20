// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package queue_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	talosprofile "github.com/siderolabs/talos/pkg/imager/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/siderolabs/image-factory/internal/asset"
	"github.com/siderolabs/image-factory/internal/asset/cache"
	factoryprofile "github.com/siderolabs/image-factory/internal/profile"
	"github.com/siderolabs/image-factory/internal/queue"
)

// memoryBroker is an in-process queue.Broker.
type memoryBroker struct {
	requests    chan queue.BuildRequest
	locks       map[string]struct{}
	lockErr     error
	subscribers []func(queue.BuildResult)
	published   []queue.BuildRequest
	mu          sync.Mutex
	dropResults bool
}

func newMemoryBroker() *memoryBroker {
	return &memoryBroker{
		requests: make(chan queue.BuildRequest, 100),
		locks:    map[string]struct{}{},
	}
}

func (b *memoryBroker) PublishRequest(_ context.Context, request queue.BuildRequest) error {
	b.mu.Lock()
	b.published = append(b.published, request)
	b.mu.Unlock()

	b.requests <- request

	return nil
}

func (b *memoryBroker) publishedRequests() []queue.BuildRequest {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]queue.BuildRequest(nil), b.published...)
}

func (b *memoryBroker) ConsumeRequests(ctx context.Context, concurrency int, handler func(context.Context, queue.BuildRequest) error) error {
	var wg sync.WaitGroup

	for range concurrency {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case request := <-b.requests:
					if err := handler(ctx, request); err != nil && ctx.Err() == nil {
						b.requests <- request // redeliver
					}
				}
			}
		})
	}

	wg.Wait()

	return nil
}

func (b *memoryBroker) PublishResult(_ context.Context, result queue.BuildResult) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.dropResults {
		return nil
	}

	for _, subscriber := range b.subscribers {
		subscriber(result)
	}

	return nil
}

func (b *memoryBroker) SubscribeResults(ctx context.Context, handler func(queue.BuildResult)) error {
	b.mu.Lock()
	b.subscribers = append(b.subscribers, handler)
	b.mu.Unlock()

	<-ctx.Done()

	return nil
}

func (b *memoryBroker) subscribed() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.subscribers)
}

func (b *memoryBroker) TryLock(_ context.Context, key string) (func(), bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.lockErr != nil {
		err := b.lockErr
		b.lockErr = nil

		return nil, false, err
	}

	if _, held := b.locks[key]; held {
		return nil, false, nil
	}

	b.locks[key] = struct{}{}

	return func() {
		b.mu.Lock()
		delete(b.locks, key)
		b.mu.Unlock()
	}, true, nil
}

func (b *memoryBroker) Stats(context.Context) (queue.Stats, error) {
	return queue.Stats{Ready: len(b.requests)}, nil
}

func (b *memoryBroker) Close() error { return nil }

// memoryCache is the asset cache shared by the frontend and the workers.
type memoryCache struct {
	assets map[string]memoryAsset
	mu     sync.Mutex
}

func newMemoryCache() *memoryCache {
	return &memoryCache{assets: map[string]memoryAsset{}}
}

func (c *memoryCache) Get(_ context.Context, profileID string) (cache.BootAsset, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cached, ok := c.assets[profileID]; ok {
		return cached, nil
	}

	return nil, cache.ErrCacheNotFound
}

func (c *memoryCache) Put(_ context.Context, profileID string, _ cache.BootAsset, _ string) error {
	c.put(profileID, "asset")

	return nil
}

func (c *memoryCache) put(profileID, content string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.assets[profileID] = memoryAsset(content)
}

type memoryAsset string

func (a memoryAsset) Size() int64 { return int64(len(a)) }

func (a memoryAsset) Reader() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader([]byte(a))), nil
}

// localBuilder stands for the asset builder of the frontend replica.
type localBuilder struct {
	builds atomic.Int32
}

func (b *localBuilder) Build(context.Context, talosprofile.Profile, string, string, string) (asset.BootAsset, error) {
	b.builds.Add(1)

	return memoryAsset("local"), nil
}

// resolver stands for the image service of the worker replica.
type resolver struct {
	cache    *memoryCache
	err      error
	started  chan struct{}
	release  chan struct{}
	hash     string
	requests []asset.ImageRequest
	mu       sync.Mutex
}

func (r *resolver) Resolve(ctx context.Context, request asset.ImageRequest) (asset.ResolvedImage, error) {
	r.mu.Lock()
	r.requests = append(r.requests, request)
	r.mu.Unlock()

	if r.started != nil {
		r.started <- struct{}{}
	}

	if r.release != nil {
		select {
		case <-r.release:
		case <-ctx.Done():
			return asset.ResolvedImage{}, ctx.Err()
		}
	}

	if r.err != nil {
		return asset.ResolvedImage{}, r.err
	}

	r.cache.put(r.hash, "built by worker")

	return asset.ResolvedImage{Asset: memoryAsset("built by worker")}, nil
}

func (r *resolver) resolved() []asset.ImageRequest {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]asset.ImageRequest(nil), r.requests...)
}

var (
	isoProfile = talosprofile.Profile{
		Arch:     "amd64",
		Platform: "metal",
		Output:   talosprofile.Output{Kind: talosprofile.OutKindISO},
	}
	isoRequest = asset.ImageRequest{
		SchematicID: "376567988ad370138ad8b2698212367b8edcb69b5fd68c80be1f2ec7d603b4ba",
		Version:     "v1.9.0",
		Path:        "metal-amd64.iso",
		Filename:    "metal-amd64.iso",
	}
)

type setup struct {
	broker     *memoryBroker
	cache      *memoryCache
	local      *localBuilder
	resolver   *resolver
	dispatcher *queue.Dispatcher
	worker     *queue.Worker
}

func newSetup(t *testing.T, startWorker bool) *setup {
	t.Helper()

	hash, err := factoryprofile.Hash(isoProfile)
	require.NoError(t, err)

	s := &setup{
		broker: newMemoryBroker(),
		cache:  newMemoryCache(),
		local:  &localBuilder{},
	}

	s.resolver = &resolver{cache: s.cache, hash: hash}

	logger := zaptest.NewLogger(t)

	s.dispatcher = queue.NewDispatcher(logger, s.local, s.cache, s.broker, queue.DispatcherOptions{
		BuildTimeout: 10 * time.Second,
		PollInterval: 20 * time.Millisecond,
	})
	s.worker = queue.NewWorker(logger, s.resolver, s.broker, queue.WorkerOptions{ID: "worker-0", Concurrency: 2})

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	wg.Go(func() { assert.NoError(t, s.dispatcher.Run(ctx)) })

	if startWorker {
		wg.Go(func() { assert.NoError(t, s.worker.Run(ctx)) })
	}

	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	require.Eventually(t, func() bool { return s.broker.subscribed() == 1 }, time.Second, time.Millisecond)

	return s
}

func readAsset(t *testing.T, bootAsset asset.BootAsset) string {
	t.Helper()

	r, err := bootAsset.Reader()
	require.NoError(t, err)

	content, err := io.ReadAll(r)
	require.NoError(t, err)

	return string(content)
}

func TestBuildOnWorker(t *testing.T) {
	t.Parallel()

	s := newSetup(t, true)

	built, err := s.dispatcher.BuildRequest(t.Context(), isoRequest, isoProfile, "1.9.0")
	require.NoError(t, err)

	assert.Equal(t, "built by worker", readAsset(t, built))
	assert.Equal(t, []asset.ImageRequest{isoRequest}, s.resolver.resolved())
	assert.Zero(t, s.local.builds.Load(), "the frontend should not build the asset")

	// the asset is in the cache now, the workers are not involved anymore
	_, err = s.dispatcher.BuildRequest(t.Context(), isoRequest, isoProfile, "1.9.0")
	require.NoError(t, err)

	assert.Len(t, s.broker.publishedRequests(), 1)
}

func TestConcurrentRequestsAreDispatchedOnce(t *testing.T) {
	t.Parallel()

	s := newSetup(t, true)
	s.resolver.started = make(chan struct{}, 1)
	s.resolver.release = make(chan struct{})

	var wg sync.WaitGroup

	for range 5 {
		wg.Go(func() {
			built, err := s.dispatcher.BuildRequest(t.Context(), isoRequest, isoProfile, "1.9.0")
			if assert.NoError(t, err) {
				assert.Equal(t, "built by worker", readAsset(t, built))
			}
		})
	}

	<-s.resolver.started
	// give the other requests the time to pile up behind the first one
	time.Sleep(50 * time.Millisecond)
	close(s.resolver.release)

	wg.Wait()

	assert.Len(t, s.broker.publishedRequests(), 1)
	assert.Len(t, s.resolver.resolved(), 1)
}

func TestBuildFailureIsReported(t *testing.T) {
	t.Parallel()

	s := newSetup(t, true)
	s.resolver.err = errors.New("imager exploded")

	_, err := s.dispatcher.BuildRequest(t.Context(), isoRequest, isoProfile, "1.9.0")
	require.ErrorContains(t, err, "asset build failed on worker worker-0: imager exploded")

	// a failed build is not redelivered
	assert.Len(t, s.resolver.resolved(), 1)
}

func TestLostResultIsRecoveredFromCache(t *testing.T) {
	t.Parallel()

	s := newSetup(t, true)
	s.broker.dropResults = true

	built, err := s.dispatcher.BuildRequest(t.Context(), isoRequest, isoProfile, "1.9.0")
	require.NoError(t, err)

	assert.Equal(t, "built by worker", readAsset(t, built))
}

func TestClientDisconnectDoesNotCancelBuild(t *testing.T) {
	t.Parallel()

	s := newSetup(t, true)
	s.resolver.started = make(chan struct{}, 1)
	s.resolver.release = make(chan struct{})

	ctx, cancel := context.WithCancel(t.Context())

	errCh := make(chan error, 1)

	go func() {
		_, err := s.dispatcher.BuildRequest(ctx, isoRequest, isoProfile, "1.9.0")
		errCh <- err
	}()

	<-s.resolver.started
	cancel()

	require.ErrorIs(t, <-errCh, context.Canceled)

	close(s.resolver.release)

	// the retry of the client joins the build in progress, or finds the asset in the cache
	built, err := s.dispatcher.BuildRequest(t.Context(), isoRequest, isoProfile, "1.9.0")
	require.NoError(t, err)

	assert.Equal(t, "built by worker", readAsset(t, built))
	assert.Len(t, s.broker.publishedRequests(), 1)
}

func TestCmdlineIsBuiltLocally(t *testing.T) {
	t.Parallel()

	s := newSetup(t, false)

	prof := isoProfile
	prof.Output.Kind = talosprofile.OutKindCmdline

	built, err := s.dispatcher.BuildRequest(t.Context(), isoRequest, prof, "1.9.0")
	require.NoError(t, err)

	assert.Equal(t, "local", readAsset(t, built))
	assert.Empty(t, s.broker.publishedRequests())
}

func TestBuildWithoutRequestIsLocal(t *testing.T) {
	t.Parallel()

	s := newSetup(t, false)

	built, err := s.dispatcher.Build(t.Context(), isoProfile, "1.9.0", "metal-amd64.iso", "metal-amd64.iso")
	require.NoError(t, err)

	assert.Equal(t, "local", readAsset(t, built))
	assert.Empty(t, s.broker.publishedRequests())
}

func TestWorkerDropsRequestBuiltByAnotherWorker(t *testing.T) {
	t.Parallel()

	s := newSetup(t, false)

	hash, err := factoryprofile.Hash(isoProfile)
	require.NoError(t, err)

	// another worker holds the lock
	unlock, acquired, err := s.broker.TryLock(t.Context(), hash)
	require.NoError(t, err)
	require.True(t, acquired)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)

		assert.NoError(t, s.worker.Run(ctx))
	}()

	require.NoError(t, s.broker.PublishRequest(ctx, queue.BuildRequest{ID: "1", ProfileHash: hash}))
	require.Eventually(t, func() bool { return len(s.broker.requests) == 0 }, time.Second, time.Millisecond)

	unlock()

	// the request was acknowledged without a build
	require.NoError(t, s.broker.PublishRequest(ctx, queue.BuildRequest{ID: "2", ProfileHash: hash}))
	require.Eventually(t, func() bool { return len(s.resolver.resolved()) == 1 }, time.Second, time.Millisecond)

	cancel()
	<-done

	assert.Len(t, s.resolver.resolved(), 1)
}

func TestWorkerRetriesWhenLockFails(t *testing.T) {
	t.Parallel()

	s := newSetup(t, false)
	s.broker.lockErr = errors.New("broker is away")

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)

		assert.NoError(t, s.worker.Run(ctx))
	}()

	built, err := s.dispatcher.BuildRequest(t.Context(), isoRequest, isoProfile, "1.9.0")
	require.NoError(t, err)

	assert.Equal(t, "built by worker", readAsset(t, built))

	cancel()
	<-done
}
