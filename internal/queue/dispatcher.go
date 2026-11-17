// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/siderolabs/talos/pkg/imager/profile"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/siderolabs/image-factory/internal/asset"
	"github.com/siderolabs/image-factory/internal/asset/cache"
	"github.com/siderolabs/image-factory/internal/ctxlog"
	factoryprofile "github.com/siderolabs/image-factory/internal/profile"
)

// DispatcherOptions configures the Dispatcher.
type DispatcherOptions struct {
	MetricsNamespace string
	// BuildTimeout limits the time a request waits for the workers.
	BuildTimeout time.Duration
	// PollInterval is the interval of the cache checks done while waiting for a result.
	//
	// The checks cover results which never arrive, e.g. because the broker connection
	// was interrupted.
	PollInterval time.Duration
	// StatsInterval is the refresh interval of the queue depth metrics.
	StatsInterval time.Duration
}

// Dispatcher is the frontend side of the queue: an asset builder which delegates builds to the workers.
type Dispatcher struct {
	local  asset.ImageAssetBuilder
	cache  cache.Cache
	broker Broker
	logger *zap.Logger

	metricCacheHits      prometheus.Counter
	metricInFlight       prometheus.Gauge
	metricLatency        prometheus.Histogram
	metricQueueReady     prometheus.Gauge
	metricQueueConsumers prometheus.Gauge
	metricDispatched     *prometheus.CounterVec

	// waiters are the builds in progress, by profile hash
	waiters map[string]chan BuildResult
	sf      singleflight.Group
	options DispatcherOptions

	waitersMu sync.Mutex
}

// NewDispatcher creates a Dispatcher.
//
// The local builder handles the builds which are not worth a round trip to a worker.
func NewDispatcher(logger *zap.Logger, local asset.ImageAssetBuilder, assetCache cache.Cache, broker Broker, options DispatcherOptions) *Dispatcher {
	if options.BuildTimeout <= 0 {
		options.BuildTimeout = 30 * time.Minute
	}

	if options.PollInterval <= 0 {
		options.PollInterval = 15 * time.Second
	}

	if options.StatsInterval <= 0 {
		options.StatsInterval = 5 * time.Second
	}

	return &Dispatcher{
		local:   local,
		cache:   assetCache,
		broker:  broker,
		logger:  logger.With(zap.String("component", "queue-dispatcher")),
		waiters: map[string]chan BuildResult{},
		options: options,

		metricDispatched: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:      "image_factory_queue_requests_dispatched_total",
				Help:      "Number of asset builds dispatched to the workers, by result.",
				Namespace: options.MetricsNamespace,
			},
			[]string{"result"},
		),
		metricCacheHits: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name:      "image_factory_queue_cache_hits_total",
				Help:      "Number of assets delivered from the cache without involving the workers.",
				Namespace: options.MetricsNamespace,
			},
		),
		metricInFlight: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name:      "image_factory_queue_requests_in_flight",
				Help:      "Number of asset builds this frontend is waiting for.",
				Namespace: options.MetricsNamespace,
			},
		),
		metricLatency: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:      "image_factory_queue_request_latency_seconds",
				Help:      "Time from dispatching an asset build until the asset is available.",
				Buckets:   []float64{1, 5, 10, 30, 60, 120, 300, 600, 1200},
				Namespace: options.MetricsNamespace,
			},
		),
		metricQueueReady: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name:      "image_factory_queue_depth",
				Help:      "Number of asset builds waiting for a worker.",
				Namespace: options.MetricsNamespace,
			},
		),
		metricQueueConsumers: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name:      "image_factory_queue_workers",
				Help:      "Number of workers connected to the queue.",
				Namespace: options.MetricsNamespace,
			},
		),
	}
}

// Run receives the build results until the context is canceled.
func (d *Dispatcher) Run(ctx context.Context) error {
	go d.runStats(ctx)

	return d.broker.SubscribeResults(ctx, d.deliver)
}

func (d *Dispatcher) runStats(ctx context.Context) {
	ticker := time.NewTicker(d.options.StatsInterval)
	defer ticker.Stop()

	for {
		if stats, err := d.broker.Stats(ctx); err == nil {
			d.metricQueueReady.Set(float64(stats.Ready))
			d.metricQueueConsumers.Set(float64(stats.Consumers))
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Build implements asset.ImageAssetBuilder.
//
// Without the image request there is nothing a worker could resolve, so the asset is built locally.
func (d *Dispatcher) Build(ctx context.Context, prof profile.Profile, versionString, path, filename string) (asset.BootAsset, error) {
	return d.local.Build(ctx, prof, versionString, path, filename)
}

// BuildRequest implements asset.ImageRequestBuilder.
//
// It serves the asset from the cache, and on a cache miss lets a worker build it.
func (d *Dispatcher) BuildRequest(ctx context.Context, request asset.ImageRequest, prof profile.Profile, versionString string) (asset.BootAsset, error) {
	// the kernel command line is a string derived from the profile, building it is cheaper than a round trip
	if prof.Output.Kind == profile.OutKindCmdline {
		return d.local.Build(ctx, prof, versionString, request.Path, request.Filename)
	}

	profileHash, err := factoryprofile.Hash(prof)
	if err != nil {
		return nil, err
	}

	cached, err := d.cache.Get(ctx, profileHash)
	if err == nil {
		d.metricCacheHits.Inc()

		return cached, nil
	}

	if !errors.Is(err, cache.ErrCacheNotFound) {
		return nil, fmt.Errorf("error getting asset from cache: %w", err)
	}

	// dispatch each asset only once, on a detached context: the build goes on
	// if the client disconnects, and the next request finds the asset in the cache.
	reqID := ctxlog.RequestID(ctx)

	ch := d.sf.DoChan(profileHash, func() (any, error) { //nolint:contextcheck
		return d.dispatch(reqID, profileHash, request)
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}

		built, ok := res.Val.(asset.BootAsset)
		if !ok {
			return nil, fmt.Errorf("unexpected result type: %T", res.Val)
		}

		return built, nil
	}
}

func (d *Dispatcher) dispatch(reqID, profileHash string, request asset.ImageRequest) (asset.BootAsset, error) {
	ctx, cancel := context.WithTimeout(ctxlog.WithRequestID(context.Background(), reqID), d.options.BuildTimeout)
	defer cancel()

	logger := ctxlog.Logger(ctx, d.logger).With(zap.String("profile_hash", profileHash))
	start := time.Now()

	d.metricInFlight.Inc()
	defer d.metricInFlight.Dec()

	built, err := d.await(ctx, logger, profileHash, request)
	if err != nil {
		d.metricDispatched.WithLabelValues("failure").Inc()

		return nil, err
	}

	d.metricDispatched.WithLabelValues("success").Inc()
	d.metricLatency.Observe(time.Since(start).Seconds())

	return built, nil
}

func (d *Dispatcher) await(ctx context.Context, logger *zap.Logger, profileHash string, request asset.ImageRequest) (asset.BootAsset, error) {
	// subscribe before publishing, so that the result can't be missed
	results := make(chan BuildResult, 1)

	d.waitersMu.Lock()
	d.waiters[profileHash] = results
	d.waitersMu.Unlock()

	defer func() {
		d.waitersMu.Lock()
		delete(d.waiters, profileHash)
		d.waitersMu.Unlock()
	}()

	buildRequest := BuildRequest{
		ID:          newRequestID(),
		ProfileHash: profileHash,
		SchematicID: request.SchematicID,
		Version:     request.Version,
		Path:        request.Path,
		Filename:    request.Filename,
		EnqueuedAt:  time.Now().UTC(),
	}

	if err := d.broker.PublishRequest(ctx, buildRequest); err != nil {
		return nil, fmt.Errorf("error dispatching asset build: %w", err)
	}

	logger.Info("dispatched asset build",
		zap.String("queue_request_id", buildRequest.ID),
		zap.String("schematic", request.SchematicID),
		zap.String("version", request.Version),
		zap.String("path", request.Path),
	)

	ticker := time.NewTicker(d.options.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timed out waiting for the asset build: %w", ctx.Err())
		case result := <-results:
			if result.Error != "" {
				return nil, fmt.Errorf("asset build failed on worker %s: %s", result.WorkerID, result.Error)
			}

			logger.Info("asset build finished",
				zap.String("queue_request_id", result.RequestID),
				zap.String("worker", result.WorkerID),
				zap.Duration("queue_wait", result.QueueWait),
				zap.Duration("duration", result.Duration),
				zap.Int64("size", result.Size),
			)

			built, err := d.cache.Get(ctx, profileHash)
			if err != nil {
				// the worker resolved the request to another profile, or failed to push the asset
				return nil, fmt.Errorf("asset is missing in the cache after the build on worker %s: %w", result.WorkerID, err)
			}

			return built, nil
		case <-ticker.C:
			if built, err := d.cache.Get(ctx, profileHash); err == nil {
				logger.Info("asset showed up in the cache")

				return built, nil
			}
		}
	}
}

func (d *Dispatcher) deliver(result BuildResult) {
	d.waitersMu.Lock()
	defer d.waitersMu.Unlock()

	// results are broadcast to all frontends, most of them are for somebody else
	if ch, ok := d.waiters[result.ProfileHash]; ok {
		select {
		case ch <- result:
		default:
		}
	}
}

func newRequestID() string {
	var id [8]byte

	rand.Read(id[:]) //nolint:errcheck // never fails

	return hex.EncodeToString(id[:])
}

// Describe implements prom.Collector interface.
func (d *Dispatcher) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(d, ch)
}

// Collect implements prom.Collector interface.
func (d *Dispatcher) Collect(ch chan<- prometheus.Metric) {
	d.metricDispatched.Collect(ch)
	d.metricCacheHits.Collect(ch)
	d.metricInFlight.Collect(ch)
	d.metricLatency.Collect(ch)
	d.metricQueueReady.Collect(ch)
	d.metricQueueConsumers.Collect(ch)
}

var (
	_ prometheus.Collector      = (*Dispatcher)(nil)
	_ asset.ImageAssetBuilder   = (*Dispatcher)(nil)
	_ asset.ImageRequestBuilder = (*Dispatcher)(nil)
)
