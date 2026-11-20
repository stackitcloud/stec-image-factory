// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package queue

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/siderolabs/image-factory/internal/asset"
)

// ImageResolver resolves an image request to an asset, building it and pushing it to the cache if needed.
type ImageResolver interface {
	Resolve(ctx context.Context, request asset.ImageRequest) (asset.ResolvedImage, error)
}

// WorkerOptions configures the Worker.
type WorkerOptions struct {
	// ID identifies the worker in the build results.
	ID               string
	MetricsNamespace string
	// Concurrency is the number of build requests handled at the same time.
	Concurrency int
}

// Worker is the worker side of the queue: it builds the assets requested by the frontends.
type Worker struct {
	resolver ImageResolver
	broker   Broker
	logger   *zap.Logger

	metricRequests  *prometheus.CounterVec
	metricInFlight  prometheus.Gauge
	metricQueueWait prometheus.Histogram
	metricDuration  prometheus.Histogram

	options WorkerOptions
}

// NewWorker creates a Worker.
func NewWorker(logger *zap.Logger, resolver ImageResolver, broker Broker, options WorkerOptions) *Worker {
	if options.Concurrency <= 0 {
		options.Concurrency = 1
	}

	return &Worker{
		resolver: resolver,
		broker:   broker,
		logger:   logger.With(zap.String("component", "queue-worker"), zap.String("worker", options.ID)),
		options:  options,

		metricRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:      "image_factory_queue_requests_handled_total",
				Help:      "Number of build requests handled by this worker, by result.",
				Namespace: options.MetricsNamespace,
			},
			[]string{"result"},
		),
		metricInFlight: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name:      "image_factory_queue_worker_requests_in_flight",
				Help:      "Number of build requests this worker is handling.",
				Namespace: options.MetricsNamespace,
			},
		),
		metricQueueWait: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:      "image_factory_queue_wait_seconds",
				Help:      "Time a build request spent in the queue before a worker picked it up.",
				Buckets:   []float64{0.1, 1, 5, 10, 30, 60, 120, 300, 600, 1200},
				Namespace: options.MetricsNamespace,
			},
		),
		metricDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:      "image_factory_queue_request_duration_seconds",
				Help:      "Time this worker spent handling a build request.",
				Buckets:   []float64{1, 5, 10, 30, 60, 120, 300, 600, 1200},
				Namespace: options.MetricsNamespace,
			},
		),
	}
}

// Run handles build requests until the context is canceled.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("waiting for build requests", zap.Int("concurrency", w.options.Concurrency))

	return w.broker.ConsumeRequests(ctx, w.options.Concurrency, w.handle)
}

// Ready reports whether the worker is able to receive build requests.
func (w *Worker) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := w.broker.Stats(ctx)

	return err
}

// handle a build request.
//
// An error is returned only if the request should be redelivered: a failed build is
// a result on its own, and retrying it would fail the same way.
func (w *Worker) handle(ctx context.Context, request BuildRequest) error {
	logger := w.logger.With(
		zap.String("queue_request_id", request.ID),
		zap.String("profile_hash", request.ProfileHash),
	)

	// Two frontends might ask for the same asset at the same time. Whoever holds the lock
	// builds it and broadcasts the result to both of them, so the duplicate can be dropped.
	// If the lock holder dies, its own request is redelivered.
	unlock, acquired, err := w.broker.TryLock(ctx, request.ProfileHash)
	if err != nil {
		w.metricRequests.WithLabelValues("error").Inc()

		return err
	}

	if !acquired {
		logger.Info("asset is being built by another worker, dropping the request")
		w.metricRequests.WithLabelValues("deduplicated").Inc()

		return nil
	}

	defer unlock()

	start := time.Now()
	queueWait := start.Sub(request.EnqueuedAt)

	w.metricQueueWait.Observe(queueWait.Seconds())
	w.metricInFlight.Inc()

	defer w.metricInFlight.Dec()

	logger.Info("handling build request",
		zap.String("schematic", request.SchematicID),
		zap.String("version", request.Version),
		zap.String("path", request.Path),
		zap.Duration("queue_wait", queueWait),
	)

	result := BuildResult{
		ProfileHash: request.ProfileHash,
		RequestID:   request.ID,
		WorkerID:    w.options.ID,
		QueueWait:   queueWait,
	}

	resolved, err := w.resolver.Resolve(ctx, asset.ImageRequest{
		SchematicID: request.SchematicID,
		Version:     request.Version,
		Path:        request.Path,
		Filename:    request.Filename,
	})

	result.Duration = time.Since(start)

	switch {
	case ctx.Err() != nil:
		// the worker is shutting down, let another one take over
		w.metricRequests.WithLabelValues("error").Inc()

		return ctx.Err()
	case err != nil:
		logger.Error("build request failed", zap.Error(err), zap.Duration("duration", result.Duration))
		w.metricRequests.WithLabelValues("failure").Inc()

		result.Error = err.Error()
	default:
		result.Size = resolved.Asset.Size()

		logger.Info("build request finished", zap.Duration("duration", result.Duration), zap.Int64("size", result.Size))
		w.metricRequests.WithLabelValues("success").Inc()
		w.metricDuration.Observe(result.Duration.Seconds())
	}

	return w.broker.PublishResult(ctx, result)
}

// Describe implements prom.Collector interface.
func (w *Worker) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(w, ch)
}

// Collect implements prom.Collector interface.
func (w *Worker) Collect(ch chan<- prometheus.Metric) {
	w.metricRequests.Collect(ch)
	w.metricInFlight.Collect(ch)
	w.metricQueueWait.Collect(ch)
	w.metricDuration.Collect(ch)
}

var _ prometheus.Collector = (*Worker)(nil)
