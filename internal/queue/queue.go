// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package queue distributes asset builds across Image Factory replicas.
//
// A frontend replica answers HTTP requests, but instead of building a missing asset
// itself it publishes a [BuildRequest] and waits for a [BuildResult]. Worker replicas
// consume the requests, build the asset and push it to the shared asset cache, where
// the frontend picks it up.
//
// A request carries the image coordinates (schematic, version, path), not the resolved
// imager profile: the profile references files on the local disk of the replica which
// resolved it, so each worker resolves the request again.
package queue

import (
	"context"
	"time"
)

// BuildRequest asks a worker to build one asset and to push it to the shared asset cache.
type BuildRequest struct {
	// EnqueuedAt is the time the frontend published the request.
	EnqueuedAt time.Time `json:"enqueuedAt"`
	// ID identifies the request in the logs of the frontend and the worker.
	ID string `json:"id"`
	// ProfileHash is the asset cache key, as computed by the frontend.
	//
	// It deduplicates concurrent builds, and correlates the result with the request.
	ProfileHash string `json:"profileHash"`
	// SchematicID is the ID of the schematic.
	SchematicID string `json:"schematicId"`
	// Version is the Talos version.
	Version string `json:"version"`
	// Path is the asset path, e.g. "metal-amd64.iso".
	Path string `json:"path"`
	// Filename is the download filename of the asset.
	Filename string `json:"filename"`
}

// BuildResult reports the outcome of a build to every frontend.
type BuildResult struct {
	// ProfileHash is the asset cache key of the request.
	ProfileHash string `json:"profileHash"`
	// RequestID is the ID of the request.
	RequestID string `json:"requestId"`
	// WorkerID identifies the worker which handled the request.
	WorkerID string `json:"workerId"`
	// Error is the build error, empty if the asset is available in the cache.
	Error string `json:"error,omitempty"`
	// Size is the asset size in bytes.
	Size int64 `json:"size"`
	// QueueWait is the time the request spent in the queue.
	QueueWait time.Duration `json:"queueWait"`
	// Duration is the time the worker spent on the request.
	Duration time.Duration `json:"duration"`
}

// Stats describes the build request queue.
type Stats struct {
	// Ready is the number of requests waiting for a worker.
	Ready int
	// Consumers is the number of connected workers.
	Consumers int
}

// Broker moves build requests and results between frontends and workers.
type Broker interface {
	// PublishRequest enqueues a build request; exactly one worker receives it.
	PublishRequest(ctx context.Context, request BuildRequest) error

	// ConsumeRequests runs the handler for incoming build requests, with at most concurrency
	// requests in flight, until the context is canceled.
	//
	// A request is acknowledged once the handler returns nil; it is redelivered (possibly to
	// another worker) if the handler fails or the worker goes away.
	ConsumeRequests(ctx context.Context, concurrency int, handler func(ctx context.Context, request BuildRequest) error) error

	// PublishResult broadcasts a build result to every subscribed frontend.
	PublishResult(ctx context.Context, result BuildResult) error

	// SubscribeResults runs the handler for every build result until the context is canceled.
	SubscribeResults(ctx context.Context, handler func(result BuildResult)) error

	// TryLock takes a lock shared by all workers.
	//
	// It reports false if the lock is held. The lock is released by calling unlock, or when
	// the worker goes away.
	TryLock(ctx context.Context, key string) (unlock func(), acquired bool, err error)

	// Stats returns the state of the build request queue.
	Stats(ctx context.Context) (Stats, error)

	// Close releases the broker resources.
	Close() error
}
