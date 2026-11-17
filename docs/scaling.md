# Horizontal Scaling

By default, an Image Factory replica does everything itself: it answers the HTTP requests, and builds the assets which are missing in the cache.
Builds are CPU and I/O heavy, so a single replica is limited by `build.maxConcurrency`.

Horizontal scaling, also called weak scaling, allows to distribute the tasks not amongst multiple cores on a single computer (node) but amongst multiple nodes.
This does not work with a load balancer, since this only would address several standalone replicas which build the same asset more than once.
Instead, what it needs is a real task queue and a split of image factory into two roles:

* **frontend** replicas serve the HTTP API and the UI.
  For an asset missing in the cache, a frontend publishes a build request and waits for the result.
* **worker** replicas consume the build requests, build the assets, and push them to the cache.

```text
            +----------+  build request   +----------+      +--------+
 client --> | frontend | ---------------> | RabbitMQ | ---> | worker | -+
            |   (N)    | <--------------- |          | <--- |  (M)   |  |
            +----+-----+   build result   +----------+      +--------+  |
                 |                                                      |
                 | serve                                           push |
                 v                                                      v
            +--------------------------------------------------------------+
            |      asset cache (OCI registry, optionally S3 and a CDN)     |
            +--------------------------------------------------------------+
```

The queue carries only the coordinates of an image (schematic ID, Talos version, path).
The asset itself goes through the cache, which every replica uses anyway.

## Configuration

The role of a replica is selected with [`queue.mode`](configuration.md#queuemode), the queue is a RabbitMQ server given by `queue.url`:

```yaml
queue:
  mode: frontend # or: worker
  url: amqp://user:password@rabbitmq:5672/
```

As any other option, both can be set via the environment (`IF_QUEUE_MODE`, `IF_QUEUE_URL`), so that frontends and workers share one configuration file.

Requirements:

* All replicas share the schematic storage (`artifacts.schematic`), the asset cache (`cache.*`) including the cache signing key, and the SecureBoot configuration.
  A worker resolves a build request the same way the frontend did; a different configuration makes it build another asset than the frontend is waiting for.
* A RabbitMQ server (tested with RabbitMQ 4).
  The factory declares what it needs: the queue `queue.requestQueue`, the fanout exchange `queue.resultExchange`, and short-lived exclusive queues named `<requestQueue>.lock.<profile hash>`.

A worker builds `build.maxConcurrency` assets at the same time, and does not take more requests from the queue than that.
It serves `/healthz` and `/readyz` on the HTTP listen address, and nothing else.

### Helm Chart

The [Helm chart](chart.md) deploys the workers as a second Deployment:

```yaml
replicaCount: 2 # frontends
queue:
  enabled: true
  existingSecret: image-factory-queue # with the AMQP URL in the key "url"
worker:
  replicaCount: 10
  maxConcurrency: 2
  resources:
    requests:
      cpu: "2"
      memory: 2Gi
```

For demos and tests the chart can bring its own dependencies, a RabbitMQ server, an OCI registry and a pull-through mirror of `ghcr.io` (`demo.rabbitmq`, `demo.registry`, `demo.registryMirror`).
They are single replicas without TLS and with static credentials, not meant for production.

## Behavior

* **Cache first.** A frontend dispatches a build only if the asset is not in the cache; cached assets are served without the queue, even if RabbitMQ is down.
* **Each asset is built once.** Concurrent requests for the same asset are collapsed on the frontend.
  If two frontends ask for the same asset at the same time, the worker holding the lock for the asset builds it, the other request is dropped, and both frontends get the result.
* **Client disconnects do not cancel builds.** The build goes on, and the next request finds the asset in the cache.
* **Failed builds are reported, not retried.** The error of the worker is returned to the client, as it would be by a standalone replica.
* **Lost workers are covered.** A request is acknowledged only after the build; if a worker goes away, RabbitMQ hands its requests to another worker.
  The same applies to a worker shutting down.
* **Lost results are covered.** While waiting, a frontend looks into the cache every 15 seconds.
  It gives up after `queue.buildTimeout`, which includes the time in the queue.

What is still built by the frontend itself:

* the kernel command line used by the PXE scripts (cheaper to compute than to dispatch),
* installer images pulled via the registry frontend,
* the SBOM assets of the enterprise build.

## Metrics

Frontends:

| Metric | Description |
| --- | --- |
| `image_factory_queue_depth` | build requests waiting for a worker; the signal to scale the workers on |
| `image_factory_queue_workers` | workers connected to the queue |
| `image_factory_queue_requests_in_flight` | builds this frontend is waiting for |
| `image_factory_queue_requests_dispatched_total{result}` | builds dispatched to the workers |
| `image_factory_queue_request_latency_seconds` | time from dispatching a build until the asset is available |
| `image_factory_queue_cache_hits_total` | assets served from the cache without the workers |

Workers (in addition to the `image_factory_assets_*` metrics of the asset builder):

| Metric | Description |
| --- | --- |
| `image_factory_queue_requests_handled_total{result}` | build requests handled: `success`, `failure`, `deduplicated`, `error` (redelivered) |
| `image_factory_queue_worker_requests_in_flight` | build requests being handled |
| `image_factory_queue_wait_seconds` | time a request spent in the queue |
| `image_factory_queue_request_duration_seconds` | time spent on a build request |
