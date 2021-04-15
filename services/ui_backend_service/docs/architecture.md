# Architecture documentation for UI Service

## Cache

![Cache architecture diagram](images/cache_architecture.png)
The Cache system is split into three main components
  - Cache client (cache interface)
    - CacheFuture (awaitable cache result)
  - Cache server (worker queueing)
  - Cache worker (execution of cache requests)

### Cache Client

The Cache client is the interface for accessing cached content. It is responsible for setting up and starting the Cache server as a *subprocess*, setting the necessary configuration variables, such as maximum allowed diskspace and number of cache workers.

The cache client instance exposes a number of Cache Actions after the server subprocess has successfully started. These are the interface for accessing cached content. The response of a cache action is an awaitable `CacheFuture`.

#### CacheFuture

The inner workings of the cache future are best explained with an example. Take the following cache action

```python
  result = await cache_client_instance.GetArtifacts("s3://location")
```
the `result` will be a CacheFuture instance, which will check if all cache keys required by the request are present on disk (cache hit).

In case of a cache miss, the CacheFuture will send a cache request through the Cache Client instance, and wait to perform another check for keys on disk. The cache keys will be finally present when the worker has finished processing the action. The future has a very generous timeout so in case the worker/server/client experiences an issue, it will take a while for the future to timeout.

### Cache Server

The cache server is responsible for receiving cache requests from the subprocess stdin, queueing the requests, and starting cache workers to process the queue, up to a limit. Note that the cache workers run their cache action as a *subprocesses* of the cache server.

For starting a cache worker, the server writes the request payload to disk as a `request.json` tempfile, which the worker process then reads at start.

### Cache Worker

The cache worker is a subprocess whose sole responsibility is to read the request payload from `request.json` and execute the corresponding cache action as a subprocess, with the inputs contained in the request, and persisting the produced cache keys to disk.

## Heartbeat monitoring

![Heartbeat monitoring architecture diagram](images/heartbeat_monitoring.png)

## Realtime events over web sockets

![Websocket architecture diagram](images/websocket_communication.png)