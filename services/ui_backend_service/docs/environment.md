# Optional environment variable configuration for the UI Service

The following are optional environment variables that can be used to fine-tune the behavior of specific components of the UI service if necessary.

- [Web socket message retention](#web-socket-message-retention)
- [Cache and data preload limits](#cache-and-data-preload-limits)
- [Feature flags](#feature-flags)
- [Heartbeat intervals](#heartbeat-intervals)
- [Baseurl configuration](#baseurl-configuration)
- [Custom navigation links for UI](#custom-navigation-links-for-ui)
- [System notifications for UI](#system-notifications-for-ui)
- [Log content restriction options](#log-content-restriction-options)
- [Card content restriction](#card-content-restriction)

## Web socket message retention

Configure amount of seconds realtime events are kept in queue (delivered to UI in case of reconnects):

- `WS_QUEUE_TTL_SECONDS` [defaults to 300 (5 minutes)]

## Cache and data limits

Configure amount of runs to prefetch during server startup (artifact cache):

- `PREFETCH_RUNS_SINCE` [in seconds, defaults to 2 days ago (86400 * 2 seconds)]
- `PREFETCH_RUNS_LIMIT` [defaults to 50]

Configure the amount of concurrent cache actions. This works similar to a database connection pool.

- `CACHE_ARTIFACT_MAX_ACTIONS` [max number of artifact cache actions. Defaults to 16]
- `CACHE_DAG_MAX_ACTIONS` [max number of DAG cache actions. Defaults to 16]

Configure the maximum usable space by the cache:

- `CACHE_ARTIFACT_STORAGE_LIMIT` [in bytes, defaults to 600000]
- `CACHE_DAG_STORAGE_LIMIT` [in bytes, defaults to 100000]

Configure the maximum size of files that should be processed by cache actions:

- `MAX_PROCESSABLE_S3_ARTIFACT_SIZE_KB` [in kilobytes, defaults to 4]

## Feature flags

All environment variables prefixed with `FEATURE_` will be publicly available under `/features` route. These are primarily used to communicate backend feature availability to the UI frontend.

> ```sh
> $ curl http://service:8083/features
> {
>   "FEATURE_CACHE": true,
>   "FEATURE_DAG": false
> }
> ```

Example values:

> ```
> FEATURE_EXAMPLE=1           -> True
> FEATURE_EXAMPLE=true        -> True
> FEATURE_EXAMPLE=t           -> True
> FEATURE_EXAMPLE=anything    -> True
> FEATURE_EXAMPLE=0           -> False
> FEATURE_EXAMPLE=false       -> False
> FEATURE_EXAMPLE=f           -> False
> ```

These feature flags are passed to frontend and can be used to dynamically control features.

## Heartbeat intervals

The threshold parameters for heartbeat checks can also be configured when necessary with the following environment variables.

`HEARTBEAT_THRESHOLD` [controls at what point a heartbeat is considered expired. Default is `WAIT_TIME * 6`]

`OLD_RUN_FAILURE_CUTOFF_TIME` [ for runs that do not have a heartbeat, controls at what point a running status run should be considered failed. Default is 1 day (in milliseconds)]

`RUN_INACTIVE_CUTOFF_TIME` [ for runs that have a heartbeat, controls how long a run with a failed heartbeat should wait for possibly queued tasks to start and resume heartbeat updates. Default is 1 day (in seconds)]

## Baseurl configuration

Use `MF_BASEURL` environment variable to overwrite the default API baseurl.
This affects API responses where meta links are provided as a response.

## Custom Navigation links for UI

You can customize the admin navigation links presented by the UI by creating a `config.custom_quicklinks.json` file on the server. See `example.custom_quicklinks.json` for a reference. Alternatively you can set an environment variable `CUSTOM_QUICKLINKS` for the backend process. The value for the environment variable should be a _stringified_ json of the format:

```json
[
  {
    "href": "https://docs.metaflow.org/",
    "label": "Metaflow documentation"
  },
  {
    "href": "https://github.com/Netflix/metaflow",
    "label": "GitHub"
  }
]
```

You are free to provide as many links as necessary.

**Local Development**
set the `CUSTOM_QUICKLINKS` environment variable

**Prebuilt docker image**
Provide the `CUSTOM_QUICKLINKS` environment variable for the docker run command

```bash
  CUSTOM_QUICKLINKS='[{"href": "https://github.com/Netflix/metaflow", "label": "GitHub"}]' docker run metaflow/ui-service

```

## System Notifications for UI

You can define notifications that are broadcasted to all clients using the UI by creating a `config.notifications.json` file on the server, see `example.notifications.json` for a reference. Alternatively you can set the environment variable `NOTIFICATIONS` for the backend process. The value for the environment variable should be a _stringified_ json of the format:

```json
[
  {
    "created": 1618404534000,
    "message": "Upcoming service maintenance"
  }
]
```

You can provide as many notifications as necessary, topmost item is considered the latest.

Following attributes are supported:

| Attribute     | Description                                                                       | Default value                                 |
| ------------- | --------------------------------------------------------------------------------- | --------------------------------------------- |
| `id`          | Notification identifier                                                           | Generated SHA1 hash `622b3a6...` - `optional` |
| `message`     | Message to display (Markdown supported with `contentType: markdown`)              | `required`                                    |
| `created`     | Notification created at (Epoch timestamp in milliseconds)                         | `required`                                    |
| `type`        | Notification type, allowed values: `success,info,warning,danger,default`          | `info` - `optional`                           |
| `contentType` | Message content-type, allowed values: `text,markdown`                             | `text` - `optional`                           |
| `url`         | Notification url                                                                  | `optional`                                    |
| `urlText`     | Human readable url title                                                          | `optional`                                    |
| `start`       | Schedule notification to be visible starting at (Epoch timestamp in milliseconds) | `null` - `optional`                           |
| `end`         | Schedule notification to disappear after (Epoch timestamp in milliseconds)        | `null` - `optional`                           |

Markdown example:

```json
[
  {
    "id": "fixed_id_attribute",
    "type": "info",
    "contentType": "markdown",
    "message": "Upcoming service maintenance [Metaflow](https://metaflow.org)",
    "created": 1618404534000,
    "start": 1618404534000,
    "end": 1618925483000
  }
]
```

Plaintext example:

```json
[
  {
    "id": "fixed_id_attribute",
    "type": "info",
    "contentType": "text",
    "message": "Upcoming service maintenance",
    "url": "https://metaflow.org",
    "urlText": "Metaflow",
    "created": 1618404534000,
    "start": 1618404534000,
    "end": 1618925483000
  }
]
```

## Log content restriction options

The `MF_LOG_LOAD_POLICY` environment variable restricts the amount of log content loaded by the UI. These values are supported:

- `full` (default): loads entire log.
- `tail`: loads the tail of the log only (up to a fixed size by number of characters. See `MF_LOG_LOAD_TAIL_SIZE` below.
- `blurb_only`: does not load the log at all. Instead return Python code snippet to access logs using Metaflow client.

`MF_LOG_LOAD_TAIL_SIZE` may be used when `MF_LOG_LOAD_POLICY=tail`. Returns the last N lines of the log file, where N is maximized without returning more than MF_LOG_LOAD_TAIL_SIZE number of characters. Defaults to `100*1024` characters.

## Card content restriction

The `MF_CARD_LOAD_POLICY` (default `full`) environment variable can be set to `blurb_only` to return a Python code snippet to access card using Metaflow client, instead of loading actual HTML card payload.


## Scaling reads using read replicas

Databases such as [Amazon Aurora](https://aws.amazon.com/rds/aurora/) provide 
[read replicas](https://aws.amazon.com/rds/features/read-replicas/) that make it easy to elastically scale beyond
the capacity constraints of single database instance for heavy read workloads. You are able to separate out the reads
and the writes of this application by setting the following two environment variables:

>```
> USE_SEPARATE_READER_POOL = 1                               
> MF_METADATA_DB_READ_REPLICA_HOST = <READ_REPLICA_ENDPOINT>
>```

As the name suggests, the `USE_SEPARATE_READER_POOL` variable creates a separate read pool with the same 
min/max pool size as the writer pool. It is also required to set this variable `MF_METADATA_DB_READ_REPLICA_HOST` to 
point to the read replica endpoint that is typically a load balancer in front of all the database's read replicas.

### Accounting for eventual consistency

When a read replica is created, there is a lag between the time a transaction is committed to the writer instance and
the time when the newly written data is available in the read replica. In Amazon Aurora, this [lag is usually much less
than 100ms](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Replication.html) because the replicas
share the same underlying storage layer as the writer instance thereby avoiding the need to copy data into the replica
nodes. This Metaflow UI service application is read heavy and hence is a great candidate for scaling reads using this model.
