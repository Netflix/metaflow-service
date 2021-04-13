# Optional environment variable configuration for the UI Service

The following are optional environment variables that can be used to fine-tune the behavior of specific components of the UI service if necessary. 

## Web socket message retention

Configure amount of seconds realtime events are kept in queue (delivered to UI in case of reconnects):

`WS_QUEUE_TTL_SECONDS` [defaults to 300 (5 minutes)]

## Cache and data preload limits 

Configure amount of runs to prefetch during server startup (artifact cache):

`PREFETCH_RUNS_SINCE` [in seconds, defaults to 2 days ago (86400 * 2 seconds)]
`PREFETCH_RUNS_LIMIT` [defaults to 50]

Configure the amount of concurrent cache actions. This works similar to a database connection pool.

`CACHE_ARTIFACT_MAX_ACTIONS` [max number of artifact cache actions. Defaults to 16]
`CACHE_DAG_MAX_ACTIONS` [max number of DAG cache actions. Defaults to 16]

Configure the maximum usable space by the cache:

`CACHE_ARTIFACT_STORAGE_LIMIT` [in bytes, defaults to 600000]
`CACHE_DAG_STORAGE_LIMIT` [in bytes, defaults to 100000]

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
`OLD_RUN_FAILURE_CUTOFF_TIME` [ for runs that do not have a heartbeat, controls at what point a running status run should be considered failed. Default is 2 weeks]

## Baseurl configuration

Use `MF_BASEURL` environment variable to overwrite the default API baseurl.
This affects API responses where meta links are provided as a response.

## Custom Navigation links for UI

You can customize the admin navigation links presented by the UI by setting an environment variable `CUSTOM_QUICKLINKS` for the backend process. The value should be a _stringified_ json of the format:

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

## Announcements for UI

You can define announcements that are broadcasted to all clients using the UI by setting an environment variable `ANNOUNCEMENTS` for the backend process. The value should be a _stringified_ json of the format:

```json
[
  {
    "message": "Upcoming service maintenance"
  }
]
```

You can provide as many announcements as necessary, topmost item is considered the latest.

Following attributes are supported:

| Attribute | Description                                                              | Default value                                 |
| --------- | ------------------------------------------------------------------------ | --------------------------------------------- |
| `id`      | Announcement identifier                                                  | Generated SHA1 hash `622b3a6...` - `optional` |
| `type`    | Announcement type, allowed values: `success,info,warning,danger,default` | `info` - `optional`                           |
| `message` | Message to display (Markdown supported)                                  | `required`                                    |
| `start`   | First time the announcement will be visible (Epoch timestamp in seconds) | `null` - `optional`                           |
| `end`     | Announcement no longer visible after (Epoch timestamp in seconds)        | `null` - `optional`                           |

Example with all the attributes:

```json
[
  {
    "id": "fixed_id_attribute",
    "type": "info",
    "message": "Upcoming service maintenance",
    "start": 1610406000,
    "end": 1610402400
  }
]
```