# Documentation for Web Socket endpoints

## Realtime state subscriptions for resources

### Subscribing and unsubscribing.
Subscribing to a RESTful resources realtime events is done by sending the following message to the
`ws://HOSTNAME/ws` endpoint.

```json
  {
    "type": "SUBSCRIBE", 
    "resource": "path-to-subscribable-restful-resource",
    "uuid": "client-generated-uuid"
  }
```

Subscribe to future events and return past data since unix time (seconds):
```json
  {
    "type": "SUBSCRIBE", 
    "resource": "path-to-subscribable-restful-resource",
    "uuid": "client-generated-uuid",
    "since": 1602752197
  }
```

Unsubscribing is done through the same endpoint with the message:
```json
  {
    "type": "UNSUBSCRIBE", 
    "uuid": "existing-client-generated-uuid"
  }
```

### Resources
Subscribable resource endpoints include. All subscriptions also adhere to the corresponding RESTful routes query parameters to further filter received messages.

* `/flow_name/runs/`
* `/flow_name/runs/run_number`
* `/flow_name/runs/run_number/steps`
* `/flow_name/runs/run_number/steps/step_name`
* `/flow_name/runs/run_number/steps/step_name/tasks`
* `/flow_name/runs/run_number/steps/step_name/tasks/task_id`
* `/flow_name/runs/run_number/steps/step_name/tasks/task_id/logs/out`
* `/flow_name/runs/run_number/steps/step_name/tasks/task_id/logs/err`

### Received messages
The web socket client can receive three types of messages for its subscription:

```json
  {
    "type": "type-of-event",
    "resource": "path/of/subscribed/resource",
    "data": {},
    "uuid": "uuid-of-subscription"
  }
```
The type can be one of `INSERT`, `UPDATE` or `DELETE`, corresponding to similar database actions.
The `data` property contains the complete object of the subscribed resource, as it would be received from a basic GET request.
# SEARCH API

The Search Api provides a way to search which tasks have matching artifacts for a given run. Searching is performed through a websocket connection.

## Searching
The endpoint to perform searches for a given run looks like
`ws://HOSTNAME/flows/flow_id/runs/run_number/search?key=ARTIFACT_NAME&value=VALUE`
where `ARTIFACT_NAME` is the name of an artifact to look for, and `VALUE` is the content of the artifact that we are searching for.

### Search Responses
When the web socket opens for the search, the client starts receiving messages. These include progress, possible errors, and eventually the results.

Progress message example:
```json
  {
    "event": {
      "type": "progress",
      "fraction": 1
    }
  }
```
The fraction is a percentage of objects loaded for the search.

Error example:
```json
  {
    "event": {
      "type": "error",
      "message": "error message",
      "id": "unique-error-id"
    }
  }
```
The unique id of an error can be one of the following:
- `s3-access-denied` - server does not have access to s3 bucket
- `s3-not-found` - s3 404 response
- `s3-bad-url` - malformed s3 url
- `s3-missing-credentials` - server does not have credentials for s3 access
- `s3-generic-error` - something went wrong with s3 access
- `artifact-handle-failed` - something went wrong with processing the artifact
- `artifact-not-accessible` - artifact location is not accessible by the server (local storage)

Results example:
```json
  {
    "event": {
      "type": "result",
      "matches": [
        {
          "flow_id": "FlowName",
          "run_number": 123,
          "step_name": "some_step",
          "task_id": 456,
          "searchable": true
        }
      ]
    }
  }
```
The `searchable` boolean of a single task conveys whether the task had an artifact that could be included in the search process.

