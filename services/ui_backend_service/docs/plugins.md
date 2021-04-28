# Plugin System

There are two ways to register a new plugin to UI service:

1. Placing a folder under `services/ui_backend_service/plugins/installed`
2. Defining a remote Git repository location

Plugins can be registered via `PLUGINS` environment variable. The value should be a _stringified_ json of the format:

```json
{
  "plugin-example": "https://github.com/Netflix/metaflow-ui-plugin-example.git"
}
```

All plugins are installed under `services/ui_backend_service/plugins/installed`. Local plugins should be placed under this folder, e.g. `services/ui_backend_service/plugins/installed/plugin-example/`.
Plugins are loaded only if `PLUGINS` environment variable contains entry for a specific plugin.

## Documentation

At minimun a plugin should contain file called `manifest.json` with following contents:

```json
{
  "name": "plugin-example",
  "version": "1.0.0",
  "entrypoint": "index.html"
}
```

Where `entrypoint` refers to a source that will be loaded inside sandboxed iframe on user's browser.

`entrypoint` can also be defined as absolute url:

```json
{
  "name": "plugin-example",
  "version": "1.0.0",
  "entrypoint": "https://hostname/index.html"
}
```

Following API routes are supported:

| Route                                  | Description                                                                   |
| -------------------------------------- | ----------------------------------------------------------------------------- |
| `GET /plugin`                          | List all successfully registered plugins                                      |
| `GET /plugin/{plugin_name}`            | List plugin details                                                           |
| `GET /plugin/{plugin_name}/{filename}` | Serve plugin files, such as `manifest.json`, `README.md` or `dist/index.html` |

## Plugin development

- Plugin development documentation can be found from [Netflix/metaflow-ui](https://github.com/Netflix/metaflow-ui) repository.
- See [example plugin](https://github.com/Netflix/metaflow-ui-plugin-example) as a reference implementation.

## Examples

Following JSON describes different ways to register plugins. Each plugin will be automatically downloaded to `services/ui_backend_service/plugins/installed`:

```json
{
  "plugin": "https://github.com/Netflix/metaflow-ui-plugin-example.git",
  "plugin-repository": {
    "repository": "https://github.com/Netflix/metaflow-ui-plugin-example.git"
  },
  "plugin-branch": {
    "repository": "https://github.com/Netflix/metaflow-ui-plugin-example.git",
    "ref": "origin/feature/test"
  },
  "plugin-commit": {
    "repository": "https://github.com/Netflix/metaflow-ui-plugin-example.git",
    "ref": "3758f6a"
  },
  "plugin-commit-with-parameters": {
    "repository": "https://github.com/Netflix/metaflow-ui-plugin-example.git",
    "ref": "3758f6a",
    "parameters": {
      "color": "blue"
    }
  }
}
```

Following JSON describes different ways to register local plugins. Each plugin should already have folder under `services/ui_backend_service/plugins/installed` with the same name:

```json
{
  "plugin-local": {},
  "plugin-local-with-parameteres": {
    "parameters": {
      "color": "yellow"
    }
  }
}
```

## Limitations and known issues

- Only HTTPS repositories are supported
- Repository should be publicly available without authentication
- Git plugin remote repository cannot be changed after plugin has been initially registered
  - Workaround is to delete the folder under `services/ui_backend_service/plugins/installed/plugin-example` and restart the service
