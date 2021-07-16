# Plugin System

There are two ways to register a new plugin to UI service:

1. Placing a folder under `services/ui_backend_service/plugins/installed`
2. Defining a remote Git repository location

Plugins can be registered via `PLUGINS` environment variable. The value should be a _stringified_ json of the format:

```json
{
  "plugin-example": "git@github.com:Netflix/metaflow-ui-plugin-example.git"
}
```

All plugins are installed under `services/ui_backend_service/plugins/installed`. Local plugins should be placed under this folder, e.g. `services/ui_backend_service/plugins/installed/plugin-example/`.
Plugins are loaded only if `PLUGINS` environment variable contains entry for a specific plugin.

Both HTTPS and SSH repositories are supported.

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

## Authentication credentials

There are multiple ways to provide authentication credentials:

- Username and password
- SSH key pair
- SSH Agent (`~/.ssh`)

Authentication credentials can be provided by using `auth` object at the top level of `PLUGINS` json
or alternatively by defining `auth` object at repository level.

```json
{
  "auth": {
    "public_key": "/path/to/id_rsa.pub",
    "private_key": "/path/to/id_rsa"
  },
  "metaflow-ui-plugin-example": "git@github.com:Netflix/metaflow-ui-plugin-example.git",
  "metaflow-ui-plugin-noauth": {
    "repository": "git@github.com:Netflix/metaflow-ui-plugin-example-noauth.git",
    "auth": null
  }
}
```

See Examples -section for reference.

## Plugin development

- Plugin development documentation can be found from [Netflix/metaflow-ui](https://github.com/Netflix/metaflow-ui) repository.
- See [example plugin](https://github.com/Netflix/metaflow-ui-plugin-example) as a reference implementation.

## Examples

Following JSON describes different ways to register plugins. Each plugin will be automatically downloaded to `services/ui_backend_service/plugins/installed`:

```json
{
  "plugin": "git@github.com:Netflix/metaflow-ui-plugin-example.git",
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
    "repository": "git@github.com:Netflix/metaflow-ui-plugin-example.git",
    "ref": "3758f6a",
    "parameters": {
      "color": "blue"
    }
  },
  "plugin-repository-multiple": {
    "repository": "https://github.com/Netflix/metaflow-ui-plugin-example.git",
    "paths": ["path/to/first-plugin", "second-plugin"]
  }
}
```

Following JSON describes different ways to provide authentication credentials for remote Git repositories:

```json
{
  "auth": {
    "public_key": "/root/id_rsa.pub",
    "private_key": "/root/id_rsa",
    "user": "git",
    "pass": "optional-passphrase"
  },
  "plugin-auth-global": "git@github.com:Netflix/metaflow-ui-plugin-example.git",
  "plugin-noauth": {
    "repository": "git@github.com:Netflix/metaflow-ui-plugin-example.git",
    "auth": null
  },
  "plugin-auth-user": {
    "repository": "git@github.com:Netflix/metaflow-ui-plugin-example.git",
    "auth": {
      "user": "username"
    }
  },
  "plugin-auth-userpass": {
    "repository": "git@github.com:Netflix/metaflow-ui-plugin-example.git",
    "auth": {
      "user": "username",
      "pass": "password"
    }
  },
  "plugin-auth-key-user": {
    "repository": "git@github.com:Netflix/metaflow-ui-plugin-example.git",
    "auth": {
      "public_key": "ssh-rsa AAAA...",
      "private_key": "-----BEGIN RSA PRIVATE KEY-----...",
      "user": "custom"
    }
  },
  "plugin-auth-key-pass": {
    "repository": "git@github.com:Netflix/metaflow-ui-plugin-example.git",
    "auth": {
      "public_key": "/root/id_rsa.pub",
      "private_key": "/root/id_rsa",
      "pass": "optional-passphrase"
    }
  },
  "plugin-auth-agent": {
    "repository": "git@github.com:Netflix/metaflow-ui-plugin-example.git",
    "auth": {
      "agent": true
    }
  },
  "plugin-auth-agent-user": {
    "repository": "git@github.com:Netflix/metaflow-ui-plugin-example.git",
    "auth": {
      "agent": true,
      "user": "custom"
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
  },
  "plugin-local-multiple": {
    "paths": ["path/to/first-plugin", "second-plugin"]
  }
}
```
