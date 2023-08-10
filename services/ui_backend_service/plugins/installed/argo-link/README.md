# argo-link plugin

This plugin provides a link to a page showing Argo workflow templates.

## Usage

- Set the `PLUGINS` environment variable.

```bash
export PLUGINS='{"argo-link":{"repository":"https://github.com/outerbounds/mfgui_plugins.git","paths":["argo-link"],"ref":"origin/main","parameters":{"sandbox":"allow-popups allow-popups-to-escape-sandbox allow-top-navigation"}}}'
```

- Restart `metaflow-service`.
- Look for the link underneath the logo in the header.
