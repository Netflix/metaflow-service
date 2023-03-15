# Release process

We follow [Semantic Versioning Specification 2.0.0](https://semver.org/spec/v2.0.0.html).

In short, given a version number MAJOR.MINOR.PATCH, increment the:

1. MAJOR version when you make incompatible API changes,
2. MINOR version when you add functionality in a backwards compatible manner, and
3. PATCH version when you make backwards compatible bug fixes.

Additional labels for pre-release and build metadata are available as extensions to the MAJOR.MINOR.PATCH format.

## Shipping a new version

The release process is mostly automated via Github Actions, however a few manual steps are required:

- [ ] [Edit `setup.py`](https://github.com/Netflix/metaflow-service/edit/master/setup.py) version in `master` branch (e.g. `"version": "1.0.0"`)
- [ ] [Edit `Dockerfile.ui_service`](https://github.com/Netflix/metaflow-service/edit/master/Dockerfile.ui_service) and [edit `Dockerfile`](https://github.com/Netflix/metaflow-service/edit/master/Dockerfile) to set `ARG UI_VERSION="v7.7.7"` to the _latest version of `metaflow-ui`_ (if changed)
- [ ] Create new tag from `master` branch (e.g. `git tag v1.0.0`, note the `v` -prefix)
- [ ] Push tag to remote (e.g. `git push origin v1.0.0`)
- [ ] Create a new release draft in [releases](https://github.com/Netflix/metaflow-service/releases)
- [ ] Edit release draft
  - [ ] Make sure current and previous version are correct
  - [ ] Edit `Compatibility` section (Correct [Netflix/metaflow-service](https://github.com/Netflix/metaflow-service/releases) release versions)
  - [ ] Edit/remove `Additional resources` section
  - [ ] Make sure release artifact is uploaded
- [ ] Publish release draft

GitHub Actions will automatically publish the docker image to [netflixoss/metaflow_metadata_service](https://hub.docker.com/r/netflixoss/metaflow_metadata_service)
