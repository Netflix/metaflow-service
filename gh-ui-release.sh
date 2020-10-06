#!/usr/bin/env bash
#
# Download and install UI release from private Netflix/metaflow-ui repository using Github API token.
# This will no longer be necessary once Netaflix/metaflow-ui repository is publicly available.
#
# Prerequisites:
#   curl, wget, jq, unzip
#
# Usage:
#
#   Download latest UI release:
#     TOKEN=GITHUB_API_TOKEN ./gh-ui-release.sh
#
#   Download latest UI release:
#     TOKEN=GITHUB_API_TOKEN ./gh-ui-release.sh latest
#
#   Download specific UI release version:
#     TOKEN=GITHUB_API_TOKEN ./gh-ui-release.sh v.0.1.2
#
#   Provide Github API token as an argument
#     ./gh-ui-release.sh v.0.1.2 GITHUB_API_TOKEN
#

VERSION=${1:-latest}
TOKEN=${2:-$TOKEN}

function gh_curl() {
  curl -H "Authorization: token $TOKEN" $@
}

if [ "$VERSION" = "latest" ]; then
  selector=".[0]"
else
  selector=".[] | select(.tag_name == \"$VERSION\")"
fi

asset=`curl -H "Authorization: token $TOKEN" -s https://api.github.com/repos/Netflix/metaflow-ui/releases | jq "$selector | .assets[0]"`

asset_id=`echo $asset | jq -r ".id"`
asset_url=`echo $asset | jq -r ".url"`
asset_url_token="https://${TOKEN}:@api.github.com/repos/Netflix/metaflow-ui/releases/assets/${asset_id}"

echo "Found asset id $asset_id"
echo "Found asset download url $asset_url"

# Download zip archive
wget -q --auth-no-challenge \
  --header='Accept:application/octet-stream' \
  $asset_url_token \
  -O metaflow-ui.zip

# Unzip and install so that UI service can serve the UI bundle
unzip -o metaflow-ui.zip -d services/ui_backend_service/ui