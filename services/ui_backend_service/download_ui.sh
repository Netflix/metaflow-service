#!/bin/bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

FILENAME="metaflow-ui.zip"
DEST=${1:-$DIR/ui}

UI_RELEASE_URL="https://github.com/Netflix/metaflow-ui/releases/download/${UI_VERSION}/metaflow-ui-${UI_VERSION}.zip"

if [ $UI_ENABLED = "1" ]
then
    echo "Download UI version ${UI_VERSION} from $UI_RELEASE_URL to $DEST"
    curl $UI_RELEASE_URL -o $FILENAME
    unzip -o $FILENAME -d $DEST
    rm $FILENAME
else
    echo "UI not enabled, skip download."
fi