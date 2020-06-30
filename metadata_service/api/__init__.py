import pkg_resources

version = pkg_resources.require("metadata_service")[0].version

METADATA_SERVICE_VERSION = version
METADATA_SERVICE_HEADER = 'METADATA_SERVICE_VERSION'
