import os

FEATURE_ENV_PREFIX = 'FEATURE_'


def get_features():
    """
    Get a dict of features that are enabled or disabled for the process

    Returns
    -------
    Dict
        example:
        {
            "FEATURE_SOME_FEAT": True
        }
    """
    features = {}
    for key, val in os.environ.items():
        if key.startswith(FEATURE_ENV_PREFIX):
            val = val.lower()
            features[key] = val != '0' and val != 'false' and val != 'f'
    return features


FEATURES = get_features()

FEATURE_PREFETCH_DISABLE = FEATURES.get('FEATURE_PREFETCH_DISABLE', False)
FEATURE_CACHE_DISABLE = FEATURES.get('FEATURE_CACHE_DISABLE', False)
FEATURE_S3_DISABLE = FEATURES.get('FEATURE_S3_DISABLE', False)
FEATURE_REFINE_DISABLE = FEATURES.get('FEATURE_REFINE_DISABLE', False)

FEATURE_PREFETCH_ENABLE = not FEATURE_PREFETCH_DISABLE
FEATURE_CACHE_ENABLE = not FEATURE_CACHE_DISABLE
FEATURE_S3_ENABLE = not FEATURE_S3_DISABLE
FEATURE_REFINE_ENABLE = not FEATURE_REFINE_DISABLE

FEATURE_WS_DISABLE = FEATURES.get('FEATURE_WS_DISABLE', False)
FEATURE_DB_LISTEN_DISABLE = FEATURES.get('FEATURE_DB_LISTEN_DISABLE', False)
FEATURE_HEARTBEAT_DISABLE = FEATURES.get('FEATURE_HEARTBEAT_DISABLE', False)

FEATURE_WS_ENABLE = not FEATURE_WS_DISABLE
FEATURE_DB_LISTEN_ENABLE = not FEATURE_DB_LISTEN_DISABLE
FEATURE_HEARTBEAT_ENABLE = not FEATURE_HEARTBEAT_DISABLE

if FEATURE_S3_DISABLE:
    os.environ["AWS_ACCESS_KEY_ID"] = "None"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "None"
    os.environ["AWS_DEFAULT_REGION"] = "None"
