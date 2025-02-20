
# This is a default but still explicitly stating it for clarity.
# ref: https://docs.tilt.dev/api.html
# 'Tilt will only push to clusters that have been allowed for local development.'
allow_k8s_contexts('minikube')

# version_settings() enforces a minimum Tilt version
# https://docs.tilt.dev/api.html#api.version_settings
version_settings(constraint='>=0.22.2')

# use the helm extension for charts
load('ext://helm_resource', 'helm_resource', 'helm_repo')
load('ext://helm_remote', 'helm_remote')

# MinIO chart
helm_remote(
    "operator",
    repo_name='minio-operator',
    repo_url="https://operator.min.io",
)

# MinIO tenant
helm_remote(
    "tenant",
    repo_name='minio-operator',
    repo_url="https://operator.min.io",
    set=[
        "tenant.configuration.name=minio-secret",
        "tenant.configSecret.name=minio-secret",
        "tenant.configSecret.accessKey=rootuser",
        "tenant.configSecret.secretKey=rootpass123",
        "tenant.certificate.requestAutoCert=false",
        "tenant.buckets[0].name=metaflow-test,tenant.buckets[0].policy=none,tenant.buckets[0].purge=false",
        "tenant.pools[0].servers=1,tenant.pools[0].name=pool-0,tenant.pools[0].volumesPerServer=1,tenant.pools[0].size=1Gi"
    ],
)
# workaround for port forwarding to MinIO API
local_resource('minio-port-forward', serve_cmd='kubectl port-forward svc/myminio-hl 9000:9000')

# Kubernetes secret for accessing MinIO
k8s_yaml(
    blob("""
apiVersion: v1
kind: Secret
metadata:
  name: k8s-minio-access
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: rootuser
  AWS_SECRET_ACCESS_KEY: rootpass123
""")
)

# Argo workflows chart
# TODO: create and specify namespace
helm_remote(
    "argo-workflows",
    repo_name="argo-workflows",
    repo_url="https://argoproj.github.io/argo-helm",
    # namespace="argo"
)

# Argo events
helm_remote(
    "argo-events",
    repo_name="argo-events",
    repo_url="https://argoproj.github.io/argo-helm",
    # namespace="argo"
)

# Argo roles
k8s_yaml(
blob("""
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: argo-workflowtaskresults-role
  namespace: default
rules:
  - apiGroups: ["argoproj.io"]
    resources: ["workflowtaskresults"]
    verbs: ["create", "patch", "get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: default-argo-workflowtaskresults-binding
  namespace: default
subjects:
  - kind: ServiceAccount
    name: default
    namespace: default
roleRef:
  kind: Role
  name: argo-workflowtaskresults-role
  apiGroup: rbac.authorization.k8s.io
""")
)


# live_update syncs changed source code files to the correct place for the Flask dev server
# and runs pip (python package manager) to update dependencies when changed
# https://docs.tilt.dev/api.html#api.docker_build
# https://docs.tilt.dev/live_update_reference.html
docker_build(
    'public.ecr.aws/outerbounds/metaflow_metadata_service',
    context='.',
    dockerfile='./Dockerfile',
    # only=['./services/'],
    live_update=[
        sync('./services/', '/root/services/'),
        run(
            'pip install -r /requirements.txt',
            trigger=['./requirements.txt']
        )
    ]
)


# Apply the Metaflow helm charts with the locally built image to our local cluster
# https://docs.tilt.dev/api.html#api.k8s_yaml
k8s_yaml(helm('.devtools/metaflow-tools/charts/metaflow'))

# Port forward direct to the metadata service
k8s_resource(
    'chart-metaflow-service',
    port_forwards='8080:8080',
)


# config.main_path is the absolute path to the Tiltfile being run
# there are many Tilt-specific built-ins for manipulating paths, environment variables, parsing JSON/YAML, and more!
# https://docs.tilt.dev/api.html#api.config.main_path
tiltfile_path = config.main_path

# print writes messages to the (Tiltfile) log in the Tilt UI
# the Tiltfile language is Starlark, a simplified Python dialect, which includes many useful built-ins
# config.tilt_subcommand makes it possible to only run logic during `tilt up` or `tilt down`
# https://github.com/bazelbuild/starlark/blob/master/spec.md#print
# https://docs.tilt.dev/api.html#api.config.tilt_subcommand
if config.tilt_subcommand == 'up':
    print("""
    \033[32m\033[32mHello World from tilt-avatars!\033[0m

    If this is your first time using Tilt and you'd like some guidance, we've got a tutorial to accompany this project:
    https://docs.tilt.dev/tutorial

    If you're feeling particularly adventurous, try opening `{tiltfile}` in an editor and making some changes while Tilt is running.
    What happens if you intentionally introduce a syntax error? Can you fix it?
    """.format(tiltfile=tiltfile_path))