#!/usr/bin/env bash

set -e

docker-compose -f docker-compose.development.yml up -d --build &

# Mac installation steps
#brew install minikube
#brew install argo
#brew install kubectl


pip install kubernetes
echo "Starting Minikube"
minikube start

alias kubectl="minikube kubectl --"
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.1.1/cert-manager.yaml
kubectl create ns argo
kubectl apply -n argo -f https://raw.githubusercontent.com/argoproj/argo-workflows/master/manifests/quick-start-postgres.yaml
kubectl -n argo port-forward deployment/argo-server 2746:2746

wait
