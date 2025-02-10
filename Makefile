
charts = .devtools/metaflow-tools
kubernetes = .devtools/minikube/minikube

$(charts) :
	@echo "Fetching metaflow-tools charts"
	git clone git@github.com:outerbounds/metaflow-tools.git -b publish-helm-chart .devtools/metaflow-tools

$(kubernetes):
	@echo "MINIKUBE INSTALL"
	@mkdir -p .devtools/minikube
	curl -L https://github.com/kubernetes/minikube/releases/latest/download/minikube-darwin-amd64 -o $(kubernetes)
	chmod +x $(kubernetes)

minio : charts kubernetes
	echo "MINIO INSTALL"

argo : charts minio kubernetes
	echo "ARGO INSTALL"

.PHONY : clean kubernetes-dev charts kubernetes

# aliases
charts : $(charts)
kubernetes : $(kubernetes)

# convenience
kubernetes-dev : kubernetes
	$(kubernetes) start --cpus 2 --memory 2048

clean :
	$(kubernetes) stop
	rm -rf .tox .devtools