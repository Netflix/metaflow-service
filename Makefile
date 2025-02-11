
charts = .devtools/metaflow-tools
kubernetes = .devtools/minikube/minikube

ifeq ($(shell uname), Darwin)
	os = darwin
else
	os = linux
endif

ifeq ($(shell uname -m), x86_64)
	arch = amd64
else
	arch = arm64
endif


$(charts) :
	@echo "Fetching metaflow-tools charts"
	git clone git@github.com:outerbounds/metaflow-tools.git .devtools/metaflow-tools

$(kubernetes) :
	@echo "MINIKUBE INSTALL"
	@mkdir -p .devtools/minikube
	curl -L https://github.com/kubernetes/minikube/releases/latest/download/minikube-$(os)-$(arch) -o $(kubernetes)
	chmod +x $(kubernetes)
	@echo "Enabling ingress for minikube..."
	$(kubernetes) addons enable ingress

minio : charts kubernetes
	echo "MINIO INSTALL"

argo : charts minio kubernetes
	echo "ARGO INSTALL"

.PHONY : clean kubernetes-dev charts kubernetes

# aliases
charts : $(charts)
kubernetes : $(kubernetes)

# convenience
kubernetes-dev : kubernetes charts
	$(kubernetes) start --cpus 2 --memory 2048
	tilt up

clean :
	$(kubernetes) stop
	rm -rf .tox .devtools