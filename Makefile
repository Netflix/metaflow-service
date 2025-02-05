
charts = .devtools/metaflow-tools

$(charts) :
	git clone git@github.com:outerbounds/metaflow-tools.git -b publish-helm-chart .devtools/metaflow-tools

kubernetes : charts
	echo "MINIKUBE INSTALL"

minio : charts kubernetes
	echo "MINIO INSTALL"

argo : charts minio kubernetes
	echo "ARGO INSTALL"


.PHONY : clean kubernetes-dev charts

# aliases
charts : $(charts)

# convenience
kubernetes-dev :
	docker-compose -f docker-compose.development.yml up

clean :
	rm -rf .tox $(charts)