
# EKSCTL Approach (Not used in favor of Terraform, but could be...)
NOTE: you must clean up complete from other approaches before running this.

Login to AWS.  You need to login with a user that has sufficient permissions.
For now we are using Admin, but will work to define the minimum set of permissions required.
```bash
aws configure
```

Create cluster
```bash
cd eksctl
eksctl create cluster --config-file=teehr-hub.eksctl.yaml
cd ..
```

Create NSF
```bash
cd eksctl-terraform
terraform init
terraform apply -var-file=teehr-hub.tfvars
cd ..
```

Apply Helmcharts
```bash
cd helm-chart

# teehr
helm repo add dask https://helm.dask.org/
helm upgrade --cleanup-on-fail \
  --install teehr-hub dask/daskhub \
  --namespace teehr-hub \
  --create-namespace \
  --version 2023.1.0 \
  --values config.yaml

# autoscaler
helm repo add autoscaler https://kubernetes.github.io/autoscaler
helm upgrade \
  --cleanup-on-fail \
  --install teehr-ca autoscaler/cluster-autoscaler \
  --namespace teehr-hub \
  --set autoDiscovery.clusterName=teehr-hub \
  --set awsRegion=us-east-2
  cd ..
```

Set DNS
```bash
kubectl --namespace=teehr-hub get svc proxy-public
```
Set alias in DNS provider


Delete pods and let it recreate
```bash
kubectl delete pods -l component=autohttps -n teehr-hub
```

Update NFS DNS in manifests, then run
cd ../eksctl-terraform/ && terraform output nfs_server_dns && cd -
```bash
cd kubernetes
kubectl apply -f nfs.yaml -n teehr-hub
kubectl apply -f nfs-data-creator.yaml -n teehr-hub
cd ..
```

## Uninstalling
Teehrhub
```bash
cd helm-chart
helm list --all-namespaces
helm uninstall teehr-hub --namespace=teehr-hub
helm uninstall teehr-ca --namespace=teehr-hub
cd ..
```

NFS
```bash
cd kubernetes
kubectl delete -f nfs.yaml -n teehr-hub
kubectl delete -f nfs-data-creator.yaml -n teehr-hub
cd ..
```

NFS Terraform
```bash
cd eksctl-terraform
terraform destroy -var-file=teehr-hub.tfvars
cd ..
```

Cluster
```bash
cd eksctl
eksctl delete cluster --config-file=teehr-hub.eksctl.yaml
cd ..
```
