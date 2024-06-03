# Terraform Approach
This `README.md` will walk you through standing up the `teehr-hub`` in an AWS account.
The instructions should generally work with other providers, but some steps will certainly be different.

NOTE: you must clean up complete from other approaches before running this.

Login to AWS.  You need to login with a user that has sufficient permissions.
We used Admin for testing but need to determine the minimum set of permissions needed.
```bash
aws configure
```

Apply Terraform (can take ~15 mins)
```bash
cd terraform
terraform init
terraform apply -var-file=teehr-hub.tfvars
cd ..
```

Connect to cluster
```bash
aws eks update-kubeconfig --name teehr-hub --region us-east-2
kubectl config set-context $(kubectl config current-context) --namespace teehr-hub
k9s
```

Apply Helmcharts
Add repo
```bash
cd helm-chart
helm repo add dask https://helm.dask.org/
helm repo add autoscaler https://kubernetes.github.io/autoscaler
cd ..
```

This next step requires that you have a `.env` file the contains the
`GH_OAUTH_CLIENT_ID` and `GH_OAUTH_CLIENT_SECRET` values needed to use GitHub auth.
A sample `.env-sample` is provided.
```bash
cd helm-chart
source .env
envsubst < config.yaml > secret-config.yaml
helm upgrade \
  --cleanup-on-fail \
  --install teehr-hub dask/daskhub \
  --namespace teehr-hub \
  --create-namespace \
  --version 2024.1.0 \
  --values secret-config.yaml
rm secret-config.yaml
helm upgrade \
  --cleanup-on-fail \
  --install teehr-ca autoscaler/cluster-autoscaler \
  --namespace teehr-hub \
  --set autoDiscovery.clusterName=teehr-hub \
  --set awsRegion=us-east-2
cd ..
```

Get NFS DNS
```bash
cd terraform/ && terraform output nfs_server_dns && cd ..
```

Update NFS DNS in manifests, then run
```bash
cd terraform/
export NFS_SERVER_DNS="$(terraform output -raw nfs_server_dns)"
cd ..
cd kubernetes
envsubst < nfs.yaml | kubectl apply -n teehr-hub -f -
envsubst < nfs-data-creator.yaml | kubectl delete -n teehr-hub -f -
envsubst < nfs-data-creator.yaml | kubectl apply -n teehr-hub -f -
cd ..
```

Get DNS CNAME
```bash
kubectl --namespace=teehr-hub get svc proxy-public
```
MANUAL: Set alias in DNS provider

Delete pods and let it recreate
```bash
kubectl delete pods -l component=autohttps -n teehr-hub
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
cd terraform/
export NFS_SERVER_DNS="$(terraform output -raw nfs_server_dns)"
cd ..
cd kubernetes
envsubst < nfs.yaml | kubectl delete  -n teehr-hub -f -
envsubst < nfs-data-creator.yaml | kubectl delete -n teehr-hub -f -
cd ..
```

Cluster
```bash
cd terraform
terraform destroy -var-file=teehr-hub.tfvars
cd ..
```


Update helm
```
cd helm-chart
helm repo update dask
helm repo update autoscaler
cd ..
```