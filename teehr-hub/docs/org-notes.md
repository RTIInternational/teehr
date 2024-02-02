We want to install [Pangeo](https://pangeo.io/index.html) in AWS to support on going evaluation work.

Useful guides:
- https://pangeo.io/setup_guides/azure.html
- https://z2jh.jupyter.org/en/latest/kubernetes/amazon/step-zero-aws-eks.html

# ToDo
- Use static IP Address -> easy I think
- Use eval.ciroh.org -> Arpita can help
- Terraform?
- Which GitHub account to use for auth?

# AWS

## create service role
Use AWS Console. Create Group `TeehrDeploy` and attach the minimum IAM policies.
https://eksctl.io/usage/minimum-iam-policies/

Make my user a member of group `TeehrDeploy`.

Login to AWS with `aws configure`

## create cluster
```
eksctl create cluster --name teehr --region us-east-2
```
```
eksctl delete cluster teehr
```

## enable/install CSI Driver
https://stackoverflow.com/questions/75758115/persistentvolumeclaim-is-stuck-waiting-for-a-volume-to-be-created-either-by-ex
```
eksctl utils associate-iam-oidc-provider \
    --region=us-east-2 \
    --cluster=teehr \
    --approve

eksctl create iamserviceaccount \
  --name ebs-csi-controller-sa \
  --namespace kube-system \
  --cluster teehr \
  --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \
  --approve \
  --role-only \
  --role-name AmazonEKS_EBS_CSI_DriverRole

eksctl create addon --name aws-ebs-csi-driver --cluster teehr --service-account-role-arn arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/AmazonEKS_EBS_CSI_DriverRole --force
```

## deploy pangeo (daskhub, really)
Create `config.yaml` and populate with non-default values.

```
helm upgrade --cleanup-on-fail \
  --install daskhub dask/daskhub \
  --namespace teehr-hub \
  --create-namespace \
  --version 2023.1.0 \
  --values config.yaml
```

To see all charts
```
helm list --all-namespaces
```

## get external ip address
```
kubectl --namespace teehr-hub get service proxy-public
```
Visiting the EXTERNAL-IP in the browser should take you to the JupyterHub

## set dns
For now make `ciroh-eval.rtiamanzi.org` point at the JupyterHub

## setup GitHub auth
See config.yaml

## Set up to use TEEHR Docker image
- create ERC image registry
```
aws ecr create-repository \
    --repository-name teehr \
    --image-scanning-configuration scanOnPush=true \
    --region us-east-2
```
- set up repo to push to the AWS registry

Update config to use our teehr image.

This all basically works, but to get the scaling parts to work, we need a more complex cluster and autoscaler.

# Take 2 following the 2i2c docs more closely (sort of).

The 2i2c infrastructure repo has a lot of good info about how they do it.
https://github.com/mgdenno/infrastructure

This doc in particular describes about setting up the cluster with different groups needed to taint and tolerate.
https://github.com/mgdenno/infrastructure/blob/master/docs/hub-deployment-guide/new-cluster/aws.md

Need to create a Python venv and install the deployer app.

Run the deployer as described in the link.

```bash
export CLUSTER_NAME=teehr-hub
export CLUSTER_REGION=us-east-2
export HUB_TYPE=daskhub
```

```bash
deployer generate-aws-cluster --cluster-name=$CLUSTER_NAME --cluster-region=$CLUSTER_REGION --hub-type=$HUB_TYPE
```

```bash
cd eksctl
eksctl create cluster --config-file=$CLUSTER_NAME.eksctl.yaml
```

## Daskhub (TeehrHub)
```bash
helm repo add dask https://helm.dask.org/
helm upgrade --cleanup-on-fail \
  --install teehr-hub dask/daskhub \
  --namespace teehr-hub \
  --create-namespace \
  --version 2023.1.0 \
  --values config.yaml
```


## Cluster autoscaler
```bash
helm repo add autoscaler https://kubernetes.github.io/autoscaler
# helm upgrade --cleanup-on-fail --install teehr-ca autoscaler/cluster-autoscaler --namespace teehr-hub   -f ca-config.yaml
helm upgrade \
  --cleanup-on-fail \
  --install teehr-ca autoscaler/cluster-autoscaler \
  --namespace teehr-hub \
  --set autoDiscovery.clusterName=teehr-hub \
  --set awsRegion=us-east-2
```

## Connecting
aws configure
aws eks update-kubeconfig --name teehr-hub --region us-east-2
kubectl config set-context $(kubectl config current-context) --namespace teehr-hub

## Uninstalling TeehrHub
helm list --all-namespaces
helm uninstall teehr-hub --namespace=teehr-hub
helm uninstall teehr-ca --namespace=teehr-hub


## Delete cluster
```bash
cd eksctl
eksctl create cluster --config-file=$CLUSTER_NAME.eksctl.yaml
```

## Trying to understand all the Helm Charts
Seems that daskhub uses `jupyterhub` and `dask-gateway` charts.
I think these configs describe the available settings and syntax.

Jupyterhub
https://github.com/jupyterhub/zero-to-jupyterhub-k8s/blob/main/jupyterhub/values.yaml

Dask-gateway
https://github.com/dask/dask-gateway/blob/master/resources/helm/dask-gateway/values.yaml
