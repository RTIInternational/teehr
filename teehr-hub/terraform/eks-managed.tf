module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 19.0"

  cluster_name                   = var.cluster_name
  cluster_version                = var.cluster_version
  cluster_endpoint_public_access = true

  vpc_id                   = module.vpc.vpc_id
  subnet_ids               = module.vpc.private_subnets

  iam_role_name   = "${local.cluster_name}-cluster-role"
  node_security_group_name = "${local.cluster_name}-node-security-group"
  cluster_security_group_name = "${local.cluster_name}-cluster-security-group"

  cluster_addons = {
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent = true
    }
    aws-ebs-csi-driver = {
      most_recent = true
    }
  }

  # Not 100% sure this is needed (default is false)
  manage_aws_auth_configmap = true


  # Extend cluster security group rules
  cluster_security_group_additional_rules = {
    egress_all = {
      description      = "Cluster all egress"
      protocol         = "-1"
      from_port        = 0
      to_port          = 0
      type             = "egress"
      cidr_blocks      = ["0.0.0.0/0"]
      ipv6_cidr_blocks = ["::/0"]
    }
  }

  # Extend node-to-node security group rules
  node_security_group_additional_rules = {
    ingress_self_all = {
      description = "Node to node all ports/protocols"
      protocol    = "-1"
      from_port   = 0
      to_port     = 0
      type        = "ingress"
      self        = true
    }
  }

  eks_managed_node_group_defaults = {
    # default is null
    ami_type       = "AL2_x86_64"

    subnet_ids = [module.vpc.private_subnets[0]]

    # disk_size = 80

    block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = 80
            volume_type           = "gp3"
            iops                  = 3000
            throughput            = 150
            # encrypted             = true
            # kms_key_id            = module.ebs_kms_key.key_arn
            delete_on_termination = true
          }
        }
      }

    use_name_prefix = true

    iam_role_additional_policies = {
      AmazonEBSCSIDriverPolicy = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
      cluster_autoscaler_policy = aws_iam_policy.cluster_autoscaler_policy.arn
    }

    # By default, the module creates a launch template to ensure tags are propagated to instances, etc.,
    # so we need to disable it to use the default template provided by the AWS EKS managed node group service
    # use_custom_launch_template = false

    # Add NFS security group ID to each
    vpc_security_group_ids = [ aws_security_group.efs-sg.id ]
  }

  eks_managed_node_groups = {
    core-a = {
      name            = "core-a"
      iam_role_name   = "${local.cluster_name}-core-a"

      min_size     = 1
      max_size     = 6
      desired_size = 1

      instance_types       = ["r5.xlarge"]

      labels = {
        # "cluster-name"                     = "${local.cluster_name}"
        # "nodegroup-name"                   = "core-a"
        "hub.jupyter.org/node-purpose"     = "core"
        "k8s.dask.org/node-purpose"        = "core"
        "node.kubernetes.io/instance-type" = "r5.xlarge"
      }

      taints = []

      tags = {
        "k8s.io/cluster-autoscaler/enabled"                                              = "true"
        "k8s.io/cluster-autoscaler/${local.cluster_name}"                                = "owned"
        "k8s.io/cluster-autoscaler/node-template/label/hub.jupyter.org/node-purpose"     = "core"
        "k8s.io/cluster-autoscaler/node-template/label/k8s.dask.org/node-purpose"        = "core"
        "k8s.io/cluster-autoscaler/node-template/label/node.kubernetes.io/instance-type" = "r5.xlarge"
      }
    }

    nb-r5-xlarge = {
      name            = "nb-r5-xlarge"
      iam_role_name   = "${local.cluster_name}-nb-r5-xlarge"

      min_size     = 0
      max_size     = 400
      desired_size = 0

      instance_types       = ["r5.xlarge"]

      labels = {
        # "cluster-name"                     = "${local.cluster_name}"
        # "nodegroup-name"                   = "nb-r5-xlarge"
        "hub.jupyter.org/node-purpose"     = "user"
        "k8s.dask.org/node-purpose"        = "scheduler"
        "node.kubernetes.io/instance-type" = "r5.xlarge"
      }

      taints = [
        {
          key    = "hub.jupyter.org/dedicated"
          value  = "user"
          effect = "NO_SCHEDULE"
        },
        {
          key    = "hub.jupyter.org_dedicated"
          value  = "user"
          effect = "NO_SCHEDULE"
        }
      ]

      tags = {
        "k8s.io/cluster-autoscaler/enabled"                                              = "true"
        "k8s.io/cluster-autoscaler/${local.cluster_name}"                                = "owned"
        "k8s.io/cluster-autoscaler/node-template/label/hub.jupyter.org/node-purpose"     = "user"
        "k8s.io/cluster-autoscaler/node-template/label/k8s.dask.org/node-purpose"        = "scheduler"
        "k8s.io/cluster-autoscaler/node-template/label/node.kubernetes.io/instance-type" = "r5.xlarge"
        "k8s.io/cluster-autoscaler/node-template/taint/hub.jupyter.org/dedicated"        = "user:NoSchedule"
        "k8s.io/cluster-autoscaler/node-template/taint/hub.jupyter.org_dedicated"        = "user:NoSchedule"
      }
    }


    nb-r5-4xlarge = {
      name            = "nb-r5-4xlarge"
      iam_role_name   = "${local.cluster_name}-nb-r5-4xlarge"

      min_size     = 0
      max_size     = 400
      desired_size = 0

      instance_types       = ["r5.4xlarge"]

      labels = {
        # "cluster-name"                     = "${local.cluster_name}"
        # "nodegroup-name"                   = "nb-r5-4xlarge"
        "hub.jupyter.org/node-purpose"     = "user"
        "k8s.dask.org/node-purpose"        = "scheduler"
        "node.kubernetes.io/instance-type" = "r5.4xlarge"
      }

      taints = [
        {
          key    = "hub.jupyter.org/dedicated"
          value  = "user"
          effect = "NO_SCHEDULE"
        },
        {
          key    = "hub.jupyter.org_dedicated"
          value  = "user"
          effect = "NO_SCHEDULE"
        }
      ]

      tags = {
        "k8s.io/cluster-autoscaler/enabled"                                              = "true"
        "k8s.io/cluster-autoscaler/${local.cluster_name}"                                = "owned"
        "k8s.io/cluster-autoscaler/node-template/label/hub.jupyter.org/node-purpose"     = "user"
        "k8s.io/cluster-autoscaler/node-template/label/k8s.dask.org/node-purpose"        = "scheduler"
        "k8s.io/cluster-autoscaler/node-template/label/node.kubernetes.io/instance-type" = "r5.4xlarge"
        "k8s.io/cluster-autoscaler/node-template/taint/hub.jupyter.org/dedicated"        = "user:NoSchedule"
        "k8s.io/cluster-autoscaler/node-template/taint/hub.jupyter.org_dedicated"        = "user:NoSchedule"
      }
    }

    nb-r5-16xlarge = {
      name            = "nb-r5-16xlarge"
      iam_role_name   = "${local.cluster_name}-nb-r5-16xlarge"

      min_size     = 0
      max_size     = 400
      desired_size = 0

      instance_types       = ["r5.16xlarge"]

      labels = {
        # "cluster-name"                     = "${local.cluster_name}"
        # "nodegroup-name"                   = "nb-r5-16xlarge"
        "hub.jupyter.org/node-purpose"     = "user"
        "k8s.dask.org/node-purpose"        = "scheduler"
        "node.kubernetes.io/instance-type" = "r5.16xlarge"
      }

      taints = [
        {
          key    = "hub.jupyter.org/dedicated"
          value  = "user"
          effect = "NO_SCHEDULE"
        },
        {
          key    = "hub.jupyter.org_dedicated"
          value  = "user"
          effect = "NO_SCHEDULE"
        }
      ]

      tags = {
        "k8s.io/cluster-autoscaler/enabled"                                              = "true"
        "k8s.io/cluster-autoscaler/${local.cluster_name}"                                = "owned"
        "k8s.io/cluster-autoscaler/node-template/label/hub.jupyter.org/node-purpose"     = "user"
        "k8s.io/cluster-autoscaler/node-template/label/k8s.dask.org/node-purpose"        = "scheduler"
        "k8s.io/cluster-autoscaler/node-template/label/node.kubernetes.io/instance-type" = "r5.16xlarge"
        "k8s.io/cluster-autoscaler/node-template/taint/hub.jupyter.org/dedicated"        = "user:NoSchedule"
        "k8s.io/cluster-autoscaler/node-template/taint/hub.jupyter.org_dedicated"        = "user:NoSchedule"
      }
    }


    dask-r5-4xlarge = {
      name            = "dask-r5-4xlarge"
      iam_role_name   = "${local.cluster_name}-dask-r5-4xlarge"

      min_size     = 0
      max_size     = 400
      desired_size = 0

      instance_types       = ["r5.4xlarge"]

      labels = {
        # "cluster-name"                     = "${local.cluster_name}"
        # "nodegroup-name"                   = "dask-r5-4xlarge"
        "k8s.dask.org/node-purpose"        = "worker"
        "node.kubernetes.io/instance-type" = "r5.4xlarge"
      }

      taints = [
        {
          key    = "k8s.dask.org/dedicated"
          value  = "worker"
          effect = "NO_SCHEDULE"
        },
        {
          key    = "k8s.dask.org_dedicated"
          value  = "worker"
          effect = "NO_SCHEDULE"
        }
      ]

      tags = {
        "k8s.io/cluster-autoscaler/enabled"                                              = "true"
        "k8s.io/cluster-autoscaler/${local.cluster_name}"                                = "owned"
        "k8s.io/cluster-autoscaler/node-template/label/k8s.dask.org/node-purpose/node-purpose"     = "worker"
        "k8s.io/cluster-autoscaler/node-template/label/node.kubernetes.io/instance-type" = "r5.4xlarge"
        "k8s.io/cluster-autoscaler/node-template/taint/k8s.dask.org/dedicated"        = "worker:NoSchedule"
        "k8s.io/cluster-autoscaler/node-template/taint/k8s.dask.org_dedicated"        = "worker:NoSchedule"
      }
    }

  }

  tags = local.tags
}

# Cluster autoscaler role
data "aws_iam_policy_document" "cluster_autoscaler" {
  statement {
    sid       = "clusterAutoscalerAll"
    effect    = "Allow"

    actions   = [
      "autoscaling:DescribeAutoScalingGroups",
      "autoscaling:DescribeAutoScalingInstances",
      "autoscaling:DescribeLaunchConfigurations",
      "autoscaling:DescribeScalingActivities",
      "autoscaling:DescribeTags",
      "ec2:DescribeInstanceTypes",
      "ec2:DescribeLaunchTemplateVersions"
    ]

    resources = ["*"]
  }

  statement {
    sid        = "clusterAutoscalerOwn"
    effect     = "Allow"

    actions    = [
      "autoscaling:SetDesiredCapacity",
      "autoscaling:TerminateInstanceInAutoScalingGroup",
      "ec2:DescribeImages",
      "ec2:GetInstanceTypesFromInstanceRequirements",
      "eks:DescribeNodegroup"
    ]

    resources  = ["*"]

  }
}

resource "aws_iam_policy" "cluster_autoscaler_policy" {
  name_prefix = "ClusterAutoscalerPolicy"
  description = "EKS cluster-autoscaler policy for cluster ${local.cluster_name}"
  policy      = data.aws_iam_policy_document.cluster_autoscaler.json
}
