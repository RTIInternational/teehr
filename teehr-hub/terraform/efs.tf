resource "aws_efs_file_system" "datadir" {
  tags = {
    Name = "hub-datadir"
  }
  lifecycle {
    # Additional safeguard against deleting the EFS
    # as this causes irreversible data loss!
    prevent_destroy = true
  }
}

resource "aws_efs_mount_target" "datadir" {
  file_system_id  = aws_efs_file_system.datadir.id
  subnet_id       = module.vpc.private_subnets[0]
  security_groups = [ aws_security_group.efs-sg.id ]
}

output "nfs_server_dns" {
  value = aws_efs_file_system.datadir.dns_name
}

resource "aws_security_group" "efs-sg" {
  name_prefix = "${local.cluster_name}-efs-sg"
  description = "Allow EFS access"
  vpc_id      = module.vpc.vpc_id

  # egress {
  #   from_port        = 0
  #   to_port          = 0
  #   protocol         = "-1"
  #   cidr_blocks      = ["0.0.0.0/0"]
  #   ipv6_cidr_blocks = ["::/0"]
  # }

  ingress {
    description = "EFS mount target"
    from_port   = 2049
    to_port     = 2049
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${local.cluster_name}-efs-sg" })
}
