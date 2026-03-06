#!/bin/bash

echo "ECS_CLUSTER=openjdk-mail-cluster" >> /etc/ecs/ecs.config

sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" >> /etc/sysctl.conf

INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)
AZ=$(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone)

VOLUME_ID=$(aws ec2 describe-volumes \
  --region "$REGION" \
  --filters "Name=tag:Name,Values=openjdk-mail-es-data" "Name=availability-zone,Values=$AZ" \
  --query "Volumes[0].VolumeId" --output text)

aws ec2 attach-volume \
  --region "$REGION" \
  --volume-id "$VOLUME_ID" \
  --instance-id "$INSTANCE_ID" \
  --device /dev/xvdf

# Wait for device to appear (may show as /dev/nvme1n1 on nitro instances)
for i in $(seq 1 30); do
  DATA_DEV=$(lsblk -o NAME,SIZE,TYPE -d | awk '/disk/ && !/nvme0/ && !/xvda/ {print "/dev/"$1; exit}')
  [ -n "$DATA_DEV" ] && break
  sleep 2
done

mkdir -p /data/elasticsearch
blkid "$DATA_DEV" || mkfs.ext4 "$DATA_DEV"
mount "$DATA_DEV" /data/elasticsearch
chown 1000:1000 /data/elasticsearch
