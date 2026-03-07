#!/bin/bash
set -euo pipefail

echo "ECS_CLUSTER=openjdk-mail-cluster" >> /etc/ecs/ecs.config

sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" >> /etc/sysctl.conf
