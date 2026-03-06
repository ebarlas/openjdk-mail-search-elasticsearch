# OpenJDK Mail Search — AWS Infrastructure

All commands use `--region us-west-1`.

## 1. Create EBS data volume

```
aws ec2 create-volume \
  --profile personal \
  --region us-west-1 \
  --availability-zone us-west-1a \
  --size 50 \
  --volume-type gp3 \
  --tag-specifications 'ResourceType=volume,Tags=[{Key=Name,Value=openjdk-mail-es-data}]'
```

## 2. Create ECS cluster

```
aws ecs create-cluster \
  --profile personal \
  --region us-west-1 \
  --cluster-name openjdk-mail-cluster
```

## 3. Create key pair

```
aws ec2 create-key-pair \
  --profile personal \
  --region us-west-1 \
  --key-name openjdk-mail-key \
  --query 'KeyMaterial' \
  --output text > openjdk-mail-key.pem
chmod 400 openjdk-mail-key.pem
```

## 4. Create security group

```
aws ec2 create-security-group \
  --profile personal \
  --region us-west-1 \
  --group-name openjdk-mail-sg \
  --description "OpenJDK Mail Search - SSH only"
```

```
aws ec2 authorize-security-group-ingress \
  --profile personal \
  --region us-west-1 \
  --group-name openjdk-mail-sg \
  --protocol tcp --port 22 --cidr <your-ip>/32
```

## 5. Create launch template

```
aws ec2 create-launch-template \
  --profile personal \
  --region us-west-1 \
  --launch-template-name openjdk-mail-template \
  --version-description "Initial version" \
  --launch-template-data file://launch-template.json
```

## 6. Launch EC2 instance

```
aws ec2 run-instances \
  --profile personal \
  --region us-west-1 \
  --launch-template LaunchTemplateName=openjdk-mail-template \
  --key-name openjdk-mail-key \
  --security-groups openjdk-mail-sg
```

## 7. Register ECS task definition

```
aws ecs register-task-definition \
  --profile personal \
  --region us-west-1 \
  --cli-input-json file://openjdk-mail-es-task.json
```

## 8. Create ECS service

```
aws ecs create-service \
  --profile personal \
  --region us-west-1 \
  --cluster openjdk-mail-cluster \
  --service-name openjdk-mail-es-service \
  --task-definition openjdk-mail-es-task \
  --desired-count 1 \
  --launch-type EC2 \
  --deployment-configuration minimumHealthyPercent=0,maximumPercent=100
```

## 9. Verify

SSH tunnel into the instance and check ES:

```
ssh -L 9200:localhost:9200 -i openjdk-mail-key.pem ec2-user@<instance-ip>
```

```
curl http://localhost:9200/_cluster/health?pretty
```

## Updating the ECS service

```
aws ecs update-service \
  --profile personal \
  --region us-west-1 \
  --cluster openjdk-mail-cluster \
  --service openjdk-mail-es-service \
  --task-definition openjdk-mail-es-task \
  --force-new-deployment \
  --deployment-configuration minimumHealthyPercent=0,maximumPercent=100
```
