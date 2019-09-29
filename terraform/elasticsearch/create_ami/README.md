# Elasticsearch and Kibana machine images

This Packer configuration will generate Ubuntu images with Elasticsearch, Kibana and other important tools for deploying and managing Elasticsearch clusters on the cloud.

The output of running Packer here would be two machine images, as below:

* elasticsearch node image, containing latest Elasticsearch installed (latest version 7.x) and configured with best-practices.
* kibana node image, based on the elasticsearch node image, and with Kibana (7.x, latest), nginx with basic proxy and authentication setip, and Kopf.

## On Amazon Web Services (AWS)

Using the AWS builder will create the two images and store them as AMIs.

As a convention the Packer builders will use a dedicated IAM roles, which you will need to have present.

```bash
aws iam create-role --role-name packer --assume-role-policy-document '{
  "Version": "2012-10-17",
  "Statement": {
    "Effect": "Allow",
    "Principal": {"Service": "ec2.amazonaws.com"},
    "Action": "sts:AssumeRole",
    "Sid": ""
  }
}'
```

Response will look something like this:

```json
{
    "Role": {
        "AssumeRolePolicyDocument": {
            "Version": "2012-10-17",
            "Statement": {
                "Action": "sts:AssumeRole",
                "Effect": "Allow",
                "Principal": {
                    "Service": "ec2.amazonaws.com"
                }
            }
        },
        "RoleId": "AROAJ7Q2L7NZJHZBB6JKY",
        "CreateDate": "2016-12-16T13:22:47.254Z",
        "RoleName": "packer",
        "Path": "/",
        "Arn": "arn:aws:iam::611111111117:role/packer"
    }
}
```

Follow up by execting the following

```bash
aws iam create-instance-profile --instance-profile-name packer
aws iam add-role-to-instance-profile  --instance-profile-name packer --role-name packer

```

By default, AWS builder will pick a subnet from the default VPC for running the builder instance. It is required for that subnet to have Public IPs auto-assignment enabled. Otherwise, packer won't be able to make a SSH connection to the instance and will hang on `Waiting for SSH to become available...`
If you don't want to enable public IPs auto-assignment on your default VPC subnets, you can explicitly set the subnet by setting `vpc_id` and `subnet_id` keys in *.packer.json files `amazon-ebs` builder definitions.


## Building

Building the AMIs is done using the following commands:

```bash
packer build -only=amazon-ebs -var-file=variables.json elasticsearch7-node.packer.json
packer build -only=amazon-ebs -var-file=variables.json kibana7-node.packer.json
```

Replace the `-only` parameter to `azure-arm` to build images for Azure instead of AWS.
