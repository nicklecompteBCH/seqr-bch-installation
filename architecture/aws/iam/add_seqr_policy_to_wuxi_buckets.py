import boto3

import json
from architecture.aws.iam.service_roles import (
    EMR_ROLE,
    SEQR_ROLE,
)
from architecture.aws.iam.private_buckets import BUCKETS

def generate_read_only_s3_policy_for_iam_role(bucket,iam_principals):
    return {
        'Effect' : 'Allow',
        'Principal' : {
            'AWS': principals
        },
        'Action' : ['s3:GetObject','s3:ListObject']
        'Resource' : bucket
    }

def

