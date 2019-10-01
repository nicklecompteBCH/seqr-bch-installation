from enum import Enum
from typing import Union

class AWSServiceType(Enum):
    EC2 = 1,
    S3 = 2,
    EMR = 3,
    IAM = 4

class AWSRegion(Enum):
    USEast1 = 1
    USEast2 = 2

class AWSResourceDetail(Enum):
    User = 1,
    Instance = 2

class ARN:
    """
arn:partition:service:region:account-id:resource-id
arn:partition:service:region:account-id:resource-type/resource-id
arn:partition:service:region:account-id:resource-type:resource-id
    """

    def __init__(
        self,
        service: AWSServiceType,
        region: AWSRegion,
        account_id: str,
        resource_id: str,
        resource_type: Union[AWSServiceType,AWSResourceDetail,None] = None
        partition: str = 'aws',
    )

    def __str__(self)