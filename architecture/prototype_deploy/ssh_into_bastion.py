import os
import subprocess

import boto3


BASTION_NAME = "nick-bastion"

"""
dict_keys(['AmiLaunchIndex', 'ImageId', 'InstanceId', 'InstanceType', 'KeyName',
'LaunchTime', 'Monitoring', 'Placement', 'PrivateDnsName', 'PrivateIpAddress',
'ProductCodes', 'PublicDnsName', 'PublicIpAddress', 'State',
'StateTransitionReason', 'SubnetId', 'VpcId', 'Architecture',
'BlockDeviceMappings', 'ClientToken', 'EbsOptimized', 'EnaSupport',
'Hypervisor', 'IamInstanceProfile', 'NetworkInterfaces', 'RootDeviceName',
'RootDeviceType', 'SecurityGroups', 'SourceDestCheck', 'Tags', 'VirtualizationType',
 'CpuOptions', 'CapacityReservationSpecification', 'HibernationOptions'])

Only tag with the name is [('Tags', [{'Key': 'Name', 'Value': 'nick-bastion'}])]!!!
AWS is kind of terrible sometimes.
"""

def get_dict_of_running_instance(instance_name: str):
    ec2_client = boto3.client('ec2')
    raw_instance_list = ec2_client.describe_instances(
            Filters=[{
            'Name': 'instance-state-name',
            'Values' : ['running']
        },{
            'Name' : 'tag:Name',
            'Values' : [BASTION_NAME]
        }]
    )
    bastion_instance_list = bastion['Reservations'][0]['Instances']
    if not bastion_instance_list:
        raise ValueError("Bastion is not running!")
    else:
        return bastion_instance_list[0]

def get_public_ip_from_ec2_instance_dict(instance_dict:dict):
    return instance_dict['PublicIpAddress']

def run_commands_on_bastion(command_str: str):

