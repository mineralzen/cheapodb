import os
import json
import time
import logging
from datetime import datetime
import boto3

log = logging.getLogger(__name__)


def create_session(**kwargs):
    return boto3.session.Session(
        region_name=kwargs.get('aws_default_region', os.getenv('AWS_DEFAULT_REGION')),
        aws_access_key_id=kwargs.get('aws_access_key_id', os.getenv('AWS_ACCESS_KEY_ID')),
        aws_secret_access_key=kwargs.get('aws_secret_access_key', os.getenv('AWS_SECRET_ACCESS_KEY'))
    )


def normalize_table_name(name):
    """Check if the table name is obviously invalid."""
    if not isinstance(name, str):
        raise ValueError()
    name = name.replace('-', '_').strip()
    if not len(name):
        raise ValueError(f'Invalid table name: {name}')
    return name


def create_iam_role(name: str, client, bucket: str, account: str) -> str:
    """
    Create an AWS IAM service role with the appropriate permissions for Glue and the database's S3 bucket.

    :param name: name of the AWS IAM role to create
    :param client: AWS IAM client
    :param bucket: AWS S3 bucket name
    :param account: AWS account ID
    :return:
    """
    try:
        response = client.create_role(
            RoleName=name,
            Path='/service-role/',
            Description=f'IAM role created by CheapoDB on {datetime.now():%Y-%m-%d %H:%M:%S}',
            AssumeRolePolicyDocument=json.dumps(dict(
                Version='2012-10-17',
                Statement=[
                    {
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': 'glue.amazonaws.com'
                        },
                        'Action': 'sts:AssumeRole'
                    },
                    {
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': 'firehose.amazonaws.com'
                        },
                        'Action': 'sts:AssumeRole',
                        'Condition': {
                            'StringEquals': {
                                'sts:ExternalId': account
                            }
                        }
                    }
                ]
            ))
        )
        log.debug(response)
        time.sleep(5)

        iam_role_arn = response['Role']['Arn']
        log.info(f'IAM Role ARN: {iam_role_arn}')

        response = client.attach_role_policy(
            RoleName=name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        )
        log.debug(response)

        response = client.put_role_policy(
            RoleName=name,
            PolicyName='CheapoDBRolePolicy',
            PolicyDocument=json.dumps(dict(
                Version='2012-10-17',
                Statement=[
                    {
                        'Effect': 'Allow',
                        'Action': [
                            's3:GetObject',
                            's3:PutObject'
                        ],
                        'Resource': [
                            f'arn:aws:s3:::{bucket}*'
                        ]
                    }
                ]
            ))
        )
        log.debug(response)
        time.sleep(5)
    except client.exceptions.EntityAlreadyExistsException:
        log.warning(f'Role exists for database: CheapoDBRole-{bucket}')
        iam_role_arn = client.get_role(
            RoleName=name
        )['Role']['Arn']

    return iam_role_arn
