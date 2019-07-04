import os
import time
import logging
from datetime import datetime
from typing import Union

from pyathena import connect
from pyathena.cursor import Cursor

from cheapodb.utils import CheapoDBException, create_cheapodb_role, normalize_table_name, create_session

log = logging.getLogger(__name__)


class Database(object):
    """
    A database object represents the components that make up a database in AWS Glue.

    Provides methods for Glue, Athena and S3
    """
    def __init__(self, name: str, description: str = None, auto_create=False, results_prefix='results/',
                 create_iam_role=False, iam_role_arn=None, **kwargs):
        """
        Initialize a database object

        :param name: name of the database
        :param description: optional description of the database (shown in Glue catalog)
        :param auto_create: if True, automatically create the database if it doesn't already exist
        :param results_prefix: optional S3 prefix to store query results
        :param create_iam_role: True to automatically create an IAM role with necessary permissions. One of
        create_iam_role or iam_role_arn is required
        :param iam_role_arn: an optional ARN of an existing IAM role. One of create_iam_role or iam_role_arn is
        required
        :param kwargs:
        """
        self.name = name
        self.description = description
        self.auto_create = auto_create
        self.results_prefix = results_prefix
        self.iam_role_arn = iam_role_arn

        self.session = create_session(
            region_name=kwargs.pop('aws_default_region', os.getenv('AWS_DEFAULT_REGION')),
            aws_access_key_id=kwargs.pop('aws_access_key_id', os.getenv('AWS_ACCESS_KEY_ID')),
            aws_secret_access_key=kwargs.pop('aws_secret_access_key', os.getenv('AWS_SECRET_ACCESS_KEY'))
        )
        self.s3 = self.session.client('s3')
        self.bucket = self.session.resource('s3').Bucket(self.name)
        self.glue = self.session.client('glue')
        self.firehose = self.session.client('firehose')
        self.iam = self.session.client('iam')

        if create_iam_role and not self.iam_role_arn:
            self.iam_role_arn = create_cheapodb_role(
                name=f'{self.name}-CheapoDBExecutionRole',
                client=self.iam,
                bucket=self.name,
                account=self.session.client('sts').get_caller_identity()['Account']
            )

        elif not create_iam_role and not self.iam_role_arn:
            msg = f'No IAM role ARN provided and create_iam_role was False. ' \
                  f'Provide either an existing role ARN or set create_iam_role to True.'
            raise CheapoDBException(msg)

        if self.auto_create:
            self.create()

    def create(self):
        """
        Create the database, which consists of an S3 bucket and Glue database object

        :return:
        """
        log.info(f'Creating database {self.name} in {self.session.region_name}')

        bucket_params = dict(
            CreateBucketConfiguration=dict(
                LocationConstraint=self.session.region_name
            )
        )
        response = self.bucket.create(**bucket_params)
        log.debug(response)

        db_params = dict(Name=self.name)
        if self.description:
            db_params['Description'] = self.description
        self.glue.create_database(
            DatabaseInput=db_params
        )
        log.debug(response)

        return

    @property
    def tables(self) -> list:
        """
        Get a listing of all tables that exist in the database

        :return:
        """
        tables = list()
        while True:
            payload = dict(
                DatabaseName=self.name,
                MaxResults=100
            )
            response = self.glue.get_tables(**payload)
            log.debug(response)

            if not response['TableList']:
                break
            if response['NextToken']:
                payload['NextToken'] = response['NextToken']
            tables += response['TableList']

        return tables

    def query(self, sql: str, results_path: str = None) -> dict:
        """
        Execute a query

        :param sql: the Athena-compliant SQL to execute
        :param results_path: optional S3 path to write results. Defaults to current DB bucket and
        instance results_prefix
        :return:
        """
        if not results_path:
            results_path = f's3://{self.name}/{self.results_prefix}'

        cursor = self.export(sql, results_path)
        columns = [column[0] for column in cursor.description]
        log.info(cursor.description)
        for row in cursor:
            yield dict(zip(columns, row))

    def export(self, sql: str, results_path: str) -> Cursor:
        """
        Execute a query and return the cursor.

        Useful for building the athena query results as a file export to a destination bucket/prefix

        :param sql: the Athena-compliant SQL to execute
        :param results_path: required S3 path to write export
        :return: a pyathena cursor object
        """
        cursor = connect(
            s3_staging_dir=results_path,
            region_name=self.session.region_name,
            role_arn=self.iam_role_arn
        ).cursor()
        cursor.execute(sql)
        return cursor

    def create_crawler(self, name, schedule: str = None, table_prefix: str = None, target: str = None,
                       update_behavior='UPDATE_IN_DATABASE', delete_behavior='DELETE_FROM_DATABASE',
                       description: str = None) -> str:
        """
        Create a new Glue crawler.

        Either loads a crawler or creates it if it doesn't exist. The provided name of the crawler
        corresponds to the prefix of the DB bucket it will target.

        :param name: the DB bucket prefix to crawl
        :param schedule: an optional schedule in cron syntax to run the crawler
        :param table_prefix: an optional prefix to apply to the created tables
        :param target: an optional target path to crawl
        :param update_behavior: how the crawler should handle schema updates
        :param delete_behavior: how the crawler should handle deletions
        :param description: Optional crawler description
        :return: the name of the created crawler
        """
        log.info(f'Creating crawler {name}')
        if not target:
            target = f'{self.name}/{name}/'
        if not description:
            description = f'Crawler created by CheapoDB on {datetime.now():%Y-%m-%d %H:%M:%S}'
        try:
            payload = dict(
                Name=name,
                Role=self.iam_role_arn,
                DatabaseName=self.name,
                Description=description,
                Targets=dict(
                    S3Targets=[
                        {
                            'Path': target
                        }
                    ]
                ),
                TablePrefix=f'{normalize_table_name(table_prefix)}_',
                SchemaChangePolicy=dict(
                    UpdateBehavior=update_behavior,
                    DeleteBehavior=delete_behavior
                )
            )
            if schedule:
                payload['Schedule'] = schedule
            self.glue.create_crawler(**payload)
            log.info(f'Created crawler {name}')
            return self.glue.get_crawler(Name=name)['Crawler']['Name']
        except self.glue.exceptions.AlreadyExistsException:
            log.warning(f'Crawler {name} already exists')
            return self.glue.get_crawler(Name=name)['Crawler']['Name']

    def delete_crawler(self, name: str) -> None:
        """
        Delete a Glue crawler

        :param name: the name of the crawler to delete
        :return:
        """
        log.info(f'Deleting crawler {name}')
        self.glue.delete_crawler(
            Name=name
        )
        return

    def update_tables(self, crawler, wait: Union[bool, int] = 60) -> None:
        """
        Run the provided crawler to update the tables in the prefix.
        Optionally wait for completion, which is the default.

        :param crawler: the name of the Glue Crawler to execute
        :param wait: optionally wait for the crawler to complete by providing the number of seconds to wait between
        polling requests. False if this method should return immediately after starting the crawler.
        :return:
        """
        log.info(f'Updating tables with crawler {crawler}')
        response = self.glue.start_crawler(
            Name=crawler
        )
        log.debug(response)
        if wait:
            log.info(f'Waiting for table update to complete...')
            while True:
                response = self.glue.get_crawler(Name=crawler)
                elapsed = response['Crawler']['CrawlElapsedTime'] / 1000
                if response['Crawler']['State'] == 'RUNNING':
                    log.debug(response)
                    log.info(f'Crawler in RUNNING state. Elapsed time: {elapsed} secs')
                    time.sleep(wait)
                    continue
                elif response['Crawler']['State'] == 'STOPPING':
                    log.debug(response)
                    log.info(f'Crawler in STOPPING state')
                    time.sleep(wait)
                    continue
                else:
                    status = response['Crawler']['LastCrawl']['Status']
                    log.debug(response)
                    log.info(f'Crawler in READY state. Table update {status}')
                    break

        return
