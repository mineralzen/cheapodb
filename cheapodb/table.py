import os
import logging
from typing import List

from dask.dataframe import DataFrame

from cheapodb.database import Database

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)-8s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S'
)

log = logging.getLogger(__name__)


class Table(object):
    """
    A Table object represents the components that make up a table in AWS Glue.

    Provides methods for Glue, Athena and S3
    """
    def __init__(self, name: str, db: Database, prefix: str):
        """
        Create a Table instance

        :param name: the name of the Table
        :param db: the Database that will contain the Table
        :param prefix: the prefix in the Database where the Table data will reside
        """
        self.name = name
        self.db = db
        self.prefix = prefix

    @property
    def columns(self) -> List[dict]:
        """
        Get a list of table columns

        :return: list of dicts describing the table columns
        """
        return self.describe()['Table']['StorageDescriptor']['Columns']

    def get_versions(self) -> List[dict]:
        """
        Get a list of table versions

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_table_versions

        :return:
        """
        versions = list()
        while True:
            payload = dict(
                DatabaseName=self.db.name,
                TableName=self.name,
                MaxResults=100
            )
            response = self.db.glue.get_table_versions(**payload)
            if not response['TableVersions']:
                break
            if response['NextToken']:
                payload['NextToken'] = response['NextToken']
            versions += response['TableVersions']
        return versions

    def describe(self) -> dict:
        """
        Get a reference to the table with metadata.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_table

        :return: the Glue get_table response
        """
        return self.db.glue.get_table(
            DatabaseName=self.db.name,
            Name=self.name
        )

    def upload(self, f) -> None:
        """
        Upload the table data file(s) to the database bucket and prefix

        :param f: path to the file
        :return:
        """
        target = os.path.join(self.prefix, self.name, self.name)
        log.info(f'Uploading file {f} to {target}')
        self.db.bucket.upload_file(f, target)
        return

    def from_dataframe(self, df: DataFrame, **kwargs) -> None:
        """
        Load a Dask DataFrame as parquet to the database bucket and prefix

        :param df: the Dask DataFrame object to load as parquet
        :param kwargs: additional keyword arguments provided to Dask DataFrame.to_parquet
        :return:
        """
        target = f's3://{os.path.join(self.db.name, self.prefix, self.name)}'
        df.to_parquet(path=target, engine='fastparquet', **kwargs)
        return
