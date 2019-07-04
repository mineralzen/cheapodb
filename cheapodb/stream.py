import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from itertools import islice, chain
from typing import Union, Generator, List

from cheapodb import Database

log = logging.getLogger(__name__)


class Stream(object):
    """
    A Stream object represents the a Firehose delivery stream.
    """
    def __init__(self, db: Database, name: str, prefix: str):
        """
        Create a Stream instance

        :param db: the Database associated with the delivery stream
        :param name: the name of the delivery stream
        :param prefix: a prefix for the delivery stream. The stream name will be added after the prefix name,
        resulting in a prefix like prefix/name/
        """
        self.db = db
        self.name = name
        self.prefix = f'{prefix}/{self.name}/'

    def initialize(self, error_output_prefix: str = None, buffering: dict = None,
                   compression: str = 'UNCOMPRESSED') -> dict:
        """
        Initialize a delivery stream

        :param error_output_prefix: optional S3 prefix for persisting error output
        :param buffering: optional buffering configuration. If not provided, uses the service defaults
        :param compression: optional compression, see boto3 documentation for valid values
        :return: dict describing the delivery stream
        """
        if self.exists:
            return self.describe()
        if not buffering:
            buffering = dict(
                SizeInMBs=5,
                IntervalInSeconds=300
            )
        s3config = dict(
            RoleARN=self.db.iam_role_arn,
            BucketARN=f'arn:aws:s3:::{self.db.bucket.name}',
            Prefix=self.prefix,
            BufferingHints=buffering,
            CompressionFormat=compression
        )
        if error_output_prefix:
            s3config['ErrorOutputPrefix'] = error_output_prefix

        config = dict(
            DeliveryStreamName=self.name,
            DeliveryStreamType='DirectPut',
            ExtendedS3DestinationConfiguration=s3config
        )
        response = self.db.firehose.create_delivery_stream(**config)
        while True:
            if self.exists:
                break
            time.sleep(10)

        return response

    def delete(self) -> bool:
        """
        Delete a delivery stream

        :return: bool
        """
        if self.exists:
            self.db.firehose.delete_delivery_stream(
                DeliveryStreamName=self.name
            )
            return True
        else:
            log.warning(f'Delivery stream {self.name} does not exist')
            return False

    @property
    def exists(self) -> bool:
        """
        Return True if table exists in the database, false otherwise

        :return: bool
        """
        try:
            self.describe()
            return True
        except self.db.firehose.exceptions.ResourceNotFoundException:
            return False

    def describe(self) -> dict:
        """
        Describe an existing delivery stream

        :return: dict
        """
        return self.db.firehose.describe_delivery_stream(
            DeliveryStreamName=self.name
        )

    @staticmethod
    def _chunks(iterable: Union[Generator, list], size: int):
        """
        Chunk up iterable, useful for yielding discrete chunks of a generator without prewalking it

        https://stackoverflow.com/a/24527424/3479672

        :param iterable:
        :param size:
        :return:
        """
        iterator = iter(iterable)
        for first in iterator:
            if not first:
                break
            yield chain([first], islice(iterator, size - 1))

    def from_records(self, records: Union[Generator, List[dict]], threads: int = 4) -> None:
        """
        Ingest from a generator or list of dicts

        :param records: a generator or list of dicts
        :param threads: number of threads for batch putting
        :return:
        """
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for i, chunk in enumerate(self._chunks(records, size=500), start=1):
                executor.submit(
                    self.db.firehose.put_record_batch,
                    DeliveryStreamName=self.name,
                    Records=[{'Data': f'{json.dumps(record)}\n'.encode()} for record in chunk]
                )
                log.debug(f'Processed {i} chunks')
        return
