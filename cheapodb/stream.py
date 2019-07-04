import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from itertools import islice, chain
from typing import Union, Generator, List

from cheapodb import Database

log = logging.getLogger(__name__)


class Stream(object):
    def __init__(self, db: Database, name: str, prefix: str):
        self.db = db
        self.name = name
        self.prefix = f'{prefix}/{self.name}/'

    def initialize(self, error_output_prefix: str = None, buffering: dict = None,
                   compression: str = 'UNCOMPRESSED') -> dict:
        if self.exists:
            return self.describe
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

    def delete(self) -> dict:
        if self.exists:
            response = self.db.firehose.delete_delivery_stream(
                DeliveryStreamName=self.name
            )
            return response
        else:
            log.warning(f'Delivery stream {self.name} does not exist')

    @property
    def describe(self) -> dict:
        return self.db.firehose.describe_delivery_stream(
            DeliveryStreamName=self.name
        )

    @property
    def exists(self) -> bool:
        try:
            s = self.describe
            return True
        except self.db.firehose.exceptions.ResourceNotFoundException:
            return False

    @staticmethod
    def _chunks(iterable: Union[Generator, list], size: int):
        iterator = iter(iterable)
        for first in iterator:
            yield chain([first], islice(iterator, size - 1))

    def from_records(self, records: Union[Generator, List[dict]], threads: int = 4) -> None:
        """
        Ingest from a generator or list of dicts

        :param records: a generator or list of dicts
        :param threads: number of threads for batch putting
        :return:
        """
        with ThreadPoolExecutor(max_workers=threads) as executor:
            executor.submit(
                self.db.firehose.put_record_batch,
                DeliveryStreamName=self.name,
                Records=[{'Data': f'{json.dumps(x)}\n'.encode()} for x in self._chunks(records, size=500)]
            )
        return
