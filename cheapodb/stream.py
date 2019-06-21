import json
import time
from itertools import islice, chain
from typing import Union, Generator, List

import boto3
from joblib import Parallel, delayed


class Stream(object):
    def __init__(self, name: str):
        self.name = name
        self.firehose = boto3.client('firehose')

    def initialize(self, config):
        if self.exists:
            return
        config = json.loads(config)
        response = self.firehose.create_delivery_stream(**config)
        while True:
            if self.exists:
                break
            time.sleep(10)

        return response

    @property
    def describe(self):
        return self.firehose.describe_delivery_stream(
            DeliveryStreamName=self.name
        )

    @property
    def exists(self) -> bool:
        try:
            self.describe()
            return True
        except self.firehose.exceptions.ResourceNotFoundException:
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
        Parallel(n_jobs=threads, prefer='threads')(delayed(self.firehose.put_record_batch)(
            DeliveryStreamName=self.name,
            Records=[{'Data': json.dumps(x).encode()} for x in chunk]
        ) for chunk in self._chunks(records, size=500))

        return
