import os

from pyathena import connect

from cheapodb import logger
from cheapodb.schema import Schema


class Table(object):
    def __init__(self, name: str, schema: Schema, create_prefix=True):
        self.name = name
        self.schema = schema
        self.create_prefix = create_prefix

    def upload(self, f) -> None:
        if self.create_prefix:
            target = os.path.join(self.schema.name, self.name, self.name)
        else:
            target = os.path.join(self.schema.name, self.name)
        logger.info(f'Uploading file {f} to {target}')
        self.schema.db.bucket.upload_file(f, target)
        return

    def query(self, sql: str):
        cursor = connect(
            s3_staging_dir=self.schema.db.staging_subdir,
            region_name=self.schema.db.region
        ).cursor()
        cursor.execute(sql)
        for row in cursor:
            yield row
