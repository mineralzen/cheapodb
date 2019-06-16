import os

from cheapodb import logger
from cheapodb.database import Database


class Table(object):
    def __init__(self, name: str, db: Database, prefix: str):
        self.name = name
        self.db = db
        self.prefix = prefix

    def upload(self, f) -> None:
        target = os.path.join(self.prefix, self.name, self.name)
        logger.info(f'Uploading file {f} to {target}')
        self.db.bucket.upload_file(f, target)
        return
