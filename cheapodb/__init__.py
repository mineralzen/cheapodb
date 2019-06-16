import logging
from cheapodb.database import Database
from cheapodb.table import Table

__all__ = ['Database', 'Table', 'logger']

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)-8s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S'
)

logger = logging.getLogger(__name__)
