import logging

import config
from src.models import LexDanNet

logging.basicConfig(level=config.loglevel)
logger = logging.getLogger(__name__)

# Usage
url_zip_handler = LexDanNet()
url_zip_handler.start()
