import logging

import config
from src.models import LexDanNet

logging.basicConfig(level=config.loglevel)
logger = logging.getLogger(__name__)

# Usage
url_zip_handler = LexDanNet(
    zip_file_path="https://repository.clarin.dk/repository/xmlui/bitstream/handle/20.500.12115/25/DanNet-2.2_owl.zip"
)
url_zip_handler.start()
