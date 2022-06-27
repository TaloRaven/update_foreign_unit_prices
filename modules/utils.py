import logging

# Basic config to create logs operation from the action taken by script
logging.basicConfig(level=logging.INFO,
                    filename="logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')

logger = logging.getLogger(__name__)
