import logging

logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format="%(asctime)s -%(levelname)s- %(message)s"
)

logging.info("Pipeline Started")
logging.info("Extraction begin")