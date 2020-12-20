import logging

from faust_project.app import app
from faust_project.codecs.avro import avro_medicare_value_serializer, avro_medicare_key_serializer
from faust_project.medicare.models import MedicareValueModel, MedicareKeyModel

medicare_topic = app.topic('medicare', partitions=1, value_type=MedicareValueModel)

logger = logging.getLogger(__name__)


@app.agent(medicare_topic)
async def medicare(medicare):
    async for record in medicare:
        logger.info("Event received in topic org.science.medicare")
        logger.info(f"Provider type: {record.provider_type}")
