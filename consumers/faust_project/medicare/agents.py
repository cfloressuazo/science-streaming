import logging
from faust_project.app import app
from faust_project.medicare.models import MedicareValueModel, ProceduresAndProviders

logger = logging.getLogger(__name__)

medicare_topic = app.topic('medicare', partitions=None, value_type=MedicareValueModel)

page_views = app.Table('page_views', default=int)


@app.agent(medicare_topic)
async def count_page_views(procedures):
    async for procedure in procedures.group_by(MedicareValueModel.hcpcs_code):
        page_views[procedure.hcpcs_code] += 1


@app.agent(medicare_topic)
async def process_amount(records):
    async for record in records:
        if record.average_medicare_payment_amt > record.average_medicare_allowed_amt:
            logger.info(f"""
            ******** REAL-TIME TRACK OF EXCESS OF MEDICARE ALLOWED PAYMENT BY PROVIDER TYPE ********
            Provider type: {record.provider_type}
            City: {record.nppes_provider_city}
            Average Medicare Allowed Amount: {record.average_medicare_allowed_amt}
            Average Medicare Payment Amount: {record.average_medicare_payment_amt}
            """)
