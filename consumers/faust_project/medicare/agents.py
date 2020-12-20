import logging
from faust_project.app import app
from faust_project.medicare.models import MedicareValueModel

medicare_topic = app.topic('medicare', partitions=1, value_type=MedicareValueModel)

logger = logging.getLogger(__name__)


@app.agent(medicare_topic)
async def medicare(records):
    async for record in records:
        logger.info(f"""
        ******** REAL-TIME TRACK OF PROVIDER TYPE INFORMATION ********
        Provider type: {record.provider_type}
        Number of Services: {record.line_srvc_cnt}
        Number of Medicare Beneficiaries: {record.bene_unique_cnt}
        Percentage of Medicare Beneficiaries per service: {round((record.bene_unique_cnt*100)/record.line_srvc_cnt, 2)}
        Number of Distinct Medicare Beneficiary/Per Day Services: {record.bene_day_srvc_cnt}
        Average Medicare Allowed Amount: {record.average_medicare_allowed_amt}
        Average Submitted Charged Amount: {record.average_submitted_chrg_amt}
        Average Medicare Payment Amount: {record.average_medicare_payment_amt}
        """)


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
