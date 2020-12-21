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
#
# provider_type_to_total = app.Table('provider_type_to_total', default=int, partitions=1)
# city_to_total = app.Table(
#     'city_to_total', default=int, partitions=1).tumbling(5.0, expires=5.0)
#
#
# @app.agent(medicare_topic)
# async def find_large_provider_type_medicare(records):
#     async for record in records:
#         provider_type_to_total[record.provider_type] += record.average_medicare_payment_amt
#
#
# @app.agent(medicare_topic)
# async def find_large_city_medicare(records):
#     async for record in records.group_by(MedicareValueModel.nppes_provider_city):
#         city_to_total[record.nppes_provider_city] += record.average_medicare_payment_amt
