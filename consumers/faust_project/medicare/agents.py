import logging
from faust_project.app import app
from faust_project.medicare.models import MedicareValueModel, ProceduresAndProviders

logger = logging.getLogger(__name__)

medicare_topic = app.topic('medicare', partitions=None, value_type=MedicareValueModel)

procedure_by_provider_ny_topic = app.topic(
    'procedure_by_provider_ny',
    key_type=str,
    value_type=ProceduresAndProviders
)

procedure_by_provider_table = app.Table(
    "procedure_by_provider_ny_table",
    default=ProceduresAndProviders,
    changelog_topic=procedure_by_provider_ny_topic,
)

# #
# procedure_by_provider_ny_to_total = app.Table(
#     'procedure_by_provider_ny_to_total', default=int).tumbling(10.0, expires=10.0)
#
# procedure_by_provider_ny_stream = app.topic('procedure_by_provider_ny', value_type=ProceduresAndProviders).stream()
# procedures_by_providers_ny = procedure_by_provider_ny_stream.group_by(ProceduresAndProviders.hcpcs_code)
#
#
# @app.agent(procedure_by_provider_ny_topic)
# async def process_withdrawal(procedures):
#     async for procedure in procedures.group_by(ProceduresAndProviders.hcpcs_code):
#         procedure_by_provider_ny_to_total[procedure.hcpcs_code] += 1


# @app.agent(medicare_topic)
# async def medicare(records):
#     async for record in records:
#         logger.info(f"""
#         ******** REAL-TIME TRACK OF PROVIDER TYPE INFORMATION ********
#         Provider type: {record.provider_type}
#         Number of Services: {record.line_srvc_cnt}
#         Number of Medicare Beneficiaries: {record.bene_unique_cnt}
#         Percentage of Medicare Beneficiaries per service: {round((record.bene_unique_cnt * 100) / record.line_srvc_cnt, 2)}
#         Number of Distinct Medicare Beneficiary/Per Day Services: {record.bene_day_srvc_cnt}
#         Average Medicare Allowed Amount: {record.average_medicare_allowed_amt}
#         Average Submitted Charged Amount: {record.average_submitted_chrg_amt}
#         Average Medicare Payment Amount: {record.average_medicare_payment_amt}
#         """)


@app.agent(medicare_topic)
async def medicare(records):
    async for record in records:
        if record.nppes_provider_state == 'NY':
            procedures_and_providers = ProceduresAndProviders(
                hcpcs_code=record.hcpcs_code,
                hcpcs_description=record.hcpcs_description,
                npi=record.npi,
                nppes_provider_city=record.nppes_provider_city,
                line_srvc_cnt=record.line_srvc_cnt,
                bene_unique_cnt=record.bene_unique_cnt,
                bene_day_srvc_cnt=record.bene_day_srvc_cnt,
                average_medicare_allowed_amt=record.average_medicare_allowed_amt,
                average_submitted_chrg_amt=record.average_submitted_chrg_amt,
                average_medicare_payment_amt=record.average_medicare_payment_amt,
                average_medicare_standard_amt=record.average_medicare_standard_amt,
            )

            await procedure_by_provider_ny_topic.send(key=record.hcpcs_code, value=procedures_and_providers)


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
