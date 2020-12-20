import faust

class MedicareKeyModel(faust.Record, serializer='avro_medicare_key'):
    npi: int

class MedicareValueModel(faust.Record, serializer='avro_medicare_value'):
    npi: int
    nppes_provider_last_org_name: str
    nppes_provider_first_name: str
    nppes_provider_mi: str
    nppes_credentials: str
    nppes_provider_gender: str
    nppes_entity_code: str
    nppes_provider_street1: str
    nppes_provider_street2: str
    nppes_provider_city: str
    nppes_provider_zip: str
    nppes_provider_state: str
    nppes_provider_country: str
    provider_type: str
    medicare_participation_indicator: str
    place_of_service: str
    hcpcs_code: str
    hcpcs_description: str
    hcpcs_drug_indicator: str
    line_srvc_cnt: float
    bene_unique_cnt: int
    bene_day_srvc_cnt: int
    average_medicare_allowed_amt: float
    average_submitted_chrg_amt: float
    average_medicare_payment_amt: float
    average_medicare_standard_amt: float
