from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import FaustSerializer

SCHEMA_REGISTRY_URL = "http://localhost:8081"

# from faust_project.users.models import AdvanceUserModel

# Initialize Schema Registry Client
client = SchemaRegistryClient(url=SCHEMA_REGISTRY_URL)

avro_medicare_key_schema = schema.AvroSchema({
    "type": "record",
    "name": "providerKey",
    "namespace": "org.science.medicare",
    "fields": [
        {
            "name": "npi",
            "type": "int"
        }
    ],
    "connect.name": "org.science.medicare.providerKey"
})

avro_medicare_value_schema = schema.AvroSchema(
    """
    {"type":"record","name":"provider","namespace":"org.science.medicare","fields":[{"name":"npi","type":"int"},
    {"name":"nppes_provider_last_org_name","type":["null","string"],"default":"null"},
    {"name":"nppes_provider_first_name","type":["null","string"],"default":"null"},{"name":"nppes_provider_mi",
    "type":["null","string"],"default":"null"},{"name":"nppes_credentials","type":["null","string"],
    "default":"null"},{"name":"nppes_provider_gender","type":["null","string"],"default":"null"},
    {"name":"nppes_entity_code","type":["null","string"],"default":"null"},{"name":"nppes_provider_street1",
    "type":["null","string"],"default":"null"},{"name":"nppes_provider_street2","type":["null","string"],
    "default":"null"},{"name":"nppes_provider_city","type":["null","string"],"default":"null"},
    {"name":"nppes_provider_zip","type":["null","string"],"default":"null"},{"name":"nppes_provider_state",
    "type":["null","string"],"default":"null"},{"name":"nppes_provider_country","type":["null","string"],
    "default":"null"},{"name":"provider_type","type":["null","string"],"default":"null"},
    {"name":"medicare_participation_indicator","type":["null","string"],"default":"null"},{"name":"place_of_service",
    "type":["null","string"],"default":"null"},{"name":"hcpcs_code","type":["null","string"],"default":"null"},
    {"name":"hcpcs_description","type":["null","string"],"default":"null"},{"name":"hcpcs_drug_indicator",
    "type":["null","string"],"default":"null"},{"name":"line_srvc_cnt","type":["null","float"],"default":"null"},
    {"name":"bene_unique_cnt","type":["null","int"],"default":"null"},{"name":"bene_day_srvc_cnt","type":["null",
    "int"],"default":"null"},{"name":"average_medicare_allowed_amt","type":["null","float"],"default":"null"},
    {"name":"average_submitted_chrg_amt","type":["null","float"],"default":"null"},
    {"name":"average_medicare_payment_amt","type":["null","float"],"default":"null"},
    {"name":"average_medicare_standard_amt","type":["null","float"],"default":"null"}],
    "connect.name":"org.science.medicare.provider"}
    """)

avro_medicare_key_serializer = FaustSerializer(client, "medicare-key", avro_medicare_key_schema)
avro_medicare_value_serializer = FaustSerializer(client, "test-value", avro_medicare_value_schema)


# example of how to use it with dataclasses-avroschema
# avro_advance_user_serializer = FaustSerializer(
#     client, "advance_users", AdvanceUserModel.avro_schema())

def avro_medicare_key_codec():
    return avro_medicare_key_serializer


def avro_medicare_value_codec():
    return avro_medicare_value_serializer

# def avro_advance_user_codec():
#     return avro_advance_user_serializer
