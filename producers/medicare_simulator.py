import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import pandas as pd
from pathlib import Path
from confluent_kafka import avro
from producer import Producer
from utils.logger import Logger

logger = Logger


class MedicareSimulator:
    """"""

    def __init__(self, base_filepath: str = None, base_data_filename: str = "data.csv"):
        self.base_filepath = base_filepath
        self.base_data_filename = base_data_filename
        self.raw_data = self._read_source_data()
        self.categorical_columns = self.get_categorical_columns()
        self.measure_columns = self.get_measure_columns()

        self.topic_name = 'org.science.medicare'
        self.key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/key_schema.json")
        self.value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/value_schema.json")
        self.num_partitions = 1
        self.num_replicas = 1

        self.producer = self.get_producer()

    def _read_source_data(self) -> pd.DataFrame:
        """
        internal method tries reading csv file from processed or unprocessed folder
        as a base for synthetic data stream.
        """
        df = None
        try:
            logger.info("reading csv base file under processed folder", class_name=self.__class__.__name__)
            df = pd.read_csv(
                f"{Path(__file__).parents[1]}/data/processed/{self.base_data_filename}"
            )
        except FileNotFoundError:
            logger.warning("base file not processed, trying under unprocessed folder",
                           class_name=self.__class__.__name__)
            try:
                df = pd.read_csv(
                    f"{Path(__file__).parents[1]}/data/unprocessed/{self.base_data_filename}"
                )
            except FileNotFoundError:
                logger.error("base file not found... exiting", class_name=self.__class__.__name__)
                exit(1)
        return df

    def generate_random_record(self, n: int = 1) -> pd.DataFrame:
        """
        method to generate random n records from the base dataset
        :param n: integer, specify the number of random records to generate
        :return: random_df
        """
        keys = self.raw_data[self.categorical_columns].sample(n=n)
        measures = self.raw_data[self.measure_columns].sample(n=n)

        keys.reset_index(drop=True, inplace=True)
        measures.reset_index(drop=True, inplace=True)

        dtypes = {
            "National Provider Identifier": int,
            "Last Name/Organization Name of the Provider": str,
            "First Name of the Provider": str,
            "Middle Initial of the Provider": str,
            "Credentials of the Provider": str,
            "Gender of the Provider": str,
            "Entity Type of the Provider": str,
            "Street Address 1 of the Provider": str,
            "Street Address 2 of the Provider": str,
            "City of the Provider": str,
            "Zip Code of the Provider": str,
            "State Code of the Provider": str,
            "Country Code of the Provider": str,
            "Provider Type": str,
            "Medicare Participation Indicator": str,
            "Place of Service": str,
            "HCPCS Code": str,
            "HCPCS Description": str,
            "HCPCS Drug Indicator": str,
            "Number of Services": float,
            "Number of Medicare Beneficiaries": int,
            "Number of Distinct Medicare Beneficiary/Per Day Services": int,
            "Average Medicare Allowed Amount": float,
            "Average Submitted Charge Amount": float,
            "Average Medicare Payment Amount": float,
            "Average Medicare Standardized Amount": float
        }

        random_df = pd.concat([keys, measures], axis=1).astype(dtype=dtypes)

        return random_df

    def send_stream_to_kafka(self, df: pd.DataFrame) -> None:
        """
        Method will iterate over rows in the dataframe and create two dictionaries key, value
        to send to kafka via avroProducer.
        The key `npi` is in position index 0 in the dataframe, passed as a list.
        The values from the dataframe are passed as list to the structure value dictionary
        """
        for row in df.values.tolist():
            value = dict(zip(self.get_value_structure(), row))
            # key = dict(zip(self.get_key_structure(), [row[0]]))
            key = {"npi": int(row[0])}
            self.producer.producer.produce(topic=self.topic_name, key=key, value=value)
            logger.info(f"sent event to kafka with key: {key} and value: {value}", class_name=self.__class__.__name__)

    @staticmethod
    def get_categorical_columns() -> list:
        """static method to return the name of categorical columns in medicare dataset"""
        return [
            "National Provider Identifier",
            "Last Name/Organization Name of the Provider",
            "First Name of the Provider",
            "Middle Initial of the Provider",
            "Credentials of the Provider",
            "Gender of the Provider",
            "Entity Type of the Provider",
            "Street Address 1 of the Provider",
            "Street Address 2 of the Provider",
            "City of the Provider",
            "Zip Code of the Provider",
            "State Code of the Provider",
            "Country Code of the Provider",
            "Provider Type",
            "Medicare Participation Indicator",
            "Place of Service",
            "HCPCS Code",
            "HCPCS Description",
            "HCPCS Drug Indicator"
        ]

    @staticmethod
    def get_measure_columns() -> list:
        """static method to return the list of measure columns in the medicare dataset"""
        return [
            'Number of Services',
            'Number of Medicare Beneficiaries',
            'Number of Distinct Medicare Beneficiary/Per Day Services',
            'Average Medicare Allowed Amount',
            'Average Submitted Charge Amount',
            'Average Medicare Payment Amount',
            'Average Medicare Standardized Amount'
        ]

    @staticmethod
    def get_value_structure() -> list:
        return [
            "npi",
            "nppes_provider_last_org_name",
            "nppes_provider_first_name",
            "nppes_provider_mi",
            "nppes_credentials",
            "nppes_provider_gender",
            "nppes_entity_code",
            "nppes_provider_street1",
            "nppes_provider_street2",
            "nppes_provider_city",
            "nppes_provider_zip",
            "nppes_provider_state",
            "nppes_provider_country",
            "provider_type",
            "medicare_participation_indicator",
            "place_of_service",
            "hcpcs_code",
            "hcpcs_description",
            "hcpcs_drug_indicator",
            "line_srvc_cnt",
            "bene_unique_cnt",
            "bene_day_srvc_cnt",
            "average_medicare_allowed_amt",
            "average_submitted_chrg_amt",
            "average_medicare_payment_amt",
            "average_medicare_standard_amt"
        ]

    @staticmethod
    def get_key_structure() -> list:
        return [
            "npi"
        ]

    def get_producer(self) -> Producer:
        return Producer(
            topic_name=self.topic_name,
            key_schema=self.key_schema,
            value_schema=self.value_schema,
        )

    def run(self):
        logger.info("Beginning simulation, press Ctrl+C to exit at any time", class_name=self.__class__.__name__)
        try:
            while True:
                random_df = self.generate_random_record()
                self.send_stream_to_kafka(random_df)

        except KeyboardInterrupt as e:
            logger.info("Shutting down simulation", class_name=self.__class__.__name__)
            self.producer.close()


if __name__ == "__main__":
    med = MedicareSimulator(base_data_filename="data_2.csv")
    med.run()
