from utils.data_store.abstract_data_store import AbstractDataStore
import pandas as pd
from pyspark.sql.types import *


class DruidDataStore(AbstractDataStore):
    def __init__(self, spark):
        """
        TODO complete docstring
        :param src_dir:
        """
        self.spark = spark
        # ensure path ends with a forward slash

    def read_pandas_df_from_data_store(self, **args):
        """
        Read table into PANDAS dataframe

        TODO complete docstring

        """
        return

    def read_spark_df_from_data_store(self, **args):
        """
        Read table into SPARK dataframe

        TODO complete docstring

        """
        spark = args["spark"]
        actor_type = args["actor_type"]
        datasource = args["table"]
        print(args)
        if actor_type == "hospital":
            query = {
                "queryType": "groupBy",
                "dataSource": datasource,
                "dimensions": [
                    "datetime"
                ],
                "aggregations": [],
                "granularity": "all",
                "postAggregations": [],
                "intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00",
                "limitSpec": {
                    "type": "default",
                    "limit": 2,
                    "columns": [
                        {
                            "dimension": "datetime",
                            "direction": "descending"
                        }
                    ]
                }
            }

            import requests
            r = requests.post('http://0.0.0.0:28082/druid/v2/', json=query)
            r.status_code
            datetime = r.json()[1]["event"]["datetime"]

            query = {
                "queryType": "select",
                "dataSource": datasource,
                "descending": "false",
                "metrics": ['hospital_id', 'maximum_claim_amount', 'maximum_patient_age', 'minimum_claim_amount',
                            'minimum_patient_age', 'total_claim_amount', 'total_claim_count'],
                "granularity": "all",
                "intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00",
                "filter": {
                    "type": "selector",
                    "dimension": "datetime",
                    "value": datetime
                },
                "pagingSpec": {"pagingIdentifiers": {}, "threshold": 1000}
            }

            import requests
            r = requests.post('http://0.0.0.0:28082/druid/v2/', json=query)
            r.status_code
            query_result = r.json()[0]["result"]
            x = query_result["events"]
            query_result = [i["event"] for i in x]

            df = pd.DataFrame.from_records(query_result)[
                ['hospital_id', 'maximum_claim_amount', 'maximum_patient_age', 'minimum_claim_amount',
                 'minimum_patient_age', 'total_claim_amount', 'total_claim_count']].copy()
            df.head(10)

            schema = StructType([StructField("hospital_id", StringType(), True),
                                 StructField("maximum_claim_amount", FloatType(), True)
                                    , StructField("maximum_patient_age", LongType(), True)
                                    , StructField("minimum_claim_amount", FloatType(), True)
                                    , StructField("minimum_patient_age", LongType(), True)
                                    , StructField("total_claim_amount", FloatType(), True)
                                    , StructField("total_claim_count", LongType(), True)
                                 ])
            sdf = spark.createDataFrame(df, schema=schema)
        else:
            query = {
                "queryType": "groupBy",
                "dataSource": datasource,
                "dimensions": [
                    "datetime"
                ],
                "aggregations": [],
                "granularity": "all",
                "postAggregations": [],
                "intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00",
                "limitSpec": {
                    "type": "default",
                    "limit": 2,
                    "columns": [
                        {
                            "dimension": "datetime",
                            "direction": "descending"
                        }
                    ]
                }
            }

            import requests
            r = requests.post('http://0.0.0.0:28082/druid/v2/', json=query)
            r.status_code
            datetime = r.json()[1]["event"]["datetime"]

            query = {
                "queryType": "select",
                "dataSource": datasource,
                "descending": "false",
                "metrics": ['hospital_id', 'maximum_claim_amount', 'minimum_claim_amount',
                            'total_claim_amount', 'total_claim_count'],
                "granularity": "all",
                "intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00",
                "filter": {
                    "type": "selector",
                    "dimension": "datetime",
                    "value": datetime
                },
                "pagingSpec": {"pagingIdentifiers": {}, "threshold": 1000}
            }

            import requests
            r = requests.post('http://0.0.0.0:28082/druid/v2/', json=query)
            r.status_code
            query_result = r.json()[0]["result"]
            x = query_result["events"]
            query_result = [i["event"] for i in x]

            df = pd.DataFrame.from_records(query_result)[
                ['patient_id', 'maximum_claim_amount', 'minimum_claim_amount',
                 'total_claim_amount', 'total_claim_count']].copy()
            df.head(10)

            schema = StructType([StructField("patient_id", StringType(), True),
                                 StructField("maximum_claim_amount", FloatType(), True)
                                    , StructField("minimum_claim_amount", FloatType(), True)
                                    , StructField("total_claim_amount", FloatType(), True)
                                    , StructField("total_claim_count", LongType(), True)
                                 ])
            sdf = spark.createDataFrame(df, schema=schema)
        return sdf