import requests

druid_host = "localhost"

datasource_configs = {
    "datasource_configs": [
        {
            "type": "kafka",
            "dataSchema": {
                "dataSource": "claim_event",
                "parser": {
                    "type": "string",
                    "parseSpec": {
                        "format": "json",
                        "timestampSpec": {
                            "column": "datetime",
                            "format": "iso"
                        },
                        "dimensionsSpec": {
                            "dimensions": [
                                "datetime",
                                "claim_id",
                                "hour",
                                "date",
                                "month",
                                "year",
                                "patient_id",
                                "patient_name",
                                {
                                    "type": "long",
                                    "name": "patient_age"
                                },
                                "patient_gender",
                                "patient_state_code",
                                "patient_state_name",
                                "patient_city_name",
                                "patient_city_code",
                                "hospital_id",
                                "hospital_name",
                                "hospital_state_name",
                                "hospital_state_code",
                                "hospital_city_name",
                                "hospital_city_code",
                                "claim_type",
                                {
                                    "type": "double",
                                    "name": "claim_amount"
                                },
                                "diagnosis_id",
                                "procedure_name",
                                "procedure_type",
                                "doctor_id"
                            ]
                        }
                    }
                },
                "metricsSpec": [],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "HOUR",
                    "queryGranularity": "none",
                    "rollup": True
                }
            },
            "tuningConfig": {
                "type": "kafka",
                "maxRowsPerSegment": 5000000
            },
            "ioConfig": {
                "topic": "claim_event_topic",
                "consumerProperties": {
                    "bootstrap.servers": "localhost:9092"
                },
                "taskCount": 1,
                "replicas": 1,
                "taskDuration": "PT1M"
            }
        },
        {
            "type": "kafka",
            "dataSchema": {
                "dataSource": "hospital_profile_two_minute",
                "parser": {
                    "type": "string",
                    "parseSpec": {
                        "format": "json",
                        "timestampSpec": {
                            "column": "datetime",
                            "format": "iso"
                        },
                        "dimensionsSpec": {
                            "dimensions": [
                                "datetime",
                                "timestamp_start",
                                "timestamp_end",
                                "hospital_id",
                                {
                                    "type": "double",
                                    "name": "total_claim_amount"
                                },
                                {
                                    "type": "long",
                                    "name": "total_claim_count"
                                },
                                {
                                    "type": "double",
                                    "name": "minimum_claim_amount"
                                },
                                {
                                    "type": "double",
                                    "name": "maximum_claim_amount"
                                },
                                {
                                    "type": "long",
                                    "name": "minimum_patient_age"
                                },
                                {
                                    "type": "long",
                                    "name": "maximum_patient_age"
                                },
{
                                    "type": "double",
                                    "name": "score"
                                }
                            ]
                        }
                    }
                },
                "metricsSpec": [],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "HOUR",
                    "queryGranularity": "second",
                    "rollup": True
                }
            },
            "tuningConfig": {
                "type": "kafka",
                "maxRowsPerSegment": 5000000
            },
            "ioConfig": {
                "topic": "hospital_profile_two_minute_topic",
                "consumerProperties": {
                    "bootstrap.servers": "localhost:9092"
                },
                "taskCount": 1,
                "replicas": 1,
                "taskDuration": "PT1M"
            }
        },
        {
            "type": "kafka",
            "dataSchema": {
                "dataSource": "hospital_profile_three_minute",
                "parser": {
                    "type": "string",
                    "parseSpec": {
                        "format": "json",
                        "timestampSpec": {
                            "column": "datetime",
                            "format": "iso"
                        },
                        "dimensionsSpec": {
                            "dimensions": [
                                "datetime",
                                "timestamp_start",
                                "timestamp_end",
                                "hospital_id",
                                {
                                    "type": "double",
                                    "name": "total_claim_amount"
                                },
                                {
                                    "type": "long",
                                    "name": "total_claim_count"
                                },
                                {
                                    "type": "double",
                                    "name": "minimum_claim_amount"
                                },
                                {
                                    "type": "double",
                                    "name": "maximum_claim_amount"
                                },
                                {
                                    "type": "long",
                                    "name": "minimum_patient_age"
                                },
                                {
                                    "type": "long",
                                    "name": "maximum_patient_age"
                                },
{
                                    "type": "double",
                                    "name": "score"
                                }
                            ]
                        }
                    }
                },
                "metricsSpec": [],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "HOUR",
                    "queryGranularity": "second",
                    "rollup": True
                }
            },
            "tuningConfig": {
                "type": "kafka",
                "maxRowsPerSegment": 5000000
            },
            "ioConfig": {
                "topic": "hospital_profile_three_minute_topic",
                "consumerProperties": {
                    "bootstrap.servers": "localhost:9092"
                },
                "taskCount": 1,
                "replicas": 1,
                "taskDuration": "PT1M"
            }
        },
        {
            "type": "kafka",
            "dataSchema": {
                "dataSource": "patient_profile_two_minute",
                "parser": {
                    "type": "string",
                    "parseSpec": {
                        "format": "json",
                        "timestampSpec": {
                            "column": "datetime",
                            "format": "iso"
                        },
                        "dimensionsSpec": {
                            "dimensions": [
                                "datetime",
                                "timestamp_start",
                                "timestamp_end",
                                "patient_id",
                                {
                                    "type": "double",
                                    "name": "total_claim_amount"
                                },
                                {
                                    "type": "long",
                                    "name": "total_claim_count"
                                },
                                {
                                    "type": "double",
                                    "name": "minimum_claim_amount"
                                },
                                {
                                    "type": "double",
                                    "name": "maximum_claim_amount"
                                },
{
                                    "type": "double",
                                    "name": "score"
                                }
                            ]
                        }
                    }
                },
                "metricsSpec": [],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "HOUR",
                    "queryGranularity": "second",
                    "rollup": True
                }
            },
            "tuningConfig": {
                "type": "kafka",
                "maxRowsPerSegment": 5000000
            },
            "ioConfig": {
                "topic": "patient_profile_two_minute_topic",
                "consumerProperties": {
                    "bootstrap.servers": "localhost:9092"
                },
                "taskCount": 1,
                "replicas": 1,
                "taskDuration": "PT1M"
            }
        },
        {
            "type": "kafka",
            "dataSchema": {
                "dataSource": "patient_profile_three_minute",
                "parser": {
                    "type": "string",
                    "parseSpec": {
                        "format": "json",
                        "timestampSpec": {
                            "column": "datetime",
                            "format": "iso"
                        },
                        "dimensionsSpec": {
                            "dimensions": [
                                "datetime",
                                "timestamp_start",
                                "timestamp_end",
                                "patient_id",
                                {
                                    "type": "double",
                                    "name": "total_claim_amount"
                                },
                                {
                                    "type": "long",
                                    "name": "total_claim_count"
                                },
                                {
                                    "type": "double",
                                    "name": "minimum_claim_amount"
                                },
                                {
                                    "type": "double",
                                    "name": "maximum_claim_amount"
                                },
{
                                    "type": "double",
                                    "name": "score"
                                }
                            ]
                        }
                    }
                },
                "metricsSpec": [],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "HOUR",
                    "queryGranularity": "second",
                    "rollup": True
                }
            },
            "tuningConfig": {
                "type": "kafka",
                "maxRowsPerSegment": 5000000
            },
            "ioConfig": {
                "topic": "patient_profile_three_minute_topic",
                "consumerProperties": {
                    "bootstrap.servers": "localhost:9092"
                },
                "taskCount": 1,
                "replicas": 1,
                "taskDuration": "PT1M"
            }
        }
    ]
}


def main():
    r = requests.get(url="http://" + druid_host + ":28082/status")
    print(r.json())

    for datasource_config in datasource_configs["datasource_configs"]:
        r = requests.post(url="http://" + druid_host + ":28090/druid/indexer/v1/supervisor", json=datasource_config,
                          headers={'Content-Type': 'application/json'})
        print(r.json())


if __name__ == "__main__":
    main()
