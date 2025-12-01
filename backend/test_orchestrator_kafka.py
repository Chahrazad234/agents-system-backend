# backend/test_orchestrator_kafka.py

from backend.orchestrator.engine import AgentOrchestrator


def main():
    config = {
        "data_parser": {
            "extract_fields": ["supplier", "invoice_date", "ht", "tva", "ttc"],
            "remove_nulls": True,
        },
        "formatter": {
            "strip_strings": True,
            "empty_to_none": True,
            "date_fields": ["invoice_date"],
            "input_date_formats": ["%d/%m/%Y", "%Y-%m-%d"],
            "output_date_format": "%Y-%m-%d",
            "numeric_fields": ["ht", "tva", "ttc"],
            "decimals": 2,
            "as_string": True,
        },
        "calculator": {},
        "kafka_producer": {
            "bootstrap_servers": "localhost:9092",
            "topic": "prediction_requests",
        },
        "kafka_response_wait": {
            "bootstrap_servers": "localhost:9092",
            "topic": "prediction_responses",
            "timeout_seconds": 30,
        },
    }

    orchestrator = AgentOrchestrator(config=config)

    raw_input = {
        "supplier": "SuperMarché Alpha",
        "invoice_date": "31/01/2023",
        "ht": "100",
        "tva": "20",
        "ttc": "120",
    }

    result = orchestrator.run_kafka_workflow(raw_input)
    print("Résultat orchestrateur Kafka :")
    print(result)


if __name__ == "__main__":
    main()
