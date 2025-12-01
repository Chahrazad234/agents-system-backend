# backend/test_orchestrator_http.py

from backend.orchestrator.engine import AgentOrchestrator


def main():
    # ⚠ Ici, tu mets une config simple / fake pour commencer
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
        "calculator": {
            # selon ta config de CalculatorAgent
        },
        "api_caller": {
            "method": "POST",
            "url": "http://localhost:8000/predict",  # Ismail mettra ça en place
            "timeout": 10,
        },
    }

    orchestrator = AgentOrchestrator(config=config)

    raw_input = {
        "supplier": "SuperMarché Alpha",
        "invoice_date": "31/01/2023",
        "ht": "100",
        "tva": "20",
        "ttc": "120",
        "comment": "  bonjour  ",
    }

    result = orchestrator.run_http_workflow(raw_input)
    print("Résultat orchestrateur HTTP :")
    print(result)


if __name__ == "__main__":
    main()
