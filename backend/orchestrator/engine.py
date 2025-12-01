# backend/orchestrator/engine.py

from typing import Any, Dict

from backend.agents.simple.data_parser import DataParserAgent
from backend.agents.simple.formatter import FormatterAgent
from backend.agents.simple.calculator import CalculatorAgent
from backend.agents.simple.api_caller import APICallerAgent
from backend.agents.integration.kafka_producer import KafkaProducerAgent
from backend.agents.integration.kafka_response_wait import KafkaResponseWaitAgent


class AgentOrchestrator:
    """
    Orchestrateur interne des AGENTS.
    - Chaine DataParser -> Formatter -> Calc
    - Puis choisit HTTP ou Kafka suivant le mode
    """

    def __init__(self, config: Dict[str, Any]):
        """
        config = {
            "data_parser": {...},
            "formatter": {...},
            "calculator": {...},
            "api_caller": {...},
            "kafka_producer": {...},
            "kafka_response_wait": {...},
        }
        """
        self.config = config

    # ------------- PIPELINE COMMUN (préparation) -------------

    def _run_preprocessing(self, raw_input: Dict[str, Any]) -> Dict[str, Any]:
        """DataParser -> Formatter -> Calc."""

        # 1) DataParser
        parser = DataParserAgent(config=self.config.get("data_parser", {}))
        parsed = parser.run(raw_input)

        # 2) Formatter
        formatter = FormatterAgent(config=self.config.get("formatter", {}))
        formatted = formatter.run(parsed)

        # 3) Calculator
        calculator = CalculatorAgent(config=self.config.get("calculator", {}))
        enriched = calculator.run(formatted)

        return enriched

    # ------------- MODE HTTP SYNCHRONE -------------

    def run_http_workflow(self, raw_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pipeline:
        - DataParser -> Formatter -> Calc
        - APICallerAgent (HTTP /predict)
        """
        data = self._run_preprocessing(raw_input)

        api_agent = APICallerAgent(config=self.config.get("api_caller", {}))
        external_response = api_agent.run(data)

        return {
            "mode": "http",
            "prepared_data": data,
            "external_response": external_response,
        }

    # ------------- MODE KAFKA ASYNCHRONE -------------

    def run_kafka_workflow(self, raw_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pipeline:
        - DataParser -> Formatter -> Calc
        - KafkaProducerAgent -> KafkaResponseWaitAgent
        """
        data = self._run_preprocessing(raw_input)

        producer = KafkaProducerAgent(config=self.config.get("kafka_producer", {}))
        # Le producer renvoie normalement un correlation_id
        correlation_id = producer.run(data)

        waiter = KafkaResponseWaitAgent(
            config=self.config.get("kafka_response_wait", {}),
            correlation_id=correlation_id,
        )
        external_response = waiter.run()

        return {
            "mode": "kafka",
            "prepared_data": data,
            "correlation_id": correlation_id,
            "external_response": external_response,
        }
