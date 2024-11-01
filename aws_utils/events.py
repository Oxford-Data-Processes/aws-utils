import boto3
import os
import json
import logging
from typing import Any, Dict, Tuple, Optional, List
from jsonschema import validate, ValidationError

logger = logging.getLogger(__name__)


class EventsHandler:
    def publish_event(
        self, event_bus_name: str, event_source: str, detail_type: str, detail: Any
    ) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
        """
        Publishes an event to the specified event bus.

        Args:
            event_bus_name (str): The name of the event bus to publish the event to.
            event_source (str): The source of the event.
            detail_type (str): The type of detail for the event.
            detail (Any): The detail of the event, which will be serialized to JSON.

        Returns:
            Tuple[Dict[str, Any], List[Dict[str, str]]]: A tuple containing the response from the
            put_events call and the list of entries that were published.
        """
        eventbridge_client = boto3.client(
            "events", region_name=os.environ["AWS_REGION"]
        )

        entries = [
            {
                "Source": event_source,
                "DetailType": detail_type,
                "Detail": json.dumps(detail),
                "EventBusName": event_bus_name,
            }
        ]

        response = eventbridge_client.put_events(Entries=entries)
        return response, entries

    def get_schema(self, event_name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves the schema for a specified event from the schema registry.

        Args:
            event_name (str): The name of the event whose schema is to be retrieved.

        Returns:
            Optional[Dict[str, Any]]: The schema of the event as a dictionary, or None if an error occurs.
        """
        session = boto3.Session(region_name=os.environ["AWS_REGION"])
        schemas_client = session.client("schemas")
        try:
            response = schemas_client.describe_schema(
                RegistryName="GlobalEventSchemaRegistry", SchemaName=event_name
            )
            schema_json = json.loads(response["Content"])
            logger.info(f"Schema: {schema_json}")
            return schema_json

        except Exception as e:
            logger.error(f"Error retrieving schema: {e}")
            return None

    def validate_event(self, event_detail: Any, event_schema: Dict[str, Any]) -> bool:
        """
        Validates the provided event detail against the specified schema.

        Args:
            event_detail (Any): The detail of the event to be validated.
            event_schema (Dict[str, Any]): The schema against which the event detail will be validated.

        Returns:
            bool: True if the event detail is valid according to the schema, False otherwise.

        Logs:
            Info: Indicates if the event detail is valid.
            Error: Logs any validation errors encountered during the process.
        """
        try:
            validate(instance=event_detail, schema=event_schema)
            logger.info("Event detail is valid.")
            return True
        except ValidationError as e:
            logger.error(f"Event detail validation error: {e.message}")
            return False
