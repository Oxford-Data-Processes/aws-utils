import logging
from aws_utils import events
from typing import Dict, Any, Tuple, List

logger = logging.getLogger(__name__)


class S3Router:
    @staticmethod
    def extract_s3_info(event: Dict[str, Any]) -> Tuple[str, str]:
        """
        Extracts S3 bucket name and object key from the event.

        Args:
            event (Dict[str, Any]): The event containing S3 information.

        Returns:
            Tuple[str, str]: A tuple containing the bucket name and object key.
        """
        s3_info = event["Records"][0]["s3"]
        return s3_info["bucket"]["name"], s3_info["object"]["key"]

    @staticmethod
    def create_event_detail(bucket_name: str, object_key: str) -> Dict[str, Any]:
        """
        Create event detail dictionary.

        Args:
            bucket_name (str): The name of the S3 bucket.
            object_key (str): The key of the S3 object.

        Returns:
            Dict[str, Any]: A dictionary containing event details.
        """
        return {
            "event_type": "S3PutObject",
            "bucket": bucket_name,
            "object_key": object_key,
        }

    @staticmethod
    def publish_event(
        events_handler: events.EventsHandler,
        detail: Dict[str, Any],
        lambda_function_name: str,
    ) -> Dict[str, Any]:
        """
        Publishes an event to EventBridge.

        Args:
            events_handler (events.EventsHandler): The event handler instance.
            detail (Dict[str, Any]): The event detail to publish.
            lambda_function_name (str): The name of the Lambda function.

        Returns:
            Dict[str, Any]: The response from the publish event call.
        """
        response, entries = events_handler.publish_event(
            f"{lambda_function_name}-event-bus",
            "com.oxforddataprocesses",
            detail["event_type"],
            detail,
        )
        logger.info(f"Entries: {entries}")
        logger.info(f"Event published to EventBridge: {response}")
        return response

    @staticmethod
    def is_valid_prefix(object_key: str, prefixes: List[str]) -> bool:
        """
        Checks if the object key starts with any of the given prefixes.

        Args:
            object_key (str): The S3 object key.
            prefixes (List[str]): A list of valid prefixes.

        Returns:
            bool: True if the object key starts with any prefix, False otherwise.
        """
        return any(object_key.startswith(prefix) for prefix in prefixes)

    @staticmethod
    def process_event_for_lambda(
        events_handler: events.EventsHandler,
        detail: Dict[str, Any],
        lambda_name: str,
        object_key: str,
        prefixes: List[str],
    ) -> None:
        """
        Processes the event for the specified Lambda function if the object key is valid.

        Args:
            events_handler (events.EventsHandler): The event handler instance.
            detail (Dict[str, Any]): The event detail.
            lambda_name (str): The name of the Lambda function.
            object_key (str): The S3 object key.
            prefixes (List[str]): A list of valid prefixes.

        Raises:
            Exception: If the event fails to publish.
        """
        if S3Router.is_valid_prefix(object_key, prefixes):
            response = S3Router.publish_event(events_handler, detail, lambda_name)
            if response["FailedEntryCount"] != 0:
                raise Exception(
                    f"Failed to publish event for {lambda_name}: {response}"
                )

    @staticmethod
    def handle_s3_event(event: Dict[str, Any], config: Dict[str, Any]) -> None:
        """
        Handles the S3 event and processes it according to the configuration.

        Args:
            event (Dict[str, Any]): The S3 event to handle.
            config (Dict[str, Any]): The configuration for processing the event.

        Raises:
            Exception: If the event validation fails.
        """
        events_handler = events.EventsHandler()
        bucket_name, object_key = S3Router.extract_s3_info(event)
        detail = S3Router.create_event_detail(bucket_name, object_key)
        schema_json = events_handler.get_schema("S3PutObject")
        events_handler.validate_event(detail, schema_json)

        for key in config.keys():
            prefixes = config[key]["prefixes"]
            lambda_name = config[key]["lambda_name"]
            logger.info(f"Processing event for {lambda_name}")
            S3Router.process_event_for_lambda(
                events_handler, detail, lambda_name, object_key, prefixes
            )
