import logging
from aws_utils import events  # Ensure to import the events module

logger = logging.getLogger(__name__)


class S3Router:
    @staticmethod
    def extract_s3_info(event):
        s3_info = event["Records"][0]["s3"]
        return s3_info["bucket"]["name"], s3_info["object"]["key"]

    @staticmethod
    def create_event_detail(bucket_name, object_key):
        """Create event detail dictionary."""
        return {
            "event_type": "S3PutObject",
            "bucket": bucket_name,
            "object_key": object_key,
        }

    @staticmethod
    def publish_event(events_handler, detail, lambda_function_name):
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
    def is_valid_prefix(object_key, prefixes):
        return any(object_key.startswith(prefix) for prefix in prefixes)

    @staticmethod
    def process_event_for_lambda(
        events_handler, detail, lambda_name, object_key, prefixes
    ):
        if S3Router.is_valid_prefix(object_key, prefixes):
            response = S3Router.publish_event(events_handler, detail, lambda_name)
            if response["FailedEntryCount"] != 0:
                raise Exception(
                    f"Failed to publish event for {lambda_name}: {response}"
                )

    @staticmethod
    def handle_s3_event(event, config):
        events_handler = events.EventsHandler()
        bucket_name, object_key = S3Router.extract_s3_info(event)
        detail = S3Router.create_event_detail(bucket_name, object_key)
        schema_json = events_handler.get_schema("S3PutObject")
        events_handler.validate_event(detail, schema_json)

        for key in config.keys():
            prefixes = config[key]["prefixes"]
            lambda_name = config[key]["lambda_name"]
            S3Router.process_event_for_lambda(
                events_handler, detail, lambda_name, object_key, prefixes
            )
