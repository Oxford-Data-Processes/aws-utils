import boto3
import os
import json


class EventsHandler:
    def publish_event(self, event_bus_name, event_source, detail_type, detail):
        eventbridge_client = boto3.client(
            "events", region_name=os.environ["AWS_REGION"]
        )

        response = eventbridge_client.put_events(
            Entries=[
                {
                    "Source": event_source,
                    "DetailType": detail_type,
                    "Detail": json.dumps(detail),
                    "EventBusName": event_bus_name,
                }
            ]
        )
        return response
