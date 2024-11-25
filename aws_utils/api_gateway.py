import boto3
import os
from typing import Optional


class APIGatewayHandler:
    def __init__(self) -> None:
        """
        Initializes the APIGatewayHandler by creating an APIGateway client using AWS credentials
        from environment variables.
        """
        self.api_gateway_client = boto3.client(
            "apigateway", region_name=os.environ["AWS_REGION"]
        )

    def search_api_by_name(self, api_name: str) -> Optional[str]:
        """
        Searches for an API by its name and returns its ID.

        Args:
            api_name (str): The name of the API to search for.

        Returns:
            Optional[str]: The ID of the matching API if found, otherwise None.
        """
        response = self.api_gateway_client.get_rest_apis()
        apis = response.get("items", [])
        matching_apis = [api for api in apis if api_name.lower() in api["name"].lower()]
        return matching_apis[0]["id"] if matching_apis else None
