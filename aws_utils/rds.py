import boto3
import os
from typing import Optional, Dict, Any


class RDSHandler:
    def __init__(self) -> None:
        """
        Initializes the RDSHandler by creating an RDS client using AWS credentials
        from environment variables.
        """
        self.rds_client = boto3.client("rds", region_name=os.environ["AWS_REGION"])

    def get_rds_instance_by_identifier(
        self, identifier: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieves an RDS instance by its identifier.

        Args:
            identifier (str): The identifier of the RDS instance.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing details of the RDS instance if found, otherwise None.
        """
        response = self.rds_client.describe_db_instances()
        instances = response["DBInstances"]

        for instance in instances:
            if instance["DBInstanceIdentifier"] == identifier:
                return {
                    "DBInstanceIdentifier": instance["DBInstanceIdentifier"],
                    "DBInstanceStatus": instance["DBInstanceStatus"],
                    "DBInstanceClass": instance["DBInstanceClass"],
                    "Engine": instance["Engine"],
                    "Endpoint": instance.get("Endpoint", {}).get("Address", None),
                }
        return None
