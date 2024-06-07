from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    DateTime,
    text,
)
import pandas as pd
import random
import time
import logging
from google.cloud.bigquery.table import TimePartitioning

logger = logging.getLogger(__name__)


class SQLBigQueryDataSource:
    """
    A class representing a data source using SQLAlchemy for BigQuery.

    Attributes:
        project_id (str): The Google Cloud project ID.
        dataset_id (str): The BigQuery dataset ID.
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for BigQuery.
        metadata (sqlalchemy.MetaData): The metadata associated with the engine.
    """

    def __init__(self, project_id, dataset_id) -> None:
        """
        Initializes a new instance of SQLBigQueryDataSource.

        Args:
            project_id (str): The Google Cloud project ID.
            dataset_id (str): The BigQuery dataset ID.
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.engine = create_engine(f"bigquery://{self.project_id}")
        self.metadata = MetaData(bind=self.engine)

    def execute_query(self, query: str):
        """
        Executes a given SQL query.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            result: The result of the query execution.
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df

    def get_table(self, table_id: str) -> Table:
        """
        Retrieves a Table object based on the provided table ID.

        Args:
            table_id (str): The ID of the BigQuery table.

        Returns:
            Table: The SQLAlchemy Table object.
        """
        table = None
        try:
            table = Table(f"{self.dataset_id}.{table_id}", self.metadata, autoload=True)
        except Exception as e:
            logger.debug(f"Error: {e}")
            logger.debug(
                "Table not available or other error occurred. Check logs for details."
            )

        return table

    def create_table(self, table_id: str, schema: dict, time_partitioning_field=None):
        """
        Creates a new table in BigQuery with the specified schema.

        Args:
            table_id (str): The ID of the table to be created.
            schema (dict): A dictionary containing column names and their data types.
                Example: {"column1": String, "column2": Integer}
        """
        table_name = f"{self.dataset_id}.{table_id}"
        new_table = Table(table_name, self.metadata)
        for column_name, column_type in schema.items():
            new_table.append_column(Column(column_name, column_type))

        try_count = 0
        max_retries = 5
        while try_count < max_retries:
            try:
                self.metadata.create_all()
                print(f"Table '{table_name}' created successfully.")
                return True
            except Exception as e:
                if "Job exceeded rate limits" in str(e):
                    delay = (2**try_count) + random.uniform(0, 1)
                    logger.debug(
                        f"Rate limit exceeded. Retrying table creation in {delay} seconds..."
                    )
                    time.sleep(delay)
                    try_count += 1
                else:
                    raise e
        return False

        # if time_partitioning_field:
        #     new_table.append_column(Column(time_partitioning_field, DateTime))

    def delete_table(self, table_id: str):
        """
        Deletes the export table.

        Prints a success message upon successful deletion.
        """
        table_name = f"{self.dataset_id}.{table_id}"
        delete_statement = text(f"DROP TABLE IF EXISTS {table_name}")

        retry_count = 0
        max_retries = 5
        while retry_count < max_retries:
            try:
                with self.engine.connect() as connection:
                    connection.execute(delete_statement)
                print(f"Table '{table_name}' deleted successfully.")
                return
            except Exception as e:
                if "exceeded quota for table update operations" in str(e):
                    retry_delay = (2**retry_count) * 0.1
                    logger.debug(
                        f"Table deletion failed. Retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                    retry_count += 1
                else:
                    raise e
        logger.debug("Exceeded maximum number of retries. Table deletion failed.")
