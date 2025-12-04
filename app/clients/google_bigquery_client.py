import logging
import os
from datetime import datetime
import google.auth
from dotenv import load_dotenv
from google.auth.credentials import with_scopes_if_required
from google.cloud import bigquery

from app.utils import is_empty, remove_empty_elements

load_dotenv()
GOOGLE_SCOPES = ["https://www.googleapis.com/auth/bigquery"]
BIGQUERY_DATASET_ID = os.environ.get("BIGQUERY_DATASET_ID")
PROJECT_ID = BIGQUERY_DATASET_ID.split(".")[0]


class BigQueryClient:
    def __init__(self):
        """Authentication"""
        creds, _ = google.auth.default()
        try:
            creds = with_scopes_if_required(creds, GOOGLE_SCOPES)
            self.client = bigquery.Client(project=PROJECT_ID, credentials=creds)
            logging.info("BigQuery client successfully initialized.")

        except Exception as e:
            logging.error(f"Failed to initialize BigQuery client: {e}")
            raise

    def update_table_in_bigquery_dataset_truncate(
        self, table_name: str, data: list, schema_json: list | None = None
    ) -> None:
        dataset = self.client.get_dataset(BIGQUERY_DATASET_ID)

        # Clean
        cleaned_data = remove_empty_elements(data)

        if is_empty(cleaned_data):
            logging.info(f"Loaded 0 rows for table {table_name}.")
            return

        if dataset:
            table_id = f"{BIGQUERY_DATASET_ID}.{table_name}"

            # We replace the data
            job_config = bigquery.LoadJobConfig(
                # autodetect=(not schema_json),
                schema=schema_json,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # WRITE_APPEND
                ignore_unknown_values=True,
            )

            load_job = self.client.load_table_from_json(
                cleaned_data,
                table_id,
                job_config=job_config,
            )  # Make an API request.

            try:
                load_job.result()  # Waits for the job to complete.
                logging.info(f"Loaded {len(cleaned_data)} rows for table {table_name}.")
            except Exception:
                logging.info(load_job.errors)
                raise

            destination_table = self.client.get_table(table_id)
            logging.info(
                f"There is now a total of {destination_table.num_rows} rows for table {table_name}."
            )
        else:
            logging.info(f"Issue with the access to the dataset {BIGQUERY_DATASET_ID}")

    def update_table_in_bigquery_dataset_delete_current_and_append(
        self, table_name: str, data: list, schema_json: list | None = None
    ) -> None:
        dataset = self.client.get_dataset(BIGQUERY_DATASET_ID)

        # Clean
        cleaned_data = remove_empty_elements(data)

        if is_empty(cleaned_data):
            logging.info(f"Loaded 0 rows for table {table_name}.")
            return

        if dataset:
            table_id = f"{BIGQUERY_DATASET_ID}.{table_name}"

            # We delete the data from this month and after
            date = datetime.strftime(datetime.now(), "%Y-%m-01")
            dml_statement = f"DELETE {table_id} WHERE date >= '{date}' "
            query_job = self.client.query(dml_statement)  # API request
            query_job.result()  # Waits for statement to finish

            # We append data
            job_config = bigquery.LoadJobConfig(
                autodetect=(not schema_json),
                schema=schema_json,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # WRITE_TRUNCATE
                ignore_unknown_values=True,
            )
            load_job = self.client.load_table_from_json(
                cleaned_data,
                table_id,
                job_config=job_config,
            )  # Make an API request.

            try:
                load_job.result()  # Waits for the job to complete.
                logging.info(f"Loaded {len(cleaned_data)} rows for table {table_name}.")
            except Exception:
                logging.info(load_job.errors)
                raise

            destination_table = self.client.get_table(table_id)
            logging.info(
                f"There is now a total of {destination_table.num_rows} rows for table {table_name}."
            )
        else:
            logging.info(f"Issue with the access to the dataset {BIGQUERY_DATASET_ID}")
