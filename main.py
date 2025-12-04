import json
import logging
import os

from datetime import datetime
from dotenv import load_dotenv

from app.clients.google_bigquery_client import BigQueryClient  # noqa: F401
from app.clients.whoz_client import WhozClient
from app.utils import get_months_list, remove_structure_from_talents
from app.data_processing.data_filtering import ProcessForBigQuery
from schemas.schemas import (
    schema_talents
)

logging.getLogger().setLevel(logging.INFO)
load_dotenv()
WHOZ_CLIENT_ID = os.environ.get("WHOZ_CLIENT_ID")
WHOZ_SECRET = os.environ.get("WHOZ_SECRET")
WHOZ_FEDERATION_ID = os.environ.get("WHOZ_FEDERATION_ID")
BIGQUERY_TABLE_NAME_TALENTS = os.environ.get("BIGQUERY_TABLE_NAME_TALENTS")

def main(e=None) -> None:
    start = datetime.now()
    logging.info(f"Start: {start}")
    timestamp = str(start)

    whoz_client = WhozClient(WHOZ_CLIENT_ID, WHOZ_SECRET, WHOZ_FEDERATION_ID)

    # Month list generation
    # Get the months list with 3 months in the future
    months = get_months_list(datetime.today().strftime("%Y-%m"), 3)
    logging.info(months)

    """
    Get workspaces via federation
    Federation is part of the keys we need to access Whoz API so no need to retrieve it
    """
    whoz_workspaces = whoz_client.get_workspaces()
    workspace_ids = {t.get("id") for t in whoz_workspaces}
    logging.info(f"Nb workspaces: {str(len(workspace_ids))}")

    # Declaration before so we can append talents from different workspaces

    whoz_talents = []

    for workspace_id in workspace_ids:
        # Get talents via  workspace_id
        workspace_talents = whoz_client.get_talents(workspace_id)
       
        workspace_talents = remove_structure_from_talents(workspace_talents)
        whoz_talents += workspace_talents
        talent_ids = {t.get("id") for t in workspace_talents}
        logging.info(f"In the workspace {workspace_id}, there are {str(len(talent_ids))} unique talents.")


    # We only keep the data matching the schema of each table
    logging.info("Cleaning data")

    whoz_talents = ProcessForBigQuery.clean_talents(whoz_talents, timestamp)
    
    logging.info("Loading into BigQuery")
    bq_client = BigQueryClient()

    bq_client.update_table_in_bigquery_dataset_truncate(BIGQUERY_TABLE_NAME_TALENTS, whoz_talents, schema_talents)

    logging.info(f"Start: {start}")
    end = datetime.now()
    logging.info(f"End: {end}")
    return "Ok"


if __name__ == "__main__":
    main()
