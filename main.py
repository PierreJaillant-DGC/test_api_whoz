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
    schema_accounts,
    schema_dossiers,
    schema_projects,
    schema_talents,
    schema_tasks,
    schema_worklogs,
    schema_workspaces,
)

logging.getLogger().setLevel(logging.INFO)
load_dotenv()
WHOZ_CLIENT_ID = os.environ.get("WHOZ_CLIENT_ID")
WHOZ_SECRET = os.environ.get("WHOZ_SECRET")
WHOZ_FEDERATION_ID = os.environ.get("WHOZ_FEDERATION_ID")

BIGQUERY_TABLE_NAME_ACCOUNTS = os.environ.get("BIGQUERY_TABLE_NAME_ACCOUNTS")
BIGQUERY_TABLE_NAME_DOSSIERS = os.environ.get("BIGQUERY_TABLE_NAME_DOSSIERS")
BIGQUERY_TABLE_NAME_PROJECTS = os.environ.get("BIGQUERY_TABLE_NAME_PROJECTS")
BIGQUERY_TABLE_NAME_TALENTS = os.environ.get("BIGQUERY_TABLE_NAME_TALENTS")
BIGQUERY_TABLE_NAME_TASKS = os.environ.get("BIGQUERY_TABLE_NAME_TASKS")
BIGQUERY_TABLE_NAME_WORKLOGS = os.environ.get("BIGQUERY_TABLE_NAME_WORKLOGS")
BIGQUERY_TABLE_NAME_WORKSPACES = os.environ.get("BIGQUERY_TABLE_NAME_WORKSPACES")


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
    whoz_accounts = []
    whoz_dossiers = []
    whoz_projects = []
    whoz_talents = []
    whoz_tasks = []
    whoz_worklogs = []

    for workspace_id in workspace_ids:
        # Get talents via  workspace_id
        workspace_talents = whoz_client.get_talents(workspace_id)
       
        workspace_talents = remove_structure_from_talents(workspace_talents)
        whoz_talents += workspace_talents
        talent_ids = {t.get("id") for t in workspace_talents}
        logging.info(f"In the workspace {workspace_id}, there are {str(len(talent_ids))} unique talents.")

        # Get tasks via workspace_id
        workspace_tasks = whoz_client.get_tasks(workspace_id)
        whoz_tasks += workspace_tasks
        task_ids = {t.get("id") for t in workspace_tasks}
        logging.info(f"In the workspace {workspace_id}, there are {str(len(task_ids))} unique tasks.")

        # Get accounts via  workspace_id
        workspace_accounts = whoz_client.get_accounts(workspace_id)
        whoz_accounts += workspace_accounts
        account_ids = {t.get("id") for t in workspace_accounts}
        logging.info(f"In the workspace {workspace_id}, there are {str(len(account_ids))} unique accounts.")

        # Get projects via  workspace_id
        workspace_projects = whoz_client.get_projects(workspace_id)
        whoz_projects += workspace_projects
        project_ids = {t.get("id") for t in workspace_projects}
        logging.info(f"In the workspace {workspace_id}, there are {str(len(project_ids))} unique projects.")

        # Get dossiers via  workspace_id
        workspace_dossiers = whoz_client.get_dossiers(workspace_id)
        whoz_dossiers += workspace_dossiers
        dossier_ids = {t.get("id") for t in workspace_dossiers}
        logging.info(f"In the workspace {workspace_id}, there are {str(len(dossier_ids))} unique dossiers.")

        workspace_worklogs = []
        for talent_id in talent_ids:
            for month in months:
                """Get worklogs via talent_id & months & workspace_id"""
                try:
                    workspace_worklogs += whoz_client.get_worklogs(workspace_id, talent_id, month)
                except Exception:
                    logging.error(f"Error with : workspace {workspace_id}, talent {talent_id}, month {month}")
                    logging.exception(e)
        worklog_ids = {t.get("id") for t in workspace_worklogs}
        logging.info(f"In the workspace {workspace_id}, there are {str(len(worklog_ids))} unique worklogs.")
        whoz_worklogs += workspace_worklogs

    # We only keep the data matching the schema of each table
    logging.info("Cleaning data")
    whoz_accounts = ProcessForBigQuery.clean_accounts(whoz_accounts, timestamp)
    whoz_dossiers = ProcessForBigQuery.clean_dossiers(whoz_dossiers, timestamp)
    whoz_projects = ProcessForBigQuery.clean_projects(whoz_projects, timestamp)
    whoz_talents = ProcessForBigQuery.clean_talents(whoz_talents, timestamp)
    whoz_tasks = ProcessForBigQuery.clean_tasks(whoz_tasks, timestamp)
    whoz_worklogs = ProcessForBigQuery.clean_worklogs(whoz_worklogs, timestamp)
    whoz_workspaces = ProcessForBigQuery.clean_workspaces(whoz_workspaces, timestamp)
    
    logging.info("Loading into BigQuery")
    bq_client = BigQueryClient()

    bq_client.update_table_in_bigquery_dataset_truncate(BIGQUERY_TABLE_NAME_ACCOUNTS, whoz_accounts, schema_accounts)
    bq_client.update_table_in_bigquery_dataset_truncate(BIGQUERY_TABLE_NAME_DOSSIERS, whoz_dossiers, schema_dossiers)
    bq_client.update_table_in_bigquery_dataset_truncate(BIGQUERY_TABLE_NAME_PROJECTS, whoz_projects, schema_projects)
    bq_client.update_table_in_bigquery_dataset_truncate(BIGQUERY_TABLE_NAME_TALENTS, whoz_talents, schema_talents)
    bq_client.update_table_in_bigquery_dataset_truncate(BIGQUERY_TABLE_NAME_TASKS, whoz_tasks, schema_tasks)
    bq_client.update_table_in_bigquery_dataset_delete_current_and_append(
        BIGQUERY_TABLE_NAME_WORKLOGS, whoz_worklogs, schema_worklogs
    )
    bq_client.update_table_in_bigquery_dataset_truncate(
        BIGQUERY_TABLE_NAME_WORKSPACES, whoz_workspaces, schema_workspaces
    )

    logging.info(f"Start: {start}")
    end = datetime.now()
    logging.info(f"End: {end}")
    return "Ok"


if __name__ == "__main__":
    main()
