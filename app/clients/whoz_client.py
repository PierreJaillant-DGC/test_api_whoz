import os

from dotenv import load_dotenv

from app.clients.http_client import HttpClient

load_dotenv()
WHOZ_UNIQUE_WORKSPACE_ID = os.environ.get("WHOZ_UNIQUE_WORKSPACE_ID")
WHOZ_API_VERSION = os.environ.get("WHOZ_API_VERSION")
WHOZ_RESPONSE_FORMAT = os.environ.get("WHOZ_RESPONSE_FORMAT")


class WhozClient(HttpClient):
    """
    Whoz Api Client
    See doc: https://whoz.stoplight.io/docs/whoz-api/
    """

    def __init__(self, client_id: str, secret: str, federation_id: str) -> None:
        super().__init__("https://www.whoz.com")
        self.client_id = client_id
        self.secret = secret
        self.federation_id = federation_id

    def _get_token(self) -> None:
        uri = "/auth/realms/whoz/protocol/openid-connect/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.secret,
        }
        res = self.request(uri, method="POST", headers={}, data=data, token_expected=False)
        self.token = res.json().get("access_token")

    def get_federation(self) -> dict:
        """
        Get a federation
        """
        params = {"Accept-Version": WHOZ_API_VERSION, "Content-Type": WHOZ_RESPONSE_FORMAT}
        uri = f"/api/federation/{self.federation_id}"
        res = self.request(uri, params=params)
        return res.json()

    def get_workspaces(self) -> list:
        """
        Get workspaces
        """
        params = {
            "federationId": self.federation_id,
            "Accept-Version": WHOZ_API_VERSION,
            "Content-Type": WHOZ_RESPONSE_FORMAT,
        }
        uri = "/api/workspace"
        res = self.request(uri, params=params)
        
        for workspace in res.json():
            if workspace.get("id") == WHOZ_UNIQUE_WORKSPACE_ID:
                return [workspace]

    def get_talents(self, workspace_id: str) -> list:
        """
        Get talents
        """
        params = {
            "workspaceId": workspace_id,
            "Accept-Version": WHOZ_API_VERSION,
            "Content-Type": WHOZ_RESPONSE_FORMAT,
        }
        uri = "/api/talent"
        res = self.request(uri, params=params)
        return res.json()

    def get_tasks(self, workspace_id: str) -> list:
        """
        Get tasks
        """
        params = {
            "workspaceId": workspace_id,
            "Accept-Version": WHOZ_API_VERSION,
            "Content-Type": WHOZ_RESPONSE_FORMAT,
        }
        uri = "/api/task"
        res = self.request(uri, params=params)
        return res.json()

    def get_worklogs(self, workspace_id: str, talent_id: str, year_month: str) -> list:
        """
        Get worklogs
        """
        params = {
            "workspaceId": workspace_id,
            "month": year_month,
            "talentId": talent_id,
            "Accept-Version": WHOZ_API_VERSION,
            "Content-Type": WHOZ_RESPONSE_FORMAT,
        }
        uri = "/api/worklog"
        res = self.request(uri, params=params)
        return res.json()

    def get_accounts(self, workspace_id: str) -> list:
        """
        Get accounts
        """
        params = {
            "workspaceId": workspace_id,
            "Accept-Version": WHOZ_API_VERSION,
            "Content-Type": WHOZ_RESPONSE_FORMAT,
        }
        uri = "/api/account"
        res = self.request(uri, params=params)
        return res.json()

    def get_projects(self, workspace_id: str) -> list:
        """
        Get projects
        """
        params = {
            "workspaceId": workspace_id,
            "Accept-Version": WHOZ_API_VERSION,
            "Content-Type": WHOZ_RESPONSE_FORMAT,
        }
        uri = "/api/project"
        res = self.request(uri, params=params)
        return res.json()

    def get_dossiers(self, workspace_id: str) -> list:
        """
        Get dossiers
        """
        params = {
            "workspaceId": workspace_id,
            "Accept-Version": WHOZ_API_VERSION,
            "Content-Type": WHOZ_RESPONSE_FORMAT,
        }
        uri = "/api/dossier"
        res = self.request(uri, params=params)
        return res.json()

    def get_activities(self) -> dict:
        """
        Get activities
        """
        params = {"Accept-Version": WHOZ_API_VERSION, "Content-Type": WHOZ_RESPONSE_FORMAT}
        uri = "/api/activity"
        res = self.request(uri, params=params)
        
        return res.json()
