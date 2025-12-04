import logging
from abc import abstractmethod

import httpx
from httpx._models import Response
from tenacity import TryAgain, retry, retry_if_exception_type, wait_exponential
from tenacity.stop import stop_after_attempt

RETRY_STATUSES = [500, 503, 429]
DEFAULT_JSON_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}
TIMEOUT_DEFAULT = 600.0


class HttpClient:
    def __init__(
        self,
        server_url: str,
        *,
        token_header: str | None = "Authorization",
        token_prefix: str | None = "Bearer",
        verify: bool = True,
        refresh_on_401: bool = True,
    ) -> None:
        self.server_url = server_url
        self.token_header = token_header
        self.token_prefix = token_prefix
        self.verify = verify
        self.refresh_on_401 = refresh_on_401
        self.token: str | None = None

    @abstractmethod
    def _get_token(self) -> None:
        pass

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(TryAgain),
        wait=wait_exponential(multiplier=2, min=1, max=64),
    )
    def request(
        self,
        uri: str,
        *,
        method: str = "GET",
        headers: dict = DEFAULT_JSON_HEADERS.copy(),
        params: dict | None = None,
        data: dict | None = None,
        content: str | None = None,
        token_expected: bool = True,
        token: str | None = None,
        retry_statuses: list[int] = RETRY_STATUSES,
    ) -> Response:
        if token_expected:
            if not token:
                if not self.token:
                    self._get_token()

                token = self.token

            headers[self.token_header] = (
                f"{self.token_prefix} {token}" if self.token_prefix else token
            )

        url = f"{self.server_url}{uri}"
        res = httpx.request(
            method,
            url,
            params=params,
            data=data,
            content=content,
            headers=headers,
            timeout=TIMEOUT_DEFAULT,
            verify=self.verify,
        )

        if self.refresh_on_401 and res.status_code == 401:
            self.token = None
            logging.error(res.text)
            raise TryAgain

        if res.status_code in retry_statuses:
            logging.error(res.text)
            raise TryAgain

        res.raise_for_status()
        return res
