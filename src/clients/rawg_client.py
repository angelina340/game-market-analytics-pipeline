from __future__ import annotations

from typing import Any

import requests


class RawgClient:
    BASE_URL = "https://api.rawg.io/api"

    def __init__(self, api_key: str, timeout: int = 30) -> None:
        self.api_key = api_key
        self.timeout = timeout

    def fetch_games(self, page_size: int = 20) -> dict[str, Any]:
        response = requests.get(
            f"{self.BASE_URL}/games",
            params={
                "key": self.api_key,
                "page_size": page_size,
                "ordering": "-rating",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()
