from __future__ import annotations

from typing import Any

import requests


class SteamClient:
    BASE_URL = "https://partner.steam-api.com/IStoreService/GetAppList/v1/"

    def __init__(self, api_key: str, timeout: int = 30) -> None:
        self.api_key = api_key
        self.timeout = timeout

    def fetch_apps(self, limit: int = 200, last_appid: int | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {
            "key": self.api_key,
            "max_results": limit,
            "include_games": "true",
            "include_dlc": "false",
            "include_software": "false",
            "include_videos": "false",
            "include_hardware": "false",
        }
        if last_appid is not None:
            params["last_appid"] = last_appid

        response = requests.get(
            self.BASE_URL,
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()
