from __future__ import annotations

from typing import Any

import requests


class SteamClient:
    BASE_URL = "https://store.steampowered.com/api/featuredcategories"

    def __init__(self, api_key: str, timeout: int = 30) -> None:
        self.api_key = api_key
        self.timeout = timeout

    def fetch_apps(self, limit: int = 200, last_appid: int | None = None) -> dict[str, Any]:
        response = requests.get(
            self.BASE_URL,
            params={"cc": "us", "l": "en"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        items: list[dict[str, Any]] = []

        for category_key, category_value in payload.items():
            if not isinstance(category_value, dict):
                continue
            for item in category_value.get("items", []):
                if last_appid is not None and item.get("id", 0) <= last_appid:
                    continue
                enriched_item = dict(item)
                enriched_item["category_key"] = category_key
                enriched_item["category_name"] = category_value.get("name")
                items.append(enriched_item)

        return {
            "featured_categories": items[:limit],
            "total_available": len(items),
        }
