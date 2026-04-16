from __future__ import annotations

import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[3]
ENV_PATH = PROJECT_ROOT / "config" / "api_credentials.env"
OUTPUT_PATH = PROJECT_ROOT / "frontend" / "run_logs" / "ccc_project_37.json"
PROJECT_ID = 37
PAGE_SIZE = 200
TIMEOUT_SECONDS = 60


def get_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def build_api_url(base_url: str, endpoint: str) -> str:
    root = base_url.strip().rstrip("/")
    path = "/" + endpoint.strip().lstrip("/")
    if root.lower().endswith("/api"):
        return f"{root}{path}"
    return f"{root}/api{path}"


def extract_token(payload: object) -> str:
    if not isinstance(payload, dict):
        return ""

    data = payload.get("data")
    data_dict = data if isinstance(data, dict) else {}
    token = (
        payload.get("token")
        or payload.get("access_token")
        or payload.get("jwt")
        or data_dict.get("token")
        or data_dict.get("access_token")
    )
    return str(token or "").strip()


def extract_rows(payload: object) -> list[object]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("rows", "items", "results", "result", "data", "purchaseOrders"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                for nested_key in ("rows", "items", "results"):
                    nested_value = value.get(nested_key)
                    if isinstance(nested_value, list):
                        return nested_value
    return []


def main() -> int:
    try:
        if not ENV_PATH.exists():
            raise FileNotFoundError(f"Environment file not found: {ENV_PATH}")

        load_dotenv(ENV_PATH)

        base_url = get_env("CCC_API_BASE_URL")
        username = get_env("CCC_USERNAME")
        password = get_env("CCC_PASSWORD")
        get_env("CCC_API_PAGE_SIZE")

        login_url = build_api_url(base_url, "/user/login")
        login_response = requests.post(
            login_url,
            json={"username": username, "password": password},
            headers={"Accept": "application/json"},
            timeout=TIMEOUT_SECONDS,
        )

        print(f"LOGIN STATUS: {login_response.status_code}")

        if not login_response.ok:
            print(login_response.text)
            return 1

        try:
            login_payload = login_response.json()
        except ValueError:
            print("Login response was not valid JSON.")
            print(login_response.text)
            return 1

        token = extract_token(login_payload)
        print(f"TOKEN RECEIVED: {'YES' if token else 'NO'}")

        if not token:
            print("Login succeeded but no JWT token was found in the response.")
            print(json.dumps(login_payload, indent=2, ensure_ascii=True))
            return 1

        purchase_order_url = build_api_url(base_url, "/purchase_order")
        po_response = requests.get(
            purchase_order_url,
            params={
                "projectId": PROJECT_ID,
                "sorting": "D",
                "currentPage": 1,
                "pageSize": PAGE_SIZE,
            },
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            },
            timeout=TIMEOUT_SECONDS,
        )

        print(f"PO STATUS: {po_response.status_code}")

        if not po_response.ok:
            print(po_response.text)
            return 1

        try:
            po_payload = po_response.json()
        except ValueError:
            print("Purchase order response was not valid JSON.")
            print(po_response.text)
            return 1

        rows = extract_rows(po_payload)
        print(f"PROJECT ID: {PROJECT_ID}")
        print(f"ROWS RETURNED: {len(rows)}")

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(json.dumps(po_payload, indent=2, ensure_ascii=True), encoding="utf-8")
        print(f"Saved to {OUTPUT_PATH.relative_to(PROJECT_ROOT).as_posix()}")
        return 0

    except requests.RequestException as exc:
        print(f"Request error: {exc}")
        return 1
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
