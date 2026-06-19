# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pyreqwest>=0.12.0",
# ]
# ///

from collections import Counter
from collections.abc import Iterable
from datetime import date, datetime, timedelta
import os

from pyreqwest.client import SyncClientBuilder, SyncClient

try:
    import dotenv
    if dotenv.load_dotenv():
        print(".env loaded.")
except ImportError:
    print("python-dotenv not installed. Relying on env variables set externally.")


def login(client: SyncClient, username: str, password: str) -> str:
    """Logs in to usemaps and returns session token."""
    print("Sending request to log in...")
    response = (
        client
        .post("/api/login")
        .body_json({"data": {"username_or_email": username, "password": password}})
        .build()
        .send()
    )
    cookie = response.get_header("set-cookie")
    if not cookie:
        raise ValueError("Server did not return session token.")
    return cookie.split(";")[0].split("=")[1]


def logout(client: SyncClient, x_access_token: str) -> None:
    print("Sending request to log out...")
    (
        client
        .get("/api/logout")
        .header(key="x-access-token", value=x_access_token, is_sensitive=True)
        .build()
        .send()
    )


def get_layer_data(client: SyncClient, x_access_token: str, dataset_name: str) -> dict:
    print(f"Sending request to get data for dataset: {dataset_name}...")
    return (
        client
        .post(f"/api/v2/datasources-features/read/{dataset_name}")
        .header(key="x-access-token", value=x_access_token, is_sensitive=True)
        .body_json({"data": {}})
        .query(dict(
            with_features=True,
            with_geometry=True,
            with_count=False,
            with_total_count=False,
            with_total_count_without_filter=False,
            with_features_bbox=False,
            with_collection_bbox=False,
            ids_only=False,
            ids_descs_only=False,
            with_relation_values=False,
        ))
        .build()
        .send()
        .json()
    )


def send_discord_message(
    webhook_url: str,
    dt: datetime,
    num_features_updated_yesterday: int,
    num_finished_features: int,
    num_duplicate_features: int,
    num_in_progress_features: int,
    num_finished_castles: int,
    top_authors: Iterable[tuple[str, int]],
) -> None:
    client = (
        SyncClientBuilder()
        .error_for_status()
        .timeout(timedelta(minutes=1))
        .build()
    )
    description = "Autorzy mający najwięcej opracowanych obiektów:\n"
    for idx, author in enumerate(top_authors, start=1):
        description += f"{idx}. {author[0]} ({author[1]})\n"
    print("Sending discord message...")
    (
        client
        .post(webhook_url)
        .body_json({
            "embeds": [
                {
                    "color": 3066993,
                    "title": "📊  Podsumowanie",
                    "description": description,
                    "fields": [
                        {
                            "name": "📅  Liczba obiektów edytowanych wczoraj",
                            "value": f"```🔹 {num_features_updated_yesterday}```",
                            "inline": True,
                        },
                        {
                            "name": "✏️  Liczba obiektów oznaczonych 'w trakcie'",
                            "value": f"```🔹 {num_in_progress_features}```",
                            "inline": True,
                        },
                        {
                            "name": "🏰  Liczba opracowanych zamków",
                            "value": f"```🔹 {num_finished_castles}```",
                            "inline": True,
                        },
                        {
                            "name": "✅️  Liczba obiektów ukończonych",
                            "value": f"```🔹 opracowany: {num_finished_features}\n🔹 duplikat: {num_duplicate_features}```",
                            "inline": True,
                        },
                    ],
                    "footer": {"text": f"Wiadomość wygenerowana automatycznie • Dane pobrane o: {dt.isoformat(sep=' ', timespec="seconds")}."},
                },
            ],
        })
        .build()
        .send()
    )


def main(
    dataset_name: str,
    username: str | None = None,
    password: str | None = None,
    base_url: str | None = None,
    webhook_url: str | None = None,
) -> None:
    username = username or os.getenv("username")
    password = password or os.getenv("password")
    base_url = base_url or os.getenv("base_url")
    webhook_url = webhook_url or os.getenv("webhook_url")
    if not username:
        raise ValueError("You need username as env variable for the script to work.")
    if not password:
        raise ValueError("You need password as env variable for the script to work.")
    if not base_url:
        raise ValueError("You need base_url as env variable for the script to work.")
    with (
        SyncClientBuilder()
        .error_for_status()
        .timeout(timedelta(minutes=5))
        .base_url(base_url)
        .build() as client
    ):
        token = login(client=client, username=username, password=password)
        now = datetime.now()
        data = get_layer_data(client=client, x_access_token=token, dataset_name=dataset_name)
        logout(client=client, x_access_token=token)
    features = data["data"]["features"]
    print(f"There are {len(features)} features in response.")
    finished_features = [f for f in features if f["properties"]["status"] == "opracowany"]
    duplicate_features = [f for f in features if f["properties"]["status"] == "duplikat"]
    in_progress_features = [f for f in features if f["properties"]["status"] == "w trakcie"]
    finished_castles = [f for f in finished_features if f["properties"]["object_type"] == "K01.30.10 - Zamki"]
    features_updated_yesterday = [
        f
        for f in features
        if datetime.fromisoformat(f["properties"]["update_datetime"]).date() == (date.today() - timedelta(days=1))
    ]
    authors = Counter((*(f["properties"]["autor_opracowania"] for f in finished_features), *(f["properties"]["autor_opracowania"] for f in duplicate_features)))
    top_authors = authors.most_common(6)
    if webhook_url:
        send_discord_message(
            webhook_url=webhook_url,
            dt=now,
            num_duplicate_features=len(duplicate_features),
            num_features_updated_yesterday=len(features_updated_yesterday),
            num_finished_castles=len(finished_castles),
            num_finished_features=len(finished_features),
            num_in_progress_features=len(in_progress_features),
            top_authors=top_authors,
        )


if __name__ == "__main__":
    print("Hello from usemaps_get_data.py!")
    main(dataset_name="datasources_lista_assigned_2026_05_20")  # hardcoded dataset id
    print("Bye.")
