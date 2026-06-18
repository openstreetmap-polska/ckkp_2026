# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pyreqwest>=0.12.0",
# ]
# ///

# example command: uv run --with python-dotenv usemaps_upload_data.py
# expects .env file with values like
# username=...
# password=...
# base_url=...


from datetime import timedelta
import json
from pathlib import Path
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


def upload_feature(client: SyncClient, x_access_token: str, dataset_name: str, feature: dict) -> None:
    return (
        client
        .post(f"/api/v2/datasources-features/create/{dataset_name}")
        .header(key="x-access-token", value=x_access_token, is_sensitive=True)
        .body_json({
            "data": {
                "feature": feature,
            }
        })
        .build()
        .send()
        .json()
    )


def main(
    comparison_result_geojson: Path,
    dataset_name: str,
    username: str | None = None,
    password: str | None = None,
    base_url: str | None = None,
) -> None:
    username = username or os.getenv("username")
    password = password or os.getenv("password")
    base_url = base_url or os.getenv("base_url")
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
        .build() as client,
        comparison_result_geojson.open("r", encoding="utf-8") as fp
    ):
        token = login(client=client, username=username, password=password)
        data = get_layer_data(client=client, x_access_token=token, dataset_name=dataset_name)
        features = data["data"]["features"]
        print(f"There are {len(features)} features in response.")
        already_entered_osm_urls = set(f["properties"]["osm_url"] for f in features)
        comparison = json.load(fp)
        rows = [f for f in comparison["features"] if f["properties"]["result"] == "only_osm"]
        all_osm_urls_to_add = set(f"https://osm.org/{f["properties"]["osm_id"]}" for f in rows)
        osm_urls_to_add = all_osm_urls_to_add.difference(already_entered_osm_urls)
        print(f"{len(already_entered_osm_urls)} rows have osm_urls already.")
        print(f"{len(osm_urls_to_add)} rows need to be added.")
        counter = 0
        for row in rows:
            url = f"https://osm.org/{row["properties"]["osm_id"]}"
            if url in osm_urls_to_add:
                counter += 1
                coordinates = [row["geometry"]["coordinates"][0], row["geometry"]["coordinates"][1]]
                upload_feature(
                    client=client,
                    x_access_token=token,
                    dataset_name=dataset_name,
                    feature={
                        "osm_url": url,
                        "geom": {"coordinates": coordinates, "type": "Point", "crs": {"type": "name", "properties": {"name": "EPSG:4326"}}},
                    }
                )
                if counter % 50 == 0:
                    print(f"Uploaded {counter} features so far.")
        print(f"Uploaded {counter} features.")
        logout(client=client, x_access_token=token)


if __name__ == "__main__":
    print("Hello from usemaps_upload_data.py!")
    main(
        comparison_result_geojson=Path("compare_osm_and_list_2026-06-14.geojson"),
        dataset_name="datasources_lista_assigned_2026_05_20",  # hardcoded dataset name
    )
    print("Bye.")
