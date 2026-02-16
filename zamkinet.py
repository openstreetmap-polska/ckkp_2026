# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "beautifulsoup4>=4.14.3",
#     "httpx>=0.28.1",
# ]
# [tool.uv]
# exclude-newer = "2026-02-04T00:00:00Z"
# ///

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
import json
import re

import httpx
from bs4 import BeautifulSoup


@dataclass(frozen=True, slots=True, kw_only=True)
class CastleInfo:
    name: str
    latitude: float | None
    longitude: float | None
    url: str
    state_text: str
    state_description: str
    entry: str
    parking: str
    finding_difficulty_numeric: int
    finding_difficult_text: str
    finding_difficult_description: str
    last_mile_difficulty_numeric: int
    last_mile_difficulty_text: str
    last_mile_difficulty_description: str
    rating_numeric: int
    rating_text: str
    rating_description: str


URL_LIST = "http://zamki.net.pl/alfabetycznie.php"
RE_LATITUDE = re.compile(r"\(([4-5]\d\.\d+)\)")
RE_LONGITUDE = re.compile(r"\(([1-2]\d\.\d+)\)")


def to_geojson(rows: Iterable[CastleInfo]) -> dict:
    result = {
        "type": "FeatureCollection",
        "features": []
    }
    for row in rows:
        if row.longitude and row.latitude:
            result["features"].append(dict(
                type="Feature",
                properties={
                    "nazwa": row.name,
                    "url": row.url,
                    "stan_tekst": row.state_text,
                    "stan_opis": row.state_description,
                    "wstep": row.entry,
                    "parking": row.parking,
                    "trudnosc_odnalezienia_skala": row.finding_difficulty_numeric,
                    "trudnosc_odnalezienia_tekst": row.finding_difficult_text,
                    "trudnosc_odnalezienia_opis": row.finding_difficult_description,
                    "trudnosc_dojscia_skala": row.last_mile_difficulty_numeric,
                    "trudnosc_dojscia_tekst": row.last_mile_difficulty_text,
                    "trudnosc_dojscia_opis": row.last_mile_difficulty_description,
                    "ocena_skala": row.rating_numeric,
                    "ocena_tekst": row.rating_text,
                    "ocena_opis": row.rating_description,
                },
                geometry=dict(
                    type="Point",
                    coordinates=[row.longitude, row.latitude],
                )
            ))
        else:
            print(f"Skipping writing data for castle: {row.name} ({row.url}) due to missing coordinates.")
    return result


def get_list_of_castle_pages() -> list[str]:
    response = httpx.Client().get(url=URL_LIST)
    response.raise_for_status()
    soup = BeautifulSoup(markup=response.text, features="html.parser")
    div = soup.find("div", attrs={"class": "srodek-zp-srodek"})
    assert div is not None
    anchors = div.find_all("a", recursive=True)
    assert anchors is not None and len(anchors) > 0
    urls = [a["href"] for a in anchors]
    return urls


async def get_page_data(client: httpx.AsyncClient, url: str) -> CastleInfo:
    # temp variables
    name = None
    latitude = None
    longitude = None
    state_text = None
    state_description = None
    entry = None
    parking = None
    finding_difficulty_numeric = None
    finding_difficulty_text = None
    finding_difficult_description = None
    last_mile_difficulty_numeric = None
    last_mile_difficulty_text = None
    last_mile_difficulty_description = None
    rating_numeric = None
    rating_text = None
    rating_description = None
    # ---
    response_description = await client.get(url=url, params=dict(z=1))
    response_description.raise_for_status()
    soup_description = BeautifulSoup(markup=response_description.text, features="html.parser")
    title: str = soup_description.find("div", attrs={"class": "srodek-zp-gorap"}).h1.text
    assert title is not None
    name = title
    div_description = soup_description.find("div", attrs={"class": "srodek-zp-srodek"})
    assert div_description is not None
    for row in div_description.find_all("tr"):
        col1: str = row.find("td", attrs={"class": "opis1"}).text
        col2 = row.find("td", attrs={"class": "opis2"}).img
        col3: str = row.find("td", attrs={"class": "opis3"}).text
        assert col1 is not None
        assert col2 is not None
        assert col3 is not None
        match col1:
            case "Stan zachowania:":
                state_text = col2["alt"]
                state_description = col3
            case "Wstęp:":
                entry = col3
            case "Parking:":
                parking = col3
            case "Trudność odnalezienia:":
                img_url = col2["src"]
                img_alt = col2["alt"]
                finding_difficulty_numeric = float(img_url[-5:-4])
                finding_difficulty_text = img_alt
                finding_difficult_description = col3
            case "Trudność dojścia:":
                img_url = col2["src"]
                img_alt = col2["alt"]
                last_mile_difficulty_numeric = float(img_url[-5:-4])
                last_mile_difficulty_text = img_alt
                last_mile_difficulty_description = col3
            case "Subiektywna ocena:":
                img_url = col2["src"]
                img_alt = col2["alt"]
                rating_numeric = float(img_url[-5:-4])
                rating_text =img_alt
                rating_description = col3
    response_location = await client.get(url=url, params=dict(z=2))
    response_location.raise_for_status()
    soup_location = BeautifulSoup(markup=response_location.text, features="html.parser")
    div = soup_location.find(id="licznik")
    if div is not None:
        coordinates = div.text
        lat_match = RE_LATITUDE.findall(coordinates)
        lon_match = RE_LONGITUDE.findall(coordinates)
        if lat_match is not None and len(lat_match) == 1 and lon_match is not None and len(lon_match) == 1:
            latitude = float(lat_match[0])
            longitude = float(lon_match[0])
        else:
            print(f"Coordinates could not be parsed for castle: {name} at: {url}")
    else:
        print(f"Coordinates not found for castle: {name} at: {url}")

    return CastleInfo(
        name=name,
        latitude=latitude,
        longitude=longitude,
        url=url,
        state_text=state_text,
        state_description=state_description,
        entry=entry,
        parking=parking,
        finding_difficulty_numeric=finding_difficulty_numeric,
        finding_difficult_text=finding_difficulty_text,
        finding_difficult_description=finding_difficult_description,
        last_mile_difficulty_numeric=last_mile_difficulty_numeric,
        last_mile_difficulty_text=last_mile_difficulty_text,
        last_mile_difficulty_description=last_mile_difficulty_description,
        rating_numeric=rating_numeric,
        rating_text=rating_text,
        rating_description=rating_description,
    )


async def get_pages_data(urls: Iterable[str]) -> list[CastleInfo]:
    limits = httpx.Limits(
        max_connections=2,
        max_keepalive_connections=1,
        keepalive_expiry=5,
    )
    async with httpx.AsyncClient(timeout=60.0, limits=limits) as client:
        futures = [get_page_data(client=client, url=url) for url in urls]
        results = await asyncio.gather(*futures)
    return results


def main() -> None:
    print("Hello from zamkinet.py!")
    pages_urls = get_list_of_castle_pages()
    print(f"Found {len(pages_urls)} pages to scrape.")
    data = asyncio.run(get_pages_data(urls=pages_urls))
    print("writing data to geojson file.")
    geojson_dict = to_geojson(data)
    with open(f"zamkinet_{date.today().isoformat()}.geojson", "w", encoding="utf-8") as f:
        json.dump(geojson_dict, f, indent=2)
    print("Done.")


if __name__ == "__main__":
    main()
