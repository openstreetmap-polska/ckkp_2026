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
from datetime import date
import json
from typing import Any
import httpx
from bs4 import BeautifulSoup

from dataclasses import dataclass
import re

RE_WHITESPACE = re.compile(r"\s+")

URL_LISTA_TEMPLATE = "https://dworyipalace.zamkisp.pl/index.php?option=com_dip&view=dip&Itemid=33&limitstart={limitstart}"
URL_POWIATY_TEMPLATE = "https://dworyipalace.zamkisp.pl/index.php?option=com_powiaty&view=powiaty&Itemid=69&limitstart={limitstart}"
URL_GMINY_TEMPLATE = "https://dworyipalace.zamkisp.pl/index.php?option=com_gminy&view=gminy&Itemid=68&limitstart={limitstart}"

DICT_WOJEWODZTWA = {
    "B": "lubuskie",
    "C": "łódzkie",
    "D": "dolnośląskie",
    "F": "pomorskie",
    "G": "śląskie",
    "J": "warmińsko-mazurskie",
    "K": "podkarpackie",
    "L": "lubelskie",
    "M": "małopolskie",
    "O": "podlaskie",
    "P": "kujawsko-pomorskie",
    "R": "mazowieckie",
    "S": "świętokrzyskie",
    "U": "opolskie",
    "W": "wielkopolskie",
    "Z": "zachodniopomorskie",
}


@dataclass(frozen=True, slots=True, kw_only=True)
class Row:
    nazwa_sp: str
    szerokosc_geo: float
    dlugosc_geo: float
    wojewodztwo: str
    powiat: str
    gmina: str
    dwor_id_sp: str
    zamek_id_sp: str | None
    twierdza_id_sp: str | None
    punkt_oporu_id_sp: str | None
    grod_id_sp: str | None
    data_wprowadzenia: date | None
    data_aktualizacji: date | None
    opis: str
    url: str


def to_geojson(rows: Iterable[Row]) -> dict:
    result = {
        "type": "FeatureCollection",
        "features": []
    }
    for row in rows:
        result["features"].append(dict(
            type="Feature",
            properties={
                "nazwa_sp": row.nazwa_sp,
                "wojewodztwo": row.wojewodztwo,
                "powiat": row.powiat,
                "gmina": row.gmina,
                "dwor_id_sp": row.dwor_id_sp,
                "zamek_id_sp": row.zamek_id_sp,
                "twierdza_id_sp": row.twierdza_id_sp,
                "punkt_oporu_id_sp": row.punkt_oporu_id_sp,
                "grod_id_sp": row.grod_id_sp,
                "data_wprowadzenia": row.data_wprowadzenia.isoformat() if row.data_wprowadzenia else None,
                "data_aktualizacji": row.data_aktualizacji.isoformat() if row.data_aktualizacji else None,
                "opis": row.opis,
                "url": row.url,
            },
            geometry=dict(
                type="Point",
                coordinates=[row.dlugosc_geo, row.szerokosc_geo],
            )
        ))
    return result


async def get_details(
    client: httpx.AsyncClient,
    url: str,
    county_dict: dict[tuple[str, str], str],
    municipality_dict: dict[tuple[str, str, str], str],
) -> Row:
    response = await client.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(markup=response.text, features="html.parser")
    main_section = soup.find(id="userForm")
    tables = main_section.find_all("table")
    if len(tables) < 1:
        raise Exception("Expected at least one table in HTML.")
    try:
        data: dict[str, Any] = {"url": url}
        kod_woj = ""
        kod_pow = ""
        # html is broken and missing tbody
        for item in tables[0].find_all("tr"):
            cells = list(item.find_all("td"))
            if len(cells) > 2:
                col_name = cells[1].text
                col_value = cells[2].text.strip()
                if "Oznaczenie:" in col_name:
                    data["dwor_id_sp"] = col_value
                elif "Nazwa:" in col_name:
                    data["nazwa_sp"] = col_value
                elif "Województwo:" in col_name:
                    kod_woj = col_value
                    data["wojewodztwo"] = DICT_WOJEWODZTWA.get(col_value)
                elif "Powiat:" in col_name:
                    kod_pow = col_value
                    data["powiat"] = county_dict.get((kod_woj, kod_pow))
                elif "PGA:" in col_name:
                    data["gmina"] = municipality_dict.get((kod_woj, kod_pow, col_value))
                elif "Koordynaty:" in col_name:
                    lat, lon = RE_WHITESPACE.sub(
                        repl=" ", 
                        string=(
                            col_value
                            .replace(","," ")
                            .replace(";", " ")
                            .replace("°N", "")
                            .replace("°E", "")
                            .replace("°", "")
                            .strip()
                        )
                    ).split(" ")[:2]
                    data["szerokosc_geo"] = float(lat)
                    data["dlugosc_geo"] = float(lon)
                elif "Zamek:" in col_name:
                    data["zamek_id_sp"] = col_value if col_value and col_value != "---" else None
                elif "Twierdza/Fort:" in col_name:
                    data["twierdza_id_sp"] = col_value if col_value and col_value != "---" else None
                elif "Punkt oporu:" in col_name:
                    data["punkt_oporu_id_sp"] = col_value if col_value and col_value != "---" else None                    
                elif "Gród:" in col_name:
                    data["grod_id_sp"] = col_value if col_value and col_value != "---" else None
                elif "Opis:" in cells[1].text:
                    data["opis"] = cells[2].textarea.text.strip()
                elif "Data wprowadzenia:" in cells[1].text:
                    data["data_wprowadzenia"] = date.fromisoformat(col_value) if col_value != "0000-00-00" else None
                elif "Aktualizacja danych:" in cells[1].text:
                    data["data_aktualizacji"] = date.fromisoformat(col_value) if col_value != "0000-00-00" else None
    except:
        print(f"Problem with parsing entry for url: {url}")
        raise
    return Row(**data)


async def get_palaces(county_dict: dict[tuple[str, str], str], municipality_dict: dict[tuple[str, str, str], str]) -> list[Row]:
    results = []
    keep_running = True
    offset = 0
    step = 100
    limits = httpx.Limits(
        max_connections=3,
        max_keepalive_connections=1,
    )
    async with httpx.AsyncClient(limits=limits) as client:
        while keep_running:
            url = URL_LISTA_TEMPLATE.format(limitstart=offset)
            response = await client.get(url=url)
            response.raise_for_status()
            print(f"get_palaces() - offset: {offset} - Response status: {response.status_code} - url: {url}")
            soup = BeautifulSoup(markup=response.text, features="html.parser")
            main_section = soup.find(id="ja-content")
            tables = main_section.find_all("table")
            if len(tables) < 2:
                raise Exception("Expected at least two tables in HTML.")
            # html is broken and missing tbody
            if len(tables[1].find_all("tr", recursive=False)) == 0:
                keep_running = False
                break
            for row in tables[1].find_all("tr", recursive=False):
                url = "https://dworyipalace.zamkisp.pl" + row.select("td")[9].a["href"]
                row = await get_details(client=client, url=url, county_dict=county_dict, municipality_dict=municipality_dict)
                results.append(row)
            offset += step
    return results


def get_counties() -> dict[tuple[str, str], str]:
    results = {}
    keep_running = True
    offset = 0
    step = 100
    with httpx.Client() as client:
        while keep_running:
            url = URL_POWIATY_TEMPLATE.format(limitstart=offset)
            response = client.get(url=url)
            response.raise_for_status()
            print(f"get_counties() - offset: {offset} - Response status: {response.status_code} - url: {url}")
            soup = BeautifulSoup(markup=response.text, features="html.parser")
            main_section = soup.find(id="ja-content")
            tables = main_section.find_all("table")
            if len(tables) < 2:
                raise Exception("Expected at least two tables in HTML.")
            # html is broken and missing tbody
            if len(tables[1].find_all("tr", recursive=False)) == 0:
                keep_running = False
                break
            for row in tables[1].find_all("tr", recursive=False):
                kod_woj = ""
                kod_pow = ""
                nazwa = ""
                for i, val in enumerate(row.select("td")):
                    match i:
                        case 0:
                            pass
                        case 1:
                            kod_woj = val.text.strip()
                        case 2:
                            kod_pow = val.text.strip()
                        case 3:
                            nazwa = val.text.strip()
                if kod_woj and kod_pow:
                    results[(kod_woj, kod_pow)] = nazwa
            offset += step
    return results


def get_municipalities() -> dict[tuple[str, str, str], str]:
    results = {}
    keep_running = True
    offset = 0
    step = 100
    with httpx.Client() as client:
        while keep_running:
            url = URL_GMINY_TEMPLATE.format(limitstart=offset)
            response = client.get(url=url)
            response.raise_for_status()
            print(f"get_municipalities() - offset: {offset} - Response status: {response.status_code} - url: {url}")
            soup = BeautifulSoup(markup=response.text, features="html.parser")
            main_section = soup.find(id="ja-content")
            tables = main_section.find_all("table")
            if len(tables) < 2:
                raise Exception("Expected at least two tables in HTML.")
            # html is broken and missing tbody
            if len(tables[1].find_all("tr", recursive=False)) == 0:
                keep_running = False
                break
            for row in tables[1].find_all("tr", recursive=False):
                kod_woj = ""
                kod_pow = ""
                kod_gmi = ""
                nazwa = ""
                for i, val in enumerate(row.select("td")):
                    match i:
                        case 0:
                            pass
                        case 1:
                            kod_woj = val.text.strip()
                        case 2:
                            kod_pow = val.text.strip()
                        case 3:
                            kod_gmi = val.text.strip()
                        case 4:
                            nazwa = val.text.strip()
                if kod_woj and kod_pow:
                    results[(kod_woj, kod_pow, kod_gmi)] = nazwa
            offset += step
    return results


def main() -> None:
    print("Hello from zawody.py!")
    county_dict = get_counties()
    print(f"Utworzono słownik powiatów ({len(county_dict)} rekordów).")
    municipality_dict = get_municipalities()
    print(f"Utworzono słownik gmin ({len(municipality_dict)} rekordów).")
    data = asyncio.run(get_palaces(county_dict=county_dict, municipality_dict=municipality_dict))
    geojson_dict = to_geojson(data)
    with open(f"dworysp_{date.today().isoformat()}.geojson", "w", encoding="utf-8") as f:
        json.dump(geojson_dict, f, indent=2)
    print("Done.")


if __name__ == "__main__":
    main()
