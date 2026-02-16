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
import httpx
from bs4 import BeautifulSoup

from dataclasses import dataclass


URL_LISTA_TEMPLATE = "https://zamkisp.pl/index.php?option=com_zamki&view=zamki&Itemid=63&limitstart={limitstart}"
URL_POWIATY_TEMPLATE = "https://zamkisp.pl/index.php?option=com_powiaty&view=powiaty&Itemid=53&limitstart={limitstart}"
URL_GMINY_TEMPLATE = "https://zamkisp.pl/index.php?option=com_gminy&view=gminy&Itemid=62&limitstart={limitstart}"

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

DICT_TYP = {
    "M": "zniszczony",
    "F": "pozostałości",
    "R": "ruiny",
    "Z": "zachowany",
}


@dataclass(frozen=True, slots=True, kw_only=True)
class CastleListRow:
    wojewodztwo: str
    powiat: str
    gmina: str
    zamek_id: str
    nazwa: str
    typ_oryginalny: str
    typ_interpretowany: str | None
    szerokosc_geo: float
    dlugosc_geo: float
    data_wprowadzenia: date | None
    data_aktualizacji: date | None
    opis: str
    url: str


@dataclass(frozen=True, slots=True, kw_only=True)
class CastleListRowDetails:
    opis: str
    szerokosc_geo: float
    dlugosc_geo: float
    data_wprowadzenia: date
    data_aktualizacji: date
    url: str


def to_geojson(rows: Iterable[CastleListRow]) -> dict:
    result = {
        "type": "FeatureCollection",
        "features": []
    }
    for row in rows:
        result["features"].append(dict(
            type="Feature",
            properties={
                "wojewodztwo": row.wojewodztwo,
                "powiat": row.powiat,
                "gmina": row.gmina,
                "zamek_id": row.zamek_id,
                "nazwa": row.nazwa,
                "typ_oryginalny": row.typ_oryginalny,
                "typ_interpretowany": row.typ_interpretowany,
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


async def get_details(client: httpx.AsyncClient, url: str) -> CastleListRowDetails:
    response = await client.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(markup=response.text, features="html.parser")
    main_section = soup.find(id="main_full")
    tables = main_section.find_all("table")
    if len(tables) < 1:
        raise Exception("Expected at least one table in HTML.")
    opis = ""
    szerokosc_geo = None
    dlugosc_geo = None
    data_wprowadzenia = None
    data_aktualizacji = None
    # html is broken and missing tbody
    for row in tables[0].find_all("tr"):
        cells = list(row.find_all("td"))
        if len(cells) > 2:
            if "Opis:" in cells[1].text:
                opis = cells[2].textarea.text.strip()
            elif "(5x.xxxxx):" in cells[1].text:
                szerokosc_geo = float(cells[2].text.strip())
            elif "(1x.xxxx):" in cells[1].text:
                dlugosc_geo = float(cells[2].text.strip())
            elif "Data wprowadzenia:" in cells[1].text:
                val = cells[2].text.strip()
                if val != "0000-00-00":
                    data_wprowadzenia = date.fromisoformat(val)
                else:
                    data_wprowadzenia = None
            elif "Aktualizacja danych:" in cells[1].text:
                val = cells[2].text.strip()
                if val != "0000-00-00":
                    data_aktualizacji = date.fromisoformat(val)
                else:
                    data_aktualizacji = None
    return CastleListRowDetails(
        opis=opis,
        szerokosc_geo=szerokosc_geo,
        dlugosc_geo=dlugosc_geo,
        data_wprowadzenia=data_wprowadzenia,
        data_aktualizacji=data_aktualizacji,
        url=url,
    )


async def get_castles(county_dict: dict[tuple[str, str], str], municipality_dict: dict[tuple[str, str, str], str]) -> list[CastleListRow]:
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
            print(f"get_castles() - offset: {offset} - Response status: {response.status_code} - url: {url}")
            soup = BeautifulSoup(markup=response.text, features="html.parser")
            main_section = soup.find(id="main_full")
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
                zamek_id = ""
                nazwa = ""
                kod_gmi = ""
                typ_oryginalny = ""
                for i, val in enumerate(row.select("td")):
                    match i:
                        case 0:
                            pass
                        case 1:
                            kod_woj = val.text.strip()
                        case 2:
                            kod_pow = val.text.strip()
                        case 3:
                            zamek_id = val.text.strip()
                        case 4:
                            nazwa = val.text.strip()
                        case 5:
                            kod_gmi = val.text.strip()
                        case 6:
                            pass
                        case 7:
                            typ_oryginalny = val.text.strip()
                        case 8:
                            url = "https://zamkisp.pl" + val.find("a").get("href")
                            details = await get_details(client=client, url=url)
                            woj = DICT_WOJEWODZTWA.get(kod_woj)
                            pow = county_dict.get((kod_woj, kod_pow))
                            gmi = municipality_dict.get((kod_woj, kod_pow, kod_gmi))
                            typ_interpretowany = DICT_TYP.get(typ_oryginalny)
                            results.append(
                                CastleListRow(
                                    wojewodztwo=woj,
                                    powiat=pow,
                                    gmina=gmi,
                                    zamek_id=zamek_id,
                                    nazwa=nazwa,
                                    typ_oryginalny=typ_oryginalny,
                                    typ_interpretowany=typ_interpretowany,
                                    szerokosc_geo=details.szerokosc_geo,
                                    dlugosc_geo=details.dlugosc_geo,
                                    data_wprowadzenia=details.data_wprowadzenia,
                                    data_aktualizacji=details.data_aktualizacji,
                                    opis=details.opis,
                                    url=details.url,
                                )
                            )
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
            main_section = soup.find(id="main_full")
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
            main_section = soup.find(id="main_full")
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
    data = asyncio.run(get_castles(county_dict=county_dict, municipality_dict=municipality_dict))
    geojson_dict = to_geojson(data)
    with open(f"zamkisp_{date.today().isoformat()}.geojson", "w", encoding="utf-8") as f:
        json.dump(geojson_dict, f, indent=2)
    print("Done.")


if __name__ == "__main__":
    main()
