# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "duckdb==1.5.2",
#     "pytz>=2026.1.post1",
#     "pyreqwest>=0.12.0",
# ]
# ///

import os
import time
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Callable, cast

import duckdb
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


def connect_to_db(path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(
        database=path, config={"storage_compatibility_version": "latest"}
    )
    conn.install_extension("httpfs")
    conn.install_extension("spatial")
    conn.load_extension("httpfs")
    conn.load_extension("spatial")
    for stmt in [
        "SET preserve_insertion_order = false",
        "SET geometry_always_xy = true",
        "SET s3_region = 'us-west-2'",
    ]:
        conn.execute(stmt)
    conn.execute("""
create table if not exists steps (
  name text primary key,
  executed_at timestamp with time zone not null
)
    """)
    return conn


def step_executed_at(db: duckdb.DuckDBPyConnection, step_name: str) -> datetime | None:
    db.execute("SELECT executed_at FROM steps WHERE name = ?", [step_name])
    result = db.fetchone()
    if result is None:
        return None
    else:
        return result[0]


def check_table_not_empty(db: duckdb.DuckDBPyConnection, table_name: str) -> None:
    db.execute(f"SELECT count(*) FROM {table_name}")
    cnt = db.fetchone()[0]  # type: ignore
    print(f"{table_name}: {cnt} rows")
    if not cnt:
        raise ValueError(f"{table_name} table shouldn't be empty after loading data")


def step(step_name: str) -> Callable[[Callable[..., None]], Callable[..., None]]:
    """Decorator for functions with each step that handles common stuff like registering that step was run and prints consistent messages."""

    def decorator(func: Callable[..., None]) -> Callable[..., None]:
        @wraps(func)
        def wrapper(
            db: duckdb.DuckDBPyConnection, *args, overwrite: bool = False, **kwargs
        ) -> None:
            if (
                step_executed_at(db=db, step_name=step_name) is not None
                and not overwrite
            ):
                print(f"🦥 Skipping step: {step_name}.")
                return
            print(f"🪏  Executing step: {step_name}...")
            started_at = time.perf_counter()
            db.begin()
            db.execute("DELETE FROM steps WHERE name = ?", [step_name])
            try:
                result = func(db, *args, **kwargs)
            except Exception:
                db.rollback()
                raise
            db.execute(
                "INSERT INTO steps(name, executed_at) VALUES(?, now())", [step_name]
            )
            db.commit()
            elapsed = timedelta(seconds=time.perf_counter() - started_at)
            print(f"✅️ Executing step: {step_name}. DONE in {elapsed}.")
            return result

        return wrapper

    return decorator


@step("load_countries")
def load_countries(db: duckdb.DuckDBPyConnection) -> None:
    db.execute("DROP TABLE IF EXISTS countries")
    db.execute("""
create table countries as
select st_transform(geom, 'OGC:CRS84') as geom, tags::json as tags
from st_read('/vsigzip//vsicurl/https://osm-countries-geojson.monicz.dev/osm-countries-0-00001.geojson.gz')
    """)
    x = db.execute(
        "select count(*) from countries where tags ->> 'ISO3166-1' = 'PL'"
    ).fetchone()
    if x is None or x[0] == 0:
        raise RuntimeError(
            "Something went wrong when loading countries. Poland is not in the list."
        )


@step("load_overture_places")
def load_overture_places(
    db: duckdb.DuckDBPyConnection,
    *,
    overture_release: str,
) -> None:
    db.execute("DROP TABLE IF EXISTS overture_places")
    db.execute(f"""
create table overture_places as
select
  id as overture_id,
  basic_category as overture_category,
  geometry,
  st_transform(geometry, 'EPSG:2180') as geom_2180,
  array_to_string([s.dataset for s in sources], ', ') as overture_source_datasets,
  names.primary as overture_name,
  confidence as overture_confidence,
  array_to_string(websites, ' | ') as overture_websites,
  array_to_string(socials, ' | ') as overture_socials,
  operating_status as overture_operating_status
from 's3://overturemaps-us-west-2/release/{overture_release}/theme=places/type=place/*.parquet' as places
where
  basic_category in ('castle', 'fort', 'ruins', 'ruin', 'historic_site', 'palace', 'landmark_and_historical_building', 'museum', 'history_museum')
  and bbox.xmin >= 14.06
  and bbox.xmax <= 24.03
  and bbox.ymin >= 49.0
  and bbox.ymax <= 55.04
  and st_intersects(places.geometry, (select geom from countries where tags ->> 'ISO3166-1' = 'PL'))
    """)
    check_table_not_empty(db=db, table_name="overture_places")


@step("export_overture_places")
def export_overture_places(
    db: duckdb.DuckDBPyConnection, *, export_gpkg_path: Path
) -> None:
    db.execute(
        f"COPY (select * exclude(geom_2180) from overture_places) TO '{export_gpkg_path.absolute().as_uri()}' WITH (FORMAT gdal, DRIVER 'GPKG')"
    )


@step("load_castles")
def load_castles(
    db: duckdb.DuckDBPyConnection, *, zamkinet_path: Path, zamkisp_path: Path
) -> None:
    db.execute("DROP TABLE IF EXISTS zamkinet")
    db.execute("DROP TABLE IF EXISTS zamkisp")
    db.execute(f"""
create table zamkinet as
select distinct on(geom) * exclude (OGC_FID), row_number() over() as rn, st_transform(geom, 'EPSG:2180') as geom_2180
from st_read('{zamkinet_path}')
    """)
    db.execute(f"""
create table zamkisp as
select distinct on(geom) * exclude (OGC_FID), row_number() over() as rn, st_transform(geom, 'EPSG:2180') as geom_2180
from st_read('{zamkisp_path}')
    """)
    check_table_not_empty(db=db, table_name="zamkinet")
    check_table_not_empty(db=db, table_name="zamkisp")


@step("deduplicate_castles")
def deduplicate_castles(db: duckdb.DuckDBPyConnection) -> None:
    db.execute("DROP TABLE IF EXISTS castles")
    db.execute("""
create table castles as
with
matched as (
  select sp.rn as sp_rn, net.rn as net_rn
  from zamkisp sp, zamkinet net
  where ST_DWithin(sp.geom_2180, net.geom_2180, 200.0)
),
not_matched_sp as (
  select *
  from zamkisp sp
  anti join matched on sp.rn=matched.sp_rn
),
not_matched_net as (
  select *
  from zamkinet net
  anti join matched on net.rn=matched.net_rn
),
matched_data as (
  select
    sp.nazwa as nazwa_sp,
    net.nazwa as nazwa_net,
    sp.url as url_sp,
    net.url as url_net,
    sp.zamek_id as zamek_id_sp,
    sp.wojewodztwo,
    sp.powiat,
    sp.gmina,
    sp.typ_oryginalny,
    sp.typ_interpretowany,
    sp.data_wprowadzenia,
    sp.data_aktualizacji,
    sp.opis,
    net.stan_tekst,
    net.stan_opis,
    net.wstep,
    net.parking,
    net.trudnosc_odnalezienia_skala,
    net.trudnosc_odnalezienia_tekst,
    net.trudnosc_odnalezienia_opis,
    net.trudnosc_dojscia_skala,
    net.trudnosc_dojscia_tekst,
    net.trudnosc_dojscia_opis,
    net.ocena_skala,
    net.ocena_tekst,
    net.ocena_opis,
    st_centroid(st_collect([sp.geom, net.geom])) as geom,
    st_centroid(st_collect([sp.geom_2180, net.geom_2180])) as geom_2180
  from matched
  join zamkisp sp on sp.rn=matched.sp_rn
  join zamkinet net on net.rn=matched.net_rn
),
unioned as (
  select * from matched_data
  union all by name
  select * exclude(rn) rename(nazwa as nazwa_sp, url as url_sp, zamek_id as zamek_id_sp) from not_matched_sp
  union all by name
  select * exclude(rn) rename(nazwa as nazwa_net, url as url_net) from not_matched_net
)
select
  row_number() over() as rn,
  unioned.*,
  case
  	when typ_interpretowany in ('zniszczony', 'pozostałości') or stan_tekst = 'Brak śladów' then 'odrzucony automatycznie'
  	else null
  end status
from unioned
    """)
    check_table_not_empty(db=db, table_name="castles")


@step("export_castles")
def export_castles(
    db: duckdb.DuckDBPyConnection, *, export_gpkg_path: Path, export_geojson_path: Path
) -> None:
    db.execute(
        f"COPY (select * exclude(geom_2180, rn) from castles) TO '{export_gpkg_path.absolute().as_uri()}' WITH (FORMAT gdal, DRIVER 'GPKG')"
    )
    db.execute(
        f"COPY (select * exclude(geom_2180, rn) from castles) TO '{export_geojson_path.absolute().as_uri()}' WITH (FORMAT gdal, DRIVER 'GeoJSON')"
    )


@step("load_palaces")
def load_palaces(db: duckdb.DuckDBPyConnection, *, dworysp_path: Path) -> None:
    db.execute("DROP TABLE IF EXISTS palaces")
    db.execute(f"""
create table palaces as
select distinct on(geom) * exclude (OGC_FID), row_number() over() as rn, st_transform(geom, 'EPSG:2180') as geom_2180
from st_read('{dworysp_path.absolute().as_uri()}')               
    """)
    check_table_not_empty(db=db, table_name="palaces")


@step("union_castles_palaces")
def union_castles_palaces(db: duckdb.DuckDBPyConnection) -> None:
    db.execute("DROP TABLE IF EXISTS castles_and_palaces")
    db.execute("""
create table castles_and_palaces as
with
matches as (
    select castles.rn as castle_rn, palaces.rn as palace_rn
    from castles, palaces
    where ST_DWithin(castles.geom_2180, palaces.geom_2180, 25.0)               
),
matched_data as (
    select
        castles.status,
        'K01.30.10 - Zamki' as object_type,
        coalesce(castles.nazwa_sp, castles.nazwa_net, palaces.nazwa_sp) as name,
        coalesce(castles.wojewodztwo, palaces.wojewodztwo) as province,
        coalesce(castles.powiat, palaces.powiat) as district,
        coalesce(castles.gmina, palaces.gmina) as municipality,
        concat(castles.opis, e'\n\n' || palaces.opis, e'\n\n' || castles.stan_opis, e'\n\n' || castles.wstep, e'\n\n' || castles.parking, e'\n\n' || castles.trudnosc_odnalezienia_opis) as notes,
        coalesce(castles.url_sp, palaces.url) as url_sp,
        castles.url_net,
        st_centroid(st_collect([castles.geom, palaces.geom])) as geom,
        st_centroid(st_collect([castles.geom_2180, palaces.geom_2180])) as geom_2180
    from matches
    join castles on matches.castle_rn=castles.rn
    join palaces on matches.palace_rn=palaces.rn
),
not_matched_palaces as (
    select
        palaces.nazwa_sp as name,
        palaces.wojewodztwo as province,
        palaces.powiat as district,
        palaces.gmina as municipality,
        palaces.opis as notes,
        palaces.url as url_sp,
        palaces.geom,
        palaces.geom_2180
    from palaces
    anti join matches on palaces.rn=matches.palace_rn
),
not_matched_castles as (
    select
        'K01.30.10 - Zamki' as object_type,
        castles.status,
        coalesce(castles.nazwa_sp, castles.nazwa_net) as name,
        castles.wojewodztwo as province,
        castles.powiat as district,
        castles.gmina as municipality,
        concat(castles.opis, e'\n\n' || castles.stan_opis, e'\n\n' || castles.wstep, e'\n\n' || castles.parking, e'\n\n' || castles.trudnosc_odnalezienia_opis) as notes,
        castles.url_sp,
        castles.url_net,
        castles.geom,
        castles.geom_2180
    from castles
    anti join matches on castles.rn=matches.castle_rn
),
unioned as (
    select * from matched_data
    union all by name
    select * from not_matched_castles
    union all by name
    select * from not_matched_palaces
)
select *
from unioned
    """)
    check_table_not_empty(db=db, table_name="castles_and_palaces")


@step("enrich_data")
def enrich_data(db: duckdb.DuckDBPyConnection, *, addresses_path: Path) -> None:
    db.execute("DROP TABLE IF EXISTS castles_and_palaces_enriched")
    db.execute(f"""
CREATE TABLE castles_and_palaces_enriched as
with
addresses as (
    select
        kod_pocztowy as post_code,
        concat(wojewodztwo, ', ', powiat, ', ', gmina, ', ', miejscowosc, ', ' || ulica, ' ', numer_porzadkowy) as nearest_address,
        geometry
    from '{addresses_path.absolute().as_uri()}'
),
data as (
    select
        c.*,
        ov.*,
        addr.*
    from castles_and_palaces c
    left join lateral (
        select
            overture_name,
            overture_websites,
            overture_socials
        from overture_places
        where ST_DWithin(c.geom_2180, overture_places.geom_2180, 200.0)
        order by ST_Distance(c.geom_2180, overture_places.geom_2180)
        limit 1
    ) ov on true
    left join lateral (
        select
            post_code,
            nearest_address
        from addresses
        where ST_DWithin(c.geom_2180, addresses.geometry, 200.0)
        order by ST_Distance(c.geom_2180, addresses.geometry)
        limit 1
    ) addr on true
)
select
    row_number() over() as id,
    data.*
from data
    """)
    check_table_not_empty(db=db, table_name="castles_and_palaces_enriched")


@step("export_castles_and_palaces")
def export_castles_and_palaces(
    db: duckdb.DuckDBPyConnection, *, export_gpkg_path: Path, export_geojson_path: Path
) -> None:
    db.execute(
        f"COPY (select * exclude(geom_2180) from castles_and_palaces_enriched) TO '{export_gpkg_path.absolute().as_uri()}' WITH (FORMAT gdal, DRIVER 'GPKG')"
    )
    db.execute(
        f"COPY (select * exclude(geom_2180) from castles_and_palaces_enriched) TO '{export_geojson_path.absolute().as_uri()}' WITH (FORMAT gdal, DRIVER 'GeoJSON')"
    )


@step("standardize_castles_and_palaces_data")
def standardize_castles_and_palaces_data(db: duckdb.DuckDBPyConnection) -> None:
    db.execute("DROP TABLE IF EXISTS standardized_castles_and_palaces")
    db.execute("""
create table standardized_castles_and_palaces as
    with
    woj(province) as (from read_csv('wojewodztwa.csv', header := false)),
    pow(district) as (from read_csv('powiaty.csv', header := false)),
    gmi(municipality) as (from read_csv('gminy.csv', header := false)),
    data as (
        select
            status,
            object_type,
            name,
            province,
            trim(replace(district, ' miasto', '')) as district,
            trim(replace(municipality, ' (miasto)', '')) as municipality,
            trim(notes) as notes,
            url_sp,
            url_net,
            geom,
            overture_name,
            overture_websites,
            overture_socials,
            post_code as postcode,
            nearest_address
        from castles_and_palaces_enriched as t
    )
    select
        data.status,
        data.object_type,
        data.name,
        'Polska' as country,
        woj.province,
        pow.district,
        gmi.municipality,
        data.postcode,
        data.nearest_address,
        data.notes,
        null::text as state,
        null::text as description,
        null::text as accessibility,
        null::text as veracity_score,
        null::text as wikidata,
        null::text as wikipedia,
        null::text as osm_url,
        null::text as osm_status,
        null::text as autor_opracowania,
        data.overture_name,
        data.overture_websites,
        data.overture_socials,
        data.url_sp,
        data.url_net,
        data.geom
    from data
    left join woj on lower(data.province)=lower(woj.province)
    left join pow on lower(data.district)=lower(pow.district)
    left join gmi on lower(data.municipality)=lower(gmi.municipality)
    """)


@step("export_standardized_castles_and_palaces")
def export_standardized_castles_and_palaces(
    db: duckdb.DuckDBPyConnection, *, export_gpkg_path: Path, export_geojson_path: Path
) -> None:
    db.execute(
        f"COPY (select * from standardized_castles_and_palaces) TO '{export_gpkg_path.absolute().as_uri()}' WITH (FORMAT gdal, DRIVER 'GPKG')"
    )
    db.execute(
        f"COPY (select * from standardized_castles_and_palaces) TO '{export_geojson_path.absolute().as_uri()}' WITH (FORMAT gdal, DRIVER 'GeoJSON')"
    )


@step("assign_standardized_castles_and_palaces_and_export")
def assign_standardized_castles_and_palaces_and_export(
    db: duckdb.DuckDBPyConnection, *, export_gpkg_path: Path, group_size: int = 250
) -> None:
    db.execute("UPDATE standardized_castles_and_palaces SET autor_opracowania = NULL")
    db.execute(f"""
CREATE TEMP TABLE rows_to_update AS
with recursive
settings as (
    select
        {group_size} as group_size,
        ceil((select count(*) from standardized_castles_and_palaces) / group_size)::int as number_of_groups
),
groups as (
    select
        1 as group_number,
        (select group_size from settings) * (group_number - 1) as rn_gt,
        (select group_size from settings) * (group_number) as rn_lte
    union all
    select
        group_number + 1,
        (select group_size from settings) * (group_number),
        (select group_size from settings) * (group_number + 1)
    from groups
    where group_number <= (select number_of_groups - 1 from settings)
),
people as (
    select
        column0 as autor_opracowania,
        row_number() over() as rn
    from read_csv('autorzy.csv', header := false)
),
groups_with_people as (
    select
        groups.*,
        people.autor_opracowania
    from groups
    left join people on groups.group_number = people.rn
),
t as (
    select
        t.rowid,
        row_number() over() as rn
    from standardized_castles_and_palaces as t 
),
mapping as (
    select
        t.rowid as id,
        g.autor_opracowania as new_autor
    from t
    join groups_with_people as g on t.rn > g.rn_gt and t.rn <= g.rn_lte
)
select * from mapping
    """)
    db.execute("""
update standardized_castles_and_palaces as t
set autor_opracowania = new_autor
from rows_to_update
where t.rowid = id
    """)
    db.execute(
        f"COPY (select * from standardized_castles_and_palaces) TO '{export_gpkg_path.absolute().as_uri()}' WITH (FORMAT gdal, DRIVER 'GPKG')"
    )


@step("load_boundaries")
def load_boundaries(
    db: duckdb.DuckDBPyConnection,
    *,
    woj_shp_path: Path,
    pow_shp_path: Path,
    gmi_shp_path: Path,
) -> None:
    db.execute("DROP TABLE IF EXISTS woj")
    db.execute("DROP TABLE IF EXISTS pow")
    db.execute("DROP TABLE IF EXISTS gmi")
    db.execute(f"""
create table woj as
select JPT_KOD_JE as kod_woj, JPT_NAZWA_ as name_woj, st_transform(geom, 'epsg:4258', 'epsg:4326') as geom
from st_read('{woj_shp_path.absolute().as_uri()}')        
    """)
    db.execute(f"""
create table pow as
select JPT_KOD_JE as kod_pow, replace(JPT_NAZWA_, 'powiat ', '') as name_pow, st_transform(geom, 'epsg:4258', 'epsg:4326') as geom
from st_read('{pow_shp_path.absolute().as_uri()}')        
    """)
    db.execute(f"""
create table gmi as
select JPT_KOD_JE as kod_gmi, JPT_NAZWA_ as name_gmi, st_transform(geom, 'epsg:4258', 'epsg:4326') as geom
from st_read('{gmi_shp_path.absolute().as_uri()}')        
    """)


@step("load_usemaps_data")
def load_usemaps_data(db: duckdb.DuckDBPyConnection) -> None:
    username = os.getenv("username")
    password = os.getenv("password")
    base_url = os.getenv("base_url")
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
        data = get_layer_data(client=client, x_access_token=token, dataset_name="datasources_lista_assigned_2026_05_20")  # hardcoded dataset id
        logout(client=client, x_access_token=token)
    features: list[dict] = data["data"]["features"]
    if len(features) == 0:
        raise ValueError("Usemaps returned 0 features.")
    else:
        print(f"There are {len(features)} features in response.")
    db.execute("DROP TABLE IF EXISTS usemaps_features")
    db.execute("""
CREATE TABLE usemaps_features(
id int,
accessibility text,
--attachments_count text,
autor_opracowania text,
country text,
create_datetime text,
--create_user text,
description text,
district text,
--history_count text,
municipality text,
name text,
nearest_address text,
notes text,
--notes_count text,
object_type text,
osm_status text,
osm_url text,
overture_name text,
overture_socials text,
overture_websites text,
postcode text,
province text,
state text,
status text,
update_datetime text,
--update_user text,
url_net text,
url_sp text,
veracity_score text,
wikidata text,
wikipedia text,
geom Geometry('EPSG:4326'),
geom_2180 Geometry('EPSG:2180')
)
""")
    for feature in features:
        coords: list[float] = feature["geometry"]["coordinates"]
        geom = f"POINT({coords[0]} {coords[1]})"
        p = cast(dict, feature["properties"]).copy()
        p.pop("attachments_count")
        p.pop("create_user")
        p.pop("history_count")
        p.pop("notes_count")
        p.pop("update_user")
        db.execute(f"INSERT INTO usemaps_features VALUES({', '.join(['?']*28)}, ST_Transform(?::GEOMETRY, 'EPSG:4326', 'EPSG:2180'))", [feature["id"], *p.values(), geom, geom])
    check_table_not_empty(db=db, table_name="usemaps_features")


@step("prepare_nonspatial_list")
def prepare_nonspatial_list(db: duckdb.DuckDBPyConnection, *, addresses_path: Path) -> None:
    db.execute("DROP TABLE IF EXISTS castles_palaces_list_nonspatial")
    db.execute(f"""
CREATE TABLE castles_palaces_list_nonspatial as
with
addresses as (
    select
        kod_pocztowy as post_code,
        concat(wojewodztwo, ', ', powiat, ', ', gmina, ', ', miejscowosc, ', ' || ulica, ' ', numer_porzadkowy) as nearest_address,
        geometry
    from '{addresses_path.absolute().as_uri()}'
),
data as (
    select
        f.object_type,
        f.name,
        f.country,
        woj.name_woj as province,
        pow.name_pow as district,
        gmi.name_gmi as municipality,
        addr.post_code as post_code,
        addr.nearest_address,
        f.state,
        f.accessibility,
        f.veracity_score,
        f.description,
        f.wikipedia,
        f.wikidata
    from usemaps_features f
    left join woj on ST_Intersects(f.geom, woj.geom)
    left join pow on ST_Intersects(f.geom, pow.geom)
    left join gmi on ST_Intersects(f.geom, gmi.geom)
    left join lateral (
        select
            post_code,
            nearest_address
        from addresses
        where ST_DWithin(f.geom_2180, addresses.geometry, 2000.0)
        order by ST_Distance(f.geom_2180, addresses.geometry)
        limit 1
    ) addr on true
    where 
        (object_type in ('K01.30.20 - Dwory obronne', 'K01.30.10 - Zamki', 'K01.20.20 - Dwory szlacheckie', 'K01.20.30 - Dwory inne') and status = 'opracowany')
        or
        (object_type = 'K01.20.10 - Pałace' and status = 'opracowany pal')
)
select *
from data
""")
    check_table_not_empty(db=db, table_name="castles_palaces_list_nonspatial")


@step("export_nonspatial_list")
def export_nonspatial_list(db: duckdb.DuckDBPyConnection, *, export_csv_path: Path) -> None:
    db.execute(
        f"COPY (select * from castles_palaces_list_nonspatial) TO '{export_csv_path.absolute().as_uri()}' WITH (FORMAT CSV, HEADER)"
    )


@step("load_reviewed_data")
def load_reviewed_data(db: duckdb.DuckDBPyConnection, *, import_gpkg_path: Path) -> None:
    db.execute("DROP TABLE IF EXISTS list_reviewed")
    db.execute("""
        create temporary table t as
        select *, split(object_type, ' -')[1] as ot
        from st_read(?)""",
        (import_gpkg_path.absolute().as_uri(),)
    )
    db.execute("select count(*) from t")
    num = db.fetchone()[0] # type: ignore
    rids = generate_random_ids(num)
    db.execute("create temporary table i(rid text not null)")
    db.executemany("insert into i(rid) values (?)", [(rid,) for rid in rids])
    db.execute("""
    create table list_reviewed as
    select
        ST_ASTEXT(geom) as wkt_wgs84,
        ST_X(geom) as x_wgs84,
        ST_Y(geom) as y_wgs84,
        country,
        province,
        district,
        municipality,
        post_code as postcode,
        object_type as object_type_label,
        split(object_type, ' - ')[1] as object_type,
        split(object_type, ' - ')[2] as object_type_name,
        veracity_score,
        accessibility,
        state,
        concat(ot, '.', rid) as otar_id,
        name,
        description,
        wikidata,
        case when wikidata <> 'n/a' then concat('https://www.wikidata.org/wiki/', wikidata) else wikidata end wikidata_url,
        wikipedia,
        case
          when wikipedia <> 'n/a' then concat('https://', split(wikipedia, ':')[1], '.wikipedia.org/wiki/', url_encode(replace(split(wikipedia, ':')[2], ' ', '_')))
          else wikipedia
        end wikipedia_url,
        now()::date as published,
        geom
    from t positional join i
    """)
    check_table_not_empty(db=db, table_name="list_reviewed")


@step("export_reviewed_formatted_data")
def export_reviewed_formatted_data(db: duckdb.DuckDBPyConnection, *, export_gpkg_path: Path) -> None:
    db.execute(
        f"COPY (select * exclude(object_type_label) from list_reviewed) TO '{export_gpkg_path.absolute().as_uri()}' WITH (FORMAT gdal, DRIVER 'GPKG')"
    )


@step("import_list_updates")
def import_list_updates(db: duckdb.DuckDBPyConnection, *, csv_path: Path, gpkg_path: Path) -> None:
    db.execute("DROP TABLE IF EXISTS list_descriptions_updates")
    db.execute("DROP TABLE IF EXISTS list_features_updates")
    db.execute(f"CREATE TABLE list_descriptions_updates as SELECT * FROM read_csv('{csv_path.absolute().as_uri()}')")
    db.execute(f"CREATE TABLE list_features_updates as SELECT * FROM ST_Read('{gpkg_path.absolute().as_uri()}')")
    db.execute("""
CREATE TABLE list_reviewed_v2 as
SELECT
    ST_ASTEXT(f.geom) as wkt_wgs84,
    ST_X(f.geom) as x_wgs84,
    ST_Y(f.geom) as y_wgs84,
    f.country,
    coalesce(woj.name_woj, f.province) as province,
    coalesce(pow.name_pow, f.district) as district,
    coalesce(gmi.name_gmi, f.municipality) as municipality,
    f.postcode,
    f.object_type,
    f.object_type_name,
    f.veracity_score,
    f.accessibility,
    f.state,
    otar_id,
    f.name,
    coalesce(u.description_500, f.description) as description,
    f.wikidata,
    case when f.wikidata <> 'n/a' then concat('https://www.wikidata.org/wiki/', f.wikidata) else f.wikidata end wikidata_url,
    f.wikipedia,
    case
        when f.wikipedia <> 'n/a' then concat('https://', split(f.wikipedia, ':')[1], '.wikipedia.org/wiki/', url_encode(replace(split(f.wikipedia, ':')[2], ' ', '_')))
        else f.wikipedia
    end wikipedia_url,
    f.published,
    f.geom
FROM list_features_updates f
LEFT JOIN list_descriptions_updates u USING(otar_id)
left join woj on ST_Intersects(f.geom, woj.geom)
left join pow on ST_Intersects(f.geom, pow.geom)
left join gmi on ST_Intersects(f.geom, gmi.geom)
    """)
    check_table_not_empty(db=db, table_name="list_reviewed_v2")


@step("export_reviewed_formatted_data_v2")
def export_reviewed_formatted_data_v2(
    db: duckdb.DuckDBPyConnection,
    *,
    export_gpkg_path: Path,
    export_geojson_path: Path,
    export_csv_path: Path,
) -> None:
    db.execute(
        f"COPY (select * from list_reviewed_v2 order by otar_id) TO '{export_gpkg_path.absolute().as_uri()}' WITH (FORMAT gdal, DRIVER 'GPKG')"
    )
    db.execute(
        f"COPY (select * from list_reviewed_v2 order by otar_id) TO '{export_geojson_path.absolute().as_uri()}' WITH (FORMAT gdal, DRIVER 'GeoJSON', LAYER_CREATION_OPTIONS ('ID_GENERATE=YES'))"
    )
    db.execute(
        f"COPY (select * exclude(geom) from list_reviewed_v2 order by otar_id) TO '{export_csv_path.absolute().as_uri()}' WITH (FORMAT CSV, HEADER)"
    )


def generate_random_ids(num_ids: int) -> list[str]:
    """Identifier generator
    CHARSET: uppercase letters excluding I and O; digits 1-9 excluding 0
    Length: 6
    """
    import secrets

    CHARSET = 'ABCDEFGHJKLMNPQRSTUVWXYZ123456789'
    ID_LENGTH = 6

    def generate_id(length: int = ID_LENGTH) -> str:
        return ''.join(secrets.choice(CHARSET) for _ in range(length))

    result = [generate_id() for _ in range(num_ids)]
    return result


def main(
    *,
    db_path: Path,
    zamkinet_path: Path,
    zamkisp_path: Path,
    zamki_gpkg: Path,
    zamki_geojson: Path,
    dworysp_path: Path,
    addresses_path: Path,
    export_gpkg_path: Path,
    export_geojson_path: Path,
    export_standardized_gpkg_path: Path,
    export_standardized_geojson_path: Path,
    export_assigned_gpkg_path: Path,
    overture_release: str,
    woj_shp_path: Path,
    pow_shp_path: Path,
    gmi_shp_path: Path,
    export_nonspatial_csv_path: Path,
    import_reviewed_gpkg_path: Path,
    export_reviewed_formatted_gpkg_path: Path,
    import_descriptions_updates_path: Path,
    import_list_updates_path: Path,
    export_reviewed_v2_formatted_gpkg_path: Path,
    export_reviewed_v2_formatted_geojson_path: Path,
    export_reviewed_v2_formatted_csv_path: Path,
    overwrite: bool = False,
) -> None:
    print("🔌 Connecting to:", db_path)
    db = connect_to_db(path=db_path)
    load_countries(db=db, overwrite=overwrite)
    load_overture_places(
        db=db,
        overture_release=overture_release,
        overwrite=overwrite,
    )
    export_overture_places(db=db, export_gpkg_path=overture_gpkg, overwrite=overwrite)
    load_castles(
        db=db,
        zamkinet_path=zamkinet_path,
        zamkisp_path=zamkisp_path,
        overwrite=overwrite,
    )
    deduplicate_castles(db=db)
    export_castles(
        db=db,
        export_gpkg_path=zamki_gpkg,
        export_geojson_path=zamki_geojson,
    )
    load_palaces(db=db, dworysp_path=dworysp_path)
    union_castles_palaces(db=db)
    enrich_data(db=db, addresses_path=addresses_path)
    export_castles_and_palaces(
        db=db,
        export_gpkg_path=export_gpkg_path,
        export_geojson_path=export_geojson_path,
    )
    standardize_castles_and_palaces_data(db=db)
    export_standardized_castles_and_palaces(
        db=db,
        export_gpkg_path=export_standardized_gpkg_path,
        export_geojson_path=export_standardized_geojson_path,
    )
    assign_standardized_castles_and_palaces_and_export(
        db=db,
        export_gpkg_path=export_assigned_gpkg_path,
    )
    load_boundaries(
        db=db,
        woj_shp_path=woj_shp_path,
        pow_shp_path=pow_shp_path,
        gmi_shp_path=gmi_shp_path,
    )
    load_usemaps_data(db=db)
    prepare_nonspatial_list(db=db, addresses_path=addresses_path)
    export_nonspatial_list(db=db, export_csv_path=export_nonspatial_csv_path)
    load_reviewed_data(db=db, import_gpkg_path=import_reviewed_gpkg_path)
    export_reviewed_formatted_data(db=db, export_gpkg_path=export_reviewed_formatted_gpkg_path)
    import_list_updates(db=db, csv_path=import_descriptions_updates_path, gpkg_path=import_list_updates_path)
    export_reviewed_formatted_data_v2(
        db=db,
        export_gpkg_path=export_reviewed_v2_formatted_gpkg_path,
        export_geojson_path=export_reviewed_v2_formatted_geojson_path,
        export_csv_path=export_reviewed_v2_formatted_csv_path,
    )
    db.close()
    print("🗄️  Shutdown complete.")


if __name__ == "__main__":
    from sys import argv

    db_path = Path("dane.duckdb")
    overture_release = "2026-04-15.0"
    overture_gpkg = db_path.parent / f"overture_places_{overture_release}.gpkg"
    zamkinet_path = db_path.parent / "zamkinet_2026-02-16.geojson"
    zamkisp_path = db_path.parent / "zamkisp_2026-02-16.geojson"
    zamki_gpkg = db_path.parent / "zamki_deduplikowane_2026-02-16.gpkg"
    zamki_geojson = db_path.parent / "zamki_deduplikowane_2026-02-16.geojson"
    dworysp_path = db_path.parent / "dworysp_2026-02-19.geojson"
    addresses_path = Path("/mnt/nvme/git/prg_convert/test_data/prg_dl_2180.parquet")
    export_gpkg_path = db_path.parent / "lista_2026-05-20.gpkg"
    export_geojson_path = db_path.parent / "lista_2026-05-20.geojson"
    export_standardized_gpkg_path = db_path.parent / "lista_std_2026-05-20.gpkg"
    export_standardized_geojson_path = db_path.parent / "lista_std_2026-05-20.geojson"
    export_assigned_gpkg_path = db_path.parent / "lista_assigned_2026-05-20.gpkg"
    woj_shp_path = db_path.parent / "granice" / "A01_Granice_wojewodztw.shp"
    pow_shp_path = db_path.parent / "granice" / "A02_Granice_powiatow.shp"
    gmi_shp_path = db_path.parent / "granice" / "A03_Granice_gmin.shp"
    export_nonspatial_csv_path = db_path.parent / "lista_tabelaryczna_2026-07-24.csv"
    import_reviewed_gpkg_path = db_path.parent / "tab_list_20260813.gpkg"
    export_reviewed_formatted_gpkg_path = db_path.parent / "tab_list_20260814.gpkg"
    import_descriptions_updates_path = db_path.parent / "tab_list_updates.csv"
    import_list_updates_path = db_path.parent / "tab_list_20260814_updated.gpkg"
    export_reviewed_v2_formatted_gpkg_path = db_path.parent / "tab_list_20260830.gpkg"
    export_reviewed_v2_formatted_geojson_path = db_path.parent / "tab_list_20260830.geojson"
    export_reviewed_v2_formatted_csv_path = db_path.parent / "tab_list_20260830.csv"
    overwrite = argv[1].lower() == "overwrite" if len(argv) > 1 else False
    main(
        db_path=db_path,
        zamkinet_path=zamkinet_path,
        zamkisp_path=zamkisp_path,
        zamki_gpkg=zamki_gpkg,
        zamki_geojson=zamki_geojson,
        dworysp_path=dworysp_path,
        addresses_path=addresses_path,
        export_gpkg_path=export_gpkg_path,
        export_geojson_path=export_geojson_path,
        export_standardized_gpkg_path=export_standardized_gpkg_path,
        export_standardized_geojson_path=export_standardized_geojson_path,
        export_assigned_gpkg_path=export_assigned_gpkg_path,
        overture_release=overture_release,
        woj_shp_path=woj_shp_path,
        pow_shp_path=pow_shp_path,
        gmi_shp_path=gmi_shp_path,
        export_nonspatial_csv_path=export_nonspatial_csv_path,
        import_reviewed_gpkg_path=import_reviewed_gpkg_path,
        export_reviewed_formatted_gpkg_path=export_reviewed_formatted_gpkg_path,
        import_descriptions_updates_path=import_descriptions_updates_path,
        import_list_updates_path=import_list_updates_path,
        export_reviewed_v2_formatted_gpkg_path=export_reviewed_v2_formatted_gpkg_path,
        export_reviewed_v2_formatted_geojson_path=export_reviewed_v2_formatted_geojson_path,
        export_reviewed_v2_formatted_csv_path=export_reviewed_v2_formatted_csv_path,
        overwrite=overwrite,
    )
