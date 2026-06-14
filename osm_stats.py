import marimo

__generated_with = "0.23.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb

    print("duckdb version:", duckdb.version())
    duckdb.install_extension("spatial")
    duckdb.load_extension("spatial")
    return duckdb, mo


@app.cell
def _(duckdb):
    _ = duckdb.execute("""
    CREATE OR REPLACE TABLE dane_osm AS
    SELECT
        id,
        historic,
        name,
        alt_name,
        building,
        coalesce(condition, "building:condition") as condition,
        coalesce(description, "description:pl") as description,
        "ref:ckkp" as ref_ckkp,
        ruins,
        access,
        coalesce("addr:city", "addr:place") as city,
        "addr:street" as street,
        "addr:housenumber" as housenumber,
        "addr:postcode" as postcode,
        wikidata,
        wikipedia,
        castle_type,
        heritage,
        tourism,
        "heritage:operator" as heritage_operator
    FROM ST_Read('zamki_dwory_osm_wszystkie_01_06_2026.gpkg')
    """)
    return


@app.cell
def _(dane_osm, mo):
    _dane_osm = mo.sql(
        f"""
        SELECT * FROM dane_osm
        """
    )
    return


@app.cell(hide_code=True)
def _(dane_osm, mo):
    _osm_obj_types = mo.sql(
        f"""
        SELECT string_split(id, '/')[1] as obj_type, count(*) as number_of_objects FROM dane_osm GROUP BY 1
        """
    )
    return


@app.cell(hide_code=True)
def _(dane_osm, mo):
    _cnt = mo.sql(
        f"""
        SELECT
            count(*) as number_of_objects,
            count(*) FILTER (name is null) as name_is_null,
            count(*) FILTER (building is not null) as building_not_null,
            count(*) FILTER (condition is not null) as condition_not_null,
            count(*) FILTER (ref_ckkp is not null) as ref_ckkp_not_null,
            count(*) FILTER (ruins is not null) as ruins_not_null,
            count(*) FILTER (access is not null) as access_not_null,
            count(*) FILTER (housenumber is not null) as housenumber_not_null,
            count(*) FILTER (postcode is not null) as postcode_not_null,
            count(*) FILTER (wikidata is not null) as wikidata_not_null,
            count(*) FILTER (wikipedia is not null) as wikipedia_not_null
        FROM dane_osm
        """
    )
    return


@app.cell(hide_code=True)
def _(dane_osm, mo):
    _grp_by_historic = mo.sql(
        f"""
        SELECT historic, count(*) as no_of_objects
        FROM dane_osm
        GROUP BY historic
        """
    )
    return


@app.cell(hide_code=True)
def _(dane_osm, mo):
    _grp_by_condition = mo.sql(
        f"""
        SELECT condition, count(*) as number_of_objects FROM dane_osm GROUP BY condition order by 1 nulls first
        """
    )
    return


@app.cell(hide_code=True)
def _(dane_osm, mo):
    _grp_by_building = mo.sql(
        f"""
        SELECT building, count(*) as number_of_objects FROM dane_osm GROUP BY building order by 2 desc nulls first
        """
    )
    return


@app.cell
def _(dane_osm, mo):
    _df = mo.sql(
        f"""
        SELECT castle_type, count(*) as no_of_objects
        FROM dane_osm
        GROUP BY castle_type
        order by no_of_objects desc
        """
    )
    return


@app.cell
def _(dane_osm, mo):
    _df = mo.sql(
        f"""
        SELECT heritage, count(*) as no_of_objects
        FROM dane_osm
        GROUP BY heritage
        order by no_of_objects desc
        """
    )
    return


@app.cell
def _(dane_osm, mo):
    _df = mo.sql(
        f"""
        SELECT tourism, count(*) as no_of_objects
        FROM dane_osm
        GROUP BY tourism
        order by no_of_objects desc
        """
    )
    return


@app.cell
def _(dane_osm, mo):
    _df = mo.sql(
        f"""
        SELECT heritage_operator, count(*) as no_of_objects
        FROM dane_osm
        GROUP BY heritage_operator
        order by no_of_objects desc
        """
    )
    return


if __name__ == "__main__":
    app.run()
