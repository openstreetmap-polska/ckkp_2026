load spatial;
SET geometry_always_xy = true;

CREATE OR REPLACE TABLE dane_osm AS
SELECT *
FROM ST_Read('zamki_dwory_osm_wszystkie_01_06_2026.gpkg');

copy(
  select * exclude (geom, id), row_number() over() as fid, st_astext(geom) as wkt, st_x(geom) as x_wgs84, st_y(geom) as y_wgs84
  from dane_osm
) to 'osm_list_final.csv' (header)
;
