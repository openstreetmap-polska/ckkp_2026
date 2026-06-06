load spatial;

CREATE OR REPLACE TABLE dane_osm AS
SELECT *
FROM ST_Read('zamki_dwory_osm_wszystkie_01_06_2026.gpkg');

copy(
with
unpivoted as (
unpivot (select columns(* exclude ("id", "@id", geom)) is not null from dane_osm)
on columns(*)
into
  name tag
  value val
)
select tag, count(*) filter(val = true) as values_not_null--, count(*) as total_objects
from unpivoted
group by tag
) to 'osm_stats.csv' (header)
;
