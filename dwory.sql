load spatial;

select * from st_read('/mnt/nvme/git/ckkp_2026/dworysp_2026-02-19.geojson') limit 10;

create table overture_places as
select * from st_read('/mnt/nvme/git/ckkp_2026/overture_places_2026-01-21.gpkg')
;

drop table if exists palaces;

create table palaces as
with
p as (
  select distinct on(geom) *
  from st_read('/mnt/nvme/git/ckkp_2026/dworysp_2026-02-19.geojson')
),
joined as (
  select
    p.*,
    ov.* exclude(geom)
  from p
  left join lateral (
    select *
    from overture_places
    where ST_DWithin_Spheroid(p.geom, overture_places.geom, 100.0)
    order by ST_Distance_Spheroid(p.geom, overture_places.geom)
    limit 1
  ) ov on true
)
select null::text as ckkp_status, joined.*
from joined
;

select * from palaces;

COPY palaces TO '/mnt/nvme/git/ckkp_2026/dwory_2026-02-19.geojson' WITH (FORMAT gdal, DRIVER 'GeoJSON');

COPY palaces TO '/mnt/nvme/git/ckkp_2026/dwory_2026-02-19.gpkg' WITH (FORMAT gdal, DRIVER 'GPKG');
