--install spatial;
--install httpfs;

load spatial;
LOAD httpfs;
-- overture data location
SET s3_region='us-west-2';

select * from st_read('/mnt/nvme/git/ckkp_2026/zamkisp_2026-02-16.geojson') limit 10;

select * from st_read('/mnt/nvme/git/ckkp_2026/zamkinet_2026-02-16.geojson') limit 10;

create table countries as
select *
from st_read('/vsigzip//vsicurl/https://osm-countries-geojson.monicz.dev/osm-countries-0-00001.geojson.gz')
;

select st_extent(geom) from countries where tags::json ->> 'ISO3166-1' = 'PL';

create table overture_categories as
select basic_category, count(*)
from 's3://overturemaps-us-west-2/release/2026-01-21.0/theme=places/type=place/*.parquet' as places
where
  bbox.xmin >= 14.06
  and bbox.xmax <= 24.03
  and bbox.ymin >= 49.0
  and bbox.ymax <= 55.04
  and st_intersects(places.geometry, (select geom from countries where tags::json ->> 'ISO3166-1' = 'PL'))
group by 1
;

select *
from overture_categories
order by 1
;

select *
from overture_categories
where basic_category like '%hist%'
;

drop table if exists overture_places;

create table overture_places as
select
  id as overture_id,
  basic_category as overture_category,
  geometry,
  array_to_string([s.dataset for s in sources], ', ') as overture_source_datasets,
  names.primary as overture_name,
  confidence as overture_confidence,
  array_to_string(websites, ' | ') as overture_websites,
  array_to_string(socials, ' | ') as overture_socials,
  operating_status as overture_operating_status
from 's3://overturemaps-us-west-2/release/2026-01-21.0/theme=places/type=place/*.parquet' as places
where
  basic_category in ('castle', 'fort', 'ruins', 'ruin', 'historic_site', 'palace', 'landmark_and_historical_building', 'museum', 'history_museum')
  and bbox.xmin >= 14.06
  and bbox.xmax <= 24.03
  and bbox.ymin >= 49.0
  and bbox.ymax <= 55.04
  and st_intersects(places.geometry, (select geom from countries where tags::json ->> 'ISO3166-1' = 'PL'))
;

COPY overture_places TO '/mnt/nvme/git/ckkp_2026/overture_places_2026-01-21.gpkg' WITH (FORMAT gdal, DRIVER 'GPKG');

create table castles as
WITH 
sp as (
  select distinct on(geom) *, row_number() over() as rn
  from st_read('/mnt/nvme/git/ckkp_2026/zamkisp_2026-02-16.geojson')
),
net as (
  select distinct on(geom) *, row_number() over() as rn
  from st_read('/mnt/nvme/git/ckkp_2026/zamkinet_2026-02-16.geojson')
),
matched as (
  select sp.rn as sp_rn, net.rn as net_rn
  from sp, net
  where ST_DWithin_Spheroid(sp.geom, net.geom, 200.0)
),
not_matched_sp as (
  select *
  from sp
  anti join matched on sp.rn=matched.sp_rn
),
not_matched_net as (
  select *
  from net
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
    st_centroid(st_collect([sp.geom, net.geom])) as geom
  from matched
  join sp on sp.rn=matched.sp_rn
  join net on net.rn=matched.net_rn
),
unioned as (
  select * from matched_data
  union all by name
  select * exclude(rn) rename(nazwa as nazwa_sp, url as url_sp, zamek_id as zamek_id_sp) from not_matched_sp
  union all by name
  select * exclude(rn) rename(nazwa as nazwa_net, url as url_net) from not_matched_net
)
select
  unioned.*,
  case
  	when typ_interpretowany = 'zniszczony' or stan_tekst = 'Brak śladów' then 'odrzucony'
  	else null
  end ckkp_status,
  ov.* exclude(geometry)
from unioned
left join lateral (
    select *
    from overture_places
    where ST_DWithin_Spheroid(unioned.geom, overture_places.geometry, 200.0)
    order by ST_Distance_Spheroid(unioned.geom, overture_places.geometry)
    limit 1
) ov on true
;

select * from castles;

COPY castles TO '/mnt/nvme/git/ckkp_2026/zamki_deduplikowane_2026-02-16.geojson' WITH (FORMAT gdal, DRIVER 'GeoJSON');

COPY castles TO '/mnt/nvme/git/ckkp_2026/zamki_deduplikowane_2026-02-16.gpkg' WITH (FORMAT gdal, DRIVER 'GPKG');
