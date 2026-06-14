load spatial;
SET geometry_always_xy = true;

create table results as
with 
osm_data as (
    select row_number() over() as rn, geom, st_point2d(st_y(geom), st_x(geom)) as point_2d, id
    from st_read('zamki_dwory_osm_wszystkie_01_06_2026.gpkg')
),
our_list as (
    select row_number() over() as rn, geom, st_point2d(st_y(geom), st_x(geom)) as point_2d, * exclude(geom, autor_opracowania)
    from st_read('lista_std_2026-05-20.gpkg')
),
matched as (
    select osm_data.rn as osm_rn, our_list.rn as list_rn
    from osm_data, our_list
    where ST_DWithin_Spheroid(osm_data.point_2d, our_list.point_2d, 100.0)
),
too_many_matches_osm as (
    select osm_rn
    from matched
    group by osm_rn
    having count(*) > 1
),
too_many_matches_list as (
    select list_rn
    from matched
    group by list_rn
    having count(*) > 1
),
matched_data as (
    select
        'matched' as result,
        osm_data.id as osm_id,
        our_list.* exclude(geom, point_2d, rn),
        st_centroid(st_collect([osm_data.geom, our_list.geom])) as geom,
        st_centroid(st_collect([osm_data.point_2d::geometry, our_list.point_2d::geometry]))::point_2d as point_2d
    from matched
    anti join too_many_matches_osm using(osm_rn)
    anti join too_many_matches_list using(list_rn)
    join osm_data on osm_data.rn=matched.osm_rn
    join our_list on our_list.rn=matched.list_rn
),
not_matched_osm as (
    select
        'only_osm' as result,
        osm_data.id as osm_id,
        osm_data.geom,
        osm_data.point_2d
    from osm_data
    anti join matched on osm_data.rn=matched.osm_rn
    anti join too_many_matches_osm on osm_data.rn=too_many_matches_osm.osm_rn
),
not_matched_list as (
    select
        'only_list' as result,
        our_list.* exclude(rn)
    from our_list
    anti join matched on our_list.rn=matched.list_rn
    anti join too_many_matches_list on our_list.rn=too_many_matches_list.list_rn
),
too_many_candidates_osm as (
    select
        osm_data.rn,
        'too_many_candidates_osm' as result,
        osm_data.id as osm_id,
        osm_data.geom,
        osm_data.point_2d
    from osm_data
    join too_many_matches_osm on osm_data.rn=too_many_matches_osm.osm_rn
    union all by name
    select
        osm_data.rn,
        'too_many_candidates_list' as result,
        osm_data.id as osm_id,
        osm_data.geom,
        osm_data.point_2d
    from osm_data
    join matched on osm_data.rn=matched.osm_rn
    join too_many_matches_list using(list_rn)
),
too_many_candidates_list as (
    select
        our_list.rn,
        'too_many_candidates_list' as result,
        our_list.* exclude(rn)
    from our_list
    join too_many_matches_list on our_list.rn=too_many_matches_list.list_rn
    union all by name
    select
        our_list.rn,
        'too_many_candidates_osm' as result,
        our_list.* exclude(rn)
    from our_list
    join matched on our_list.rn=matched.list_rn
    join too_many_matches_osm using(osm_rn)
),
unioned as (
    select matched_data.* from matched_data
    union all by name
    select not_matched_osm.* from not_matched_osm
    union all by name
    select not_matched_list.* from not_matched_list
    union all by name
    select distinct on (too_many_candidates_osm.rn) too_many_candidates_osm.* exclude(rn) from too_many_candidates_osm
    union all by name
    select distinct on (too_many_candidates_list.rn) too_many_candidates_list.* exclude(rn) from too_many_candidates_list
)
select * exclude(point_2d)
from unioned
;

COPY results TO '/mnt/nvme/git/ckkp_2026/compare_osm_and_list_2026-06-14.geojson' WITH (FORMAT gdal, DRIVER 'GeoJSON');

COPY results TO '/mnt/nvme/git/ckkp_2026/compare_osm_and_list_2026-06-14.gpkg' WITH (FORMAT gdal, DRIVER 'GPKG');
