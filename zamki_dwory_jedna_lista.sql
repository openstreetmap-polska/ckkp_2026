load spatial;

create or replace table adresy_prg as
  select st_transform(geom, 'epsg:4326', 'epsg:2180') as geom, gmina, miejscowosc, ulica, numer_porzadkowy, kod_pocztowy
  from st_read('/mnt/nvme/git/prg_convert/test_data/nowe4326.fgb')
;

create index idx_prg on adresy_prg using rtree (geom);

create table lista as
with
dwory as (
    select
        OGC_FID,
        case
            when regexp_matches(opis, '(pałac)|(palac)', 'is') then 'odrzucony automatycznie'
            else ckkp_status
        end decyzja,
        wojewodztwo,
        powiat,
        gmina,
        case
            when regexp_matches(opis, '(dwór)|(dwor)', 'is') then 'dwór'
            when regexp_matches(opis, '(pałac)|(palac)', 'is') then 'pałac'
            else null
        end kategoria,
        nazwa_sp as nazwa,
        opis,
        null::text as stan,
        null::text as stan_tekst,
        null::text as stan_opis,
        null::text as trudnosc_odnalezienia_opis,
        coalesce(overture_websites, overture_socials) as potencjalne_dopasowanie_strony,
        null::text as url_net,
        url as url_sp,
        zamek_id_sp,
        geom, st_point2d(st_y(geom), st_x(geom)) as point_2d
    from st_read('/mnt/nvme/git/ckkp_2026/dwory_2026-02-19.geojson')
),
zamki as (
    select
        OGC_FID,
        ckkp_status as decyzja,
        wojewodztwo,
        powiat,
        gmina,
        'zamek' as kategoria,
        coalesce(nazwa_net, nazwa_sp) as nazwa,
        opis,
        typ_interpretowany as stan,
        stan_tekst,
        stan_opis,
        trudnosc_odnalezienia_opis,
        coalesce(overture_websites, overture_socials) as potencjalne_dopasowanie_strony,
        url_net,
        url_sp,
        zamek_id_sp,
        geom,
        st_point2d(st_y(geom), st_x(geom)) as point_2d
    from st_read('/mnt/nvme/git/ckkp_2026/zamki_deduplikowane_2026-02-16.geojson')
),
matched as (
    select dwory.OGC_FID as dwory_rn, zamki.OGC_FID as zamki_rn
    from dwory, zamki
    where ST_DWithin_Spheroid(dwory.point_2d, zamki.point_2d, 25.0) or dwory.zamek_id_sp = zamki.zamek_id_sp
),
matched_data as (
    select
        coalesce(zamki.decyzja, dwory.decyzja) as decyzja,
        coalesce(zamki.kategoria, dwory.kategoria) as kategoria,
        coalesce(zamki.nazwa, dwory.nazwa) as nazwa,
        coalesce(zamki.wojewodztwo, dwory.wojewodztwo) as wojewodztwo,
        coalesce(zamki.powiat, dwory.powiat) as powiat,
        coalesce(zamki.gmina, dwory.gmina) as gmina,
        concat(zamki.opis, e'\n', dwory.opis) as opis,
        zamki.stan,
        zamki.stan_tekst,
        zamki.stan_opis,
        zamki.trudnosc_odnalezienia_opis,
        coalesce(zamki.potencjalne_dopasowanie_strony, dwory.potencjalne_dopasowanie_strony) as potencjalne_dopasowanie_strony,
        zamki.url_net,
        coalesce(zamki.url_sp, dwory.url_sp) as url_sp,
        zamki.zamek_id_sp,
        st_centroid(st_collect([dwory.geom, zamki.geom])) as geom,
        st_centroid(st_collect([dwory.point_2d::geometry, zamki.point_2d::geometry]))::point_2d as point_2d
    from matched
    join dwory on dwory.OGC_FID=matched.dwory_rn
    join zamki on zamki.OGC_FID=matched.zamki_rn
),
not_matched_dwory as (
  select *
  from dwory
  anti join matched on dwory.OGC_FID=matched.dwory_rn
),
not_matched_zamki as (
  select *
  from zamki
  anti join matched on zamki.OGC_FID=matched.zamki_rn
),
unioned as (
  select * from matched_data
  union all by name
  select * exclude(OGC_FID) from not_matched_zamki
  union all by name
  select * exclude(OGC_FID) from not_matched_dwory
)
select *
from unioned
;

create or replace table lista_z_adr as
select * exclude(point_2d)
from lista
left join lateral (
    select
        case
            when ulica is null then concat(kod_pocztowy, ' ', miejscowosc, ' ', numer_porzadkowy, ' (gmina: ', gmina, ')')
            else concat(ulica, ' ', numer_porzadkowy, ', ', kod_pocztowy, ' ', miejscowosc, ' (gmina: ', gmina, ')')
        end najblizszy_adres
    from adresy_prg
    where ST_DWithin(st_transform(lista.geom, 'epsg:4326', 'epsg:2180'), adresy_prg.geom, 200.0)
    order by ST_Distance(st_transform(lista.geom, 'epsg:4326', 'epsg:2180'), adresy_prg.geom)
    limit 1
) prg on true
;

COPY lista_z_adr TO '/mnt/nvme/git/ckkp_2026/lista_2026-04-21.geojson' WITH (FORMAT gdal, DRIVER 'GeoJSON');

COPY lista_z_adr TO '/mnt/nvme/git/ckkp_2026/lista_2026-04-21.gpkg' WITH (FORMAT gdal, DRIVER 'GPKG');
