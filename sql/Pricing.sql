truncate table ccrs_ri.RulesEngineSRCPricingDataSet;

Insert into ccrs_re.re_audit_log
(target_table,query_name)
values
('RulesEngineSRCPricingDataSet','REPricing');

Insert into ccrs_ri.RulesEngineSRCPricingDataSet 
select 
chain_id,
chain_nm,
chain_cd,
brand_id,
brand_nm,
brand_cd,
hotel_id,
hotel_short_nm,
region_name,
country_name,
country_code,
state_name,
city_name,
rate_typ_guid,
rate_typ_cd,
rate_typ_nm,
rate_type,
rate_category,
mkt_seg_cd,
rate_class,
derive_typ_id,
season_start_date,
season_end_date,
min(price_amt) min_price_amt,
max(price_amt) max_price_amt,
to_char(current_timestamp,'yyyy-mm-dd hh24:mi:ss') load_dt
from
(select 
C1.chain_id,
c1.chain_nm,
C1.chain_cd,
c.chain_id as brand_id,
c.chain_nm as brand_nm,
c.chain_cd as brand_cd,
h.hotel_id,
h.hotel_short_nm,
gr.geo_region_desc AS region_name,
gc.geo_desc AS country_name,
gc.oddr_geo_cd as country_code,
ci.contact_state AS state_name,
ci.contact_city AS city_name,
rt.rate_typ_guid,
rt.rate_typ_cd,
rt.rate_typ_nm,
(case when rt.apply_bar_flag=1 then 'Bar'
when rt.corp_flag=1 then 'Negotiated'
else 'Public' end) as rate_type,
rtg.rate_typ_grp_cd as rate_category,
msl.mkt_seg_nm as mkt_seg_cd,
rc.rate_class_desc as rate_class,
cal_s.cal_dt as season_start_date,
cal_e.cal_dt as season_end_date,
ps.derive_typ_id,
ps.price_amt
from shscrsrepl.product p
inner join shscrsrepl.rate_typ rt on rt.rate_typ_guid=p.rate_typ_guid
inner join shscrsrepl.rm_typ rm on rm.rm_typ_guid=p.rm_typ_guid
inner join shscrsrepl.hotel h on h.hotel_guid=p.hotel_guid
inner join shscrsrepl.contact_info ci on ci.hotel_guid=h.hotel_guid and h.contact_info_guid = ci.contact_info_guid and ci.hotel_primary_flag = 1
inner join shscrsrepl.geo_loc gc on gc.geo_cd=ci.contact_country and PARENT_GEO_LOC_ID is null
inner join shscrsrepl.geo_region gr on gr.geo_region_id=gc.geo_region_id
inner join shscrsrepl.hotel_chain hc on hc.hotel_guid=h.hotel_guid
inner join shscrsrepl.chain c on c.chain_guid=hc.chain_guid
inner join shscrsrepl.chain c1 on c1.chain_guid=c.parent_chain_guid 
inner join shscrsrepl.price_season ps on ps.product_guid=p.product_guid
inner join shscrsrepl.cal cal_s on cal_s.cal_id=ps.start_Cal_id
inner join shscrsrepl.cal cal_e on cal_e.cal_id=ps.end_Cal_id
left join shscrsrepl.rate_typ_grp rtg on rtg.rate_typ_grp_guid=rt.rate_typ_grp_guid
left join shscrsrepl.mkt_seg_lang msl on msl.mkt_seg_guid=rt.mkt_seg_guid
left join shscrsrepl.rate_class rc on rc.rate_class_id=rt.rate_class_id
where  c.chain_typ_id=3
AND hc.default_flag = 1
AND to_date('2000-05-14','YYYY-MM-DD') + cast (h.ACTIVE_END_CAL_ID as integer) >= CURRENT_DATE
--and (date '2000-05-14' + h.active_end_cal_id) >= current_date
AND rt.delete_flag=0
AND UPPER(rt.rate_typ_nm) NOT LIKE '%TEST%'
AND UPPER(rt.rate_typ_nm) not like '% QA %'
AND UPPER(rt.rate_typ_cd) NOT LIKE '%MIRRORRATE%'
and rt.delete_flag=0 
and c1.chain_id in (select CHAIN_ID from ccrs_re.RE_EXTRACT_CONFIG where SOURCE_ODS_SCHEMA = 'shscrsrepl' and DISCONTINUE_DT IS NULL)
--and c1.chain_id=6657
--and h.hotel_id=14008
and cal_e.cal_dt>=current_date
--AND h.hotel_guid = '\x93fbe580c4ca634c871f078557791cfa'
--AND h.hotel_guid = NOT IN (SELECT DISTINCT a.hotel_guid FROM shscrsrepl.hotel a INNER JOIN shscrsrepl.hotel_attr b ON a.hotel_guid = b.hotel_guid AND b.attr_field_id = 876 AND upper(b.hotel_attr_val)='TRUE')
AND UPPER(h.hotel_short_nm) NOT LIKE '%TEST%'
AND UPPER(h.hotel_short_nm) NOT LIKE '%QA %'
AND UPPER(h.hotel_short_nm) NOT LIKE '%TRAINING%'
AND UPPER(h.hotel_short_nm) NOT LIKE '%DO%NOT%'
AND UPPER(h.hotel_short_nm) NOT LIKE '%DELET%'
AND UPPER (h.hotel_short_nm) NOT LIKE 'ZZ%'
AND UPPER(h.hotel_short_nm) NOT LIKE '%TBD%'
AND UPPER(h.hotel_short_nm) NOT LIKE '%RMMRKTER%'
AND UPPER(h.hotel_short_nm) NOT LIKE '%DEMO %'
AND UPPER(h.hotel_short_nm) NOT LIKE '%TO BE DEL%'
AND UPPER(h.hotel_long_nm) NOT LIKE '%TEST%'
AND UPPER(h.hotel_long_nm) NOT LIKE '%TRAINING%'
AND UPPER(h.hotel_long_nm) NOT LIKE '%DO%NOT%'
AND UPPER(h.hotel_long_nm) NOT LIKE '%QA %'
AND UPPER(h.hotel_long_nm) NOT LIKE '%DELET%'
AND UPPER(h.hotel_long_nm) NOT LIKE '%TO BE DEL%'
AND UPPER(h.hotel_long_nm) NOT LIKE '%MODIFY%'
AND UPPER(h.hotel_long_nm) NOT LIKE '%TBD%'
AND UPPER(h.hotel_long_nm) NOT LIKE '%RMMRKTER%'
AND UPPER(h.hotel_long_nm) NOT LIKE '%DEMO %'
AND UPPER (h.hotel_long_nm) NOT LIKE 'ZZ%'
) AS xyz
group by 
chain_id,
chain_nm,
chain_cd,
brand_id,
brand_nm,
brand_cd,
hotel_id,
hotel_short_nm,
region_name,
country_name,
country_code,
state_name,
city_name,
rate_typ_guid,
rate_typ_cd,
rate_typ_nm,
rate_type,
rate_category,
mkt_seg_cd,
rate_class,
season_start_date,
season_end_date,
derive_typ_id;