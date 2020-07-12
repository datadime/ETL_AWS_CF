truncate table ccrs_ri.riratetranslations;

Insert into ccrs_re.re_audit_log
(target_table,query_name)
values
('RiRateTranslations','RiRateTranslationsExtract');

Insert into ccrs_ri.riratetranslations
select --GDS/IDS rate translations kept in Rate type channel START PART 1
0 AS chain_id
,crs_chain_id
,chain_code
,chain_name
,brand_code
,brand_name
,0 AS hotel_id
,crs_hotel_id
,hotel_name
,channel_name
,sub_channel_name
,crs_rate_type_code
,rate_type_name
,booking_code
,(CASE WHEN code_translated='Yes' then Cast(ncl.corp_nm as text) else null END)  AS corporate_name
, rate_level
,code_translated
,duplicate_booking_code
,region_name
,country_name
,state_name
,city_name
,to_char(current_date,'yyyy-mm-dd hh24:mi:ss') AS eda_load_dt
,access_code AS rate_typ_access_cd
from
(
select crs_chain_id
,chain_code
,chain_name
,brand_code
,brand_name
,crs_hotel_id
,hotel_name
,channel_name
,sub_channel_name
,crs_rate_type_code
,rate_type_name
,booking_code
,rate_level
,code_translated
,(CASE WHEN booking_code IS NOT NULL AND COUNT(crs_rate_type_code) over (PARTITION BY crs_hotel_id,booking_code,channel_name,sub_channel_name) > 1 THEN 'Yes' ELSE 'No' END) AS duplicate_booking_code
,region_name
,country_name
,state_name
,city_name
,rate_typ_guid
,access_code
from
(
select
crs_chain_id
,chain_code
,chain_name
,brand_code
,brand_name
,crs_hotel_id
,hotel_name
,channel_name
,sub_channel_name
,crs_rate_type_code
,rate_type_name
,booking_code
, rate_level
,(CASE when count(1) over (partition by crs_hotel_id,crs_rate_type_code,channel_name) <=1 and sub_channel_name is null and booking_code is null then 'No' Else 'Yes' END) AS code_translated
--,count(1) over (partition by crs_hotel_id,crs_rate_type_code,channel_name)
,region_name
,country_name
,state_name
,city_name
,rate_typ_guid
,access_code
from
(
select
chb.chain_id as crs_chain_id
,chb.chain_cd as chain_code
,chb.chain_nm as chain_name
,chb.brand_cd as brand_code
,chb.brand_nm as brand_name
,chb.hotel_id as crs_hotel_id
,chb.hotel_short_nm AS hotel_name
,(case when ch.parent_channel_id is null then ch.channel_desc else ch1.channel_desc  end) AS channel_name
,(case when ch.parent_channel_id is not null then ch.channel_desc  else null end) AS sub_channel_name
,r.rate_typ_cd AS crs_rate_type_code
,r.rate_typ_nm AS rate_type_name
,rtc.rate_typ_booking_cd AS booking_code
,rtc.rate_typ_access_cd As access_code
,(CASE WHEN r.hotel_guid IS NOT NULL AND r.parent_rate_typ_guid IS NOT NULL THEN 'Chain'
WHEN r.hotel_guid IS NOT NULL AND r.parent_brand_rate_typ_guid IS NOT NULL THEN 'Brand'
ELSE 'Hotel'
END) AS rate_level
,gr.geo_region_desc AS region_name
, gc.geo_desc AS country_name
, ci.contact_state AS state_name
, ci.contact_city AS city_name
,r.rate_typ_guid
from (select
      hotel_guid,
      contact_info_guid,
      hotel_id,
      hotel_short_nm,
      chain_id,
      chain_cd,
      chain_nm,
      brand_cd,
      brand_nm
      from
      (select
       h.hotel_guid,
       h.contact_info_guid,
       h.hotel_id,
       h.hotel_short_nm,
       (case when c.parent_chain_guid is null then c.chain_id else c1.chain_id end) as chain_id,
       (case when c.parent_chain_guid is null then c.chain_cd else c1.chain_cd end) as chain_cd,
       (case when c.parent_chain_guid is null then c.chain_nm else c1.chain_nm end) as chain_nm,
       (case when c.parent_chain_guid is not null then c.chain_id else -1 end) as brand_id,
       (case when c.parent_chain_guid is not null then c.chain_cd else 'No Brand' end) as brand_cd,
       (case when c.parent_chain_guid is not null then c.chain_nm else 'No Brand' end) as brand_nm,
        count(1) over (partition by h.hotel_id,h.hotel_short_nm) as brand_chain_indicator,
        c.chain_typ_id
        from shscrsrepl.hotel h
        inner join shscrsrepl.hotel_chain hc on hc.hotel_guid=h.hotel_guid
        inner join shscrsrepl.chain c on c.chain_guid=hc.chain_guid
        left join shscrsrepl.chain c1 on c1.chain_guid=c.parent_chain_guid
        where  (c.chain_typ_id=0 or (c.chain_typ_id=3 and hc.default_flag=1)) and
        h.hotel_id in
        (select
         h.hotel_id
         from shscrsrepl.chain c
         inner join shscrsrepl.hotel_chain hc on hc.chain_guid=c.chain_guid
         inner join shscrsrepl.hotel h on h.hotel_guid=hc.hotel_guid
         where c.chain_id in (select CHAIN_ID from ccrs_re.RE_EXTRACT_CONFIG where SOURCE_ODS_SCHEMA = 'shscrsrepl' and DISCONTINUE_DT IS NULL))
         and h.hotel_guid NOT IN (SELECT DISTINCT a.hotel_guid FROM shscrsrepl.hotel a INNER JOIN shscrsrepl.hotel_attr b ON a.hotel_guid = b.hotel_guid AND b.attr_field_id = 876 AND upper(b.hotel_attr_val)='TRUE')
         AND to_date('2000-05-14','YYYY-MM-DD') + cast (h.ACTIVE_END_CAL_ID as integer) >= CURRENT_DATE
         and UPPER(h.hotel_short_nm) NOT LIKE '%TEST%'
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
       ) x
       where (chain_typ_id=0 and brand_chain_indicator=1) or (chain_typ_id=3 and brand_chain_indicator>1)) chb
       inner join shscrsrepl.contact_info ci on ci.hotel_guid=chb.hotel_guid and chb.contact_info_guid = ci.contact_info_guid and ci.hotel_primary_flag = 1
       inner join shscrsrepl.geo_loc gc on gc.geo_cd=ci.contact_country and PARENT_GEO_LOC_ID is null
       inner join shscrsrepl.geo_region gr on gr.geo_region_id=gc.geo_region_id
       inner join shscrsrepl.rate_typ r on r.hotel_guid=chb.hotel_guid
       inner join shscrsrepl.rate_typ_channel rtc on rtc.rate_typ_guid=r.rate_typ_guid and rtc.channel_id in (select channel_id from shscrsrepl.channel where (parent_channel_id=13 and channel_level=2 or channel_id=13) or (parent_channel_id=11 and channel_level=2 or channel_id=11))-- filtering only gDS/IDS assigned rates
       inner join shscrsrepl.channel ch on ch.channel_id=rtc.channel_id
       left outer join shscrsrepl.channel ch1 on ch1.channel_id=ch.parent_channel_id
       left outer join shscrsrepl.rate_typ_channel_access rtca on rtca.rate_typ_guid =r.rate_typ_guid
where 
--\$CONDITIONS AND
 r.active_flag = 1
AND r.delete_flag = 0
AND rtca.rate_typ_guid is null-- Get only GDS/IDS rates which have translation in rate_typ_channel only
AND UPPER(r.rate_typ_cd) NOT LIKE '%MIRRORRATE%'
AND UPPER(r.rate_typ_cd) NOT LIKE '%TEST%'
AND UPPER(r.rate_typ_cd) NOT LIKE '%QA %'
) y
) z
where sub_channel_name is not null or code_translated='No'
) rtc_f
left outer join shscrsrepl.neg_corp_rate ncr on ncr.rate_typ_guid=rtc_f.rate_typ_guid
left outer join shscrsrepl.neg_corp nc on nc.neg_corp_guid=ncr.neg_corp_guid and neg_typ_id=1
left outer join shscrsrepl.neg_corp_lang ncl on ncl.neg_corp_guid=nc.neg_corp_guid --3,950,872
UNION ALL
select --Channel connect OTA code mapping rates START PART2
0 AS chain_id
,chb.chain_id as crs_chain_id
,chb.chain_cd as chain_code
,chb.chain_nm as chain_name
,chb.brand_cd as brand_code
,chb.brand_nm as brand_name
,0 AS hotel_id
,chb.hotel_id as crs_hotel_id
,chb.hotel_short_nm AS hotel_name
,(case when ch.parent_channel_id is null then ch.channel_desc else ch1.channel_desc  end) AS channel_name
,(case when ch.parent_channel_id is not null then ch.channel_desc  else null end) AS sub_channel_name
,r.rate_typ_cd AS crs_rate_type_code
,r.rate_typ_nm AS rate_type_name
,rtc.rate_typ_cd AS booking_code
,null AS corporate_name
,(CASE WHEN r.hotel_guid IS NOT NULL AND r.parent_rate_typ_guid IS NOT NULL THEN 'Chain'
WHEN r.hotel_guid IS NOT NULL AND r.parent_brand_rate_typ_guid IS NOT NULL THEN 'Brand'
ELSE 'Hotel'
END) AS rate_level
,(CASE WHEN rtc.rate_typ_cd IS NULL THEN 'No'
ELSE 'Yes'
END) AS code_translated
,(CASE WHEN rtc.rate_typ_cd IS NOT NULL AND COUNT(r.rate_typ_cd) over (PARTITION BY chb.hotel_id, rtc.rate_typ_cd, ch1.channel_desc, ch.channel_desc) > 1 THEN 'Yes'
ELSE 'No'
END) AS duplicate_booking_code
,gr.geo_region_desc AS region_name
, gc1.geo_desc AS country_name
, ci.contact_state AS state_name
, ci.contact_city AS city_name
, To_char(CURRENT_TIMESTAMP,'yyyy-mm-dd hh24:mi:ss') AS eda_load_dt
,rtc.rate_typ_access_cd AS rate_typ_access_cd
from (select
      hotel_guid,
      contact_info_guid,
      hotel_id,
      hotel_short_nm,
      chain_id,
      chain_cd,
      chain_nm,
      brand_cd,
      brand_nm
      from
      (select
       h.hotel_guid,
       h.contact_info_guid,
       h.hotel_id,
       h.hotel_short_nm,
       (case when c.parent_chain_guid is null then c.chain_id else c1.chain_id end) as chain_id,
       (case when c.parent_chain_guid is null then c.chain_cd else c1.chain_cd end) as chain_cd,
       (case when c.parent_chain_guid is null then c.chain_nm else c1.chain_nm end) as chain_nm,
       (case when c.parent_chain_guid is not null then c.chain_id else -1 end) as brand_id,
       (case when c.parent_chain_guid is not null then c.chain_cd else 'No Brand' end) as brand_cd,
       (case when c.parent_chain_guid is not null then c.chain_nm else 'No Brand' end) as brand_nm,
        count(1) over (partition by h.hotel_id,h.hotel_short_nm) as brand_chain_indicator,
        c.chain_typ_id
        from shscrsrepl.hotel h
        inner join shscrsrepl.hotel_chain hc on hc.hotel_guid=h.hotel_guid
        inner join shscrsrepl.chain c on c.chain_guid=hc.chain_guid
        left join shscrsrepl.chain c1 on c1.chain_guid=c.parent_chain_guid
        where  (c.chain_typ_id=0 or (c.chain_typ_id=3 and hc.default_flag=1)) and
        h.hotel_id in
        (select
         h.hotel_id
         from shscrsrepl.chain c
         inner join shscrsrepl.hotel_chain hc on hc.chain_guid=c.chain_guid
         inner join shscrsrepl.hotel h on h.hotel_guid=hc.hotel_guid
         where c.chain_id in (select CHAIN_ID from ccrs_re.RE_EXTRACT_CONFIG where SOURCE_ODS_SCHEMA = 'shscrsrepl' and DISCONTINUE_DT IS NULL))
         and h.hotel_guid NOT IN (SELECT DISTINCT a.hotel_guid FROM shscrsrepl.hotel a INNER JOIN shscrsrepl.hotel_attr b ON a.hotel_guid = b.hotel_guid AND b.attr_field_id = 876 AND upper(b.hotel_attr_val)='TRUE')
         AND To_date('2000-05-14','YYYY-MM-DD') + Cast (h.active_end_cal_id AS INTEGER) >= CURRENT_DATE
         and UPPER(h.hotel_short_nm) NOT LIKE '%TEST%'
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
       ) x
		where (chain_typ_id=0 and brand_chain_indicator=1) or (chain_typ_id=3 and brand_chain_indicator>1)) chb
        inner join shscrsrepl.contact_info ci on ci.hotel_guid=chb.hotel_guid and chb.contact_info_guid = ci.contact_info_guid and ci.hotel_primary_flag = 1
        inner join shscrsrepl.geo_loc gc1 on gc1.geo_cd=ci.contact_country and PARENT_GEO_LOC_ID is null
        inner join shscrsrepl.geo_region gr on gr.geo_region_id=gc1.geo_region_id
        inner join shscrsrepl.rate_typ r on r.hotel_guid=chb.hotel_guid
        inner join shscrsrepl.rate_typ_channel rtc on rtc.rate_typ_guid=r.rate_typ_guid
        inner join shscrsrepl.channel ch on ch.channel_id=rtc.channel_id
        left outer join shscrsrepl.channel ch1 on ch1.channel_id=ch.parent_channel_id
        where  r.active_flag = 1
        AND r.delete_flag = 0
        AND ch.code_req_flag = 1--97867
        AND UPPER(r.rate_typ_cd) NOT LIKE '%MIRRORRATE%'
        AND UPPER(r.rate_typ_cd) NOT LIKE '%TEST%'
        AND UPPER(r.rate_typ_cd) NOT LIKE '%QA %'

--Channel connect OTA code mapping rates END PART2
UNION ALL
select
0 AS chain_id --GDS/IDS NRAC rate translations are kept in rate_type_channel_access PART 3
,chb.chain_id as crs_chain_id
,chb.chain_cd as chain_code
,chb.chain_nm as chain_name
,chb.brand_cd as brand_code
,chb.brand_nm as brand_name
,0 AS hotel_id
,chb.hotel_id as crs_hotel_id
,chb.hotel_short_nm AS hotel_name
,(case when ch.parent_channel_id is null then ch.channel_desc else ch1.channel_desc  end) AS channel_name
,(case when ch.parent_channel_id is not null then ch.channel_desc  else null end) AS sub_channel_name
,r.rate_typ_cd AS crs_rate_type_code
,r.rate_typ_nm AS rate_type_name
,rtca.rate_booking_cd AS booking_code
,gc.corp_nm AS corporate_name
,(CASE WHEN r.hotel_guid IS NOT NULL AND r.parent_rate_typ_guid IS NOT NULL THEN 'Chain'
WHEN r.hotel_guid IS NOT NULL AND r.parent_brand_rate_typ_guid IS NOT NULL THEN 'Brand'
ELSE 'Hotel'
END) AS rate_level
,(CASE WHEN rtca.rate_booking_cd IS NULL THEN 'No'
ELSE 'Yes'
END) AS code_translated
,(CASE WHEN rtca.rate_booking_cd IS NOT NULL AND COUNT(r.rate_typ_cd) over (PARTITION BY chb.hotel_id,rtca.rate_booking_cd,ch1.channel_desc,ch.channel_desc) > 1 THEN 'Yes'
ELSE 'No'
END) AS duplicate_booking_code
,gr.geo_region_desc AS region_name
, gc1.geo_desc AS country_name
, ci.contact_state AS state_name
, ci.contact_city AS city_name
, to_char(current_date,'yyyy-mm-dd hh24:mi:ss') AS eda_load_dt
,rtca.default_rate_access_cd AS rate_typ_access_cd
from (select
      hotel_guid,
      contact_info_guid,
      hotel_id,
      hotel_short_nm,
      chain_id,
      chain_cd,
      chain_nm,
      brand_cd,
      brand_nm
      from
      (select
       h.hotel_guid,
       h.contact_info_guid,
       h.hotel_id,
       h.hotel_short_nm,
       (case when c.parent_chain_guid is null then c.chain_id else c1.chain_id end) as chain_id,
       (case when c.parent_chain_guid is null then c.chain_cd else c1.chain_cd end) as chain_cd,
       (case when c.parent_chain_guid is null then c.chain_nm else c1.chain_nm end) as chain_nm,
       (case when c.parent_chain_guid is not null then c.chain_id else -1 end) as brand_id,
       (case when c.parent_chain_guid is not null then c.chain_cd else 'No Brand' end) as brand_cd,
       (case when c.parent_chain_guid is not null then c.chain_nm else 'No Brand' end) as brand_nm,
        count(1) over (partition by h.hotel_id,h.hotel_short_nm) as brand_chain_indicator,
        c.chain_typ_id
        from shscrsrepl.hotel h
        inner join shscrsrepl.hotel_chain hc on hc.hotel_guid=h.hotel_guid
        inner join shscrsrepl.chain c on c.chain_guid=hc.chain_guid
        left join shscrsrepl.chain c1 on c1.chain_guid=c.parent_chain_guid
        where  (c.chain_typ_id=0 or (c.chain_typ_id=3 and hc.default_flag=1)) and
        h.hotel_id in
        (select
         h.hotel_id
         from shscrsrepl.chain c
         inner join shscrsrepl.hotel_chain hc on hc.chain_guid=c.chain_guid
         inner join shscrsrepl.hotel h on h.hotel_guid=hc.hotel_guid
         where c.chain_id in (select CHAIN_ID from ccrs_re.RE_EXTRACT_CONFIG where SOURCE_ODS_SCHEMA = 'shscrsrepl' and DISCONTINUE_DT IS NULL))
         and h.hotel_guid NOT IN (SELECT DISTINCT a.hotel_guid FROM shscrsrepl.hotel a INNER JOIN shscrsrepl.hotel_attr b ON a.hotel_guid = b.hotel_guid AND b.attr_field_id = 876 AND upper(b.hotel_attr_val)='TRUE')
         AND To_date('2000-05-14','YYYY-MM-DD') + Cast (h.active_end_cal_id AS INTEGER) >= CURRENT_DATE
         and UPPER(h.hotel_short_nm) NOT LIKE '%TEST%'
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
       ) x
       
       where (chain_typ_id=0 and brand_chain_indicator=1) or (chain_typ_id=3 and brand_chain_indicator>1)) chb
       inner join shscrsrepl.contact_info ci on ci.hotel_guid=chb.hotel_guid and chb.contact_info_guid = ci.contact_info_guid and ci.hotel_primary_flag = 1
       inner join shscrsrepl.geo_loc gc1 on gc1.geo_cd=ci.contact_country and PARENT_GEO_LOC_ID is null
       inner join shscrsrepl.geo_region gr on gr.geo_region_id=gc1.geo_region_id
       inner join shscrsrepl.rate_typ r on r.hotel_guid=chb.hotel_guid
       inner join shscrsrepl.rate_typ_channel_access rtca on rtca.rate_typ_guid=r.rate_typ_guid
       inner join shscrsrepl.channel ch on ch.channel_id=rtca.channel_id
       left outer join shscrsrepl.channel ch1 on ch1.channel_id=ch.parent_channel_id
       inner join (select distinct rate_typ_guid from shscrsrepl.rate_typ_channel where channel_id in (11,13,14,17,18,15,16,19)) rtc on rtc.rate_typ_guid=rtca.rate_typ_guid
       LEFT JOIN shscrsrepl.global_corp_channel gcc ON gcc.global_corp_channel_guid = rtca.global_corp_channel_guid
       LEFT JOIN shscrsrepl.global_corp gc ON gc.global_corp_guid = gcc.global_corp_guid
where 
--\$CONDITIONS AND
r.active_flag = 1
AND r.delete_flag = 0
AND UPPER(r.rate_typ_cd) NOT LIKE '%MIRRORRATE%'
AND UPPER(r.rate_typ_cd) NOT LIKE '%TEST%'
AND UPPER(r.rate_typ_cd) NOT LIKE '%QA %';
