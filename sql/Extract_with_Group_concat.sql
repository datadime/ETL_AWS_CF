Truncate table ccrs_stage_db.rulesenginesrcdataset_profile_concat;

Insert into ccrs_re.re_audit_log
(target_table,query_name)
values
('rulesenginesrcdataset_profile_concat','RE_Extract_with_Group_concat');

Insert into ccrs_stage_db.rulesenginesrcdataset_profile_concat
Select 
g.chain_id,
g.chain_nm,
g.chain_cd,
g.brand_id,
g.brand_nm,
g.brand_cd,
g.hotel_id,
g.hotel_short_nm,
g.region_name,
g.country_name,
g.Country_code,
g.state_name,
g.city_name,
decode (g.rate_typ_guid_hex, 'escape') as rate_typ_guid,
g.rate_typ_cd,
g.rate_typ_nm,
g.rate_type,
g.rate_category,
g.mkt_seg_cd,
g.rate_class,
g.rateActive,
g.commissionableFlag,
g.hurdle,
g.breakfast,
g.loayltyredemption,
g.eligibleforaccrual,
g.managedCRSguranteePolicy,
g.managedCRScancelpolicy,
g.managedCRSstay,
g.managedCRSrateseasons,
g.derive_typ_id,
g.min_lead_days,
g.max_lead_days,
g.max_sell_date,
g.min_sell_date,
g.min_stay_arrival,
g.max_stay_arrival,
g.min_stay_thru,
g.max_stay_thru,
g.no_arrival,
g.no_arrival_sun,
g.no_arrival_mon,
g.no_arrival_tue,
g.no_arrival_wed,
g.no_arrival_thu,
g.no_arrival_fri,
g.no_arrival_sat,
g.no_departure,
g.mobile_web,
g.booking_engine,
g.voice,
g.PMS,
g.IDS,
g.GDS,
g.channel_connect,
g.retail,
g.CRM,
g.google,
g.cancelpolicy,
g.guaranteepolicy,
g.commissionpolicy,
g.closed_managed_in_cr,
g.sell_limit_manage_in_cr,
g.min_stay_arrival_micr,
g.max_stay_arrival_micr,
g.min_lead_time_micr,
g.max_lead_time_micr,
g.min_stay_through_micr,
g.max_stay_through_micr,
g.min_sell_date_micr,
g.max_sell_date_micr,
g.THST_managed_in_cr,
g.end_time_managed_in_cr,
g.pricing_record_exists,
g.current_date as eda_load_dt,
g.rate_typ_guid_hex,
string_agg(g.profileid, ',') as Profileid
From
(
SELECT    d.*,
          nc.corp_cd AS profileid,
          To_char(current_timestamp,'yyyy-mm-dd hh24:mi:ss') as current_date,
          d.rate_typ_guid AS rate_typ_guid_hex --cast(collect(nc.corp_cd) as sys.DBMSOUTPUT_LINESARRAY) as profileId,
From
(
select c.chain_id,
c.chain_nm,
c.chain_cd,
c.brand_id,
c.brand_nm,
c.brand_cd,
c.hotel_id,
c.hotel_short_nm,
c.region_name,
c.country_name,
c.country_code,
c.state_name,
c.city_name,
encode (c.rate_typ_guid, 'HEX' ) as rate_typ_guid,
c.rate_typ_cd,
c.rate_typ_nm,
c.rate_type,
c.rate_category,
c.mkt_seg_cd,
c.rate_class,
c.rateActive,
c.CommissionableFlag,
c.Hurdle,
c.Breakfast,
c.LoayltyRedemption,
c.EligibleforAccrual,
c.ManagedCRSGuranteePolicy,
c.ManagedCRSCancelPolicy,
c.ManagedCRSStay,
c.ManagedCRSRateSeasons,
c.derive_typ_id,
c.Min_Lead_Days,
c.Max_Lead_Days,
c.Max_Sell_Date,
c.Min_Sell_Date,
c.Min_Stay_Arrival,
c.Max_Stay_Arrival,
c.Min_Stay_Thru,
c.Max_Stay_Thru,
(case when (c.No_Arrival_Sun is not null or c.No_Arrival_Mon is not null or c.No_Arrival_Tue is not null or c.No_Arrival_Wed is not null or c.No_Arrival_Thu is not null or c.No_Arrival_Fri is not null or c.No_Arrival_Sat is not null)  then 1 else 0 end) as No_arrival,
(case when c.No_Arrival_Sun is not null then 1 else 0 end) as No_Arrival_Sun,
(case when c.No_Arrival_Mon is not null then 1 else 0 end) as No_Arrival_Mon,
(case when c.No_Arrival_Tue is not null then 1 else 0 end) as No_Arrival_Tue,
(case when c.No_Arrival_Wed is not null then 1 else 0 end) as No_Arrival_Wed,
(case when c.No_Arrival_Thu is not null then 1 else 0 end) as No_Arrival_Thu,
(case when c.No_Arrival_Fri is not null then 1 else 0 end) as No_Arrival_Fri,
(case when c.No_Arrival_Sat is not null then 1 else 0 end) as No_Arrival_Sat,
(case when c.No_Departure is not null then 1 else 0 end) as No_Departure,
c.Mobile_Web,
c.Booking_Engine,
c.Voice,
c.PMS,
c.IDS,
c.GDS,
c.Channel_Connect,
c.Retail,
c.CRM,
c.Google,
c.CancelPolicy,
c.GuaranteePolicy,
c.CommissionPolicy,
(case when c.closed_managed_in_cr is not null then 1 else 0 end) as closed_managed_in_cr ,
(case when c.sell_limit_manage_in_cr is not null then 1 else 0 end) as sell_limit_manage_in_cr,
(case when c.min_stay_arrival_micr is not null then 1 else 0 end) as min_stay_arrival_micr,
(case when c.max_stay_arrival_micr is not null then 1 else 0 end) as max_stay_arrival_micr,
(case when c.min_lead_time_micr is not null then 1 else 0 end) as min_lead_time_micr,
(case when c.max_lead_time_micr is not null then 1 else 0 end) as max_lead_time_micr,
(case when c.min_stay_through_micr is not null then 1 else 0 end) as min_stay_through_micr,
(case when c.max_stay_through_micr is not null then 1 else 0 end) as max_stay_through_micr,
(case when c.min_sell_date_micr is not null then 1 else 0 end) as min_sell_date_micr,
(case when c.max_sell_date_micr is not null then 1 else 0 end) as max_sell_date_micr,
(case when c.THST_managed_in_cr is not null then 1 else 0 end) as THST_managed_in_cr,
(case when c.end_time_managed_in_cr is not null then 1 else 0 end) as end_time_managed_in_cr,
(case when pre.rate_typ_guid is not null then 1 else 0 end ) as pricing_record_exists
from
(
Select w.*,
min(case when w.cntrl_typ_desc = 'Open/Close' Then cntrl_typ_id End) as closed_managed_in_cr,
min(case when w.cntrl_typ_desc = 'Sell Limit' Then cntrl_typ_id End) as sell_limit_manage_in_cr,
min(case when w.cntrl_typ_desc = 'Min Stay Arrival' then cntrl_typ_id	end) AS min_stay_arrival_micr,
min(case when w.cntrl_typ_desc = 'Max Stay Arrival' Then cntrl_typ_id End) as max_stay_arrival_micr,
min(case when w.cntrl_typ_desc = 'Min Lead Days' Then cntrl_typ_id End) as min_lead_time_micr,
min(case when w.cntrl_typ_desc = 'Max Lead Days' Then cntrl_typ_id End) as max_lead_time_micr,
min(case when w.cntrl_typ_desc = 'Min Stay Thru' Then cntrl_typ_id End) as min_stay_through_micr,
min(case when w.cntrl_typ_desc = 'Max Stay Thru' Then cntrl_typ_id End) as max_stay_through_micr,
min(case when w.cntrl_typ_desc = 'Min Sell Date' Then cntrl_typ_id End) as min_sell_date_micr,
min(case when w.cntrl_typ_desc = 'Max Sell Date' Then cntrl_typ_id End) as max_sell_date_micr,
min(case when w.cntrl_typ_desc = 'Total Hotel Sell Threshold' Then cntrl_typ_id End) as thst_managed_in_cr,
min(case when w.cntrl_typ_desc = 'End Time' Then cntrl_typ_id End) as end_time_managed_in_cr
From
(
SELECT b.chain_id, 
       b.chain_nm, 
       b.chain_cd, 
       b.brand_id, 
       b.brand_nm, 
       b.brand_cd, 
       b.hotel_id, 
       b.hotel_short_nm, 
       b.region_name, 
       b.country_name, 
       b.country_code, 
       b.state_name, 
       b.city_name, 
       b.rate_typ_guid, 
       b.rate_typ_cd, 
       b.rate_typ_nm, 
       b.rate_type, 
       b.rate_category, 
       b.mkt_seg_cd, 
       b.rate_class, 
       b.rateactive, 
       b.commissionableflag, 
       b.hurdle, 
       b.breakfast, 
       b.loayltyredemption, 
       b.eligibleforaccrual, 
       b.managedcrsguranteepolicy, 
       b.managedcrscancelpolicy, 
       b.managedcrsstay, 
       b.managedcrsrateseasons, 
       b.derive_typ_id, 
       b.min_lead_days, 
       b.max_lead_days, 
       b.max_sell_date, 
       b.min_sell_date, 
       b.min_stay_arrival, 
       b.max_stay_arrival, 
       b.min_stay_thru, 
       b.max_stay_thru, 
       b.no_arrival_sun, 
       no_arrival_mon, 
       no_arrival_tue, 
       no_arrival_wed, 
       no_arrival_thu, 
       no_arrival_fri, 
       no_arrival_sat, 
       b.no_departure, ( 
       CASE 
              WHEN b.mobile_web IS NULL THEN 'N/A' 
              ELSE mobile_web 
       END)mobile_web, ( 
       CASE 
              WHEN b.booking_engine IS NULL THEN 'N/A' 
              ELSE booking_engine 
       END)booking_engine, ( 
       CASE 
              WHEN b.voice IS NULL THEN 'N/A' 
              ELSE voice 
       END) AS voice, ( 
       CASE 
              WHEN b.pms IS NULL THEN 'N/A' 
              ELSE pms 
       END) AS pms, ( 
       CASE 
              WHEN b.ids IS NULL THEN 'N/A' 
              ELSE ids 
       END) AS ids, ( 
       CASE 
              WHEN b.gds IS NULL THEN 'N/A' 
              ELSE gds 
       END) AS gds, ( 
       CASE 
              WHEN b.channel_connect IS NULL THEN 'N/A' 
              ELSE channel_connect 
       END) AS channel_connect, ( 
       CASE 
              WHEN b.retail IS NULL THEN 'N/A' 
              ELSE retail 
       END) AS retail, ( 
       CASE 
              WHEN b.crm IS NULL THEN 'N/A' 
              ELSE crm 
       END) AS crm, ( 
       CASE 
              WHEN b.google IS NULL THEN 'N/A' 
              ELSE google 
       END) AS google, 
       b.cancelpolicy, 
       b.guaranteepolicy, 
       b.commissionpolicy, 
       ct.cntrl_typ_id, 
       ct.cntrl_typ_desc 
FROM
(
Select z.*,
min(case when z.channel_name = 'Mobile Web' then channel_name||':'||confidentiality end) as Mobile_Web,
min(case when z.channel_name = 'Booking Engine' then channel_name||':'||confidentiality end) as Booking_Engine,
min(case when z.channel_name = 'Voice' then channel_name||':'||confidentiality end) as Voice,
min(case when z.channel_name = 'PMS' then channel_name||':'||confidentiality end) as PMS,
min(case when z.channel_name = 'IDS' then channel_name||':'||confidentiality end) as IDS,
min(case when z.channel_name = 'GDS' then channel_name||':'||confidentiality end) as GDS,
min(case when z.channel_name = 'Channel Connect' then channel_name||':'||confidentiality end) as Channel_Connect,
min(case when z.channel_name = 'Retail' then channel_name||':'||confidentiality end) as Retail,
min(case when z.channel_name = 'CRM' then channel_name||':'||confidentiality end) as CRM,
min(case when z.channel_name = 'Google' then channel_name||':'||confidentiality end) as Google
FROM
(
SELECT a.* ,
(CASE 
        WHEN rtc.promo_flag = 0 THEN 'N'
        WHEN rtc.promo_flag IS NULL THEN 'NA'
        ELSE 'Y' END) AS confidentiality,
(CASE 
        WHEN ch.parent_channel_id IS NULL THEN ch.channel_desc
        ELSE ch1.channel_desc END) AS channel_name
FROM
(
Select y.*,
min(case when y.dow = '1' then dow end) as no_arrival_sun,
min(case when y.dow = '2' then dow end) as no_arrival_mon,
min(case when y.dow = '3' then dow end) as no_arrival_tue,
min(case when y.dow = '4' then dow end) as no_arrival_wed,
min(case when y.dow = '5' then dow end) as no_arrival_thu,
min(case when y.dow = '6' then dow end) as no_arrival_fri,
min(case when y.dow = '7' then dow end) as no_arrival_sat
from

(
Select dow, no_no_departure.*
From
(
Select x.*,
min(case when x.cntrl_typ_desc = 'Min Lead Days' Then cntrl_val End) as Min_Lead_Days,
min(case when x.cntrl_typ_desc = 'Max Lead Days' Then cntrl_val End) as Max_Lead_Days,
min(case when x.cntrl_typ_desc = 'Max Sell Date' Then cntrl_val End) as Max_Sell_Date,
min(case when x.cntrl_typ_desc = 'Min Sell Date' Then cntrl_val End) as Min_Sell_Date,
min(case when x.cntrl_typ_desc = 'Min Stay Arrival' Then cntrl_val End) as Min_Stay_Arrival,
min(case when x.cntrl_typ_desc = 'Max Stay Arrival' Then cntrl_val End) as Max_Stay_Arrival,
min(case when x.cntrl_typ_desc = 'Min Stay Thru' Then cntrl_val End) as Min_Stay_Thru,
min(case when x.cntrl_typ_desc = 'Max Stay Thru' Then cntrl_val End) as Max_Stay_Thru,
min(case when x.cntrl_typ_desc = 'No Departure' Then cntrl_val End) as No_Departure
from
(
SELECT c1.chain_id, 
       c1.chain_nm, 
       c1.chain_cd, 
       c.chain_id                       AS brand_id, 
       c.chain_nm                       AS brand_nm, 
       c.chain_cd                       AS brand_cd, 
       h.hotel_id, 
       h.hotel_short_nm, 
       h.hotel_guid, 
       gr.geo_region_desc               AS region_name, 
       gc.geo_desc                      AS country_name, 
       gc.oddr_geo_cd                   AS country_code, 
       ci.contact_state                 AS state_name, 
       ci.contact_city                  AS city_name, 
       rt.rate_typ_guid, 
       rt.mirrored_rate_typ_guid, 
       rt.rate_typ_cd, 
       rt.rate_typ_nm, 
       ( CASE 
           WHEN rt.apply_bar_flag = 1 THEN 'Bar' 
           WHEN rt.corp_flag = 1 THEN 'Negotiated' 
           ELSE 'Public' 
         END )                          AS rate_type, 
       rtg.rate_typ_grp_cd              AS rate_category, 
       msl.mkt_seg_nm                   AS mkt_seg_cd, 
       rc.rate_class_desc               AS rate_class, 
       rt.active_flag                   AS rateactive, 
       rt.commission_flag               AS commissionableflag, 
       rt.apply_hurdle_flag             AS hurdle, 
       rt.breakfast_inc_flag            AS breakfast, 
       rt.loyalty_redeemable_flag       AS loayltyredemption, 
       rt.accrual_flag                  AS eligibleforaccrual, 
       rt.manage_in_crs_gtd_policy_flag AS managedcrsguranteepolicy, 
       rt.manage_in_crs_cxl_policy_flag AS managedcrscancelpolicy, 
       rt.manage_in_crs_stay_cntrl_flag AS managedcrsstay, 
       rt.manage_in_crs_rate_seasn_flag AS managedcrsrateseasons, 
       hcp.hotel_cancel_policy_cd       AS cancelpolicy, 
       hbp.hotel_book_policy_cd         AS guaranteepolicy, 
       cp.commission_cd                 AS commissionpolicy, 
       rt.derive_typ_id, 
       ct.cntrl_typ_desc,
       ( CASE 
        WHEN rtct.cntrl_typ_id IN ( 15, 10 ) THEN 
        to_char((to_date('2000-05-14', 'YYYY-MM-DD') + cast (rtct.cntrl_val as integer)), 'DD-MON-YY')
        ELSE To_char(cntrl_val, 'FM999999999999999999') 
        END ) AS cntrl_val
FROM   shscrsrepl.rate_typ rt 
       inner join shscrsrepl.hotel h 
               ON h.hotel_guid = rt.hotel_guid 
       inner join shscrsrepl.contact_info ci 
               ON ci.hotel_guid = h.hotel_guid 
                  AND h.contact_info_guid = ci.contact_info_guid 
                  AND ci.hotel_primary_flag = 1 
       inner join shscrsrepl.geo_loc gc 
               ON gc.geo_cd = ci.contact_country 
                  AND parent_geo_loc_id IS NULL 
       inner join shscrsrepl.geo_region gr 
               ON gr.geo_region_id = gc.geo_region_id 
       inner join shscrsrepl.hotel_chain hc 
               ON hc.hotel_guid = h.hotel_guid 
       inner join shscrsrepl.chain c 
               ON c.chain_guid = hc.chain_guid 
       inner join shscrsrepl.chain c1 
               ON c1.chain_guid = c.parent_chain_guid 
       left join shscrsrepl.hotel_cancel_policy hcp 
              ON hcp.hotel_cancel_policy_guid = rt.default_cancel_policy_guid 
       left join shscrsrepl.hotel_book_policy hbp 
              ON hbp.hotel_book_policy_guid = rt.default_book_policy_guid 
       left join shscrsrepl.commission_policy cp 
              ON cp.commission_policy_guid = rt.commission_policy_guid 
       left join shscrsrepl.rate_typ_grp rtg 
              ON rtg.rate_typ_grp_guid = rt.rate_typ_grp_guid 
       left join shscrsrepl.mkt_seg_lang msl 
              ON msl.mkt_seg_guid = rt.mkt_seg_guid 
       left join shscrsrepl.rate_class rc 
              ON rc.rate_class_id = rt.rate_class_id 
       left join shscrsrepl.rate_typ_cntrl_typ rtct 
              ON rtct.rate_typ_guid = CASE 
                                        WHEN rt.mirrored_rate_typ_guid IS NULL 
                                      THEN 
                                        rt.rate_typ_guid 
                                        ELSE mirrored_rate_typ_guid 
                                      END 
       left join shscrsrepl.cntrl_typ ct 
              ON ct.cntrl_typ_id = rtct.cntrl_typ_id 
WHERE  c.chain_typ_id = 3 
       AND hc.default_flag = 1 
       --AND ( DATE '2000-05-14' + h.active_end_cal_id ) > = current_date
       AND to_date('2000-05-14','YYYY-MM-DD') + cast (h.ACTIVE_END_CAL_ID as integer) >= CURRENT_DATE
       --AND rt.active_flag=1 
       AND rt.delete_flag = 0 
       ---AND UPPER(rt.rate_typ_nm) NOT LIKE '%TEST%' 
       ---AND UPPER(rt.rate_typ_nm) not like '% QA %' 
       ---AND UPPER(rt.rate_typ_cd) NOT LIKE '%MIRRORRATE%' 
	    and c1.chain_id in (select CHAIN_ID from ccrs_re.RE_EXTRACT_CONFIG where SOURCE_ODS_SCHEMA = 'shscrsrepl' and DISCONTINUE_DT IS NULL)
       --AND c1.chain_id = 5136 
       ---and h.hotel_id in (82231) 
       AND NOT EXISTS (SELECT 1 
                       FROM   shscrsrepl.stay_cntrl_hotel sch 
                       WHERE  sch.hotel_guid = h.hotel_guid 
                              AND sch.cntrl_typ_id = 19 
                              AND date_trunc('day',current_date) BETWEEN ( 
                                  DATE '2000-05-14' + cast (start_cal_id as integer)
                                                         ) 
                                                         AND ( 
                                  DATE 
                                  '2000-05-14' + cast (end_cal_id as integer)) 
       AND h.hotel_guid NOT IN (SELECT DISTINCT a.hotel_guid 
                                FROM   shscrsrepl.hotel a 
                                       inner join shscrsrepl.hotel_attr b 
                                               ON a.hotel_guid = b.hotel_guid 
                                                  AND b.attr_field_id = 876 
                                                  AND 
                                       Upper(b.hotel_attr_val) = 'TRUE'))
---AND UPPER(h.hotel_short_nm) NOT LIKE '%TEST%' 
---AND UPPER(h.hotel_short_nm) NOT LIKE '%QA %' 
---AND UPPER(h.hotel_short_nm) NOT LIKE '%TRAINING%' 
---AND UPPER(h.hotel_short_nm) NOT LIKE '%DO%NOT%' 
---AND UPPER(h.hotel_short_nm) NOT LIKE '%DELET%' 
---AND UPPER (h.hotel_short_nm) NOT LIKE 'ZZ%' 
---AND UPPER(h.hotel_short_nm) NOT LIKE '%TBD%' 
---AND UPPER(h.hotel_short_nm) NOT LIKE '%RMMRKTER%' 
---AND UPPER(h.hotel_short_nm) NOT LIKE '%DEMO %' 
---AND UPPER(h.hotel_short_nm) NOT LIKE '%TO BE DEL%' 
---AND UPPER(h.hotel_long_nm) NOT LIKE '%TEST%' 
---AND UPPER(h.hotel_long_nm) NOT LIKE '%TRAINING%' 
---AND UPPER(h.hotel_long_nm) NOT LIKE '%DO%NOT%' 
---AND UPPER(h.hotel_long_nm) NOT LIKE '%QA %' 
---AND UPPER(h.hotel_long_nm) NOT LIKE '%DELET%' 
---AND UPPER(h.hotel_long_nm) NOT LIKE '%TO BE DEL%' ---AND UPPER(h.hotel_long_nm) NOT LIKE '%MODIFY%' 
---AND UPPER(h.hotel_long_nm) NOT LIKE '%TBD%' 
---AND UPPER(h.hotel_long_nm) NOT LIKE '%RMMRKTER%' 
---AND UPPER(h.hotel_long_nm) NOT LIKE '%DEMO %' 
---AND UPPER (h.hotel_long_nm) NOT LIKE 'ZZ%' 
) x
Group By 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38
) no_no_departure
left join shscrsrepl.rate_typ_cntrl_typ rtct
ON rtct.rate_typ_guid=
CASE WHEN no_no_departure.mirrored_rate_typ_guid IS NULL THEN no_no_departure.rate_typ_guid
ELSE no_no_departure.mirrored_rate_typ_guid END
) y
Group By 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48
) a
LEFT JOIN shscrsrepl.rate_typ_channel rtc
ON rtc.rate_typ_guid=a.rate_typ_guid
LEFT JOIN shscrsrepl.channel ch
ON ch.channel_id=rtc.channel_id --and  (ch.PARENT_CHANNEL_ID is not null or ch.channel_id != 6)
left outer join shscrsrepl.channel ch1
ON   ch1.channel_id=ch.parent_channel_id
inner join shscrsrepl.product_channel pc
ON ch.channel_id=pc.channel_id
AND pc.hotel_guid =a.hotel_guid
AND pc.rate_typ_guid=a.rate_typ_guid
) z
Group By 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57
) b
left join shscrsrepl.manage_in_CR_rate_typ micrt on micrt.rate_typ_guid=b.rate_typ_guid
left join shscrsrepl.cntrl_typ ct on ct.cntrl_typ_id=micrt.cntrl_typ_id
) w
Group By 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62
)c
LEFT join 
( 
           SELECT     rt.rate_typ_guid, 
                      h.hotel_id 
                      /*getting distinct rate records which have future pricing records by joinin with price_season*/
           FROM       shscrsrepl.hotel h 
           inner join shscrsrepl.rate_typ rt 
           ON         rt.hotel_guid=h.hotel_guid 
           inner join shscrsrepl.product p 
           ON         p.rate_typ_guid=rt.rate_typ_guid 
           inner join shscrsrepl.rm_typ rm 
           ON         rm.rm_typ_guid=p.rm_typ_guid 
           inner join shscrsrepl.price_season ps 
           ON         ps.product_guid=p.product_guid 
           WHERE      --\$conditions 
                      --and h.hotel_id in (82231) 
                      --AND
                      rt.delete_flag=0 
           AND        rm.delete_flag=0 
           AND       to_date('2000-05-14','YYYY-MM-DD') + cast (ps.end_cal_id as integer) >= CURRENT_DATE
           --(DATE '2000-05-14' + ps.end_cal_id) >= current_date 
           AND        upper(rt.rate_typ_nm) NOT LIKE '%TEST%' 
           AND        upper(rt.rate_typ_nm) NOT LIKE '% QA %' 
           AND        upper(rt.rate_typ_cd) NOT LIKE '%MIRRORRATE%' 
           GROUP BY   rt.rate_typ_guid, 
                      h.hotel_id ) pre ON pre.rate_typ_guid =c.rate_typ_guid 
AND 
pre.hotel_id=c.hotel_id ) d
left join shscrsrepl.neg_corp_rate ncr
ON      encode (ncr.rate_typ_guid, 'HEX') = d.rate_typ_guid
left join shscrsrepl.neg_corp nc
ON        nc.neg_corp_guid=ncr.neg_corp_guid
AND       neg_typ_id=1
) g
Group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76
