Truncate table ccrs_ri.RulesEngineSRCDataSet;

Insert into ccrs_re.re_audit_log
(target_table,query_name)
values
('RulesEngineSRCDataSet','GDS_translate_populate');

insert into ccrs_ri.RulesEngineSRCDataSet
SELECT chain_id,
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
       rateactive,
       commissionableflag,
       hurdle,
       breakfast,
       loayltyredemption,
       eligibleforaccrual,
       managedcrsguranteepolicy,
       managedcrscancelpolicy,
       managedcrsstay,
       managedcrsrateseasons,
       derive_typ_id,
       min_lead_days,
       max_lead_days,
       max_sell_date,
       min_sell_date,
       min_stay_arrival,
       max_stay_arrival,
       min_stay_thru,
       max_stay_thru,
       no_arrival,
       no_arrival_sun,
       no_arrival_mon,
       no_arrival_tue,
       no_arrival_wed,
       no_arrival_thu,
       no_arrival_fri,
       no_arrival_sat,
       no_departure,
       mobile_web,
       booking_engine,
       voice,
       pms,
       ids,
       gds,
       channel_connect,
       retail,
       crm,
       google,
       cancelpolicy,
       guaranteepolicy,
       commissionpolicy,
       closed_managed_in_cr,
       sell_limit_manage_in_cr,
       min_stay_arrival_micr,
       max_stay_arrival_micr,
       min_lead_time_micr,
       max_lead_time_micr,
       min_stay_through_micr,
       max_stay_through_micr,
       min_sell_date_micr,
       max_sell_date_micr,
       thst_managed_in_cr,
       end_time_managed_in_cr,
       pricing_record_exists,
       profileid,
       f.gdstranslate,
       current_date AS eda_load_dt,
       rate_typ_guid_hex
From
(SELECT *
        FROM   ccrs_stage_db.rulesenginesrcdataset_profile_concat) e
Left join
(SELECT 
        crs_chain_id,
        brand_code,
        crs_hotel_id,
        hotel_name,
        crs_rate_type_code,
        ( CASE
         WHEN strpos(concat_code_translated, 'No') > 0 THEN 0
         ELSE 1
         END ) AS GDStranslate
                From  (SELECT crs_chain_id,
                               brand_code,
                               crs_hotel_id,
                               hotel_name,
                               crs_rate_type_code,
                               string_agg(code_translated, ',') AS concat_code_translated
                                                           
                                                  FROM   (SELECT *
                                                          FROM   ccrs_ri.riratetranslations
                                                          WHERE  channel_name != 'Channel Connect')a
                        Group BY 1,2,3,4,5
                        )b) f
ON f.crs_chain_id = e.chain_id
AND f.brand_code = e.brand_cd
AND f.crs_hotel_id = e.hotel_id
AND f.crs_rate_type_code = e.rate_typ_cd

