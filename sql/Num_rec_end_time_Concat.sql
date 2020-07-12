with new_records as 
(
select 
target_table
,query_name
,start_time
,end_time
,(select count(*) from ccrs_stage_db.rulesenginesrcdataset_profile_concat) N
from ccrs_re.re_audit_log 
where target_table = 'rulesenginesrcdataset_profile_concat'
and query_name = 'RE_Extract_with_Group_concat'
Order BY start_time)
update ccrs_re.re_audit_log as a
SET 
end_time = current_timestamp,
Num_records_inserted = new_records.N
from new_records
where a.end_time is NULL
and a.Num_records_inserted is NULL
and a.start_time = new_records.start_time;