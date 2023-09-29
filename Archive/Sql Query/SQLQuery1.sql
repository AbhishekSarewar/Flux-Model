select *, CAST(Time as datetime) as typecast_time from master_output_table where Time>='2022-05-12 00:00:00' order by typecast_time desc;
select *, CAST(Time as datetime) as typecast_time from master_output_table where Shell='Starting Point' order by typecast_time desc;
		
SELECT * FROM DynaMix.dbo.fixed_user_inputs
select * from SIF_FrL3_TOPRPEDO order by INSERT_DATE desc
select Distinct SUPPLY_AREA from SIF_FrL3_TOPRPEDO 
select * from Geofence_data order by INSERT_DATE desc
select * from Pi_Server
select * from SIF_FB_EAF_DASHBOARD where HEAT_NUMBER = '22300569' order by MSG_TIME_STAMP desc

SELECT * FROM DeS.dbo.output_table 
WHERE convert(DATE, msg_time_stamp) >='2022-05-01 00:00:00' 
and convert(DATE, msg_time_stamp) <='2022-05-30 23:59:59' order by MSG_TIME_STAMP asc




SELECT * FROM DynaMix.dbo.sif_fb_eaf_dashboard WHERE convert(DATE, START_TIME) >='2022-02-01 00:00:00' and convert(DATE, START_TIME) < '2022-03-01 00:00:00'  

SELECT * FROM DynaMix.dbo.dynamix_compliance WHERE convert(DATE, Time) >='2022-05-28 00:00:00' and convert(DATE, Time) <'2022-05-30 00:00:00' order by TIME asc
SELECT * FROM DynaMix.dbo.dynamix_compliance where shell_number != 0 and Feasibility = 'Optimal' order by TIME asc
SELECT * FROM DynaMix.dbo.power_compliance WHERE convert(DATE, START_TIME) >'2022-03-01 00:00:00' order by start_time asc

SELECT DATA_TYPE, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE 
     TABLE_NAME   = 'dynamix_compliance' 

select * from DynaMix.dbo.dynamix_compliance_backup 
SELECT * FROM DynaMix.dbo.dynamix_compliance
Truncate table DynaMix.dbo.dynamix_compliance

ALTER TABLE DynaMix.dbo.dynamix_compliance ADD "process_to_be_used_in_next_heat" varchar(50);


ALTER TABLE DynaMix.dbo.power_compliance ADD date_added varchar(50);
ALTER TABLE DynaMix.dbo.dynamix_compliance ADD date_added varchar(50);
