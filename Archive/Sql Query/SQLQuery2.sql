	
select * from grade_mapping
INSERT INTO grade_mapping (GRADE, GRADE_TYPE) VALUES ('JDHSG52AHZ', 'VAG');


select * from output_table where convert(DATE, msg_time_stamp) >='2022-03-31 00:00:00' order by MSG_TIME_STAMP desc
select * from heat_analysis where HEAT_NUMBER='22401478'  order by MSG_TIME_STAMP desc
select * from lf_heat_data where HEAT_NUMBER ='22401478' order by MSG_TIME_STAMP desc

select * from output_table where SEQ_VALUE>5930 order by MSG_TIME_STAMP desc

select * from output_table where HEAT_NUMBER='22302313' order by MSG_TIME_STAMP desc 
select * from heat_analysis where HEAT_NUMBER='22202329' order by MSG_TIME_STAMP desc 
select * from lf_heat_data where HEAT_NUMBER='22302312' order by MSG_TIME_STAMP desc 


select * from heat_analysis where convert(DATE, msg_time_stamp) >='2022-05-12 00:00:00' order by MSG_TIME_STAMP desc
select * from lf_heat_data where convert(DATE, msg_time_stamp) >='2022-05-12 00:00:00' order by MSG_TIME_STAMP desc
select * from grade_mapping where GRADE = 'JDHCG06EXZ'

select * from conarc_tap_mat order by MSG_TIMESTAMP desc
select * from fa_output_table order by MSG_TIME_STAMP desc

select * from fa_output_table where HEAT_NUMBER='22301236' order by MSG_TIME_STAMP desc
select * from conarc_tap_mat where HEAT_NUMBER='22301236' order by MSG_TIME_STAMP desc
select * from heat_analysis where HEAT_NUMBER='22301162' order by MSG_TIME_STAMP desc
select * from lf_heat_data where HEAT_NUMBER='22301162' order by MSG_TIME_STAMP desc
select * from output_table where HEAT_NUMBER='22301236' order by MSG_TIME_STAMP desc

select * from grade_constraints where Grade= 'JDHSG52AHZ'
select * from grade_mapping where Grade= 'JDHSG52AHZ'

select * from Flux_Model.dbo.flux_output order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.conarc_flux_data where convert(DATE, msg_time_stamp) >='2022-05-16 00:00:00' and STATUS_DESCRIPTION='EAF Start' and HM_WT!=0 order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.INT_JSW_FLUX_PRE order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.INT_JSW_FLUX_ARC order by MSG_TIME_STAMP desc

select * from Flux_Model.dbo.conarc_flux_data where convert(DATE, msg_time_stamp) >'2022-05-07 00:00:00' and SHIFT_TEAM ='Gr4' and  STATUS_DESCRIPTION='EAF Tapping End' order by MSG_TIME_STAMP desc

select * from Flux_Model.dbo.conarc_flux_data where convert(DATE, msg_time_stamp) >='2022-05-27 00:00:00' and  STATUS_DESCRIPTION='EAF Tapping Start' order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.flux_output where convert(DATE, msg_time_stamp) >='2022-06-07 00:00:00' and  STATUS_DESCRIPTION='Blowing Predictions' order by MSG_TIME_STAMP desc

select * from Flux_Model.dbo.flux_output where convert(DATE, msg_time_stamp) >='2022-05-01 00:00:00' and  STATUS_DESCRIPTION='P Predictions' order by MSG_TIME_STAMP desc

select * from Flux_Model.dbo.conarc_flux_data order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.conarc_flux_data where convert(DATE, msg_time_stamp) >='2022-05-01 00:00:00' and convert(DATE, msg_time_stamp) <'2022-06-30 22:00:00'  and  STATUS_DESCRIPTION='EAF Tapping End' order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.flux_output where convert(DATE, msg_time_stamp) >='2022-05-01 00:00:00' and convert(DATE, msg_time_stamp) <'2022-06-30 00:00:00' and STATUS_DESCRIPTION='Blowing Predictions' order by MSG_TIME_STAMP desc


select * from Flux_Model.dbo.conarc_flux_data where HEAT_NUMBER='22402714' order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.flux_output where HEAT='22102632' order by MSG_TIME_STAMP desc

select * from Flux_Model.dbo.flux_output order by MSG_TIME_STAMP desc

select * from Flux_Model.dbo.INT_JSW_FLUX_PRE where heat_number='22402713' order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.INT_JSW_FLUX_ARC where heat_number = '22402713' order by MSG_TIME_STAMP desc 




Delete from Flux_Model.dbo.flux_output where HEAT= '22402749' and  STATUS_DESCRIPTION!='P Predictions'
Delete from Flux_Model.dbo.INT_JSW_FLUX_PRE where heat_number= '22402749' 
Delete from Flux_Model.dbo.INT_JSW_FLUX_ARC where heat_number= '22402749' 

Select * from Flux_Model.dbo.conarc_flux_data order by MSG_TIME_STAMP desc



Select * from mat_chem_ana where MAT_TYPE = 'DOLO' order by INSERT_DATE desc
Select * from mat_chem_ana where MAT_TYPE = 'LIME' order by INSERT_DATE desc
Select * from mat_chem_ana where MAT_TYPE = 'DRI' order by INSERT_DATE desc

select * from Flux_Model.dbo.conarc_flux_data where convert(DATE, msg_time_stamp) >='2022-05-06 00:00:00' and  STATUS_DESCRIPTION='EAF Start' order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.flux_output where convert(DATE, msg_time_stamp) >='2022-06-08 00:00:00' order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.flux_output where heat like '221%' and status_description = 'P Predictions' order by MSG_TIME_STAMP desc




select * from Flux_Model.dbo.conarc_flux_data where HEAT_NUMBER='22302775' order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.flux_output where HEAT='22302679' order by MSG_TIME_STAMP desc

select * from Flux_Model.dbo.conarc_flux_data order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.flux_output where HEAT='22302645' order by MSG_TIME_STAMP desc


select * from Flux_Model.dbo.flux_output where STATUS_DESCRIPTION!='P Predictions' order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.flux_output where STATUS_DESCRIPTION='P Predictions' order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.conarc_flux_data where convert(DATE, msg_time_stamp) >='2022-05-01 00:00:00'  order by MSG_TIME_STAMP desc

select * from Flux_Model.dbo.conarc_flux_data where HEAT_NUMBER='22402994' order by MSG_TIME_STAMP desc

select * from Flux_Model.dbo.INT_JSW_FLUX_PRE where convert(DATE, msg_time_stamp) >='2022-06-21 00:00:00' order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.INT_JSW_FLUX_arc where convert(DATE, msg_time_stamp) >='2022-06-21 00:00:00' order by MSG_TIME_STAMP desc
select * from Flux_Model.dbo.flux_output where convert(DATE, msg_time_stamp) >='2022-07-04 00:00:00' order by MSG_TIME_STAMP desc


Select * from mat_chem_ana where MAT_TYPE = 'Dolo' order by INSERT_DATE desc

Select * from mat_chem_ana order by INSERT_DATE desc

SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'flux_output'

ALTER TABLE flux_output
   ADD seq_value INT IDENTITY

   ALTER TABLE flux_output
   ADD msg_flag varchar(50) 