#!/usr/bin/env python
# coding: utf-8

# Libraries
import pandas as pd
import numpy as np
import time
import datetime
from datetime import timedelta
import pulp
from pulp import *
import pyodbc
import warnings
from tqdm import tqdm
warnings.filterwarnings('ignore')

pd.set_option('max_rows',500)
pd.set_option('max_columns',500)

# registering progress apply
tqdm.pandas(desc = "Processing Dataframe")

# run label
now = datetime.datetime.now()
now = now.strftime('%Y_%m_%d_%H_%M_%S')

# output path
output_path = f'../Output Files/dir_{now}/'

# input path
input_path = "../Input Files/"


# In[6]:


conn = pyodbc.connect('Driver={SQL Server};'
                     'Server=STEELDNA;'
                     'Database=Flux_Model;'
                     'UID=sa;'
                     'PWD=admin@123;'
                     'Trusted_Connection=no;')

cursor = conn.cursor()

######################### checking if model needs to be run #####################

# getting list of heats for which model has not yet been run


# ### Phosphorus Predictions

# In[8]:


def phosphorus_prediction_opti(row):
    """

    This function will calculate the phosphorus
    """

    if (row['o2decarb'] > 2468.5) and (row['dri_blow'] > 10.55) and (row['toplanceflowrate'] > 230.45) and (row['coke_f'] <= 1031.3):
        row['lm_p_predicted_o'] = -0.0002 * row['dololime_setup_o'] + 6.64e-05 * row['dri_blow'] + 8.425e-05 * row['dri_arc'] + -0.0002 * row['lime_blowi_o'] + -7.735e-05 * row['lime_arc_o'] + -6.731e-05 * row['dolo_blow_o'] + -0.0004 * row['dolo_arc_o'] + -6.845000000000001e-07 * row['o2decarb'] + 0.0011 * row['hm_c_tons'] + 0.0002 * row['hm_si_tons'] + 0.0214 * row['hm_p_tons'] + 0.0003 * row['hm_mn_tons']

    elif (row['o2decarb'] > 2468.5) and (row['dri_blow'] > 10.55) and (row['toplanceflowrate'] <= 230.45) and (row['coke_f'] > 340.15):
        row['lm_p_predicted_o'] = -0.0002 * row['dololime_setup_o'] + 5.605e-05 * row['dri_blow'] + 6.982e-05 * row['dri_arc'] + -0.0003 * row['lime_blowi_o'] + -2.663e-06 * row['lime_arc_o'] + -0.0005 * row['dolo_blow_o'] + -0.0004 * row['dolo_arc_o'] + -5.209000000000001e-07 * row['o2decarb'] + 0.0017 * row['hm_c_tons'] + 0.0004 * row['hm_si_tons'] + 0.006 * row['hm_p_tons'] + 0.0008 * row['hm_mn_tons']

    elif (row['o2decarb'] <= 2468.5) and (row['dri_blow'] > 12.75) and (row['coke_f'] <= 398.5) and (row['dololime_setup_o'] > 0.15):
        row['lm_p_predicted_o'] = 0.0004 * row['dololime_setup_o'] + 8.103e-05 * row['dri_blow'] + 7.429e-05 * row['dri_arc'] + -0.0003 * row['lime_blowi_o'] + 2.236e-06 * row['lime_arc_o'] + -0.0005 * row['dolo_blow_o'] + -0.0002 * row['dolo_arc_o'] + -4.2950000000000003e-07 * row['o2decarb'] + 0.0009 * row['hm_c_tons'] + 0.0009 * row['hm_si_tons'] + 0.0161 * row['hm_p_tons'] + 0.0014 * row['hm_mn_tons']

    elif (row['o2decarb'] <= 2468.5) and (row['dri_blow'] > 12.75) and (row['coke_f'] > 398.5) and (row['bathwt'] > 171.35):
        row['lm_p_predicted_o'] = -0.0004 * row['dololime_setup_o'] + 8.741e-05 * row['dri_blow'] + 5.692e-05 * row['dri_arc'] + -0.0002 * row['lime_blowi_o'] + 0.0003 * row['lime_arc_o'] + -0.0009 * row['dolo_blow_o'] + -0.0001 * row['dolo_arc_o'] + -4.207e-07 * row['o2decarb'] + 0.0012 * row['hm_c_tons'] + 0.0003 * row['hm_si_tons'] + 0.0185 * row['hm_p_tons'] + 0.001 * row['hm_mn_tons']

    elif (row['o2decarb'] > 2468.5) and (row['dri_blow'] > 10.55) and (row['toplanceflowrate'] <= 230.45) and (row['coke_f'] <= 340.15):
        row['lm_p_predicted_o'] = -0.0002 * row['dololime_setup_o'] + 5.155e-05 * row['dri_blow'] + 9.918e-05 * row['dri_arc'] + -0.0001 * row['lime_blowi_o'] + 1.504e-07 * row['lime_arc_o'] + 0.0001 * row['dolo_blow_o'] + -0.0002 * row['dolo_arc_o'] + -8.667e-07 * row['o2decarb'] + 0.0012 * row['hm_c_tons'] + -0.0004 * row['hm_si_tons'] + 0.0122 * row['hm_p_tons'] + 0.0011 * row['hm_mn_tons']

    elif (row['o2decarb'] > 2468.5) and (row['dri_blow'] <= 10.55) and (row['lime_arc_o'] <= 4.85) and (row['dri_arc'] <= 75.6):
        row['lm_p_predicted_o'] = -0.0004 * row['dololime_setup_o'] + 0.0001 * row['dri_blow'] + 7.293e-05 * row['dri_arc'] + -0.0002 * row['lime_blowi_o'] + -0.0004 * row['lime_arc_o'] + -0.0004 * row['dolo_blow_o'] + -0.0006 * row['dolo_arc_o'] + -2.0449999999999998e-07 * row['o2decarb'] + 0.0011 * row['hm_c_tons'] + 0.0004 * row['hm_si_tons'] + 0.0209 * row['hm_p_tons'] + 0.0017 * row['hm_mn_tons']

    elif (row['o2decarb'] > 2468.5) and (row['dri_blow'] > 10.55) and (row['toplanceflowrate'] > 230.45) and (row['coke_f'] > 1031.3):
        row['lm_p_predicted_o'] = -0.0002 * row['dololime_setup_o'] + 2.853e-05 * row['dri_blow'] + 5.935e-05 * row['dri_arc'] + 0.0002 * row['lime_blowi_o'] + 3.917e-06 * row['lime_arc_o'] + -0.0016 * row['dolo_blow_o'] + -0.0003 * row['dolo_arc_o'] + -8.309000000000001e-07 * row['o2decarb'] + 0.0025 * row['hm_c_tons'] + -0.001 * row['hm_si_tons'] + -0.0218 * row['hm_p_tons'] + 0.002 * row['hm_mn_tons']

    elif (row['o2decarb'] > 2468.5) and (row['dri_blow'] <= 10.55) and (row['lime_arc_o'] > 4.85) and (row['o2toplance'] <= 6647.3):
        row['lm_p_predicted_o'] = -0.0004 * row['dololime_setup_o'] + 0.0002 * row['dri_blow'] + 9.411e-05 * row['dri_arc'] + -0.0002 * row['lime_blowi_o'] + -0.0002 * row['lime_arc_o'] + -0.0014 * row['dolo_blow_o'] + -0.0004 * row['dolo_arc_o'] + -2.145e-07 * row['o2decarb'] + 0.0008 * row['hm_c_tons'] + 0.0011 * row['hm_si_tons'] + 0.0114 * row['hm_p_tons'] + 0.0005 * row['hm_mn_tons']

    elif (row['o2decarb'] <= 2468.5) and (row['dri_blow'] <= 12.75) and (row['o2decarb'] <= 2009.0) and (row['bathwt'] <= 232.25):
        row['lm_p_predicted_o'] = -0.0001 * row['dololime_setup_o'] + 0.0002 * row['dri_blow'] + 6.984e-05 * row['dri_arc'] + -0.0004 * row['lime_blowi_o'] + -0.0002 * row['lime_arc_o'] + -0.001 * row['dolo_blow_o'] + -0.0007 * row['dolo_arc_o'] + 3.047e-06 * row['o2decarb'] + 0.0005 * row['hm_c_tons'] + 0.0001 * row['hm_si_tons'] + 0.034 * row['hm_p_tons'] + -0.0004 * row['hm_mn_tons']

    elif (row['o2decarb'] <= 2468.5) and (row['dri_blow'] > 12.75) and (row['coke_f'] <= 398.5) and (row['dololime_setup_o'] <= 0.15):
        row['lm_p_predicted_o'] = 0.0325 * row['dololime_setup_o'] + 0.0001 * row['dri_blow'] + 1.104e-05 * row['dri_arc'] + -0.0004 * row['lime_blowi_o'] + 0.0006 * row['lime_arc_o'] + -0.001 * row['dolo_blow_o'] + -0.0009 * row['dolo_arc_o'] + 2.108e-06 * row['o2decarb'] + -0.0001 * row['hm_c_tons'] + 0.0027 * row['hm_si_tons'] + 0.0538 * row['hm_p_tons'] + 0.0032 * row['hm_mn_tons']

    elif (row['o2decarb'] <= 2468.5) and (row['dri_blow'] <= 12.75) and (row['o2decarb'] > 2009.0) and (row['dri_arc'] > 54.65):
        row['lm_p_predicted_o'] = -0.0009 * row['dololime_setup_o'] + 1.602e-05 * row['dri_blow'] + 7.277e-05 * row['dri_arc'] + -0.0001 * row['lime_blowi_o'] + -0.0004 * row['lime_arc_o'] + -0.0019 * row['dolo_blow_o'] + -0.0002 * row['dolo_arc_o'] + -1.5980000000000002e-06 * row['o2decarb'] + 0.0017 * row['hm_c_tons'] + -0.0004 * row['hm_si_tons'] + 0.0321 * row['hm_p_tons'] + 8.575e-05 * row['hm_mn_tons']

    elif (row['o2decarb'] <= 2468.5) and (row['dri_blow'] <= 12.75) and (row['o2decarb'] > 2009.0) and (row['dri_arc'] <= 54.65):
        row['lm_p_predicted_o'] = 0.0003 * row['dololime_setup_o'] + 0.0002 * row['dri_blow'] + 8.018e-05 * row['dri_arc'] + -0.0004 * row['lime_blowi_o'] + -0.0003 * row['lime_arc_o'] + 0.0002 * row['dolo_blow_o'] + -0.0003 * row['dolo_arc_o'] + 3.903e-06 * row['o2decarb'] + -0.0004 * row['hm_c_tons'] + 0.0001 * row['hm_si_tons'] + 0.0104 * row['hm_p_tons'] + 0.0006 * row['hm_mn_tons']

    elif (row['o2decarb'] > 2468.5) and (row['dri_blow'] <= 10.55) and (row['lime_arc_o'] <= 4.85) and (row['dri_arc'] > 75.6):
        row['lm_p_predicted_o'] = -0.0002 * row['dololime_setup_o'] + 0.0002 * row['dri_blow'] + 6.856e-05 * row['dri_arc'] + -0.0006 * row['lime_blowi_o'] + -0.0013 * row['lime_arc_o'] + 0.0007 * row['dolo_blow_o'] + -0.0007 * row['dolo_arc_o'] + -2.157e-07 * row['o2decarb'] + 0.0026 * row['hm_c_tons'] + -0.0046 * row['hm_si_tons'] + 0.0453 * row['hm_p_tons'] + 0.0005 * row['hm_mn_tons']

    elif (row['o2decarb'] > 2468.5) and (row['dri_blow'] <= 10.55) and (row['lime_arc_o'] > 4.85) and (row['o2toplance'] > 6647.3):
        row['lm_p_predicted_o'] = 0.0006 * row['dololime_setup_o'] + 0.0002 * row['dri_blow'] + 0.0001 * row['dri_arc'] + -0.0007 * row['lime_blowi_o'] + -0.001 * row['lime_arc_o'] + -0.0013 * row['dolo_blow_o'] + -0.0005 * row['dolo_arc_o'] + -1.0409999999999999e-06 * row['o2decarb'] + 0.0031 * row['hm_c_tons'] + 0.0022 * row['hm_si_tons'] + -0.0196 * row['hm_p_tons'] + -0.0019 * row['hm_mn_tons']

    elif (row['o2decarb'] <= 2468.5) and (row['dri_blow'] > 12.75) and (row['coke_f'] > 398.5) and (row['bathwt'] <= 171.35):
        row['lm_p_predicted_o'] = -0.0004 * row['dololime_setup_o'] + -0.0005 * row['dri_blow'] + 8.026e-05 * row['dri_arc'] + -0.0009 * row['lime_blowi_o'] + 0.0005 * row['lime_arc_o'] + -0.0864 * row['dolo_blow_o'] + 0.0052 * row['dolo_arc_o'] + -4.778e-07 * row['o2decarb'] + 0.0068 * row['hm_c_tons'] + 0.0018 * row['hm_si_tons'] + -0.0065 * row['hm_p_tons'] + -0.0051 * row['hm_mn_tons']

    elif (row['o2decarb'] <= 2468.5) and (row['dri_blow'] <= 12.75) and (row['o2decarb'] <= 2009.0) and (row['bathwt'] > 232.25):
        row['lm_p_predicted_o'] = -0.0007 * row['dololime_setup_o'] + 0.0005 * row['dri_blow'] + 0.0001 * row['dri_arc'] + 0.0002 * row['lime_blowi_o'] + 0.0004 * row['lime_arc_o'] + 9.891e-16 * row['dolo_blow_o'] + -0.0012 * row['dolo_arc_o'] + -2.588e-06 * row['o2decarb'] + 0.0076 * row['hm_c_tons'] + -0.0048 * row['hm_si_tons'] + -0.202 * row['hm_p_tons'] + 0.0041 * row['hm_mn_tons']
    else:
        print("weak node")
        row['lm_p_predicted_o'] = None

    return row


def phos_pred_func(heat):


    #input_features=pd.read_sql("SELECT TOP(1000) * FROM conarc_flux_data order by MSG_TIME_STAMP desc", conn)
    querry_1="SELECT * FROM conarc_flux_data where HM_WT >0 and HEAT_NUMBER = " + heat
    input_features=pd.read_sql(querry_1, conn)

    shell_number=input_features['SHELL'].astype(int).values[0]

    ads=input_features.copy()

    #print(ads)

    # cleaning up column names for ease of processing
    rename_dict={'HEAT_NUMBER':'HEAT','START_TIME':'START','BATH_WT':'BathWt'
                 ,'HM_WT':'HotMetal','STEEL_GRADE':'Grade','SCRAP':'Scrap'
                 ,'DOLO_KG':'Dolo','DRI_KG':'DRI','LIME_KG':'Lime','COKE_FINES':'coke_f'
                ,'HM_C_PER':'HM_C','HM_P_PER':'HM_P','HM_MN_PER':'HM_MN','HM_S_PER':'HM_S','HM_SI_PER':'HM_SI'}

    ads.rename(columns=rename_dict,inplace=True)
    ads.columns = [x.split('[')[0].strip() for x in ads.columns]

    weights =['HotMetal', 'BathWt','Scrap','Dolo','DRI','Lime','coke_f']

    for col in weights:
        ads[col]=ads[col]/1000

    ads['Updated Arcing Flag']=0
    ads['Updated Arcing Flag'] = np.where((ads['HotMetal'] > 160), 0,1)

    #ads['Updated Arcing Flag']=1

    hm_wt_check=ads['HotMetal'].values[0]

    if hm_wt_check > 160:
        bath_wt_assumed=225
    else:
        bath_wt_assumed=215

    # standardising ads column names
    ads.columns = [x.replace(" ","_").replace(".","").replace("-","_").lower() for x in ads.columns]

    # hm chemistry columns
    # these are all percentages, and need to be multiplied with hm values to get
    # the actual hm chemistry
    hm_chem_cols = ['hm_c', 'hm_si', 'hm_s', 'hm_p', 'hm_mn']

    # ads['hm_si']=.4


    for each in hm_chem_cols:
        ads[each + "_tons"] = ads[each] * ads['hotmetal']/100



    print('hm wt',hm_wt_check)

    if hm_wt_check<160:

        if 'EAF 1st Blowing End' in ads['status_description'].unique():
            blow_data=ads[(ads['status_description']=='EAF 1st Blowing End')].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])[['heat','dri','dolo','lime','updated_arcing_flag']]
        else:
            print('considering blowing start for blow data')
            blow_data=ads[(ads['status_description']=='EAF 1st Blowing Start')].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])[['heat','dri','dolo','lime','updated_arcing_flag']]

        if 'EAF 1st Arcing Start' in ads['status_description'].unique():
            ads=ads[(ads['status_description']=='EAF 1st Arcing Start')].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])
        else:
            print('considering blowing start for flux data')
            ads=ads[(ads['status_description']=='EAF 1st Blowing Start')].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])

    else:
        blow_data=ads[(ads['status_description']=='EAF 1st Blowing Start')].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])[['heat','dri','dolo','lime','updated_arcing_flag']]
        ads=ads[(ads['status_description']=='EAF 1st Blowing Start')].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])

    ads=ads[['heat','msg_time_stamp','shell','hotmetal','bathwt','grade','coke_f','status_description','hm_c_tons','hm_si_tons','hm_s_tons','hm_p_tons','hm_mn_tons','scrap']]

    print(ads)

    blow_data[['dri','dolo','lime']]=blow_data[['dri','dolo','lime']].fillna(0)

    ads=pd.merge(ads,blow_data.rename(columns={'dri':'dri_blow','dolo':'dolo_blow_o','lime':'lime_blowi_o'}),on=['heat'], how='left')



    #print(ads)
    ads['dri_arc']=0
    ads['dri_arc'][ads['updated_arcing_flag']==0]=0
    ads['dri_arc'][ads['updated_arcing_flag']==1]=bath_wt_assumed-ads['hotmetal']-ads['dri_blow']-ads['scrap']


    querry_2="SELECT * FROM flux_output where heat = " + heat


    opti_df=pd.read_sql(querry_2, conn)
    opti_df=opti_df[opti_df['status_description']=='Arcing Predictions'].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])
    opti_df=opti_df[['heat','start_time','dololime_setup_o','dolo_arc_o','lime_arc_o']]

    opti_df[['dololime_setup_o','dolo_arc_o','lime_arc_o']]=opti_df[['dololime_setup_o','dolo_arc_o','lime_arc_o']].fillna(0)
    print(opti_df['dolo_arc_o'])
    print(opti_df[['dololime_setup_o','dolo_arc_o','lime_arc_o']])


    ads=pd.merge(ads,opti_df, on=['heat'],how='left')

    if len(opti_df) ==0:
        ads['dololime_setup_o']=0
        ads['lime_arc_o']=0
        ads['dolo_arc_o']=0


    pi_server = pd.read_sql('SELECT * FROM DynaMix.dbo.Pi_server', conn)

    pi_server['TAG_DESCRIPTION']= pi_server['TAG_DESCRIPTION'].str.replace('Tripple','Consumption',regex=True)
    # Calculating bf rate
    toplance_o2 = pi_server[pi_server['TAG_DESCRIPTION'].str.contains("O2 Consumption")].sort_values(by="TIME", ascending=False).drop_duplicates(subset=['TAG'],keep="first")
    toplance_o2_flow_rate = pi_server[pi_server['TAG_DESCRIPTION'].str.contains("O2_Flow")].sort_values(by="TIME", ascending=False).drop_duplicates(subset=['TAG'],keep="first")
    o2_decarb = pi_server[pi_server['TAG_DESCRIPTION'].str.contains("O2DECARB")].sort_values(by="TIME", ascending=False).drop_duplicates(subset=['TAG'],keep="first")
    toplance_o2[['A', 'shell', 'Tag']] = toplance_o2['TAG_DESCRIPTION'].str.split(' ', 2, expand=True)
    toplance_o2_flow_rate['shell']=toplance_o2_flow_rate['TAG_DESCRIPTION'].str.slice(start =2, stop =3)
    o2_decarb[['A', 'shell', 'Tag']] = o2_decarb['TAG_DESCRIPTION'].str.split(' ', 2, expand=True)

    ads=pd.merge(ads,toplance_o2_flow_rate[['shell','VALUE']].rename(columns={'VALUE':'toplanceflowrate'}), on = 'shell', how='left')
    ads=pd.merge(ads,toplance_o2[['shell','VALUE']].rename(columns={'VALUE':'o2toplance'}), on = 'shell', how='left')
    ads=pd.merge(ads,o2_decarb[['shell','VALUE']].rename(columns={'VALUE':'o2decarb'}), on = 'shell', how='left')

    if ads['o2toplance'].values[0]==0:
        ads['o2toplance']=9062#ads['o2toplance']
    if ads['o2decarb'].values[0]==0:
        ads['o2decarb']=660#ads['o2toplance']
    if ads['toplanceflowrate'].values[0]==0:
        ads['toplanceflowrate']=7#ads['o2toplance']
    # ads['dololime_setup_o']=3.33
    # ads['dolo_blow_o']=0
    # ads['lime_blowi_o']=7.69
    # ads['lime_arc_o']=0
    # ads['dolo_arc_o']=0


    list_features = ['heat',
                     'msg_time_stamp',
                     'shell',
                     'hotmetal',
                     'bathwt',
                     'start_time',
                     'grade',
     'o2decarb',
     'o2toplance',
     'toplanceflowrate',
     'dololime_setup_o',
     'dri_blow',
     'dri_arc',
     'lime_blowi_o',
     'lime_arc_o',
     'dolo_blow_o',
     'dolo_arc_o',
     'coke_f',
     'hm_p_tons',
     'hm_mn_tons',
     'hm_si_tons',
     'hm_c_tons',

    ]



    ads=ads[list_features]


    print(ads)
    # getting lm_p_predicted for original data
    predicted_df = ads.apply(phosphorus_prediction_opti, axis = 1)

    #print(predicted_df)
    predicted_df['status_description']='P Predictions'
    now = datetime.datetime.now()
    now = now.strftime('%Y-%m-%d %H:%M:%S')
    predicted_df['msg_time_stamp'] = now
    predicted_df['model_feasibility']='feasible'

    keep_col=['msg_time_stamp','heat','start_time','status_description','bathwt','hotmetal'
             ,'grade','model_feasibility','dololime_setup_o','dolo_blow_o','lime_blowi_o','dolo_arc_o','lime_arc_o','lm_p_predicted_o']
    #print(predicted_df.columns)
    predicted_df=predicted_df[keep_col]

    predicted_df[['dololime_setup_o','dolo_blow_o','lime_blowi_o','dolo_arc_o','lime_arc_o','lm_p_predicted_o']]=predicted_df[['dololime_setup_o','dolo_blow_o','lime_blowi_o','dolo_arc_o','lime_arc_o','lm_p_predicted_o']].fillna(0)

    if len(predicted_df)==0:
        predicted_df.loc[0]=[now,heat_num,'',now,status,0,0,'','infeasible',0,0,0,0,0,0]

    predicted_df['shell_value']=shell_number
    predicted_df['shell_value']=predicted_df['shell_value'].astype(int)

    predicted_df=predicted_df.merge(ads[['heat','o2decarb','o2toplance','toplanceflowrate']], on='heat', how='left')
    print(predicted_df)


    cursor = conn.cursor()

    predicted_df['now'] =datetime.datetime.now()


#     for index, row in predicted_df.iterrows():
#
#         cursor.execute(
#         "INSERT INTO Flux_Model.dbo.flux_output (msg_time_stamp, heat, process_type, start_time, status_description, bathwt, hotmetal, grade, model_feasibility, dololime_setup_o, dolo_blow_o, lime_blowi_o, dolo_arc_o, lime_arc_o, p_prediction, o2decarb, o2toplance, toplanceflowrate) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
#          row['now'],
#          row['heat'],
#          '',
#          row['msg_time_stamp'],
#          row['status_description'],
#          row['bathwt'],
#          row['hotmetal'],
#          row['grade'],
#          row['model_feasibility'],
#          row['dololime_setup_o'],
#          row['dolo_blow_o'],
#          row['lime_blowi_o'],
#          row['dolo_arc_o'],
#          row['lime_arc_o'],
#          row['lm_p_predicted_o'],
#          row['o2decarb'],
#          row['o2toplance'],
#          row['toplanceflowrate']
#          )
#
#     # commiting any changes made and closing the cursor
#     conn.commit()
#     cursor.close()
#
#
#     cursor = conn.cursor()
#
#     seq_value_phos= pd.read_sql("SELECT max(seq_value) From INT_JSW_FLUX_PHOS", conn).values[0]
#
#     for index, row in predicted_df.iterrows():
#         cursor.execute(
#         "INSERT INTO Flux_Model.dbo.INT_JSW_FLUX_PHOS (update_date, heat_number, p_pre, msg_flag, aggregate, msg_time_stamp) values (?,?,?,?,?,?)",
#          row['msg_time_stamp'],
# #         now,
#          row['heat'],
#          row['lm_p_predicted_o'],
#          'N',
#          row['shell_value'],
#          row['msg_time_stamp']
#          )
#
#     # commiting any changes made and closing the cursor
#     conn.commit()
#     cursor.close()



def main():
    # getting list of heats for which model has not yet been run
    #conarc_flux_data= pd.read_sql("SELECT TOP(1000) * FROM conarc_flux_data where HM_WT >0 order by MSG_TIME_STAMP desc", conn)
    conarc_flux_data=pd.read_sql("SELECT * FROM conarc_flux_data where HEAT_NUMBER in ('22100830') and HM_WT >0", conn)
    #print(conarc_flux_data)

    conarc_flux_data['key']=0
    heat_status_check=conarc_flux_data[['HEAT_NUMBER','key']].drop_duplicates()

    status_check = pd.DataFrame({
        'key': [0],
        'status_description': ['P Predictions'],
        'model_run': [3]
    })

    heat_status_check=pd.merge(heat_status_check,status_check, on=['key'], how='left')
    #cursor = conn.cursor()
    flux_output_data= pd.read_sql("SELECT TOP(1000) * FROM flux_output order by MSG_TIME_STAMP desc", conn)
    flux_output_data['Run']=1

    heat_status_check=pd.merge(heat_status_check,flux_output_data[['heat','status_description','Run']].rename(columns={'heat':'HEAT_NUMBER'}), on=['HEAT_NUMBER','status_description'],how='left')
    print(heat_status_check)
    #heat_status_check=heat_status_check[heat_status_check['Run'].isna()]

    #print(heat_status_check)
    heat_status_check=heat_status_check[heat_status_check['HEAT_NUMBER']=='22100830']
    #print(heat_status_check)

    for index, row in heat_status_check.iterrows():
        heat=row['HEAT_NUMBER']
        model_run=row['model_run']
        running_heats=pd.DataFrame()
        if model_run == 3:
            print('p')
            #conarc_flux_data= pd.read_sql("SELECT TOP(1000) * FROM conarc_flux_data where HM_WT >0 order by MSG_TIME_STAMP desc", conn)
            #querry="SELECT * FROM conarc_flux_data where HEAT_NUMBER = " + heat "
            querry="SELECT * FROM conarc_flux_data where HEAT_NUMBER = " + heat +" "
            #+" and STATUS_DESCRIPTION = 'EAF Start'
            #querry="SELECT * FROM conarc_flux_data where HEAT_NUMBER = " + heat +" and STATUS_DESCRIPTION = 'EAF 1st Arcing Start'"
            p_pred_data=pd.read_sql(querry, conn)

            hm_wt=p_pred_data.sort_values('MSG_TIME_STAMP', ascending = False).drop_duplicates(subset=['HEAT_NUMBER'])['HM_WT'].iloc[0]
            hm_wt=hm_wt/1000
            shell_used=p_pred_data.sort_values('MSG_TIME_STAMP', ascending = False).drop_duplicates(subset=['HEAT_NUMBER'])['SHELL'].iloc[0]
            shell_used="Shell "+str(shell_used)

            if hm_wt>160:
                process_type_used="100% Blowing with Scrap"
            else:
                process_type_used="CONARC with Scrap"


            fixed_input_parameters = pd.read_sql('SELECT * FROM DynaMix.dbo.fixed_input_parameter', conn)
            fixed_input_param_pivot_df = pd.melt(fixed_input_parameters, id_vars=['Parameter'], value_vars=fixed_input_parameters.columns[1:])

            def creating_columns(row):
                """

                This function will create the shell and process columns

                It just renames the columns to a standard form

                """
                if "Shell1" in row['variable']:
                    row['Shell'] = "Shell 1"
                elif "Shell2" in row['variable']:
                    row['Shell'] = "Shell 2"
                elif "Shell3" in row['variable']:
                    row['Shell'] = "Shell 3"
                elif "Shell4" in row['variable']:
                    row['Shell'] = "Shell 4"
                else:
                    print(row['variable'])

                if "100per_blowing_wo_scrap" in row['variable']:
                    row['Process'] = "100% Blowing Without Scrap"
                elif "100per_blowing_scrap" in row['variable']:
                    row['Process'] = "100% Blowing with Scrap"
                elif "CONARC_wo_scrap" in row['variable']:
                    row['Process'] = "CONARC without Scrap"
                elif "CONARC_scrap" in row['variable']:
                    row['Process'] = "CONARC with Scrap"
                else:
                    print(row['variable'])

                return row

            # Calling the function to run it
            fixed_input_param_pivot_df = fixed_input_param_pivot_df.apply(creating_columns, axis = 1)

            print(shell_used, process_type_used)
            #print(fixed_input_param_pivot_df)

            Blow_O2_Per_HM = float(fixed_input_param_pivot_df[(fixed_input_param_pivot_df['Shell']==shell_used) & (fixed_input_param_pivot_df['Process']==process_type_used) & (fixed_input_param_pivot_df['Parameter'] == "Blow_O2_Per_HM")]['value'].iloc[0])
            TL_O2_Flow_Per_Heat = float(fixed_input_param_pivot_df[(fixed_input_param_pivot_df['Shell']==shell_used) & (fixed_input_param_pivot_df['Process']==process_type_used) & (fixed_input_param_pivot_df['Parameter'] == "TL_O2_Flow_Per_Heat")]['value'].iloc[0])
            # Blow_O2_Per_HM = float(fixed_input_param_pivot_df[(fixed_input_param_pivot_df['Parameter'] == "Blow_O2_Per_HM")]['value'].iloc[0])
            # TL_O2_Flow_Per_Heat = float(fixed_input_param_pivot_df[(fixed_input_param_pivot_df['Parameter'] == "TL_O2_Flow_Per_Heat")]['value'].iloc[0])
            Total_O2_Per_LM = 62.5 - Blow_O2_Per_HM
            # #Dri_Melting_Rate_Per_Heat = float(fixed_input_param_pivot_df[(fixed_input_param_pivot_df['Shell']==shell_used) & (fixed_input_param_pivot_df['Process']==process_type_used) & (fixed_input_param_pivot_df['Parameter'] == "Dri_Melting_Rate_Per_Heat")]['value'].iloc[0])
            Dri_Melting_Rate_Per_Heat = float(fixed_input_param_pivot_df[(fixed_input_param_pivot_df['Parameter'] == "Dri_Melting_Rate_Per_Heat")]['value'].iloc[0])
            TL_O2 = hm_wt * Blow_O2_Per_HM * (1/0.95)
            blowing_time = TL_O2 * (1 / TL_O2_Flow_Per_Heat)
            Total_O2 = hm_wt * Total_O2_Per_LM
            # # getting the ADRI value from shell_tapout_time_df3, for the given shell and process
            # #adri = float(shell_tapout_time_df3[(shell_tapout_time_df3['Shell']==row['Shell']) & (shell_tapout_time_df3['Process']==row['Process'])]['Actual Time'])
            #
            #
            print(heat)
            check_p = 0
            if hm_wt < 160:
                if len(p_pred_data[p_pred_data['STATUS_DESCRIPTION']=='EAF 1st Arcing Start'].sort_values('MSG_TIME_STAMP', ascending = True).drop_duplicates(subset=['HEAT_NUMBER'])['MSG_TIME_STAMP'])>0:
                    print(p_pred_data[p_pred_data['STATUS_DESCRIPTION']=='EAF 1st Arcing Start'].sort_values('MSG_TIME_STAMP', ascending = True).drop_duplicates(subset=['HEAT_NUMBER'])['MSG_TIME_STAMP'])

                    start_time=p_pred_data[p_pred_data['STATUS_DESCRIPTION']=='EAF 1st Arcing Start'].sort_values('MSG_TIME_STAMP', ascending = True).drop_duplicates(subset=['HEAT_NUMBER'])['MSG_TIME_STAMP'].iloc[0]

                    dri_blow=p_pred_data[p_pred_data['STATUS_DESCRIPTION']=='EAF 1st Blowing End'].sort_values('MSG_TIME_STAMP', ascending = False).drop_duplicates(subset=['HEAT_NUMBER'])['DRI_KG'].iloc[0]
                    #flux_output_data[(flux_output_data['heat']==heat) & (flux_output_data['status_description']=='Arcing Predictions')]
                    print(hm_wt)
                    dri_arc=229-hm_wt-dri_blow/1000

                    print('dri',dri_arc)
                    print('Dri_Melting_Rate_Per_Heat',Dri_Melting_Rate_Per_Heat)
                #
                #
                #
                #     #arcing_time1 = adri * (1 / Dri_Melting_Rate_Per_Heat)
                    arcing_time1 = dri_arc * (1 / Dri_Melting_Rate_Per_Heat)
                    end_time=start_time + timedelta(minutes = arcing_time1)
                    now = datetime.datetime.now()
                    print(start_time,arcing_time1,end_time,now)
                    check_p = 1
                else:
                    print(heat,' at eaf start')


                #
                #     #end_time=p_pred_data.sort_values('MSG_TIME_STAMP', ascending = False).drop_duplicates(subset=['HEAT_NUMBER'])['MSG_TIME_STAMP'].iloc[0]
                #
            else: #if (hm_wt < 160) & (hm_wt > 0):
                #if len(p_pred_data[p_pred_data['STATUS_DESCRIPTION']=='EAF 1st Blowing Start'].sort_values('MSG_TIME_STAMP', ascending = True).drop_duplicates(subset=['HEAT_NUMBER'])['MSG_TIME_STAMP'])>0:
                    start_time=p_pred_data[p_pred_data['STATUS_DESCRIPTION']=='EAF Start'].sort_values('MSG_TIME_STAMP', ascending = True).drop_duplicates(subset=['HEAT_NUMBER'])['MSG_TIME_STAMP'].iloc[0]
                    end_time=start_time + timedelta(minutes = blowing_time)
                    #print(start_time,blowing_time,end_time)
                    now = datetime.datetime.now()
                    print(start_time,blowing_time,end_time,now)
                    check_p = 1

            #now = now.strftime('%Y-%m-%d %H:%M:%S')
            if check_p ==1:
                time_delta=(end_time - now).total_seconds() / 60.0
                print(time_delta)
                if time_delta <=5:
                   print("phos prediction for heat -", heat)

            phos_pred_func(heat)

                #phos_pred_func(heat)

if __name__ == "__main__":

    main()
