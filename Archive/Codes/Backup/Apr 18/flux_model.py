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

pd.options.mode.chained_assignment = None
warnings.filterwarnings("ignore", message="Spaces are not permitted in the name")
warnings.filterwarnings("ignore", message="Parameter fracGap is being depreciated for gapRel")


pd.set_option('max_rows',500)
pd.set_option('max_columns',500)

# registering progress apply
tqdm.pandas(desc = "Processing Dataframe")

# run label
now = datetime.datetime.now()
now = now.strftime('%Y_%m_%d_%H_%M_%S')

conn = pyodbc.connect('Driver={SQL Server};'
                     'Server=STEELDNA;'
                     'Database=Flux_Model;'
                     'UID=sa;'
                     'PWD=admin@123;'
                     'Trusted_Connection=no;')

cursor = conn.cursor()

# output path
#output_path = f'../Output Files/dir_{now}/'
#Path(output_path).mkdir(parents = True, exist_ok = True)

# input path
#input_path = "../Input Files/"


def optimization_func(running_heats, model_run):

    heat_num = running_heats['HEAT_NUMBER'].values[0]
    print(f"Optimizing for {heat_num} and model_run: {model_run}")

    # cleaning up column names for ease of processing
    rename_dict={'HEAT_NUMBER':'HEAT','START_TIME':'START','BATH_WT':'BathWt'
                 ,'HM_WT':'HotMetal','STEEL_GRADE':'Grade','SCRAP':'Scrap'
                 ,'DOLO_KG':'Dolo','DRI_KG':'DRI','LIME_KG':'Lime'
                ,'HM_C_PER':'HM_C','HM_P_PER':'HM_P','HM_MN_PER':'HM_MN','HM_S_PER':'HM_S','HM_SI_PER':'HM_SI'}

    running_heats.rename(columns=rename_dict,inplace=True)
    running_heats.columns = [x.split('[')[0].strip() for x in running_heats.columns]

    shell_number=running_heats['SHELL'].astype(int).values[0]


    # creating ads
    ads = running_heats.copy()

    # ### Preprocessing and Null Value Treatment

    # cols to fill with 0
    cols = [
        'Scrap',
        'Dolo',
        'DRI',
        'Lime',
        ]
    ads[cols] = ads[cols].fillna(0)

    weights =['HotMetal', 'BathWt','Scrap','Dolo','DRI','Lime']
    for col in weights:
        ads[col]=ads[col]/1000


    hm_wt_input=ads['HotMetal'].values[0]

    # assumptions and fixed constraints

    # assumptions and fixed constraints
    basicity = 2.2

    if hm_wt_input <= 140:
        basicity = 1.8
    elif hm_wt_input <= 150:
        basicity = 1.9
    else:
        basicity = 2

    cao_perc = 0.36
    dri_blowing_perc=.23
    dolo_setup_limit=2.6
    dolo_setup_limit=3
    dolo_setup_at_blowing_end=0
    sio2_hm_perc = 0

    full_blow_wt_check = 160
    bath_wt_assumed=225
    bath_wt_assumed_full_blow=215

    arcing_for_full_blow=0

    #mgo_perc = 0.075
    mgo_perc = 0.065
    mgo_perc = 0.07
    mgo_perc_full_blow = 0.06
    mgo_perc_full_blow = 0.035

    if hm_wt_input < 105:
        mgo_perc=3.5/100
    elif hm_wt_input < 140:
        mgo_perc=7.5/100
    elif hm_wt_input < 150:
        mgo_perc=7.0/100
    elif hm_wt_input < 160:
        mgo_perc=5.9/100
    elif hm_wt_input < 165:
        mgo_perc=4.8/100
    elif hm_wt_input >= 165:
        mgo_perc=3.5/100

    print('mgo perc =', mgo_perc)


    ads['Updated Arcing Flag']=0
    ads['Updated Arcing Flag'] = np.where((ads['HotMetal'] > 160), 0,1)

    if ((model_run==2) & (ads['HotMetal'].values[0] >= full_blow_wt_check) & ('EAF 1st Arcing Start' in ads['STATUS_DESCRIPTION'].unique())):
        ads['Updated Arcing Flag']=1
        arcing_for_full_blow=1
        print('----arcing_for_full_blow----')

    # standardising ads column names
    ads.columns = [x.replace(" ","_").replace(".","").replace("-","_").lower() for x in ads.columns]

    # hm chemistry columns
    # these are all percentages, and need to be multiplied with hm values to get
    # the actual hm chemistry
    hm_chem_cols = ['hm_c', 'hm_si', 'hm_s', 'hm_p', 'hm_mn']
    for each in hm_chem_cols:
        ads[each + "_tons"] = ads[each] * ads['hotmetal']/100

    material_chem_data= pd.read_sql("SELECT TOP(1000) * FROM mat_chem_ana order by INSERT_DATE desc", conn)

    if model_run==1:
        opti_df=ads[(ads['status_description']=='EAF Start') & (ads['hotmetal']!=0)].sort_values('msg_time_stamp').drop_duplicates(subset=['heat'])
    elif model_run==2:
        if 'EAF 2nd Blowing End' in ads['status_description'].unique():
            opti_df=ads[(ads['status_description']=='EAF 2nd Blowing End')].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])
        else:
            opti_df=ads[(ads['status_description']=='EAF 1st Blowing End')].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])
        print('opti_Df =', opti_df['lime'].values[0])

    opti_df['bath_wt_assumed']=bath_wt_assumed
    opti_df['bath_wt_assumed'][opti_df['updated_arcing_flag']==0]=bath_wt_assumed_full_blow

    print('bath wt =', opti_df['bath_wt_assumed'].values[0])

    if ads['hm_si'].values[0] >= 1:
        if opti_df['scrap'].values[0] >= 20:
            basicity = 2.2
        else:
            basicity = 1.8
    else:
        if opti_df['scrap'].values[0] >= 20:
            basicity = 2.4

    print('hm_wt =', hm_wt_input)
    print('hm_si =', ads['hm_si'].values[0])
    print('scrap =', opti_df['scrap'].values[0])
    print('basicity =', basicity)


    mat_chem_data_read=material_chem_data[(material_chem_data['MAT_TYPE']!='DRI') & (material_chem_data['MGO_PER']!=0) & (material_chem_data['CAO_PER']!=0)].sort_values(['INSERT_DATE'],ascending=False)
    lime_chem_data=mat_chem_data_read[mat_chem_data_read['CAO_PER']>=80]
    dolo_chem_data=mat_chem_data_read[(mat_chem_data_read['CAO_PER']<80) & (mat_chem_data_read['CAO_PER']>0)]

    #lime_chem_data=material_chem_data[(material_chem_data['MAT_TYPE']=='LIME')].sort_values(['INSERT_DATE'],ascending=False)
    #lime_chem_data=lime_chem_data[(lime_chem_data['MGO_PER']!=0) & (lime_chem_data['CAO_PER']!=0)]

    #dolo_chem_data=material_chem_data[material_chem_data['MAT_TYPE']=='DOLO'].sort_values(['INSERT_DATE'],ascending=False)
    #dolo_chem_data=dolo_chem_data[(dolo_chem_data['MGO_PER']!=0) & (dolo_chem_data['CAO_PER']!=0)]

    dri_chem_data=material_chem_data[(material_chem_data['MAT_TYPE']=='DRI') & (material_chem_data['CAO_PER'] != 0) & (material_chem_data['MGO_PER']!=0) & (material_chem_data['SIO2_PER'] !=0)].sort_values(['INSERT_DATE'],ascending=False)

    # source of cao/mgo
    # these are the percentages of cao/mgo present in lime/dololime/dri

    #print(lime_chem_data['CAO_PER'].values[0]/100,lime_chem_data['MGO_PER'].values[0]/100,dolo_chem_data['CAO_PER'].values[0]/100,dolo_chem_data['MGO_PER'].values[0]/100)

    #cao
    cao_lime_perc = lime_chem_data['CAO_PER'].values[0]/100 #0.90
    cao_dololime_perc = dolo_chem_data['CAO_PER'].values[0]/100 #0.60
    cao_dri_perc = dri_chem_data['CAO_PER'].values[0]/100 #0.015

    #mgo
    mgo_lime_perc = lime_chem_data['MGO_PER'].values[0]/100 #0.015
    mgo_dololime_perc =dolo_chem_data['MGO_PER'].values[0]/100 #0.30
    mgo_dri_perc = dri_chem_data['MGO_PER'].values[0]/100 #0

    #cao
    # cao_lime_perc = 0.90
    # cao_dololime_perc = 0.60
    # cao_dri_perc = 0.015
    #
    # #mgo
    # mgo_lime_perc = 0.015
    # mgo_dololime_perc =0.30
    # mgo_dri_perc = 0

    print('cao_lime_perc =', cao_lime_perc)
    print('cao_dololime_perc =', cao_dololime_perc)
    print('mgo_lime_perc =', mgo_lime_perc)
    print('mgo_dololime_perc =', mgo_dololime_perc)

    #sio2
    sio2_dri_perc = dri_chem_data['SIO2_PER'].values[0]/100 #0.05
    p_dri_perc = dri_chem_data['P_PER'].values[0]/100 #0.05
    al2o3_dri_perc = dri_chem_data['AL2O3_PER'].values[0]/100 #0.05

    print('sio2_dri_perc =', sio2_dri_perc)
    print('p_dri_perc =', p_dri_perc)
    print('al2o3_dri_perc =', al2o3_dri_perc)


    #Calculating DRI assumptions
    opti_df['dri_blow_assumed']=0
    opti_df['dri_blow_assumed'][opti_df['updated_arcing_flag']==0]=opti_df['bath_wt_assumed']-opti_df['hotmetal']-opti_df['scrap']
    opti_df['dri_blow_assumed'][opti_df['updated_arcing_flag']==1]=opti_df['hotmetal']*dri_blowing_perc-opti_df['scrap']
    opti_df['dri_blow_assumed'][opti_df['dri_blow_assumed']<0]=0

    if model_run==1:
        opti_df['dri_blow']=opti_df['dri_blow_assumed']
        opti_df['lime_blowi']=0
        opti_df['dolo_blow']=0

    elif model_run==2:
        #opti_df['dri_blow']=opti_df['dri']
        opti_df['dri_blow']=opti_df['dri_blow_assumed']
        opti_df['lime_blowi']=opti_df['lime']
        opti_df['dolo_blow']=opti_df['dolo']
        dolo_setup_at_blowing_end=opti_df['dolo']


    opti_df['dri_arc']=0
    opti_df['dri_arc'][opti_df['updated_arcing_flag']==0]=0
    opti_df['dri_arc'][opti_df['updated_arcing_flag']==1]=opti_df['bath_wt_assumed']-opti_df['hotmetal']-opti_df['dri_blow']-opti_df['scrap']

    if arcing_for_full_blow==1:
         opti_df['dri_arc'][opti_df['dri_arc']<10]=10


    # Sio2 in hm content
    # hot metal is in tons and needs to converted to KGs
    # hm_si is in percentage, so that needs to be divided by 100
    opti_df['sio2_content_in_hm'] = (opti_df['hotmetal'] * 1000) * (opti_df['hm_si']/100) * 2.17 + (opti_df['hotmetal'] * 1000) * sio2_hm_perc

    opti_df['mno_content_in_hm'] = (opti_df['hotmetal'] * 1000) * (opti_df['hm_mn']/100) * (56/55)
    opti_df['p_content_in_hm'] = (opti_df['hotmetal'] * 1000) * (opti_df['hm_p']/100) * (56/62)

    # sio2 in DRI blowing and arcing
    # need to convert tons to kgs first
    #
    opti_df['sio2_content_in_dri_blowing'] = (opti_df['dri_blow'] * 1000) * sio2_dri_perc
    opti_df['sio2_content_in_dri_arcing'] = (opti_df['dri_arc'] * 1000) * sio2_dri_perc

    opti_df['cao_from_p_dri_blowing'] = (opti_df['dri_blow'] * 1000) * p_dri_perc * (56/62)
    opti_df['cao_from_p_dri_arcing'] = (opti_df['dri_arc'] * 1000) * p_dri_perc * (56/62)

    opti_df['cao_from_al2o3_dri_blowing'] = (opti_df['dri_blow'] * 1000) * al2o3_dri_perc * (56/102)
    opti_df['cao_from_al2o3_dri_arcing'] = (opti_df['dri_arc'] * 1000) * al2o3_dri_perc * (56/102)

    opti_df['sio2_content_in_dri_blowing_assumed'] = (opti_df['dri_blow_assumed'] * 1000) * sio2_dri_perc
    opti_df['cao_from_p_dri_blowing_assumed'] = (opti_df['dri_blow_assumed'] * 1000) * p_dri_perc * (56/62)
    opti_df['cao_from_al2o3_dri_blowing_assumed'] = (opti_df['dri_blow_assumed'] * 1000) * al2o3_dri_perc * (56/102)

    # calculating total blowing and arcing SiO2 content
    opti_df['blowing_sio2'] = opti_df['sio2_content_in_hm'] + opti_df['sio2_content_in_dri_blowing']
    opti_df['arcing_sio2'] = opti_df['sio2_content_in_dri_arcing']

    opti_df['blowing_sio2_assumed'] = opti_df['sio2_content_in_hm'] + opti_df['sio2_content_in_dri_blowing_assumed']

    # calculating total blowing and arcing CaO required
    opti_df['blowing_cao'] = opti_df['blowing_sio2'] * basicity + opti_df['cao_from_p_dri_blowing'] + opti_df['cao_from_al2o3_dri_blowing'] + opti_df['mno_content_in_hm'] + opti_df['p_content_in_hm']
    opti_df['arcing_cao'] = opti_df['arcing_sio2'] * basicity + opti_df['cao_from_p_dri_arcing'] + opti_df['cao_from_al2o3_dri_arcing'] #+ opti_df['mno_content_in_hm'] + opti_df['p_content_in_hm']

    opti_df['blowing_cao_assumed'] = opti_df['blowing_sio2_assumed'] * basicity + opti_df['cao_from_p_dri_blowing_assumed'] + opti_df['cao_from_al2o3_dri_blowing_assumed'] + opti_df['mno_content_in_hm'] + opti_df['p_content_in_hm']

    opti_df['mgo_perc']=mgo_perc
    opti_df['mgo_perc'][opti_df['updated_arcing_flag']==0]=mgo_perc_full_blow

    # calculation total blowing and arcing MgO required
    opti_df['blowing_mgo'] = opti_df['blowing_cao'] * mgo_perc/cao_perc
    opti_df['arcing_mgo'] = opti_df['arcing_cao'] * mgo_perc/cao_perc

    opti_df['blowing_mgo_assumed'] = opti_df['blowing_cao_assumed'] * mgo_perc/cao_perc

    print(opti_df)

    def optimizer_test(row):
#        model_run=row['model_run_number']
        # declare problem
        prob = LpProblem("Min Lime and Dolo",LpMinimize)

        # create list with vars to optimize
        flux_inputs = ['lime_blowing','dololime_blowing','lime_arcing','dololime_arcing']

        # creating variables
        flux_vars = LpVariable.dicts("Flux",flux_inputs, lowBound = 0, cat = 'Continuous')

        if model_run == 1:
            # cao blowing constraints
            prob += flux_vars['lime_blowing'] * cao_lime_perc + flux_vars['dololime_blowing'] * cao_dololime_perc + row['dri_blow'] * 1000 * cao_dri_perc >= row['blowing_cao']

            # mgo blowing constraints
            prob += flux_vars['lime_blowing'] * mgo_lime_perc + flux_vars['dololime_blowing'] * mgo_dololime_perc + row['dri_blow'] * 1000 * mgo_dri_perc >= row['blowing_mgo']

            # cao arcing constraints
            prob += flux_vars['lime_arcing'] * cao_lime_perc + flux_vars['dololime_arcing'] * cao_dololime_perc + row['dri_arc'] * 1000 * cao_dri_perc >= row['arcing_cao']

            # mgo arcing constraints
            prob += flux_vars['lime_arcing'] * mgo_lime_perc + flux_vars['dololime_arcing'] * mgo_dololime_perc + row['dri_arc'] *1000 * mgo_dri_perc >= row['arcing_mgo']
        else:

            row['blowing_cao_actual']=row['lime_blowi'] * 1000* cao_lime_perc + row['dolo_blow'] * 1000* cao_dololime_perc + row['dri_blow'] * 1000 * cao_dri_perc
            row['blowing_cao_delta']=row['blowing_cao']-row['blowing_cao_actual']

            row['blowing_mgo_actual']=row['lime_blowi'] * 1000* mgo_lime_perc + row['dolo_blow'] * 1000* mgo_dololime_perc + row['dri_blow'] * 1000 * mgo_dri_perc
            row['blowing_mgo_delta']=row['blowing_mgo']-row['blowing_mgo_actual']

            prob += flux_vars['lime_blowing'] ==0
            prob += flux_vars['dololime_blowing']==0

            # cao arcing constraints
            prob += flux_vars['lime_arcing'] * cao_lime_perc + flux_vars['dololime_arcing'] * cao_dololime_perc + row['dri_arc'] * 1000 * cao_dri_perc >= (row['arcing_cao']+row['blowing_cao_delta'])*row['updated_arcing_flag']

            # mgo arcing constraints
            prob += flux_vars['lime_arcing'] * mgo_lime_perc + flux_vars['dololime_arcing'] * mgo_dololime_perc + row['dri_arc'] *1000 * mgo_dri_perc >= (row['arcing_mgo']+row['blowing_mgo_delta'])*row['updated_arcing_flag']


        # creating objective function
        prob += lpSum(flux_vars)

        print(prob)

        # adding constraints
        prob.solve(pulp.PULP_CBC_CMD(msg=0))

        # model status
        status = prob.status

        # values
        vals = {}
        for v in prob.variables():
            vals[v.name] = v.varValue

        return [status, vals]


    # calling optimizer
    try:
        opti_df['optimized_solution'] = opti_df.progress_apply(optimizer_test, axis = 1)
        print(f"Optimized for {heat_num} and model_run: {model_run}")



        # separating output list
        opti_df['model_feasibility'] = opti_df['optimized_solution'].apply(lambda x: "feasible" if x[0] == 1 else "infeasible")

        # model calculated vals
        opti_df['dolo_arc_o'] = opti_df['optimized_solution'].apply(lambda x: x[1]['Flux_dololime_arcing']/1000)
        opti_df['dolo_blow_o'] = opti_df['optimized_solution'].apply(lambda x: x[1]['Flux_dololime_blowing']/1000)
        opti_df['lime_arc_o'] = opti_df['optimized_solution'].apply(lambda x: x[1]['Flux_lime_arcing']/1000)
        opti_df['lime_blowi_o'] = opti_df['optimized_solution'].apply(lambda x: x[1]['Flux_lime_blowing']/1000)

        print('dolo_arc_o',opti_df['dolo_arc_o'].values[0])
        print('dolo_blow_o',opti_df['dolo_blow_o'].values[0])
        print('lime_arc_o',opti_df['lime_arc_o'].values[0])
        print('lime_blowi_o',opti_df['lime_blowi_o'].values[0])


        # calculating totals
        opti_df['dololime_excess'] = opti_df['dolo_blow_o']-dolo_setup_limit
        opti_df['dololime_excess'][opti_df['dololime_excess']<0]=0
        opti_df['lime_excess']=opti_df['dololime_excess']*cao_dololime_perc/cao_lime_perc
        #opti_df['dololime_setup_o'] = opti_df['dolo_blow_o'].apply(lambda x: 3 if x > 2.5 else 2)
        opti_df['dololime_deficit'] = dolo_setup_limit-opti_df['dolo_blow_o']
        opti_df['dololime_deficit'][opti_df['dololime_deficit']<0]=0
        opti_df['lime_deficit']=opti_df['dololime_deficit']*cao_dololime_perc/cao_lime_perc

        opti_df['dolo_arc_o'] = (opti_df['dolo_arc_o'] + opti_df['dololime_excess'])*opti_df['updated_arcing_flag']
        opti_df['lime_arc_o'] = (opti_df['lime_arc_o'])*opti_df['updated_arcing_flag']

        if model_run==1:
            opti_df['dololime_setup_o'] = opti_df['dolo_blow_o'].apply(lambda x: dolo_setup_limit if x > dolo_setup_limit else x)+ opti_df['dololime_deficit']
            opti_df['dolo_blow_o'] = 0 # (opti_df['dolo_blow_o']-opti_df['dololime_setup_o'])*(1-opti_df['updated_arcing_flag']) #0
            opti_df['lime_blowi_o'][opti_df['updated_arcing_flag']==0] = opti_df['lime_blowi_o']+ opti_df['lime_excess']-opti_df['lime_deficit']
            opti_df['status_description']='Blowing Predictions'

        else:
            opti_df['dololime_setup_o'] = dolo_setup_at_blowing_end
            opti_df['lime_blowi_o']=opti_df['lime_blowi']
            opti_df['dolo_blow_o']=0
            opti_df['status_description']='Arcing Predictions'

    except Exception as e:
        print(e)
        print(f"Not Optimized for {heat_num} and model_run: {model_run}")

        if model_run==1:
            opti_df['status_description']='Blowing Predictions'
            status='Blowing Predictions'
        else:
            opti_df['status_description']='Arcing Predictions'
            status='Arcing Predictions'
        opti_df['model_feasibility']='infeasible'
        opti_df['dololime_setup_o']=0
        opti_df['dolo_blow_o']=0
        opti_df['lime_blowi_o']=0
        opti_df['dolo_arc_o']=0
        opti_df['lime_arc_o']=0


    now = datetime.datetime.now()
    now = now.strftime('%Y-%m-%d %H:%M:%S')
    opti_df['msg_time_stamp'] = now


    keep_col=['msg_time_stamp','heat','process_type','start','status_description','bathwt','hotmetal'
             ,'grade','model_feasibility','dololime_setup_o','dolo_blow_o','lime_blowi_o','dolo_arc_o','lime_arc_o']
    opti_df=opti_df[keep_col]

    print(opti_df)

    if len(opti_df)==0:
        opti_df.loc[0]=[now,heat_num,'',now,status,0,0,'','infeasible',0,0,0,0,0]


    cursor = conn.cursor()

    for index, row in opti_df.iterrows():
        cursor.execute(
        "INSERT INTO Flux_Model.dbo.flux_output (msg_time_stamp, heat, process_type, start_time, status_description, bathwt, hotmetal, grade, model_feasibility, dololime_setup_o, dolo_blow_o, lime_blowi_o, dolo_arc_o, lime_arc_o) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
         row['msg_time_stamp'],
         row['heat'],
         row['process_type'],
         row['start'],
         row['status_description'],
         row['bathwt'],
         row['hotmetal'],
         row['grade'],
         row['model_feasibility'],
         row['dololime_setup_o'],
         row['dolo_blow_o'],
         row['lime_blowi_o'],
         row['dolo_arc_o'],
         row['lime_arc_o']
         )

    # commiting any changes made and closing the cursor
    conn.commit()
    cursor.close()

    opti_df['shell_value']=shell_number
    opti_df['shell_value']=opti_df['shell_value'].astype(int)
    #print(opti_df['shell_value'])

    if model_run ==1:
        cursor = conn.cursor()

        seq_value_pre= pd.read_sql("SELECT max(seq_value) From INT_JSW_FLUX_Pre", conn).values[0]


        for index, row in opti_df.iterrows():
            cursor.execute(
            "INSERT INTO Flux_Model.dbo.INT_JSW_FLUX_Pre ( heat_number, dolo_setup, dolo_blow, lime_blow, dolo_arc, lime_arc,  msg_flag, aggregate, msg_time_stamp) values (?,?,?,?,?,?,?,?,?)",
             #row['msg_time_stamp'],
             row['heat'],
             row['dololime_setup_o'],
             row['dolo_blow_o'],
             row['lime_blowi_o'],
             row['dolo_arc_o'],
             row['lime_arc_o'],
             'N',
             row['shell_value'],
             row['msg_time_stamp']
             )

        # commiting any changes made and closing the cursor
        conn.commit()
        cursor.close()

        cursor = conn.cursor()

        for index, row in opti_df.iterrows():
            cursor.execute(
            "INSERT INTO Flux_Model.dbo.INT_JSW_FLUX_ARC ( heat_number,  msg_flag, aggregate, msg_time_stamp) values (?,?,?,?)",
             #row['msg_time_stamp'],
             row['heat'],
             'N',
             row['shell_value'],
             row['msg_time_stamp']
             )

        conn.commit()
        cursor.close()

        cursor = conn.cursor()

        for index, row in opti_df.iterrows():
            cursor.execute(
            "INSERT INTO Flux_Model.dbo.INT_JSW_FLUX_PHOS ( heat_number,  msg_flag, aggregate, msg_time_stamp) values (?,?,?,?)",
             #row['msg_time_stamp'],
    #         now,
             row['heat'],
             'N',
             row['shell_value'],
             row['msg_time_stamp']
             )

        # commiting any changes made and closing the cursor
        conn.commit()
        cursor.close()



    if model_run ==2:
        cursor = conn.cursor()

        for index, row in opti_df.iterrows():

            h_num=row['heat']
            SQLCommand = ("UPDATE Flux_Model.dbo.INT_JSW_FLUX_ARC SET dolo_setup=?,dolo_blow=?,lime_blow=?,dolo_arc=?,lime_arc=?,msg_time_stamp=?,msg_flag=?  WHERE heat_number ="+ h_num+" ")
            cursor.execute(SQLCommand,row['dololime_setup_o'],0,row['lime_blowi_o'],row['dolo_arc_o'],row['lime_arc_o'],row['msg_time_stamp'],'N')

        #     cursor.execute(
        #     "INSERT INTO Flux_Model.dbo.INT_JSW_FLUX_ARC ( heat_number, dolo_setup, dolo_blow, lime_blow, dolo_arc, lime_arc,  msg_flag, aggregate, msg_time_stamp) values (?,?,?,?,?,?,?,?,?)",
        #      #row['msg_time_stamp'],
        #      row['heat'],
        #      row['dololime_setup_o'],
        #      row['dolo_blow_o'],
        #      row['lime_blowi_o'],
        #      row['dolo_arc_o'],
        #      row['lime_arc_o'],
        #      'N',
        #      row['shell_value'],
        #      row['msg_time_stamp']
        #      )

        # commiting any changes made and closing the cursor
        conn.commit()
        cursor.close()


    print(f"Output stored for {heat_num} and model_run: {model_run}")

    #opti_df.to_csv('opti_output_15Nov.csv')



def main():

    ######################### checking if model needs to be run #####################

    # getting list of heats for which model has not yet been run
    conarc_flux_data= pd.read_sql("SELECT TOP(1000) * FROM conarc_flux_data where HM_WT >0 order by MSG_TIME_STAMP desc", conn)
    # conarc_flux_data= pd.read_sql("SELECT TOP(1000) * FROM conarc_flux_data where HEAT_NUMBER = '22200810' and  HM_WT >0 order by MSG_TIME_STAMP desc", conn)

    conarc_flux_data['key']=0
    heat_status_check=conarc_flux_data[['HEAT_NUMBER','key']].drop_duplicates()

    status_check = pd.DataFrame({
        'key': [0, 0],
        'status_description': ['Blowing Predictions', 'Arcing Predictions'],
        'model_run': [1, 2]
    })

    heat_status_check=pd.merge(heat_status_check,status_check, on=['key'], how='left')
    #cursor = conn.cursor()
    flux_output_data= pd.read_sql("SELECT TOP(1000) * FROM flux_output order by MSG_TIME_STAMP desc", conn)
    flux_output_data['Run']=1

    heat_status_check=pd.merge(heat_status_check,flux_output_data[['heat','status_description','Run']].rename(columns={'heat':'HEAT_NUMBER'}), on=['HEAT_NUMBER','status_description'],how='left')
    print(heat_status_check)
    heat_status_check=heat_status_check[heat_status_check['Run'].isna()]
    # heat_status_check=heat_status_check[heat_status_check['HEAT_NUMBER']=='22200810']

    for index, row in heat_status_check.iterrows():
        heat=row['HEAT_NUMBER']
        model_run=row['model_run']
        running_heats=pd.DataFrame()
        if model_run == 1:
                running_heats=conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]
        elif model_run == 2:
            hm_wt_check=conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]['HM_WT'].values[0]
            if hm_wt_check < 160:
                if 'EAF 1st Blowing End' in conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]['STATUS_DESCRIPTION'].unique():
                    running_heats=conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]
            else:
                if 'EAF 1st Arcing Start' in conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]['STATUS_DESCRIPTION'].unique():
                    running_heats=conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]
                elif ('EAF 1st Blowing End' in conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]['STATUS_DESCRIPTION'].unique()) & ('EAF Tapping Start' in conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]['STATUS_DESCRIPTION'].unique()):
                    running_heats=conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]


        print(heat,model_run,len(running_heats))
    # heat='21105039'
    # model_run=2
    # running_heats=conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]

        if len(running_heats)>0:
            optimization_func(running_heats, model_run)


if __name__ == "__main__":
    main()
