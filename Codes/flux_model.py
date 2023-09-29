#Importing Libraries
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


#pd.set_option('max_rows',500)
#pd.set_option('max_columns',500)

# registering progress apply
tqdm.pandas(desc = "Processing Dataframe")

now = datetime.datetime.now()
now = now.strftime('%Y_%m_%d_%H_%M_%S')

#connecting to database
conn = pyodbc.connect('Driver={SQL Server};'
                     'Server=STEELDNA;'
                     'Database=Flux_Model;'
                     'UID=sa;'
                     'PWD=admin@123;'
                     'Trusted_Connection=no;')

cursor = conn.cursor()


#optimizer function
def optimization_func(running_heats, model_run):

    try:
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


        # cols to fill with 0
        cols = [
            'Scrap',
            'Dolo',
            'DRI',
            'Lime',
            ]
        ads[cols] = ads[cols].fillna(0)

        #converting KG into Tonnes
        weights =['HotMetal', 'BathWt','Scrap','Dolo','DRI','Lime']
        for col in weights:
            ads[col]=ads[col]/1000


        hm_wt_input=ads['HotMetal'].values[0]


        #basicity is decided based on hm wt
        basicity = 2.2

        if hm_wt_input <= 140:
            basicity = 1.8
        elif hm_wt_input <= 150:
            basicity = 1.9
        else:
            basicity = 2

        # assumptions and fixed constraints
        cao_perc = 0.36

        #DRI Blowing percent is decided based on Hm Si %
        dri_blowing_perc=.23

        if ads['HM_SI'].values[0] < .7:
            dri_blowing_perc = .24
        elif ads['HM_SI'].values[0] < 1:
            dri_blowing_perc = .25
        elif ads['HM_SI'].values[0] < 1.5:
            dri_blowing_perc = .26
        else:
            dri_blowing_perc = .27

        print('dri_blowing_perc=',dri_blowing_perc)

        #Dolo setup limit is decided based on hm wt
        # dolo_setup_limit=2.6

        dolo_setup_limit=3
        dolo_blowing_fix=0

        if hm_wt_input>=160:
            dolo_blowing_fix=1.2
        elif hm_wt_input>=145:
            dolo_blowing_fix=.8
        else:
            dolo_blowing_fix=.7

        dolo_setup_at_blowing_end=0
        sio2_hm_perc = 0

        full_blow_wt_check = 160
        bath_wt_assumed=225
        bath_wt_assumed_full_blow=215

        #for first heat after wash, we get a flag from data and bath wt is taken as 270 for that heat
        print(ads['FIRSTHEATAFTERWASH'].max())
        first_heat_flag=ads['FIRSTHEATAFTERWASH'].max()

        if first_heat_flag == 1:
            bath_wt_assumed=270
            bath_wt_assumed_full_blow=260

        arcing_for_full_blow=0

        #mgo_perc is decided based on hm wt
        mgo_perc = 0.07
        mgo_perc_full_blow = 0.035

        if hm_wt_input >= 160:
            mgo_perc=7/100
        else:
            mgo_perc=8/100


        # if hm_wt_input < 105:
        #     mgo_perc=3.5/100
        # elif hm_wt_input < 140:
        #     mgo_perc=7.5/100
        # elif hm_wt_input < 150:
        #     mgo_perc=7.0/100
        # elif hm_wt_input < 160:
        #     mgo_perc=5.9/100
        # elif hm_wt_input < 165:
        #     mgo_perc=4.8/100
        # elif hm_wt_input >= 165:
        #     mgo_perc=3.5/100

        print('mgo perc =', mgo_perc)


        #arcing process depends on hm wt. Creating Flag to check if arcing will happen

        ads['Updated Arcing Flag']=0
        ads['Updated Arcing Flag'] = np.where((ads['HotMetal'] > 160), 0,1)

        #checking if arcing is done inspite of full blow process due to some practical reasons.

        if ((model_run==2) & (ads['HotMetal'].values[0] >= full_blow_wt_check) & ('EAF 1st Arcing Start' in ads['STATUS_DESCRIPTION'].unique())):
            ads['Updated Arcing Flag']=1
            arcing_for_full_blow=1
            print('----arcing_for_full_blow----')

        # standardising ads column names
        ads.columns = [x.replace(" ","_").replace(".","").replace("-","_").lower() for x in ads.columns]

        #setting missing values of hm chemistry as default values
        ads['hm_c'].fillna(4.5, inplace=True)
        ads['hm_si'].fillna(.45, inplace=True)
        ads['hm_s'].fillna(.045, inplace=True)
        ads['hm_p'].fillna(.11, inplace=True)
        ads['hm_mn'].fillna(.5, inplace=True)

        #calculating ton of materials in hot metal
        hm_chem_cols = ['hm_c', 'hm_si', 'hm_s', 'hm_p', 'hm_mn']
        for each in hm_chem_cols:
            ads[each + "_tons"] = ads[each] * ads['hotmetal']/100

        #Reading Material Chemistry data
        material_chem_data= pd.read_sql("SELECT TOP(1000) * FROM mat_chem_ana order by INSERT_DATE desc", conn)

        #model_run 1 = blowing predictions. model_run 2 = arcing predictions
        #blowing predictions are made as soon as EAF Start signal is recieved
        #arcing predictions are made after EAF Blowing End signal is recieved

        if model_run==1:
            print('model run 1 ')
            opti_df=ads[(ads['status_description']=='EAF Start') & (ads['hotmetal']!=0)].sort_values('msg_time_stamp').drop_duplicates(subset=['heat'])
            if len(opti_df) ==0:
                opti_df=ads[(ads['hotmetal']!=0)].sort_values('msg_time_stamp').drop_duplicates(subset=['heat'])
                print(opti_df)

        elif model_run==2:
            if 'EAF 2nd Blowing End' in ads['status_description'].unique():
                opti_df=ads[(ads['status_description']=='EAF 2nd Blowing End')].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])
            else:
                opti_df=ads[(ads['status_description']=='EAF 1st Blowing End')].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])
                print(opti_df)
            if len(opti_df)==0:
                opti_df=ads[(ads['status_description'].str.contains('Blowing End'))].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])
            if len(opti_df)==0:
                opti_df=ads[(ads['status_description'].str.contains('EAF Tapping Start'))].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])

            print('opti_Df =', opti_df['lime'].values[0])

        opti_df['bath_wt_assumed']=bath_wt_assumed
        opti_df['bath_wt_assumed'][opti_df['updated_arcing_flag']==0]=bath_wt_assumed_full_blow
        print(opti_df)

        print('bath wt =', opti_df['bath_wt_assumed'].values[0])

        #Basicity value is updated based on Scrap value
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

        #Material Chemistry data
        mat_chem_data_read=material_chem_data[((material_chem_data['MAT_TYPE']=='DOLO') | (material_chem_data['MAT_TYPE']=='LIME') ) & (material_chem_data['MGO_PER']!=0) & (material_chem_data['CAO_PER']!=0)].sort_values(['INSERT_DATE'],ascending=False)
        #If CAO value > 80, material is lime, else dolo. (Currently marking in database is not accurate which is why we have to idetntify based on value )
        lime_chem_data=mat_chem_data_read[mat_chem_data_read['CAO_PER']>=80]
        dolo_chem_data=mat_chem_data_read[(mat_chem_data_read['CAO_PER']<80) & (mat_chem_data_read['CAO_PER']>0)]

        dri_chem_data=material_chem_data[(material_chem_data['MAT_TYPE']=='DRI') & (material_chem_data['CAO_PER'] != 0) & (material_chem_data['MGO_PER']!=0) & (material_chem_data['SIO2_PER'] !=0)].sort_values(['INSERT_DATE'],ascending=False)

        if len(dri_chem_data) == 0:
            dri_chem_data = pd.DataFrame([{'CAO_PER':1.5, 'MGO_PER':0.5, 'SIO2_PER':5.2, 'AL2O3_PER':2.7,'P_PER':.05}])

        #Setting upper and lower bound for all material chemistry data
        dri_chem_data['CAO_PER']=dri_chem_data['CAO_PER'].clip(1.5,1.9)
        dri_chem_data['MGO_PER']=dri_chem_data['MGO_PER'].clip(.5,.9)
        dri_chem_data['SIO2_PER']=dri_chem_data['SIO2_PER'].clip(5.15,5.3)
        dri_chem_data['AL2O3_PER']=dri_chem_data['AL2O3_PER'].clip(2.5,2.7)
        dri_chem_data['P_PER']=dri_chem_data['P_PER'].clip(.02,.05)


        if len(lime_chem_data) == 0:
            lime_chem_data = pd.DataFrame([{'CAO_PER':90, 'MGO_PER':1.5}])
        elif lime_chem_data['MGO_PER'].values[0] >= 2.5:
            lime_chem_data['MGO_PER']=1.5
        elif lime_chem_data['MGO_PER'].values[0] <= .05:
            lime_chem_data['MGO_PER']=1.5

        if len(dolo_chem_data) == 0:
            dolo_chem_data = pd.DataFrame([{'CAO_PER':60, 'MGO_PER':30}])
        elif dolo_chem_data['MGO_PER'].values[0] >= 40:
            dolo_chem_data['MGO_PER']=30
        elif dolo_chem_data['MGO_PER'].values[0] <= 15:
            dolo_chem_data['MGO_PER']=30

        #converting into percent
        #cao
        cao_lime_perc = lime_chem_data['CAO_PER'].values[0]/100 #0.90
        cao_dololime_perc = dolo_chem_data['CAO_PER'].values[0]/100 #0.60
        cao_dri_perc = dri_chem_data['CAO_PER'].values[0]/100 #0.015

        print(cao_lime_perc)


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
        #DRI Blow for Full Blow is Bath Wt - Hot Metal - Scrap
        #DRI Blow for Conarc Process is HM Wt * 23% - Scrap
        opti_df['dri_blow_assumed']=0
        opti_df['dri_blow_assumed'][opti_df['updated_arcing_flag']==0]=opti_df['bath_wt_assumed']-opti_df['hotmetal']-opti_df['scrap']
        opti_df['dri_blow_assumed'][opti_df['updated_arcing_flag']==1]=opti_df['hotmetal']*dri_blowing_perc-opti_df['scrap']
        opti_df['dri_blow_assumed'][opti_df['dri_blow_assumed']<0]=0


        #For bowing predictions, Lime Blow and dolo Blow are calculated
        if model_run==1:
            opti_df['dri_blow']=opti_df['dri_blow_assumed']
            opti_df['lime_blowi']=0
            opti_df['dolo_blow']=0

        #For arcing predictions, Lime Blow and dolo Blow are taken as the actual additions by the operators at the end of blowing process
        elif model_run==2:
            #opti_df['dri_blow']=opti_df['dri']
            opti_df['dri_blow']=opti_df['dri_blow_assumed']
            opti_df['lime_blowi']=opti_df['lime']
            opti_df['dolo_blow']=opti_df['dolo']
            dolo_setup_at_blowing_end=opti_df['dolo']
            lime_setup_at_blowing_end=opti_df['lime']


        opti_df['dri_arc']=0
        opti_df['dri_arc'][opti_df['updated_arcing_flag']==0]=0
        opti_df['dri_arc'][opti_df['updated_arcing_flag']==1]=opti_df['bath_wt_assumed']-opti_df['hotmetal']-opti_df['dri_blow']-opti_df['scrap']

        #Setting minimum level of DRI arc as 10 for full blowing process
        if arcing_for_full_blow==1:
             opti_df['dri_arc'][opti_df['dri_arc']<10]=10


        # Sio2 in hm content
        # hot metal is in tons and needs to converted to KGs
        # hm_si is in percentage, so that needs to be divided by 100
        opti_df['sio2_content_in_hm'] = (opti_df['hotmetal'] * 1000) * (opti_df['hm_si']/100) * 2.14 + (opti_df['hotmetal'] * 1000) * sio2_hm_perc
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

        #optimizer function

        def optimizer_test(row):
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

                #We check how much Lime and Dolo have the operators actually added. We calculate how much of CaO and MgO requirement were met/exceeded based on actual vs required and readjust the CaO and MgO requirement at the time of arcing.
                row['blowing_cao_actual']=row['lime_blowi'] * 1000* cao_lime_perc + row['dolo_blow'] * 1000* cao_dololime_perc + row['dri_blow'] * 1000 * cao_dri_perc
                row['blowing_cao_delta']=row['blowing_cao']-row['blowing_cao_actual']

                row['blowing_mgo_actual']=row['lime_blowi'] * 1000* mgo_lime_perc + row['dolo_blow'] * 1000* mgo_dololime_perc + row['dri_blow'] * 1000 * mgo_dri_perc
                row['blowing_mgo_delta']=row['blowing_mgo']-row['blowing_mgo_actual']


                #Lime Blowing and Dolo Blowing variables are set at 0 since we are not optimizing for them in this run
                prob += flux_vars['lime_blowing'] ==0
                prob += flux_vars['dololime_blowing']==0

                # cao arcing constraints
                prob += flux_vars['lime_arcing'] * cao_lime_perc + flux_vars['dololime_arcing'] * cao_dololime_perc + row['dri_arc'] * 1000 * cao_dri_perc >= (row['arcing_cao']+row['blowing_cao_delta'])*row['updated_arcing_flag']

                # mgo arcing constraints
                prob += flux_vars['lime_arcing'] * mgo_lime_perc + flux_vars['dololime_arcing'] * mgo_dololime_perc + row['dri_arc'] *1000 * mgo_dri_perc >= (row['arcing_mgo']+row['blowing_mgo_delta'])*row['updated_arcing_flag']

                #we multiply RHS with Arcing flag so that optipization is only done if arcing is happening


            # creating objective function
            #sum of all flux variables
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

            print('dolo_blow_o',opti_df['dolo_blow_o'].values[0])
            print('lime_blowi_o',opti_df['lime_blowi_o'].values[0])
            print('dolo_arc_o',opti_df['dolo_arc_o'].values[0])
            print('lime_arc_o',opti_df['lime_arc_o'].values[0])


            # calculating totals
            #Dolo is not added in blowing phase
            #Since only a limited amount of dolo can be added in setup phase, we calculate the excess dolo suggested by model and replace it with the corresponding lime qty by chemistry balance
            opti_df['dololime_excess'] = opti_df['dolo_blow_o']-dolo_setup_limit-dolo_blowing_fix
            opti_df['dololime_excess'][opti_df['dololime_excess']<0]=0
            opti_df['lime_excess']=opti_df['dololime_excess']*cao_dololime_perc/cao_lime_perc
            #opti_df['dololime_setup_o'] = opti_df['dolo_blow_o'].apply(lambda x: 3 if x > 2.5 else 2)
            opti_df['dololime_deficit'] = dolo_blowing_fix+dolo_setup_limit-opti_df['dolo_blow_o']
            opti_df['dololime_deficit'][opti_df['dololime_deficit']<0]=0
            opti_df['lime_deficit']=opti_df['dololime_deficit']*cao_dololime_perc/cao_lime_perc

            print('lime excess or deficit = ', max(opti_df['lime_excess'].values[0], opti_df['lime_deficit'].values[0]))

#            opti_df['dolo_arc_o'] = (opti_df['dolo_arc_o'] + opti_df['dololime_excess'])*opti_df['updated_arcing_flag']
            opti_df['lime_arc_o'] = (opti_df['lime_arc_o'])*opti_df['updated_arcing_flag']

            opti_df['lime_setup_o'] = 0

            #For arcing process, some lime is added in setup process. We adjust lime arcing suggestions by subtracting lime setup added
            lime_setup_at_blowing_start=ads[(ads['status_description']=='EAF Start') & (ads['hotmetal']!=0)].sort_values('msg_time_stamp', ascending = False).drop_duplicates(subset=['heat'])['lime'].values[0]

            #redistributing total flux based on conditions
            if model_run==1:
#                opti_df['dololime_setup_o'] = opti_df['dolo_blow_o'].apply(lambda x: dolo_setup_limit if x > dolo_setup_limit else x)+ opti_df['dololime_deficit']
#                opti_df['dolo_blow_o'] = 0
                opti_df['dololime_setup_o'] = dolo_setup_limit
                opti_df['dolo_blow_o'] = dolo_blowing_fix
                # opti_df['lime_blowi_o'][opti_df['updated_arcing_flag']==0] = opti_df['lime_blowi_o']+ opti_df['lime_excess']-opti_df['lime_deficit']
                opti_df['lime_blowi_o'] = opti_df['lime_blowi_o']+ opti_df['lime_excess']-opti_df['lime_deficit']
                opti_df['status_description']='Blowing Predictions'

            else:
                opti_df['dololime_setup_o'] = dolo_setup_at_blowing_end
                opti_df['lime_setup_o'] = lime_setup_at_blowing_start
                opti_df['lime_blowi_o']=opti_df['lime_blowi']-lime_setup_at_blowing_start
                opti_df['dolo_blow_o']=0
                opti_df['status_description']='Arcing Predictions'


            #Calculating totals to be displayed on screen
            opti_df['dolo_total']=opti_df['dolo_blow_o']+opti_df['dololime_setup_o']+opti_df['dolo_arc_o']
            opti_df['lime_total']=opti_df['lime_blowi_o']+opti_df['lime_setup_o']+opti_df['lime_arc_o']

            opti_df['flux_total']=opti_df['dolo_total']+opti_df['lime_total']

            print('redistributed values-')
            print('dolo_setup',opti_df['dololime_setup_o'].values[0])
            print('dolo_blow',opti_df['dolo_blow_o'].values[0])
            print('lime_blowi',opti_df['lime_blowi_o'].values[0])
            print('dolo_arc',opti_df['dolo_arc_o'].values[0])
            print('lime_arc',opti_df['lime_arc_o'].values[0])


            print(opti_df['dolo_total'].values[0], opti_df['lime_total'].values[0],opti_df['flux_total'].values[0] )


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
            opti_df['dolo_total']=0
            opti_df['lime_total']=0
            opti_df['flux_total']=0
            opti_df['lime_setup_o']=0

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
        opti_df['dolo_total']=0
        opti_df['lime_total']=0
        opti_df['flux_total']=0
        opti_df['lime_setup_o']=0

        if 'cao_lime_perc' not in locals():
            print('no')
            cao_lime_perc=0
            mgo_lime_perc=0
            cao_dololime_perc=0
            mgo_dololime_perc=0
            cao_dri_perc=0
            mgo_dri_perc=0
            sio2_dri_perc=0
            p_dri_perc=0
            al2o3_dri_perc=0
            basicity=0
            mgo_perc=0


    now = datetime.datetime.now()
    now = now.strftime('%Y-%m-%d %H:%M:%S')
    opti_df['msg_time_stamp'] = now

    #keeping relevant columns

    keep_col=['msg_time_stamp','heat','process_type','start','status_description','bathwt','hotmetal','hm_c', 'hm_p', 'hm_mn', 'hm_s',
       'hm_si','dri_blow', 'scrap'
             ,'grade','model_feasibility','dololime_setup_o','dolo_blow_o','lime_blowi_o','dolo_arc_o','lime_arc_o'
             ,'dolo_total','lime_total','flux_total','lime_setup_o']
    opti_df=opti_df[keep_col]

    opti_df['cao_lime_perc']=cao_lime_perc
    opti_df['mgo_lime_perc']=mgo_lime_perc
    opti_df['cao_dololime_perc']=cao_dololime_perc
    opti_df['mgo_dololime_perc']=mgo_dololime_perc
    opti_df['cao_dri_perc']=cao_dri_perc
    opti_df['mgo_dri_perc']=mgo_dri_perc
    opti_df['sio2_dri_perc']=sio2_dri_perc
    opti_df['p_dri_perc']=p_dri_perc
    opti_df['al2o3_dri_perc']=al2o3_dri_perc
    opti_df['basicity']=basicity
    opti_df['mgo_perc']=mgo_perc


    print(opti_df)

    if len(opti_df)==0:
        opti_df.loc[0]=[now,heat_num,'',now,status,0,0,'','infeasible',0,0,0,0,0,0,0,0,0]

    #storing output into database
    cursor = conn.cursor()

    for index, row in opti_df.iterrows():
        cursor.execute(
        "INSERT INTO Flux_Model.dbo.flux_output (msg_time_stamp, heat, process_type, start_time, status_description, bathwt, hotmetal, grade, model_feasibility, dololime_setup_o, dolo_blow_o, lime_blowi_o, dolo_arc_o, lime_arc_o,dolo_total,lime_total,flux_total,lime_setup_o,hm_c,hm_si,hm_p,hm_mn,hm_s,dri_blow,scrap,cao_lime_perc,mgo_lime_perc,cao_dololime_perc,mgo_dololime_perc,cao_dri_perc,mgo_dri_perc,sio2_dri_perc,p_dri_perc,al2o3_dri_perc,basicity,mgo_perc,msg_flag) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
         row['lime_arc_o'],
         row['dolo_total'],
         row['lime_total'],
         row['flux_total'],
         row['lime_setup_o'],

         row['hm_c'],
         row['hm_si'],
         row['hm_p'],
         row['hm_mn'],
         row['hm_s'],
         row['dri_blow'],
         row['scrap'],
         row['cao_lime_perc'],
         row['mgo_lime_perc'],
         row['cao_dololime_perc'],
         row['mgo_dololime_perc'],
         row['cao_dri_perc'],
         row['mgo_dri_perc'],
         row['sio2_dri_perc'],
         row['p_dri_perc'],
         row['al2o3_dri_perc'],
         row['basicity'],
         row['mgo_perc'],
         'N'

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
            "INSERT INTO Flux_Model.dbo.INT_JSW_FLUX_Pre ( heat_number, dolo_setup, dolo_blow, lime_blow, dolo_arc, lime_arc,  msg_flag, aggregate, msg_time_stamp,dolo_total,lime_total,flux_total,lime_setup_o) values (?,?,?,?,?,?,?,?,?,?,?,?,?)",
             #row['msg_time_stamp'],
             row['heat'],
             row['dololime_setup_o'],
             row['dolo_blow_o'],
             row['lime_blowi_o'],
             row['dolo_arc_o'],
             row['lime_arc_o'],
             'N',
             row['shell_value'],
             row['msg_time_stamp'],
             row['dolo_total'],
             row['lime_total'],
             row['flux_total'],
             row['lime_setup_o']
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
            SQLCommand = ("UPDATE Flux_Model.dbo.INT_JSW_FLUX_ARC SET dolo_setup=?,dolo_blow=?,lime_blow=?,dolo_arc=?,lime_arc=?,msg_time_stamp=?,msg_flag=?,dolo_total=?,lime_total=?,flux_total=?,lime_setup_o=?  WHERE heat_number ="+ h_num+" ")
            cursor.execute(SQLCommand,row['dololime_setup_o'],0,row['lime_blowi_o'],row['dolo_arc_o'],row['lime_arc_o'],row['msg_time_stamp'],'N',row['dolo_total'],row['lime_total'],row['flux_total'],row['lime_setup_o'])

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

    #Creating a dataframe to merge with existing outputs to check if current heat has already been run for optimization
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

    #Filtering out where heat_status_check is na i.e. no record of the heat is found in the existing output table. Model needs to be run for that heat
    heat_status_check=heat_status_check[heat_status_check['Run'].isna()]
    # heat_status_check=heat_status_check[heat_status_check['HEAT_NUMBER']=='22200810']


    #for each running heat, checking if operating conditions are met to run the model
    for index, row in heat_status_check.iterrows():
        heat=row['HEAT_NUMBER']
        model_run=row['model_run']
        running_heats=pd.DataFrame()

        #For model_run = 1 i.e Blowing Predictions, only condition is Hm_wt reading should be there
        if model_run == 1:
                running_heats=conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]
        #For model_run = 2 i.e Arcing Predictions, we check 2 conditions
        #If HM Wt < 160 i.e Conarc Process, we check for L2 signal= “EAF Blowing End” to generate Arcing predictions
        #If HM Wt >= 160 i.e Full Blowing Process ; Ideally, arcing is not done for full  blowing processes, but if the operators still perform arcing due to any practical reason, model will optimize at signal = “EAF Arcing Start”
        elif model_run == 2:
            hm_wt_check=conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]['HM_WT'].values[0]
            if hm_wt_check < 160:
                if 'EAF 1st Blowing End' in conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]['STATUS_DESCRIPTION'].unique():
                    running_heats=conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]
                elif 'EAF 2nd Blowing End' in conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]['STATUS_DESCRIPTION'].unique():
                    running_heats=conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]
                elif 'EAF 3rd Blowing End' in conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]['STATUS_DESCRIPTION'].unique():
                    running_heats=conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]
            else:
                if 'EAF 1st Arcing Start' in conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]['STATUS_DESCRIPTION'].unique():
                    running_heats=conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]
                elif ('EAF 1st Blowing End' in conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]['STATUS_DESCRIPTION'].unique()) & ('EAF Tapping Start' in conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]['STATUS_DESCRIPTION'].unique()):
                    running_heats=conarc_flux_data[conarc_flux_data['HEAT_NUMBER']==heat]


        print(heat,model_run,len(running_heats))

        #Calling the optimizer function
        if len(running_heats)>0:
            optimization_func(running_heats, model_run)


if __name__ == "__main__":
    main()
