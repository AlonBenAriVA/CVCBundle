import pandas as pd

cvc = pd.read_csv('cvc.csv')  # read csv file
 # and get only the columns you are interetsed in. Remove depulicates
cvc_df= cvc[['PatientSSN','HealthFactorType','HealthFactorDateTime',
                'AdmitDateTime','DischargeDateTime','SpecimenTakenDateTime','Organism',
                'GramStain']].drop_duplicates()

#
# whenever there is an NaN in the discharge date put today's date.
cvc_df.DischargeDateTime = cvc_df.DischargeDateTime.fillna(pd.Timestamp.today().strftime('%m-%d-%Y %H:%M:%S')) # fix NAN in discharge dates (death, still inpat etc.)
#    
patient_ssn = cvc_df.PatientSSN.unique() # get unique SSNs

patient_history = {p:cvc_df[cvc_df.PatientSSN == p] for p in patient_ssn } # get the relevant patient chunk of data from table

patient_admit = { p: patient_history[p].AdmitDateTime.unique() for p in patient_ssn } #dictionary of patient admits
patient_discharge = { p: patient_history[p].DischargeDateTime.unique() for p in patient_ssn } # dictionary of patient discharge
#
# figureout the number of  location of the cvc

def get_location(pt_hx):
    """
    A method to return the location of CVCs by LOCATION
    """
    loc_ix = [i for i,j in enumerate(pt_hx.HealthFactorType) if 'LOC' in j]
    return pt_hx.iloc[loc_ix][['HealthFactorType','HealthFactorDateTime']]


def get_type(pt_hx):
    """
    A method to return the type of CVCs by TYPE
    """
    loc_ix = [i for i,j in enumerate(pt_hx.HealthFactorType) if 'TYPE' in j]
    return pt_hx.iloc[loc_ix][['HealthFactorType','HealthFactorDateTime']]

def get_new_cl(pt_hx):
    """
    A method to return the newline of CVCs by NEW
    """
    loc_ix = [i for i,j in enumerate(pt_hx.HealthFactorType) if 'NEW' in j]
    return pt_hx.iloc[loc_ix][['HealthFactorType','HealthFactorDateTime']]

def get_maint(pt_hx):
    """
    A method to return number of maintanance
    """
    loc_ix = [i for i,j in enumerate(pt_hx.HealthFactorType) if 'STATUS' in j]
    return pt_hx.iloc[loc_ix][['HealthFactorType','HealthFactorDateTime']]



def get_events(ssn):
    """
    A method to return events  WITHIN in patient period and outside of that period
    """
    indexes = patient_history[ssn].index.tolist()
    in_index =[]
    dict_inpat= {}
    dict_outpat = {}
    dict_outpat['non_inpat']=[]
    for i in range(len(patient_admit[ssn])):
        mask_inpat = ( patient_history[ssn].HealthFactorDateTime >= patient_admit[ssn][i]) & (patient_history[ssn].HealthFactorDateTime <= patient_discharge[ssn][i])
        in_index.extend(patient_history[ssn].loc[mask_inpat.values].index.tolist())
        #
        dict_inpat[(patient_admit[ssn][i],patient_discharge[ssn][i])] = patient_history[ssn].loc[mask_inpat.values]
        #mask_outpat = ( patient_history[ssn].HealthFactorDateTime <= patient_admit[ssn][i]) & (patient_history[ssn].HealthFactorDateTime >= patient_discharge[ssn][i])
        
    if (len(in_index)==0):
        print('no inpat records')
        dict_outpat['non_inpat'] = patient_history[ssn]
    else:
        for k in in_index:
            indexes.remove(k)
        dict_outpat['non_inpat'] = patient_history[ssn].loc[indexes]
    return {'inpat':dict_inpat,'outpat':dict_outpat}



def get_stats(ssn):
    """
    A method to get stats from get_events() return a dictionary {} with the follwoing line items:
    return number days maintained as inpatient/outpatient.
    """   
    maint = {}
    maint['inpat_maint'] = 0
    maint['outpat_maint'] = 0
    # iterate over keys
    try:
        keys_events = [k for k in get_events(ssn)['inpat'].keys()]# get keys:

        print({k:get_maint(get_events(ssn)['inpat'][k]).HealthFactorDateTime
                        .apply(pd.to_datetime)
                        .dt
                        .date.unique().shape[0] for k in keys_events })

        maint['inpat_maint'] = {k:get_maint(get_events(ssn)['inpat'][k]).HealthFactorDateTime
                                .apply(pd.to_datetime)
                                .dt
                                .date.unique().shape[0] for k in keys_events }
        
    except:
        print('no Inpat logs')

    try:
        print(get_maint(get_events(ssn)['outpat']['non_inpat']).HealthFactorDateTime.apply(pd.to_datetime).dt.date.unique().shape[0]
        )
        maint['outpat_maint'] = get_maint(get_events(ssn)['outpat']['non_inpat']).HealthFactorDateTime.apply(pd.to_datetime).dt.date.unique().shape[0]
        
    except:
        print('No outpat maint notes')
    return {ssn:{'maint':maint}}

def get_bugs(ssn):
    """
   return the type  and dates of samples taken
    """
    return {ssn:patient_history[ssn][['SpecimenTakenDateTime','Organism','GramStain']].dropna().drop_duplicates()}