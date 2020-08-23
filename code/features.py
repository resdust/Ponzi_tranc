import numpy as np
import pandas as pd
import tools as tl
import color
import os
import time

def trxData(diry='data',addr=r'address\final_addr+label.csv'):
    # names = ["_st","from","to","value"]
    ex_columns=['address','val_in','time_in']
    in_columns=['address','val_out','time_out']
    ex_f = os.path.join(diry,'external_xinran.csv')
    in_f = os.path.join(diry,'internal_xinran.csv')
    ex_df = pd.read_csv(ex_f)
    in_df = pd.read_csv(in_f)
    label_df = pd.read_csv(addr)
    label_df.columns = ['address','ponzi']
    label_df['address'] = ['0x'+addr for addr in label_df['address'].values]

    values = {} #addr:[val_ins,time_ins]
    file = os.path.join(diry,'final.data')
    
    for i in range(ex_df.shape[0]):
        addr = ex_df.iloc[i]['to']
        if addr not in values.keys():
            values[addr] = [[int(ex_df.iloc[i]['value'])],[ex_df.iloc[i]['_st']]]
        else:
            values[addr][0].append(int(ex_df.iloc[i]['value']))
            values[addr][1].append(ex_df.iloc[i]['_st'])

    addr_in, val_ins, time_ins = list(values.keys()), \
        [str(values[key][0]) for key in values.keys()], [str(values[key][1]) for key in values.keys()]

    for i in range(in_df.shape[0]):
        addr = in_df.iloc[i]['action_from']
        if addr not in values.keys():
            values[addr] = [[int(in_df.iloc[i]['action_value'])],[in_df.iloc[i]['_st']]]
        else:
            values[addr][0].append(int(in_df.iloc[i]['action_value']))
            values[addr][1].append(in_df.iloc[i]['_st'])
             
    addr_out, val_outs, time_outs = list(values.keys()), \
        [str(values[key][0]) for key in values.keys()], [str(values[key][1]) for key in values.keys()]

    ins = [[addr_in[i],val_ins[i],time_ins[i]] for i in range(len(addr_in))]
    outs = [[addr_out[i],val_outs[i],time_outs[i]] for i in range(len(addr_out))]
    
    df_in = pd.DataFrame(ins, columns=['address','val_in','time_in'])
    df_out = pd.DataFrame(outs, columns=['address','val_out','time_out'])

    df = df_in.merge(df_out,how='outer')
    df = df.merge(label_df,how='inner')
    df.to_csv(file,index=None)
    
    return file

def extract(database):
    # database_ponzi = path.join('feature','nponzi_feature_raw.csv')
    color.pInfo("Dealing with transaction data data")

    raw_data = pd.read_csv(database)
    raw_data = raw_data.fillna(0)
    tx_features = []
    f_names = [#'ponzi',
                'address',
                'nbr_tx_in',
                'nbr_tx_out', 
                'Tot_in', 
                'Tot_out',
                'mean_in',
                'mean_out',
                'sdev_in',
                'sdev_out',
                'gini_in',
                'gini_out',
                'avg_time_btw_tx',
                # 'gini_time_out',
                'lifetime',
                ]
    for i in range(raw_data.shape[0]):
        # ponzi = raw_data.iloc[i]['ponzi']
        address = raw_data.iloc[i]['address']
        time_in = raw_data.iloc[i]['time_in']
        time_out = raw_data.iloc[i]['time_out']
        val_in = raw_data.iloc[i]['val_in']
        val_out = raw_data.iloc[i]['val_out']
        if val_in != '' or val_out != '':
            #f = tl.basic_features(ponzi, time_in, time_out, val_in, val_out)
            f = tl.basic_features(None,address, time_in, time_out, val_in, val_out)
            tx_features.append(f)

    tl.compute_time(t0)

    df_features = pd.DataFrame(tx_features,columns=f_names)
    name = os.path.basename(database).split('.')[0]
    f_file = os.path.join('feature',name+'_feature.csv')
    df_features.to_csv(f_file,index=None)
    color.pDone('Have written feature file '+ f_file+'.')
    return f_file

def combine(f1,f2,w_file):
    df1 = pd.read_csv(f1)
    df2 = pd.read_csv(f2)
    df = pd.concat([df1,df2],axis=0)
    df.to_csv(w_file,index=False)
    color.pDone('Written '+w_file+'.')

t0 = time.process_time()
if __name__=='__main__':
    file_data = trxData() 
    extract(file_data)
