import numpy as np
import pandas as pd
import tools as tl
import color
import os
import time

def trxData(diry='data',addr=r'merged9747_addr.csv',i=4):
    # names = ["_st","from","to","value"]
    ex_columns=['address','val_in','time_in']
    in_columns=['address','val_out','time_out']
    ex_f = os.path.join(diry,'external_transactions_prt_'+str(i)+'.csv')
    in_f = os.path.join(diry,'internal_transactions_prt_'+str(i)+'.csv')
    addr_f = os.path.join(diry,addr)
    ex_df = pd.read_csv(ex_f)
    in_df = pd.read_csv(in_f)
    addr_df = pd.read_csv(addr_f,header=None)
    addr_df.columns=['address']
    addrs = addr_df['address'].values
    # addrs = ['0x'+a for a in addrs]
    # label_df = pd.read_csv(addr)
    # label_df.columns = ['address','ponzi']
    # label_df['address'] = ['0x'+addr for addr in label_df['address'].values]

    values_in = {} #addr:[val_ins,time_ins]
    values_out = {}
    file = os.path.join(diry,'merge_prt_'+str(i)+'.data')
    
    for data in ex_df.values:
        addr = data[2]
        if addr in addrs:
            if addr not in values_in.keys():
                values_in[addr] = [[float(data[3])],[float(data[0])]]
            else:
                if len(values_in[addr][0])>=1000:
                    continue
                values_in[addr][0].append(float(data[3]))
                values_in[addr][1].append(float(data[0]))
        else:
            pass

    for data in in_df.values:
        addr_to = data[2]
        addr_from = data[1]
        if (addr_to in addrs):
            if addr_to not in values_in.keys():
                values_in[addr_to] = [[float(data[3])],[float(data[0])]]
            else:
                if len(values_in[addr_to][0])>=2000:
                    continue
                values_in[addr_to][0].append(float(data[3]))
                values_in[addr_to][1].append(float(data[0]))

        if (addr_from in addrs):
            if addr_from not in values_out.keys():
                values_out[addr_from] = [[float(data[3])],[float(data[0])]]
            else:
                if len(values_out[addr_from][0])>=1000:
                    continue
                values_out[addr_from][0].append(float(data[3]))
                values_out[addr_from][1].append(float(data[0]))

    keys = list(values_in.keys())
    print(len(keys))
    addr_in, val_ins, time_ins = keys, \
        [values_in[key][0] for key in keys], [values_in[key][1] for key in keys]

    keys = list(values_out.keys())
    print(len(keys))
    addr_out, val_outs, time_outs = keys, \
        [values_out[key][0] for key in keys], [values_out[key][1] for key in keys]

    ins = [[addr_in[i],val_ins[i],time_ins[i]] for i in range(len(addr_in))]
    outs = [[addr_out[i],val_outs[i],time_outs[i]] for i in range(len(addr_out))]
    
    df_in = pd.DataFrame(ins, columns=['address','val_in','time_in'])
    df_out = pd.DataFrame(outs, columns=['address','val_out','time_out'])

    df = df_in.merge(df_out,how='outer')
    # df = df.merge(label_df,how='inner')
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
