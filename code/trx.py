import numpy as np
import pandas as pd
import tools as tl
import color
import os
import time

def trxData(diry='data',addr=r'lgscale.csv'):
    # names = ["_st","from","to","value"]
    ex_columns=['address','val_in','time_in']
    in_columns=['address','val_out','time_out']
    ex_f = os.path.join(diry,'weimin_external.csv')
    in_f = os.path.join(diry,'weimin_internal.csv')
    addr_f = os.path.join(diry,addr)
    ex_df = pd.read_csv(ex_f) # st from to value
    in_df = pd.read_csv(in_f) # st from to value
    addr_df = pd.read_csv(addr_f)
    addrs = addr_df['address'].values
    addrs = ['0x'+a for a in addrs]
    # label_df = pd.read_csv(addr)
    # label_df.columns = ['address','ponzi']
    # label_df['address'] = ['0x'+addr for addr in label_df['address'].values]

    values_in = {} #addr:[val_ins,time_ins]
    values_out = {}
    file = os.path.join(diry,'final_weimin.data')
    
    for data in ex_df.values:
        addr = data[2]
        if addr in addrs:
            if addr not in values_in.keys():
                values_in[addr] = float(data[3])
            else:
                values_in[addr] += float(data[3])

    for data in in_df.values:
        addr_to = data[2]
        addr_from = data[1]
        if (addr_to in addrs):
            if addr_to not in values_in.keys():
                values_in[addr_to] = float(data[3])
            else:
                values_in[addr_to] += float(data[3])

        if (addr_from in addrs):
            if addr_from not in values_out.keys():
                values_out[addr_from] = float(data[3])
            else:
                values_out[addr_from] += float(data[3])
            
    keys = list(values_in.keys())
    print(len(keys))
    addr_in, val_in = keys, [values_in[key] for key in keys]

    keys = list(values_out.keys())
    print(len(keys))
    addr_out, val_out = keys, [values_out[key] for key in keys]

    ins = [[addr_in[i],val_in[i]] for i in range(len(addr_in))]
    outs = [[addr_out[i],val_out[i]] for i in range(len(addr_out))]
    
    df_in = pd.DataFrame(ins, columns=['address','val_in'])
    df_out = pd.DataFrame(outs, columns=['address','val_out'])

    df = df_in.merge(df_out,how='outer')
    df = df.fillna(0)
    total = df.values
    err = []
    for data in total:
        if data[1]<data[2]:
            err.append(data)
    err_df = pd.DataFrame(err)
    err_df.columns = ['address','Tot_in','Tot_out']
    err_df.to_csv('err_lxr.csv',index=False)

    df.to_csv(file,index=None)
    
    return file

if __name__=='__main__':
    file_data = trxData()
    
