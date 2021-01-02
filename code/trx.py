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
    ex_f = os.path.join(diry,'external_in0827.csv')
    in_f = os.path.join(diry,'internal_out0827.csv')
    in_f2 = os.path.join(diry,'internal_in0827.csv')
    type_f = os.path.join(diry,'multi_scales1.csv')
    addr_f = os.path.join(diry,addr)
    ex_df = pd.read_csv(ex_f) # st from to value
    in_df = pd.read_csv(in_f) # st from to value
    in_df2 = pd.read_csv(in_f2) # st from to value
    addr_df = pd.read_csv(type_f)
    addrs = list(addr_df['address'].values)
    # addr_df = pd.read_csv(addr_f)
    # addr_df = addr_df.merge(type_df)
    # addrs = addr_df['address'].values
    # addrs = ['0x'+a for a in addrs]
    # label_df = pd.read_csv(addr)
    # label_df.columns = ['address','ponzi']
    # label_df['address'] = ['0x'+addr for addr in label_df['address'].values]

    values_in = {} #addr:[val_ins,user_from,count]
    values_out = {} #addr:[val_outs,user_to,count]
    users_contract_from = {} # addr:[user_from,user_to]
    users_contract_to = {}
    users_eof_from = {} # addr:[user_from]
    file = os.path.join(diry,'ponzi_trx_type.csv')
    no_track = 0
    for data in ex_df.values:
        addr = data[2][2:]
        label = addr_df.iloc[addrs.index(addr)]['type']
        # if len(label)==0:
        #     no_track += 1
        # else:
        #     label = label.values[0]
        # if addr in addrs:
        if label not in values_in.keys():
            values_in[label] = [float(data[3]),set([data[1]]),1] #from eof
        else:
            values_in[label][0]+=float(data[3])
            values_in[label][1].add(data[1])
            values_in[label][2]+=1
    for label in values_in:
        users_eof_from[label] = len(values_in[label][1])

    for data in in_df.values:
        addr = data[1][2:]
        label = addr_df.iloc[addrs.index(addr)]['type']
        # if len(label)==0:
        #     no_track += 1
        # else:
        #     label = label.values[0]
        # if (addr_from in addrs):
        if label not in values_out.keys():
            values_out[label] = [float(data[3]),set([data[2]]),1] # to ?
        else:
            values_out[label][0]+=float(data[3])
            values_out[label][1].add(data[2])
            values_out[label][2]+=1
    for label in values_out:
        users_contract_to[label] = len(values_out[label][1])

    for data in in_df2.values:
        addr = data[2][2:]
        label = addr_df.iloc[addrs.index(addr)]['type']
        # if len(label)==0:
        #     no_track += 1
        # else:
        #     label = label.values[0]
        # if (addr_to in addrs):
        if label not in values_in.keys():
            values_in[label] = [float(data[3]),set([data[1]]),1] # from contract
        else:
            values_in[label][0]+=float(data[3])
            values_in[label][1].add(data[1])
            values_in[label][2]+=1
    for label in values_in:
        if label in users_eof_from:
            users_contract_from[label] = len(values_in[label][1])-users_eof_from[label]
        else:
            users_contract_from[label] = len(values_in[label][1])

    keys = list(values_in.keys())
    addr_in, val_in, count_in, user_from_e, user_from_c = \
        keys, [values_in[key][0] for key in keys], [values_in[key][2] for key in keys],\
            [(users_eof_from[key] if key in users_eof_from else 0) for key in keys], \
                [(users_contract_from[key] if key in users_contract_from else 0) for key in keys]

    keys = list(values_out.keys())
    addr_out, val_out, count_out, users_out = keys, [values_out[key][0] for key in keys],\
        [values_out[key][2] for key in keys],\
        [(users_contract_to[key] if key in users_contract_to else 0) for key in keys]

    ins = [[addr_in[i],val_in[i], count_in[i], user_from_e[i], user_from_c[i]] for i in range(len(addr_in))]
    outs = [[addr_out[i],val_out[i], count_out[i], users_out[i]] for i in range(len(addr_out))]
    
    df_in = pd.DataFrame(ins, columns=['address','val_in','count_in','user_from_eof','user_from_contract'])
    df_out = pd.DataFrame(outs, columns=['address','val_out','count_out','user_out'])
    print('total in ',np.sum(df_in['val_in'].values)) #460
    print('total out ',np.sum(df_out['val_out'].values)) #328

    df = df_in.merge(df_out,how='outer')
    df = df.fillna(0)
    df = df.sort_index(axis=1)
    # total = df.values
    # err = []
    # for data in total:
    #     if data[1]<data[2]:
    #         err.append(data)
    # err_df = pd.DataFrame(err)
    # err_df.columns = df.columns
    # err_df.to_csv('err_lxr.csv',index=False)

    df.to_csv(file,index=None)
    
    return file

if __name__=='__main__':
    file_data = trxData()
    
'''
total in  6.555671779289491e+23
total out  4.836896130560263e+23
'''