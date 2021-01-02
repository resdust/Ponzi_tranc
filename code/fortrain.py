import pandas as pd
import numpy as np
import os

fortrain = os.path.join('data','fortraindapp.csv')
dapp = os.path.join('feature','dapp.csv')
file = os.path.join('feature','fortraindapp_feature.csv')

df_train = pd.read_csv(fortrain)
addr_train = df_train['address'].values
addr_train = ['0x'+a for a in addr_train]

df_dapp = pd.read_csv(dapp)
df_dapp = df_dapp.drop(columns=['ponzi'])

datas = []
for data in df_dapp.values:
    if data[0] in addr_train:
        datas.append(data)

df = pd.DataFrame(datas)
df.columns = df_dapp.columns
df.to_csv(file,index=False)