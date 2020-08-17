# !/usr/bin/python3

"""
Run 'python3 main.py' to select contract addresses from table 'code',
which have published their source code. 
This program then automatically sends query to postgresql server for 
necessary information,
and transfers the information into feature for machine learning test.
"""

import os
import sys
import pandas as pd
import pexpect
import features as feature
import deal_sql
import color
import time

N = 1000 # query N instances each time

def examLog(log_file):
    from pathlib import Path

    path = Path(log_file)
    if not path.exists():
        with open(log_file,'w') as f:
            f.write(getTime())
            f.write('\n0\n')

def writeLog(log_file,line):
    with open(log_file,'a') as f:
        f.write(getTime())
        f.write('\n'+str(line)+'\n')

# return last line in string type
def fetchLog(log_file):
    with open(log_file,'r') as f:
        log = f.readlines()
        num = log[-1].strip()
    
    return num

def getTime():
    import time

    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def connectPSQL(psql):
    import getpass

    p = pexpect.spawn(psql)
    logfile = open(os.path.join('log','psql.log'),'ab')
    p.logfile = logfile

    p.expect('gby:') 
    pwd = getpass.getpass('Password:')
    p.sendline(pwd)
    p.expect('#')
    color.pImportant('Successfully connected to PostgreSQL!')

    return p

def collectTxnIn(p, addr, timeout=200):
    import sql_query as sq

    color.pInfo('Collecting transactions into contract')
    query_in = [
        'select block_hash,value from external_transaction where to_address=\'',
        '\' and value!=\'0\';'
        ]
    name = os.path.basename(addr).split('.')[0]
      
    # send command to sql process
    out_file = os.path.join('result',name+'_in.out')
    color.pInfo('Sending incoming transaction query to psql server')
    p.sendline('\o '+out_file)
    p.expect('#')
    sq.val_sql(addr, query_in, p)
    color.pDone('Have generated '+out_file+'.')
    
    # send command to sql process
    txn_file = os.path.join('result',name+'_in.csv')
    time_file = os.path.join('result',name+'_time.out')
    block_hash = deal_sql.deal_in(addr, out_file, txn_file)
    
    color.pInfo('Sending incoming timestamp query to psql server')
    p.sendline('\o '+time_file)
    p.expect('#')
    sq.timestamp_sql(block_hash, p)
    color.pDone('Have generated '+time_file+'.')
    
    # collect the query result into txn features
    deal_sql.deal_in_timestamp(txn_file, time_file)
    
    return txn_file

def collectTxnOut(p, addr, timeout=200):
    import sql_query as sq

    color.pInfo('Collecting transactions out of contract')
    query_out = [
        'select timestamp, value from internal_transaction where from_address=\'',
        '\' and value!=\'0\';\r',
    ]
    name = os.path.basename(addr).split('.')[0]
    
    # send command to sql process
    out_file = os.path.join('result',name+'_out.out')
    color.pInfo('Sending outcoming transaction query to psql server')
    p.sendline('\o '+out_file)
    p.expect('#')
    sq.val_sql(addr, query_out, p)
    color.pDone('Have generated '+out_file+'.')
    
    # collect the query result into txn features
    txn_file = os.path.join('result',name+'_out.csv')
    deal_sql.deal_out(addr, out_file, txn_file)

    return txn_file
   	
if __name__=='__main__':
    color.pInfo('Starting with addresse file in address folder')
    color.pInfo('Usage: python code/main.py')

    psql = 'psql --host 192.168.1.2 -U gby ethereum'
    addr = 'final_addr.csv'
    label = 'final_label.csv'
    
    # collect val and time sequence from addresses
    dirPath = 'address'
    # addrs = os.listdir(dirPath)
    p = connectPSQL(psql)
    times = [time.time()]

    color.pImportant('addr file: '+addr)
    full_path = os.path.join(dirPath,)
    
    in_csv = collectTxnIn(p,full_path)
    out_csv = collectTxnOut(p,full_path)
    times.append(time.time())
    color.pImportant('collected all txns in '+str(times[-1]-times[-2]))

    data_file = addr.split('.')[0]+'_database.csv'
    data_file = os.path.join('result',data_file)
    deal_sql.deal_feature(in_csv, out_csv, data_file)
    feature_file = feature.extract(data_file)
    times.append(time.time())
    color.pImportant('dealed all datas in '+str(times[-1]-times[-2]))
    color.pImportant('')

    feature_df = pd.read_csv(feautre_file)
    label_df = pd.read_csv(os.path.join(dirPath,label),header=None)
    label_df.columns=['ponzi']
    feature_df['ponzi'] = label_df['ponzi']
    feature_df.to_csv(feature_file,index=None)
    
    p.close()    
    color.pImportant('total time used: '+str(times[-1]-times[0]))