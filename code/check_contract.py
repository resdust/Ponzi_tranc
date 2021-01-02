import os,sys
import pandas as pd

def deal_out(in_file): 
    os.system('echo \"EOF\" >> '+in_file)

    count = 0

    with open(in_file,'r',encoding='utf-8') as f:
        line = f.readline().strip()
        index = 0
        while(line!='EOF'):
            data = []
            if line == '':
                line = f.readline().strip()
                continue

            if line[0] == '(':
                index = index+1

            if line[0] =='2':
                attributes = line.split('|')
                for i in range(len(attributes)):
                    attributes[i] = attributes[i].strip()
                data = [address[index], attributes[0], attributes[1]]
                transactions.append(data)
            line = f.readline().strip()

    df = pd.DataFrame(data=transactions, columns=names_transaction)
    df.to_csv(to_file,index=False)