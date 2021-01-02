# Load libraries
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_validate
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import make_scorer
import numpy as np
import pandas as pd
import os

def fp(y_true, y_pred): return confusion_matrix(y_true, y_pred)[0, 1]
def tp(y_true, y_pred): return confusion_matrix(y_true, y_pred)[1, 1]
def fn(y_true, y_pred): return confusion_matrix(y_true, y_pred)[1, 0]
def tn(y_true, y_pred): return confusion_matrix(y_true, y_pred)[0, 0]
def precision(matrix): return matrix[0]/(matrix[0]+matrix[2])
def recall(matrix): return matrix[0]/(matrix[0]+matrix[3])
def fscore(matrix): return 2*precision(matrix)*recall(matrix)/(precision(matrix)+recall(matrix))

data_file = os.path.join('feature','fortraindapp+ponzi.csv')
# data_file1 = os.path.join('feature','116ponzi+116dapp_rectifyied.csv')
# data_file = os.path.join('feature','116ponzi+116dapp_addr_database_feature.csv')
test_file = os.path.join('feature','merged1888_feature.csv')
df = pd.read_csv(data_file)
test_df = pd.read_csv(test_file)

c=['nbr_tx_in', 'nbr_tx_out', 'Tot_in', 'Tot_out', 'mean_in',
       'mean_out', 'sdev_in', 'sdev_out', 'gini_in', 'gini_out',
       'avg_time_btw_tx', 'lifetime']
X = df[c].values
y = df['ponzi'].values

# Create decision tree classifer object
clf = RandomForestClassifier(random_state=0, n_jobs=-1, class_weight="balanced",)
 
# Train model
# model = clf.fit(X, y)
# target_names = ['nomal','ponzi']
# test_X = test_df[c].values
# test_y = [0]*len(test_X)
# y_pred = model.predict(test_X)
# print(classification_report(test_y, y_pred, target_names=target_names))
# count = 0
# addrs = []
# for i in range(len(y_pred)):
#     if y_pred[i]!=0:
#         count += 1
#         addrs.append(test_df.iloc[i]['address'])
#         print(test_df.iloc[i]['address'])
# print(count)

# addr_df = pd.DataFrame(addrs)
# addr_df.to_csv('1-1report.csv',index=None,header=None)

# evaluate
cv = 10
scoring = {
    'tp': make_scorer(tp),
    'fp': make_scorer(fp),
    'tn': make_scorer(tn),
    'fn': make_scorer(fn),
    'accuracy':'accuracy',
    'precision':'precision',
    'recall':'recall',
    'f1':'f1',
}
m = cross_validate(clf, X, y, scoring=scoring, cv=cv)
score = ['test_accuracy', 'test_precision', 'test_recall', 'test_f1']
number = ['test_tp', 'test_tn','test_fp','test_fn']
for n in score:
    print(n,np.average(m[n]))
for n in number:
    print(n,np.sum(m[n]))
tp = np.sum(m[number[0]])
tn = np.sum(m[number[1]])+463
fp = np.sum(m[number[2]])
fn = np.sum(m[number[3]])+20
matrix_ponzi = [tp,tn,fp,fn]
matrix_dapp = [tn,tp,fn,fp]
print('         ponzi   dapp    weighted (average)',)
print('Precision    ',precision(matrix_ponzi),precision(matrix_dapp),\
    (precision(matrix_ponzi)+precision(matrix_dapp))/2)
print('Recall    ',recall(matrix_ponzi),recall(matrix_dapp),\
    (recall(matrix_ponzi)+recall(matrix_dapp))/2)
print('F-measure    ',fscore(matrix_ponzi),fscore(matrix_dapp),\
    (fscore(matrix_ponzi)+fscore(matrix_dapp))/2)
'''
116ponzi+116dapp_rectifyied
test_accuracy 0.8613636363636363
test_precision 0.868409507159507
test_recall 0.8613636363636363
test_f1 0.8605320795400887
test_tp 96
test_tn 102
test_fp 13
test_fn 19
======
无交易数据的判断为negative
dapp: 134-114=20, 
ponzi: 134-114=20,
test_tp 96
test_tn 102+20=122
test_fp 13
test_fn 19+20=39
            ponzi   dapp    weighted (average)
precision   0.8807  0.7578  0.8193
recall      0.7111  0.9037  0.8074
F-measure   0.7869  0.8243  0.8056
'''

'''
fortraindapp+ponzi.csv weighted
test_accuracy 0.9205808346456358
test_precision 0.9163777140309726
test_recall 0.9205808346456358
test_f1 0.9143566834864678
test_tp 61
test_tn 843
test_fp 25
test_fn 53
======
无交易数据的判断为negative
dapp: 1331-868=463, weight: 0.9085
ponzi: 134-114=20, weight: 0.0915
tp: 61
tn: 843+463=1306
fp: 25
fn: 53+20=73
            ponzi   dapp    weighted
precision   0.7093  0.9471  0.9253
recall      0.4552  0.9812  0.9331
F-measure   0.5545  0.9638  0.9264
'''