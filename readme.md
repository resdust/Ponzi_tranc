# USAGE
```
sudo apt-get install postgresql-client
pip install pexpect,pandas,colorama

mkdir sql
mkdir result
python code/main.py
```

## feature

| 特征名称        | 含义                                     | 类型         |
| --------------- | --------------------------------------- | ------------ |
| ponzi           | 是否为庞氏合约的标签，庞氏：1，normal：0 | nominal{0,1} |
| nbr_tx_in       | 向合约转账交易总次数                     | numerical    |
| nbr_tx_out      | 合约向外转账总次数                       | numerical    |
| Tot_in          | 向合约转账的总金额                       | numerical    |
| Tot_out         | 合约向外转账的总金额                     | numerical    |
| mean_in         | 向合约转账的平均金额                     | numerical    |
| mean_out        | 合约向外转账的平均金额                   | numerical    |
| sdev_in         | 向合约转账金额的标准方差                 | numerical    |
| sdev_out        | 合约向外转账金额的标准方差               | numerical    |
| gini_in         | 向合约转账金额的基尼系数                 | numerical    |
| gini_out        | 合约向外转账金额的基尼系数               | numerical    |
| avg_time_btw_tx | 平均多长时间有一笔交易                   | numerical    |
| lifetime        | 合约生存周期                             | numerical    |

# result

### 134ponzi+134dapp

```
weka.classifiers.trees.RandomForest -P 100 -I 100 -num-slots 1 -K 0 -M 1.0 -V 0.001 -S 1 -depth 6
```
|accuracy|precision|recall|f-score|
|:---:|:---:|:---:|:---:|
|0.890|0.890|0.890|0.890|

![image-20200821094505026](C:\Users\14415\AppData\Roaming\Typora\typora-user-images\image-20200821094505026.png)

![image-20200821094719675](C:\Users\14415\AppData\Roaming\Typora\typora-user-images\image-20200821094719675.png)

