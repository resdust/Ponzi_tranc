# USAGE
```
mkdir sql
mkdir result
python code/main.py
```

# result
after rectified feature file, deleting the contracts with only one transaction, we got the result as following.
parameter: 
```
weka.classifiers.trees.RandomForest -P 100 -I 100 -num-slots 1 -K 0 -M 1.0 -V 0.001 -S 1 -depth 4 -batch-size 70
```
|accuracy|precision|recall|f-score|
|:---:|:---:|:---:|:---:|
|0.874|0.874|0.874|0.874|
