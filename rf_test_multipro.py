
import os
import random
import pandas as pd
import numpy as np
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.metrics import precision_recall_curve,PrecisionRecallDisplay,average_precision_score
import matplotlib.pyplot as plt
from sklearn.utils import shuffle
from config import *
from multiprocessing import Process


dynamicItem = [
    "CE_number", 
    "CE_bank_number", 
    "row_number", 
    "column_number", 
    "position_number", 
    "same_row", 
    "same_column", 
    "same_position",
    # "interval",
    "avg_CE",
    "CE_window",
    "max_row", 
    "max_column", 
    "max_position"
    ]

trainItem = dynamicItem 
    # + STATIC_ITEM

# xuan
window = True
window_appendix = ''
if window:
    window_appendix = '_window'
    
def trainAndTest(time,trainItem):
    dataSetFile = os.path.join(DATA_SET_PATH,"{}{}.csv".format(time,window_appendix))
    df = pd.read_csv(os.path.join(dataSetFile))


    with open(os.path.join(TEST_MODEL_PATH,'{}{}.pkl'.format(time,window_appendix)), 'rb') as f:
        rfc = pickle.load(f)
    dataSetFile = os.path.join(DATA_SET_PATH,"{}{}.csv".format(time,window_appendix))
    df = pd.read_csv(os.path.join(dataSetFile))
    
    data = df[trainItem]
    label = df['label']
    predicted_proba = rfc.predict_proba(data)
    prec, recall, _ = precision_recall_curve(label, predicted_proba [:,1], pos_label=1)
    pr_display = PrecisionRecallDisplay(estimator_name = 'rf',precision=prec, recall=recall, average_precision=average_precision_score(label, predicted_proba [:,1], pos_label=1))
    pr_display.average_precision
    pr_display.plot()
    plt.savefig(os.path.join(TEST_PIC_PATH,'{}-p-r{}.png'.format(time,window_appendix)),dpi=1000)
    plt.cla()

if not os.path.exists(TEST_PIC_PATH):
    os.makedirs(TEST_PIC_PATH)

pList = []
for time in AHEAD_TIME_List :
    pList.append(Process(target=trainAndTest, args=(time,trainItem)))
[p.start() for p in pList]
[p.join() for p in pList]
