
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
from config import DATA_SET_PATH, MODEL_PATH, AHEAD_TIME_List,PIC_PATH,STATIC_ITEM
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

if not os.path.exists(MODEL_PATH):
    os.makedirs(MODEL_PATH)
if not os.path.exists(PIC_PATH):
    os.makedirs(PIC_PATH)


def plot_feature_importances(feature_importances,title,feature_names, picFile):
#    将重要性值标准化
    feature_importances = 100.0*(feature_importances/max(feature_importances))
    # index_sorted = np.flipud(np.argsort(feature_importances)) #上短下长
    #index_sorted装的是从小到大，排列的下标
    index_sorted = np.argsort(feature_importances)# 上长下短
#    让X坐标轴上的标签居中显示
    bar_width = 1
    # 相当于y坐标
    pos = np.arange(len(feature_importances))+bar_width/2
    plt.figure(figsize=(16,4))
    # plt.barh(y,x)
    plt.barh(pos,feature_importances[index_sorted],align='center')
    # 在柱状图上面显示具体数值,ha参数控制参数水平对齐方式,va控制垂直对齐方式
    for y, x in enumerate(feature_importances[index_sorted]):
        plt.text(x+2, y, '%.4s' %x, ha='center', va='bottom')
    plt.yticks(pos,feature_names[index_sorted])
    plt.title(title)
    plt.savefig(picFile,dpi=1000)
# xuan
window = True
window_appendix = ''
if window:
    window_appendix = '_window'
    
def trainAndTest(time,trainItem):
    dataSetFile = os.path.join(DATA_SET_PATH,"{}{}.csv".format(time,window_appendix))
    df = pd.read_csv(os.path.join(dataSetFile))


    print("提前预测时间 = {}".format(time))

    trueDf = df[df['label'] == True]

    # trueDfList = list(trueDf.groupby(by=['dimm_sn']))
    # trueDf = trueDfList[0][1][:1]
    # for i in range(len(trueDfList)):
    #     trueDf = pd.concat([trueDf,trueDfList[i][1][:1] ])

    true_sn = trueDf['dimm_sn'].drop_duplicates().tolist()
    true_sn_train = random.sample(true_sn, int(len(true_sn)*0.6))

    true_df_train = trueDf[trueDf['dimm_sn'].isin(true_sn_train)]
    true_Y_train = true_df_train['label']
    true_X_train = true_df_train.fillna(-1)

    true_df_test = trueDf[~trueDf['dimm_sn'].isin(true_sn_train)]
    true_Y_test = true_df_test['label']
    true_X_test = true_df_test.fillna(-1) 

    print(len(true_df_train),len(true_df_test))

    falseDf = df[df['label'] == False]
    false_sn = falseDf['dimm_sn'].drop_duplicates().tolist()



    false_sn_train = random.sample(false_sn, int(len(false_sn) *0.6))


    false_df_train = falseDf[falseDf['dimm_sn'].isin(false_sn_train)]
    false_Y_train = false_df_train['label']
    false_X_train = false_df_train.fillna(-1) 

    false_df_test = falseDf[~falseDf['dimm_sn'].isin(false_sn_train)]
    false_Y_test = false_df_test['label']
    false_X_test = false_df_test.fillna(-1) 



    # contcat true and false sample
    X_train = pd.concat([true_X_train,false_X_train])
    X_test = pd.concat([true_X_test,false_X_test])


    Y_train = np.concatenate((true_Y_train.tolist(), false_Y_train.tolist()))
    Y_test = np.concatenate((true_Y_test.tolist(), false_Y_test.tolist()))

    test_sn_list =  X_test['dimm_sn'].drop_duplicates().tolist()

    train_sn_list =  X_train['dimm_sn'].drop_duplicates().tolist()

    for sn in test_sn_list:
        if sn in train_sn_list:
            print(sn)


    X_train = X_train[trainItem]
    X_test = X_test[trainItem]

    rfc = RandomForestClassifier()
    rfc.fit(X_train, Y_train)

    picFile = os.path.join(PIC_PATH, "{}-importance{}.png".format(time,window_appendix))
    for i in range (len(trainItem)):
        print(trainItem[i], rfc.feature_importances_[i])
    trainItem = np.array(trainItem)
    plot_feature_importances(rfc.feature_importances_, "feature importances", trainItem,picFile)

    threshold = 0.2

    predicted_proba = rfc.predict_proba(X_test)

    Y_pred = (predicted_proba [:,1] >= threshold).astype('int')
    # Y_pred = rfc.predict(X_test) 
    print("\nModel used is: Random Forest classifier") 
    acc = accuracy_score(Y_test, Y_pred) 
    print("The accuracy is {}".format(acc))
    prec = precision_score(Y_test, Y_pred) 
    print("The precision is {}".format(prec)) 
    rec = recall_score(Y_test, Y_pred) 
    print("The recall is {}".format(rec)) 
    f1 = f1_score(Y_test, Y_pred) 
    print("The F1-Score is {}".format(f1)) 


    prec, recall, _ = precision_recall_curve(Y_test, predicted_proba [:,1], pos_label=1)
    pr_display = PrecisionRecallDisplay(estimator_name = 'rf',precision=prec, recall=recall, average_precision=average_precision_score(Y_test, predicted_proba [:,1], pos_label=1))
    pr_display.average_precision
    pr_display.plot()
    plt.savefig(os.path.join(PIC_PATH,'{}-p-r{}.png'.format(time,window_appendix)),dpi=1000)
    plt.cla()
    with open(os.path.join(MODEL_PATH,'{}{}.pkl'.format(time,window_appendix)), 'wb') as fw:
        pickle.dump(rfc, fw)

pList = []
for time in AHEAD_TIME_List :
    pList.append(Process(target=trainAndTest, args=(time,trainItem)))
[p.start() for p in pList]
[p.join() for p in pList]
