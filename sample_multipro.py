import pandas as pd
import json
from datetime import datetime, timedelta
import csv
from config import DATA_SET_PATH,SPLIT_DATA_PATH,AHEAD_TIME_List,STATIC_ITEM
import os
from threading import Thread
import math
import copy
from multiprocessing import Process, Queue
# import moxing as mox



UCE_INTERVAL = timedelta(days=1)
INTERVAL = timedelta(days=15)
CEINTERVAL = timedelta(days=7)
MAXROW = 60
MAXCOLUMN = 60



staticItem = [
    'dimm_sn'
    ] + STATIC_ITEM


dynamicItem = [
    "CE_number", 
    "CE_bank_number", 
    "row_number", 
    "column_number", 
    "position_number", 
    "same_row", 
    "same_column", 
    "same_position",
    "interval",
    "avg_CE",
    "CE_window",
    "max_row", 
    "max_column", 
    "max_position"
    ]

sampleItem = ['dimm_sn'] + dynamicItem + ['time','label']

if not os.path.exists(DATA_SET_PATH):
    os.makedirs(DATA_SET_PATH)


def get_writer(dataset):
    f1 = open(dataset, mode="w")
    writer = csv.DictWriter(f1, sampleItem)
    itemMap = {}
    for item in sampleItem:
        itemMap [item] = item
    writer.writerow(itemMap)
    return writer

def haveUCE(q, errorList, baseSample,aheadTime):
    timeList = []
    for  error in errorList: 
        if error["err_type"] != "CE":
            uceTime = error['record_datetime']
            if len(timeList) == 0 or (uceTime - timeList[len(timeList)-1] > UCE_INTERVAL):
                timeList.append(uceTime)
    for i in range (len(timeList)):
        timeList[i] = timeList[i] - aheadTime
    predictableFlag = False
    for error in errorList:
        if error["err_type"] == "CE":
            ceTime = error['record_datetime']
            if ceTime < timeList[0]:
                predictableFlag = True
        break
    if not predictableFlag:
        q.put([])
        return False
    gen_sample(q, errorList, baseSample, timeList, True)
    return True

def haveNoUCE(q, errorList, baseSample):
    timeList = []
    firsrTime = datetime.now().replace(year=2000)
    
    for error in errorList:
        if error['record_datetime'] - firsrTime > CEINTERVAL:
            timeList.append(error['record_datetime'] + CEINTERVAL)
            firsrTime = error['record_datetime']
    if len(timeList) > 0:
        gen_sample(q, errorList, baseSample, timeList, False)
    

def write_sample(bank_list, centerList, sample, CE_number, windowTime):
    for item in dynamicItem:
        sample[item] = 0
    sample["CE_number"] = CE_number
    sample["interval"] = (windowTime[0] - centerList[0][0]).days
    sample["CE_bank_number"] = len(bank_list)
    sample["CE_window"] = len(centerList)
    for bank in bank_list.values():
        sample["position_number"] += len(bank[0])
        for p in bank[0].values():
            sample["max_position"] = max(sample["max_position"], p)
        sample["row_number"] += len(bank[1])
        for p in bank[1].values():
            sample["max_row"] = max(sample["max_row"], p)
        sample["column_number"] += len(bank[2])
        for p in bank[2].values():
            sample["max_column"] = max(sample["max_column"], p)

    bank_list_value = []
    for i in bank_list.values():
        bank_list_value.append(i)
    firstBank = bank_list_value[0]

    for i in range(1, len(bank_list_value)):
        for j in firstBank[0]:
            if(j in bank_list_value[i][0]):
                sample["same_position"] += 1
        for j in firstBank[1]:
            if(j in bank_list_value[i][1]):
                sample["same_row"] += 1
        for j in firstBank[2]:
            if(j in bank_list_value[i][2]):
                sample["same_column"] += 1


    
    sample["avg_CE"] = float(sample["CE_number"]) / (sample["CE_window"]+1)
    sample["time"] = windowTime[1]
    return sample
    
    
def gen_sample(q,errorList, baseSample, timeList, flag):
    sampleList = []
    sample = {}
    for key in baseSample.keys():
        sample[key] = baseSample[key]
    if flag:
        sample['label'] = 1
    else:
        sample['label'] = 0
    for time in timeList:
        windowTime = [time, time]
        centerList = []
        bank_list = {}
        CE_number = 0
        for error in errorList:
            time = error['record_datetime']
            count = error['err_count']
            # 若故障时间大于采样时间，break
            if(windowTime[0] < time ):
                break
            # 若时间差大于30day，不记入
            if windowTime[0] - time > timedelta(days=30):
                continue
            CE_number += count
            windowTime[1] = time
            bankId = "{}_{}_{}".format(error["rank"], error['bankgroup'], error['bank'])
            
            position = (error["row"],error["column"])
            # 新的 record 与 上一个 window 开始时间的间隔超过 interval，则创建一个新的 window
            flagNewCenter = False
            for center in centerList:
                if (time - center[0] < INTERVAL 
                    and abs(center[1][0] - position[0]) < MAXROW 
                    and abs(center[1][1] - position[1]) < MAXCOLUMN
                    and center[2] == bankId):
                    flagNewCenter = True
                    break
            if flagNewCenter == False:
                centerList.append((time,position,bankId))
                
            
            if bankId not in bank_list:
                    bank_list[bankId] = [{},{},{}]
                    
            if position in bank_list[bankId][0] :
                    bank_list[bankId][0][position] += count
            else:
                bank_list[bankId][0][position] = count
            if error["row"] in bank_list[bankId][1]:
                    bank_list[bankId][1][error["row"]]+= count
            else:
                bank_list[bankId][1][error["row"]] = count
            
            if error["column"] in bank_list[bankId][2] :
                    bank_list[bankId][2][error["column"]] += count
            else:
                bank_list[bankId][2][error["column"]] = count
        if len(bank_list) > 0:
            newSample  = write_sample(bank_list, centerList, sample, CE_number, windowTime)
            sampleList.append(copy.copy(newSample))
    q.put(sampleList)

        
def getTime(a):
    return a['record_datetime']


    
    
def processDimm(id, q, dimmList, aheadTime):
    for dimm in dimmList:
        sample = {'dimm_sn':dimm}
        errorFile = os.path.join(SPLIT_DATA_PATH, dimm, dimm+"_error.csv")
        df = pd.read_csv(errorFile)
        UCEFlag = False
        errorList = []
        for  _, error in df.iterrows():
            error['record_datetime']  = datetime.strptime(error['record_datetime'], 
                                                            "%Y-%m-%d %H:%M:%S")
            errorList.append(error)
            if error['err_type'] != "CE":
                UCEFlag = True
        if len(errorList) == 0:
            q.put([])
        if UCEFlag:
            haveUCE(q, errorList, sample,aheadTime)
        else:
            haveNoUCE(q,errorList, sample)


def genDataSet(aheadTime):
    dataSetFile = "{}_window.csv".format(aheadTime)
    writer = get_writer(os.path.join(DATA_SET_PATH,dataSetFile))
    dimmList = os.listdir(SPLIT_DATA_PATH)
    q = Queue()
    processList = []
    cpuCount = os.cpu_count() * 2
    subListSize = math.ceil(len(dimmList) / cpuCount)
    for i in range(cpuCount):
        subDimm = dimmList[i*subListSize:(i + 1)*subListSize]
        processList.append(Process(target=processDimm, args=(i,q, subDimm, aheadTime)))
        
    for p in processList:
        p.start()
        
    length = len(dimmList)
    while True:
        if length == 0:
            break
        length -= 1
        sampleList = q.get()
        [writer.writerow(sample) for sample in sampleList]
        
    for p in processList:
        p.join()


for time in  AHEAD_TIME_List:
    print("生成提前预测时间为{}的数据集".format(time))
    genDataSet(time)

    
# time = timedelta(hours=6)
# print("生成提前预测时间为{}的数据集".format(time))
# genDataSet(time)
    