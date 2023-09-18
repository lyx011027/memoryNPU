import pandas as pd
from datetime import datetime, timedelta
from multiprocessing import Process, Queue
import os, math, json, sys

DIMM_INFO = "/home/hw-admin/yixuan/ProjectNPU/data/DIMMs.json"
ERR_INFO = "/home/hw-admin/yixuan/ProjectNPU/data/mem_err_events.json"

# 多进程处理框架
def multiProcess(dataList, processFunction, processArgs, mergeFunction, mergeArgs, endFlag):
    q = Queue()
    processList = []
    cpuCount = os.cpu_count() * 2
    subListSize = math.ceil(len(dataList) / cpuCount)
    for i in range(cpuCount):
        subDimm = dataList[i*subListSize:(i + 1)*subListSize]
        processList.append(Process(target=processFunction, args=[q, subDimm] + processArgs))
        
    pMerge = Process(target=mergeFunction, args=[q] + mergeArgs)
    pMerge.start()
    for p in processList:
        p.start()

    for p in processList:
        p.join()
    q.put(endFlag)
    pMerge.join()
    
# count 1
def countPredictableUEDIMMMerge(q, leadTimeList):
    UE = 0
    predictableDIMM = (len(leadTimeList)) * [0]
    while True:
        predictable = q.get()
        if len(predictable) == 0:
            print("UE DIMM count = {}".format(UE))
            for i in range(len(leadTimeList)):
                print("lead time = {}, predictable UE DIMM = {}".format(leadTimeList[i], predictableDIMM[i]))
            return
        UE += 1
        for i in range(len(predictableDIMM)):
            if predictable[i]:
                predictableDIMM[i] += 1

def countPredictableUEDIMMProcess(q, subDfList, leadTimeList):
    for item in subDfList:
        predictable = (len(leadTimeList)) * [False]
        sn = item[0]
        df = item[1]
        # df['record_datetime'] = pd.to_datetime(df['record_datetime'], format="%Y-%m-%d %H:%M:%S")
        UERDf = df[df['err_type'].isin(['UER', 'UEO'])].reset_index(drop=True)
        if UERDf.shape[0] == 0:
            continue
        firstUER = UERDf.loc[0,'record_datetime']
        
        
        CEDf = df[(df['err_type'].isin([ 'CE', 'PatrolScrubbingUEO']))].reset_index(drop=True)
        if CEDf.shape[0] > 0:
            firstCE = CEDf.loc[0, 'record_datetime']
            for i in range(len(leadTimeList)):
                if firstUER - firstCE < leadTimeList[i]:
                    predictable[i] = True
                else:
                    break
        q.put(predictable)
        
# 统计不同 lead time 时，可预测的 UE DIMM 数量
def countPredictableUEDIMM(leadTimeList,subDfList):
    multiProcess(subDfList, countPredictableUEDIMMProcess,[leadTimeList], countPredictableUEDIMMMerge, [leadTimeList], [])
    

# count 2

# 获取DIMM数量
def getDIMMNum(DIMMDf):

    print("DIMM number = {}".format(DIMMDf.drop_duplicates(subset=["dimm_sn"]).shape[0]))
    dfList = list(DIMMDf.groupby(by=['vendor',"bit_width_x","capacity", "rank_count","speed"]))
    for item in dfList:
        print("vendor = {}, DQ = {}, capacity = {}, rank num = {}, speed = {}, number = {}".format
              ( item[0][0], item[0][1], item[0][2], item[0][3], item[0][4],item[1].shape[0]))
        
# 统计 log 持续时间
def getTime(ErrorDf):
    maxIdx= ErrorDf['record_datetime'].idxmax()
    minIdx= ErrorDf['record_datetime'].idxmin()
    
    firstTime = ErrorDf.loc[minIdx, 'record_datetime']
    lastTime = ErrorDf.loc[maxIdx, 'record_datetime']
    
    print("first error time : {}, last error time : {}, period : {}".format(firstTime, lastTime, lastTime - firstTime))

# 统计发生不同 type error 的 DIMM 数量
def getErrorTypeDIMMNum(ErrorDf):
    
    errorTypeDf = ErrorDf.drop_duplicates(subset=["err_type"]).reset_index(drop=True)
    errorTypeList = []
    for i in range(errorTypeDf.shape[0]):
        errorTypeList.append(errorTypeDf.loc[i, 'err_type'])
    for errorType in errorTypeList:
        subDf = ErrorDf[ErrorDf['err_type'] == errorType].drop_duplicates(subset=["dimm_sn"]).reset_index(drop=True)
        num = subDf.shape[0]
        print("{} DIMM count = {}".format(errorType, num))
        

# 统计非突发UER中, 首次UER前首次CE与末次CE发生时间

def avg(timeList):
    base = 0
    for time in timeList:
        base += time.days
    return base/len(timeList)

def countIntervalMerge(q):
    firstTimeList = []
    lastTimeList = []
    while True:
        tmp = q.get()
        op = tmp[0] 
        if op == -1:
            for i in range(len(firstTimeList)):
                firstTimeList[i] = firstTimeList[i].days * 24* 60*60 + firstTimeList[i].seconds
                lastTimeList[i] = lastTimeList[i].days * 24* 60*60 + lastTimeList[i].seconds
            print("timing gap list of first CE = {}\n".format(firstTimeList))
            print("timing gap list of last CE = {}\n".format(lastTimeList))
            if len(firstTimeList) > 0 and len(lastTimeList) > 0:
                print("average timing gap of first CE = {}, average timing gap of last CE = {}".
                      format(sum(firstTimeList)/len(firstTimeList), sum(lastTimeList)/len(lastTimeList)))
            
            
            return
        if op > 0:
            if op > 1:
                firstTimeList.append(tmp[1])
                lastTimeList.append(tmp[2])

def countIntervalProcess(q, subDfList):
    pastTime = timedelta(minutes=5)
    leadTime = timedelta(minutes=5)
    for item in subDfList:
        sn = item[0]
        df = item[1]
        # df['record_datetime'] = pd.to_datetime(df['record_datetime'], format="%Y-%m-%d %H:%M:%S")
        UERDf = df[df['err_type'].isin(['UER', 'UEO'])].reset_index(drop=True)
        if UERDf.shape[0] == 0:
            continue
        firstUER = UERDf.loc[0,'record_datetime']
        
        CEDf = df[(df['err_type'].isin([ 'CE', 'PatrolScrubbingUEO'])) & (df['record_datetime'] < firstUER) ].reset_index(drop=True)
        if CEDf.shape[0] == 0:
            q.put([1, datetime.now(), datetime.now()])
            continue
        firstCETime = firstUER - CEDf.loc[0, 'record_datetime'] 
        lastCETime = firstUER - CEDf.loc[CEDf.shape[0] - 1, 'record_datetime']
        q.put([2, firstCETime, lastCETime])

def countInterval(dfList):
    multiProcess(dfList, countIntervalProcess,[], countIntervalMerge, [], [-1,-1,-1])


def main():
    [CPU_INFO, DIMM_INFO, ERR_INFO] = sys.argv[1:4]
    
    # 读取DIMM信息
    with open(DIMM_INFO, 'r') as json_file:
        data = json.load(json_file)
    DIMMDf = pd.json_normalize(data)
    
    
    # 读取error信息
    with open(ERR_INFO, 'r') as json_file:
        data = json.load(json_file)
    ErrorDf = pd.json_normalize(data)
    ErrorDf['record_datetime'] = pd.to_datetime(ErrorDf['record_datetime'], format="%Y-%m-%d %H:%M:%S")
    ErrorDf = ErrorDf.sort_values(by = ['record_datetime']).reset_index(drop=True)
    ErrorDf = ErrorDf.rename(columns={'phy_addr.bank':'bank' ,'phy_addr.bankgroup': 'bankgroup','phy_addr.column':'column' ,'phy_addr.device':'device' ,'phy_addr.rank':'rank' ,'phy_addr.row':'row'})
    ErrorDfList = list(ErrorDf.groupby('dimm_sn'))
    
    print("- count predictable UE DIMM")
    leadTimeList = [timedelta(seconds=0),timedelta(seconds=5),timedelta(minutes=1),timedelta(minutes=5),timedelta(minutes=15),timedelta(minutes=30), timedelta(hours=1)]
    countPredictableUEDIMM(leadTimeList, ErrorDfList)
    print("\n")
    
    print("- count DIMM info")
    getDIMMNum(DIMMDf)
    print('\n')

    print("- count error logging period")
    getTime(ErrorDf)
    print('\n')
    
    print("- count different error type")
    getErrorTypeDIMMNum(ErrorDf)
    print("\n")
    
    print("- count timing gap")
    countInterval(ErrorDfList)
    print("\n")
    
main()