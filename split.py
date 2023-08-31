# 按 dimm_sn 号切分 error event 数据集，并保存出现 error 的 dimm 静态信息

import pandas as pd
from config import *
import os, math, json, sys
from multiprocessing import Process, Queue
CPU_INFO = "/home/hw-admin/yixuan/ProjectNPU/data/CPUs.json"
DIMM_INFO = "/home/hw-admin/yixuan/ProjectNPU/data/DIMMs.json"
ERR_INFO = "/home/hw-admin/yixuan/ProjectNPU/data/mem_err_events.json"


def saveDfList(dimmMap, dfList):
    for sub in dfList:
        subdf = sub[1].reset_index(drop=True)
        deviceId = subdf.loc[0, 'device_id']
        sn = sub[0]
        if deviceId != 'cpu0' and deviceId != 'cpu1' and deviceId != 'cpu2' and deviceId != 'cpu3':
            continue
        # elif sn not in dimmMap:
        #     continue
        subPath = os.path.join(SPLIT_DATA_PATH, sn)
        if not os.path.exists(subPath):
            os.makedirs(subPath)
        errorFile = os.path.join(subPath, sn+"_error.csv")
        subdf.to_csv(errorFile, index=False)
    

def countData(dfList):
    uceDIMMCount = 0
    ceDIMMCount = 0
    unpredictableUceDIMM = 0
    for sub in dfList:
        subDf = sub[1].reset_index(drop= True)
        
        ceFlag = False
        uceFlag = False
        unpredictableFlag = False
        for _, error in subDf.iterrows():
            if error['err_type'] == "CE":
                ceFlag = True
            else:
                uceFlag = True
                if not ceFlag:
                    unpredictableFlag = True
        if ceFlag:
            ceDIMMCount += 1
        if uceFlag:
            uceDIMMCount += 1
        if unpredictableFlag:
            unpredictableUceDIMM += 1
    print("ce DIMM:{}, uce DIMM:{}, unpredictable uce DIMM:{}".format(ceDIMMCount, uceDIMMCount, unpredictableUceDIMM))
    

# 按dimm sn号切分数据，并解析
def splitByDIMM():
    if not os.path.exists(SPLIT_DATA_PATH):
        os.makedirs(SPLIT_DATA_PATH)

    # 读取CPU信息
    with open(CPU_INFO, 'r') as json_file:
        cpuList = json.load(json_file)
    cpuMap = {}
    for cpu in cpuList:
        cpuMap[cpu["cpu_sn"]] = cpu
    
    # 读取DIMM静态信息
    with open(DIMM_INFO, 'r') as json_file:
        dimmList = json.load(json_file)
    dimmMap = {}
    for dimm in dimmList:
        dimmMap[dimm["dimm_sn"]] = dimm
    # 读取error信息
    with open(ERR_INFO, 'r') as json_file:
        data = json.load(json_file)
    df = pd.json_normalize(data)
    df = df.rename(columns={'phy_addr.bank':'bank' ,'phy_addr.bankgroup': 'bankgroup','phy_addr.column':'column' ,'phy_addr.device':'device' ,'phy_addr.rank':'rank' ,'phy_addr.row':'row'})
    dfList = list(df.groupby('dimm_sn'))
    countData(dfList)
    processList = []
    cpuCount = os.cpu_count() * 2
    subListSize = math.ceil(len(dfList) / cpuCount)
    for i in range(cpuCount):
        subDimm = dfList[i*subListSize:(i + 1)*subListSize]
        processList.append(Process(target=saveDfList, args=([dimmMap, subDimm])))
        
    for p in processList:
        p.start()

    for p in processList:
        p.join()
     
[CPU_INFO, DIMM_INFO, ERR_INFO] = sys.argv[1:4]
splitByDIMM()
    

