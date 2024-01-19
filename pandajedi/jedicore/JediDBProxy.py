import atexit
import copy
import datetime
import json
import logging
import math
import os
import random
import re
import socket
import sys
import traceback
import uuid

import numpy
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandajedi.jediconfig import jedi_config
from pandaserver.taskbuffer import EventServiceUtils, JobUtils, OraDBProxy

from . import JediCoreUtils, ParseJobXML
from .InputChunk import InputChunk
from .JediCacheSpec import JediCacheSpec
from .JediDatasetSpec import JediDatasetSpec
from .JediFileSpec import JediFileSpec
from .JediTaskSpec import JediTaskSpec, push_status_changes
from .MsgWrapper import MsgWrapper
from .WorkQueueMapper import WorkQueueMapper

logger = PandaLogger().getLogger(__name__.split(".")[-1])
OraDBProxy._logger = logger


# get database backend
def get_database_backend():
    if hasattr(jedi_config.db, "backend"):
        return jedi_config.db.backend
    return None


backend = get_database_backend()
if backend == "postgres":
    import psycopg2 as psycopg

    varNUMBER = int
else:
    import cx_Oracle

    varNUMBER = cx_Oracle.NUMBER

# add handlers of filtered logger
tmpLoggerFiltered = PandaLogger().getLogger(__name__.split(".")[-1] + "Filtered")
for tmpHdr in tmpLoggerFiltered.handlers:
    tmpHdr.setLevel(logging.INFO)
    logger.addHandler(tmpHdr)
    tmpLoggerFiltered.removeHandler(tmpHdr)


# get mb proxies used in DBProxy methods
def get_mb_proxy_dict():
    if hasattr(jedi_config, "mq") and hasattr(jedi_config.mq, "configFile") and jedi_config.mq.configFile:
        # delay import to open logger file inside python daemon
        from pandajedi.jediorder.JediMsgProcessor import MsgProcAgent

        in_q_list = []
        out_q_list = ["jedi_jobtaskstatus", "jedi_contents_feeder", "jedi_job_generator"]
        mq_agent = MsgProcAgent(config_file=jedi_config.mq.configFile)
        mb_proxy_dict = mq_agent.start_passive_mode(in_q_list=in_q_list, out_q_list=out_q_list)
        # stop with atexit
        atexit.register(mq_agent.stop_passive_mode)
        # return
        return mb_proxy_dict


class DBProxy(OraDBProxy.DBProxy):
    # constructor
    def __init__(self, useOtherError=False):
        OraDBProxy.DBProxy.__init__(self, useOtherError)
        # attributes for JEDI
        #
        # list of work queues
        self.workQueueMap = WorkQueueMapper()
        # update time for work queue map
        self.updateTimeForWorkQueue = None

        # typical input cache
        self.typical_input_cache = {}

        # mb proxy
        self.jedi_mb_proxy_dict = None

    # connect to DB (just for INTR)
    def connect(
        self,
        dbhost=jedi_config.db.dbhost,
        dbpasswd=jedi_config.db.dbpasswd,
        dbuser=jedi_config.db.dbuser,
        dbname=jedi_config.db.dbname,
        dbtimeout=None,
        reconnect=False,
    ):
        return OraDBProxy.DBProxy.connect(self, dbhost=dbhost, dbpasswd=dbpasswd, dbuser=dbuser, dbname=dbname, dbtimeout=dbtimeout, reconnect=reconnect)

    # extract method name from comment
    def getMethodName(self, comment):
        tmpMatch = re.search("([^ /*]+)", comment)
        if tmpMatch is not None:
            methodName = tmpMatch.group(1).split(".")[-1]
        else:
            methodName = comment
        return methodName

    # check if exception is from NOWAIT
    def isNoWaitException(self, errValue):
        oraErrCode = str(errValue).split()[0]
        oraErrCode = oraErrCode[:-1]
        if oraErrCode == "ORA-00054":
            return True
        return False

    # dump error message
    def dumpErrorMessage(self, tmpLog, methodName=None, msgType=None, check_fatal=False):
        # error
        errtype, errvalue = sys.exc_info()[:2]
        if methodName is not None:
            errStr = methodName
        else:
            errStr = ""
        errStr += f": {errtype.__name__} {errvalue}"
        errStr.strip()
        tmp_diag = errStr
        errStr += "\n"
        errStr += traceback.format_exc()
        if msgType == "debug":
            tmpLog.debug(errStr)
        else:
            tmpLog.error(errStr)
        if check_fatal:
            err_code = str(errvalue).split()[0][:-1]
            if err_code in ["ORA-01438"]:
                return True, tmp_diag
            else:
                return False, tmp_diag

    # get work queue map
    def getWorkQueueMap(self):
        self.refreshWorkQueueMap()
        return self.workQueueMap

    # refresh work queue map
    def refreshWorkQueueMap(self):
        # avoid frequent lookup
        if self.updateTimeForWorkQueue is not None and (
            datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - self.updateTimeForWorkQueue
        ) < datetime.timedelta(minutes=10):
            return
        comment = " /* JediDBProxy.refreshWorkQueueMap */"
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")

        leave_shares = self.get_sorted_leaves()

        # SQL
        sql = self.workQueueMap.getSqlQuery()
        try:
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000
            self.cur.execute(sql + comment)
            res = self.cur.fetchall()
            if not self._commit():
                raise RuntimeError("Commit error")
            # make map
            self.workQueueMap.makeMap(res, leave_shares)
            tmpLog.debug("done")
            self.updateTimeForWorkQueue = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # get the list of datasets to feed contents to DB
    def getDatasetsToFeedContents_JEDI(self, vo, prodSourceLabel, task_id=None):
        comment = " /* JediDBProxy.getDatasetsToFeedContents_JEDI */"
        methodName = self.getMethodName(comment)
        if task_id is not None:
            methodName += f" <vo={vo} label={prodSourceLabel} taskid={task_id}>"
        else:
            methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # SQL
            varMap = {}
            varMap[":ts_running"] = "running"
            varMap[":ts_scouting"] = "scouting"
            varMap[":ts_ready"] = "ready"
            varMap[":ts_defined"] = "defined"
            varMap[":dsStatus_pending"] = "pending"
            varMap[":dsState_mutable"] = "mutable"
            if task_id is None:
                try:
                    checkInterval = jedi_config.confeeder.checkInterval
                except Exception:
                    checkInterval = 60
            else:
                checkInterval = 0
            varMap[":checkTimeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=checkInterval)
            varMap[":lockTimeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=10)
            sql = f"SELECT {JediDatasetSpec.columnNames('tabD')} "
            if task_id is None:
                sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            else:
                varMap[":task_id"] = task_id
                sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE tabT.jediTaskID=:task_id "
            sql += "AND (tabT.lockedTime IS NULL OR tabT.lockedTime<:lockTimeLimit) "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sql += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sql += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sql += "AND tabT.jediTaskID=tabD.jediTaskID "
            sql += "AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sql += f"{mapKey},"
                varMap[mapKey] = tmpType
            sql = sql[:-1]
            sql += ") "
            sql += " AND ((tabT.status=:ts_defined AND tabD.status IN ("
            for tmpStat in JediDatasetSpec.statusToUpdateContents():
                mapKey = ":dsStatus_" + tmpStat
                sql += f"{mapKey},"
                varMap[mapKey] = tmpStat
            sql = sql[:-1]
            sql += ")) OR (tabT.status IN (:ts_running,:ts_scouting,:ts_ready,:ts_defined) "
            sql += "AND tabD.state=:dsState_mutable AND tabD.stateCheckTime<=:checkTimeLimit)) "
            sql += "AND tabT.lockedBy IS NULL AND tabD.lockedBy IS NULL "
            sql += "AND NOT EXISTS "
            sql += f"(SELECT 1 FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sql += f"WHERE {jedi_config.db.schemaJEDI}.JEDI_Datasets.jediTaskID=tabT.jediTaskID "
            sql += "AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sql += f"{mapKey},"
            sql = sql[:-1]
            sql += ") AND status=:dsStatus_pending) "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            tmpLog.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            resList = self.cur.fetchall()
            returnMap = {}
            taskDatasetMap = {}
            nDS = 0
            for res in resList:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(res)
                if datasetSpec.jediTaskID not in returnMap:
                    returnMap[datasetSpec.jediTaskID] = []
                returnMap[datasetSpec.jediTaskID].append(datasetSpec)
                nDS += 1
                if datasetSpec.jediTaskID not in taskDatasetMap:
                    taskDatasetMap[datasetSpec.jediTaskID] = []
                taskDatasetMap[datasetSpec.jediTaskID].append(datasetSpec.datasetID)
            jediTaskIDs = sorted(returnMap.keys())
            # get seq_number
            sqlSEQ = f"SELECT {JediDatasetSpec.columnNames()} "
            sqlSEQ += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlSEQ += "WHERE jediTaskID=:jediTaskID AND datasetName=:datasetName "
            for jediTaskID in jediTaskIDs:
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetName"] = "seq_number"
                self.conn.begin()
                self.cur.execute(sqlSEQ + comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                resSeqList = self.cur.fetchall()
                for resSeq in resSeqList:
                    datasetSpec = JediDatasetSpec()
                    datasetSpec.pack(resSeq)
                    # append if missing
                    if datasetSpec.datasetID not in taskDatasetMap[datasetSpec.jediTaskID]:
                        taskDatasetMap[datasetSpec.jediTaskID].append(datasetSpec.datasetID)
                        returnMap[datasetSpec.jediTaskID].append(datasetSpec)
            returnList = []
            for jediTaskID in jediTaskIDs:
                returnList.append((jediTaskID, returnMap[jediTaskID]))
            tmpLog.debug(f"got {nDS} datasets for {len(jediTaskIDs)} tasks")
            return returnList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # check if item is matched with one of list items
    def isMatched(self, itemName, pattList):
        for tmpName in pattList:
            # normal pattern
            if re.search(tmpName, itemName) is not None or tmpName == itemName:
                return True
        # return
        return False

    # feed files to the JEDI contents table
    def insertFilesForDataset_JEDI(
        self,
        datasetSpec,
        fileMap,
        datasetState,
        stateUpdateTime,
        nEventsPerFile,
        nEventsPerJob,
        maxAttempt,
        firstEventNumber,
        nMaxFiles,
        nMaxEvents,
        useScout,
        givenFileList,
        useFilesWithNewAttemptNr,
        nFilesPerJob,
        nEventsPerRange,
        nChunksForScout,
        includePatt,
        excludePatt,
        xmlConfig,
        noWaitParent,
        parent_tid,
        pid,
        maxFailure,
        useRealNumEvents,
        respectLB,
        tgtNumEventsPerJob,
        skipFilesUsedBy,
        ramCount,
        taskSpec,
        skipShortInput,
        inputPreStaging,
        order_by,
        maxFileRecords,
        skip_short_output,
    ):
        comment = " /* JediDBProxy.insertFilesForDataset_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={datasetSpec.jediTaskID} datasetID={datasetSpec.datasetID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start nEventsPerFile={nEventsPerFile} nEventsPerJob={nEventsPerJob} maxAttempt={maxAttempt} maxFailure={maxFailure}")
        tmpLog.debug(f"firstEventNumber={firstEventNumber} nMaxFiles={nMaxFiles} nMaxEvents={nMaxEvents}")
        tmpLog.debug(f"useFilesWithNewAttemptNr={useFilesWithNewAttemptNr} nFilesPerJob={nFilesPerJob} nEventsPerRange={nEventsPerRange}")
        tmpLog.debug(f"useScout={useScout} nChunksForScout={nChunksForScout} userRealEventNumber={useRealNumEvents}")
        tmpLog.debug(f"includePatt={str(includePatt)} excludePatt={str(excludePatt)}")
        tmpLog.debug(f"xmlConfig={type(xmlConfig)} noWaitParent={noWaitParent} parent_tid={parent_tid}")
        tmpLog.debug(f"len(fileMap)={len(fileMap)} pid={pid}")
        tmpLog.debug(f"datasetState={datasetState} dataset.state={datasetSpec.state}")
        tmpLog.debug(f"respectLB={respectLB} tgtNumEventsPerJob={tgtNumEventsPerJob} skipFilesUsedBy={skipFilesUsedBy} ramCount={ramCount}")
        tmpLog.debug(f"skipShortInput={skipShortInput} skipShortOutput={skip_short_output} inputPreStaging={inputPreStaging} order_by={order_by}")
        # return value for failure
        diagMap = {"errMsg": "", "nChunksForScout": nChunksForScout, "nActivatedPending": 0, "isRunningTask": False}
        failedRet = False, 0, None, diagMap
        harmlessRet = None, 0, None, diagMap
        regStart = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        # mutable
        fake_mutable_for_skip_short_output = False
        if (noWaitParent or inputPreStaging) and datasetState == "mutable":
            isMutableDataset = True
        elif skip_short_output:
            # treat as mutable to skip short output by using the SR mechanism
            isMutableDataset = True
            fake_mutable_for_skip_short_output = True
        else:
            isMutableDataset = False
        tmpLog.debug(f"isMutableDataset={isMutableDataset} (fake={fake_mutable_for_skip_short_output}) respectSplitRule={taskSpec.respectSplitRule()}")
        # event level splitting
        if nEventsPerJob is not None and nFilesPerJob is None:
            isEventSplit = True
        else:
            isEventSplit = False
        try:
            # current date
            timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            # get list of files produced by parent
            if datasetSpec.checkConsistency():
                # sql to get the list
                sqlPPC = "SELECT lfn FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(jedi_config.db.schemaJEDI)
                sqlPPC += "WHERE tabD.jediTaskID=tabC.jediTaskID AND tabD.datasetID=tabC.datasetID "
                sqlPPC += "AND tabD.jediTaskID=:jediTaskID AND tabD.type IN (:type1,:type2) "
                sqlPPC += "AND tabD.datasetName IN (:dsName,:didName) AND tabC.status=:fileStatus "
                varMap = {}
                varMap[":type1"] = "output"
                varMap[":type2"] = "log"
                varMap[":jediTaskID"] = parent_tid
                varMap[":fileStatus"] = "finished"
                varMap[":didName"] = datasetSpec.datasetName
                varMap[":dsName"] = datasetSpec.datasetName.split(":")[-1]
                # begin transaction
                self.conn.begin()
                self.cur.execute(sqlPPC + comment, varMap)
                tmpPPC = self.cur.fetchall()
                producedFileList = set()
                for (tmpLFN,) in tmpPPC:
                    producedFileList.add(tmpLFN)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # check if files are 'finished' in JEDI table
                newFileMap = {}
                for guid, fileVal in fileMap.items():
                    if fileVal["lfn"] in producedFileList:
                        newFileMap[guid] = fileVal
                    else:
                        tmpLog.debug(f"{fileVal['lfn']} skipped since was not properly produced by the parent according to JEDI table")
                fileMap = newFileMap
            # get files used by another task
            usedFilesToSkip = set()
            if skipFilesUsedBy is not None:
                # sql to get the list
                sqlSFU = "SELECT lfn,startEvent,endEvent FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(jedi_config.db.schemaJEDI)
                sqlSFU += "WHERE tabD.jediTaskID=tabC.jediTaskID AND tabD.datasetID=tabC.datasetID "
                sqlSFU += "AND tabD.jediTaskID=:jediTaskID AND tabD.type IN (:type1,:type2) "
                sqlSFU += "AND tabD.datasetName IN (:dsName,:didName) AND tabC.status=:fileStatus "
                for tmpTaskID in str(skipFilesUsedBy).split(","):
                    varMap = {}
                    varMap[":type1"] = "input"
                    varMap[":type2"] = "pseudo_input"
                    varMap[":jediTaskID"] = tmpTaskID
                    varMap[":fileStatus"] = "finished"
                    varMap[":didName"] = datasetSpec.datasetName
                    varMap[":dsName"] = datasetSpec.datasetName.split(":")[-1]
                    try:
                        # begin transaction
                        self.conn.begin()
                        self.cur.execute(sqlSFU + comment, varMap)
                        tmpSFU = self.cur.fetchall()
                        for tmpLFN, tmpStartEvent, tmpEndEvent in tmpSFU:
                            tmpID = f"{tmpLFN}.{tmpStartEvent}.{tmpEndEvent}"
                            usedFilesToSkip.add(tmpID)
                        # commit
                        if not self._commit():
                            raise RuntimeError("Commit error")
                    except Exception:
                        # roll back
                        self._rollback()
                        # error
                        self.dumpErrorMessage(tmpLog)
                        return failedRet
            # include files
            if includePatt != []:
                newFileMap = {}
                for guid, fileVal in fileMap.items():
                    if self.isMatched(fileVal["lfn"], includePatt):
                        newFileMap[guid] = fileVal
                fileMap = newFileMap
            # exclude files
            if excludePatt != []:
                newFileMap = {}
                for guid, fileVal in fileMap.items():
                    if not self.isMatched(fileVal["lfn"], excludePatt):
                        newFileMap[guid] = fileVal
                fileMap = newFileMap
            # file list is given
            givenFileMap = {}
            if givenFileList != []:
                for tmpFileItem in givenFileList:
                    if isinstance(tmpFileItem, dict):
                        tmpLFN = tmpFileItem["lfn"]
                        fileItem = tmpFileItem
                    else:
                        tmpLFN = tmpFileItem
                        fileItem = {"lfn": tmpFileItem}
                    givenFileMap[tmpLFN] = fileItem
                newFileMap = {}
                for guid, fileVal in fileMap.items():
                    if fileVal["lfn"] in givenFileMap:
                        newFileMap[guid] = fileVal
                fileMap = newFileMap
            # XML config
            if xmlConfig is not None:
                try:
                    xmlConfig = ParseJobXML.dom_parser(xmlStr=xmlConfig)
                except Exception:
                    errtype, errvalue = sys.exc_info()[:2]
                    tmpErrStr = f"failed to load XML config with {errtype.__name__}:{errvalue}"
                    raise RuntimeError(tmpErrStr)
                newFileMap = {}
                for guid, fileVal in fileMap.items():
                    if fileVal["lfn"] in xmlConfig.files_in_DS(datasetSpec.datasetName):
                        newFileMap[guid] = fileVal
                fileMap = newFileMap
            # make map with LFN as key
            filelValMap = {}
            for guid, fileVal in fileMap.items():
                filelValMap[fileVal["lfn"]] = (guid, fileVal)
            # make LFN list
            listBoundaryID = []
            if order_by == "eventsAlignment" and nEventsPerJob:
                aligned = []
                unaligned = dict()
                for tmpLFN, (tmpGUID, tmpFileVar) in filelValMap.items():
                    if "events" in tmpFileVar and int(tmpFileVar["events"]) % nEventsPerJob == 0:
                        aligned.append(tmpLFN)
                    else:
                        unaligned[tmpLFN] = int(tmpFileVar["events"])
                aligned.sort()
                unaligned = sorted(unaligned, key=lambda i: unaligned[i], reverse=True)
                lfnList = aligned + unaligned
            elif xmlConfig is None:
                # sort by LFN
                lfnList = sorted(filelValMap.keys())
            else:
                # sort as described in XML
                tmpBoundaryID = 0
                lfnList = []
                for tmpJobXML in xmlConfig.jobs:
                    for tmpLFN in tmpJobXML.files_in_DS(datasetSpec.datasetName):
                        # check if the file is available
                        if tmpLFN not in filelValMap:
                            diagMap["errMsg"] = f"{tmpLFN} is not found in {datasetSpec.datasetName}"
                            tmpLog.error(diagMap["errMsg"])
                            return failedRet
                        lfnList.append(tmpLFN)
                        listBoundaryID.append(tmpBoundaryID)
                    # increment boundaryID
                    tmpBoundaryID += 1
            # truncate if necessary
            if datasetSpec.isSeqNumber():
                offsetVal = 0
            else:
                offsetVal = datasetSpec.getOffset()
            if offsetVal > 0:
                lfnList = lfnList[offsetVal:]
            tmpLog.debug(f"offset={offsetVal}")
            # randomize
            if datasetSpec.isRandom():
                random.shuffle(lfnList)
            # use perRange as perJob
            if nEventsPerJob is None and nEventsPerRange is not None:
                nEventsPerJob = nEventsPerRange
            # make file specs
            fileSpecMap = {}
            uniqueFileKeyList = []
            nRemEvents = nEventsPerJob
            totalEventNumber = firstEventNumber
            foundFileList = []
            uniqueLfnList = {}
            totalNumEventsF = 0
            lumiBlockNr = None
            for tmpIdx, tmpLFN in enumerate(lfnList):
                # collect unique LFN list
                if tmpLFN not in uniqueLfnList:
                    uniqueLfnList[tmpLFN] = None
                # check if enough files
                if nMaxFiles is not None and len(uniqueLfnList) > nMaxFiles:
                    break
                guid, fileVal = filelValMap[tmpLFN]
                fileSpec = JediFileSpec()
                fileSpec.jediTaskID = datasetSpec.jediTaskID
                fileSpec.datasetID = datasetSpec.datasetID
                fileSpec.GUID = guid
                fileSpec.type = datasetSpec.type
                fileSpec.status = "ready"
                fileSpec.proc_status = "ready"
                fileSpec.lfn = fileVal["lfn"]
                fileSpec.scope = fileVal["scope"]
                fileSpec.fsize = fileVal["filesize"]
                fileSpec.checksum = fileVal["checksum"]
                fileSpec.creationDate = timeNow
                fileSpec.attemptNr = 0
                fileSpec.failedAttempt = 0
                fileSpec.maxAttempt = maxAttempt
                fileSpec.maxFailure = maxFailure
                fileSpec.ramCount = ramCount
                tmpNumEvents = None
                if "events" in fileVal:
                    try:
                        tmpNumEvents = int(fileVal["events"])
                    except Exception:
                        pass
                if skipShortInput and tmpNumEvents is not None:
                    # set multiples of nEventsPerJob if actual nevents is small
                    if tmpNumEvents >= nEventsPerFile:
                        fileSpec.nEvents = nEventsPerFile
                    else:
                        fileSpec.nEvents = int(tmpNumEvents // nEventsPerJob) * nEventsPerJob
                        if fileSpec.nEvents == 0:
                            tmpLog.debug(f"skip {fileSpec.lfn} due to nEvents {tmpNumEvents} < nEventsPerJob {nEventsPerJob}")
                            continue
                        else:
                            tmpLog.debug(f"set nEvents to {fileSpec.nEvents} from {tmpNumEvents} for {fileSpec.lfn} to skip short input")
                elif nEventsPerFile is not None:
                    fileSpec.nEvents = nEventsPerFile
                elif "events" in fileVal and fileVal["events"] not in ["None", None]:
                    try:
                        fileSpec.nEvents = int(fileVal["events"])
                    except Exception:
                        fileSpec.nEvents = None
                if "lumiblocknr" in fileVal:
                    try:
                        fileSpec.lumiBlockNr = int(fileVal["lumiblocknr"])
                    except Exception:
                        pass
                # keep track
                if datasetSpec.toKeepTrack():
                    fileSpec.keepTrack = 1
                tmpFileSpecList = []
                if xmlConfig is not None:
                    # splitting with XML
                    fileSpec.boundaryID = listBoundaryID[tmpIdx]
                    tmpFileSpecList.append(fileSpec)
                elif (
                    ((nEventsPerJob is None or nEventsPerJob <= 0) and (tgtNumEventsPerJob is None or tgtNumEventsPerJob <= 0))
                    or fileSpec.nEvents is None
                    or fileSpec.nEvents <= 0
                    or ((nEventsPerFile is None or nEventsPerFile <= 0) and not useRealNumEvents)
                ):
                    if firstEventNumber is not None and nEventsPerFile is not None:
                        fileSpec.firstEvent = totalEventNumber
                        totalEventNumber += fileSpec.nEvents
                    # file-level splitting
                    tmpFileSpecList.append(fileSpec)
                else:
                    # event-level splitting
                    tmpStartEvent = 0
                    # change nEventsPerJob if target number is specified
                    if tgtNumEventsPerJob is not None and tgtNumEventsPerJob > 0:
                        # calcurate to how many chunks the file is split
                        tmpItem = divmod(fileSpec.nEvents, tgtNumEventsPerJob)
                        nSubChunk = tmpItem[0]
                        if tmpItem[1] > 0:
                            nSubChunk += 1
                        if nSubChunk <= 0:
                            nSubChunk = 1
                        # get nEventsPerJob
                        tmpItem = divmod(fileSpec.nEvents, nSubChunk)
                        nEventsPerJob = tmpItem[0]
                        if tmpItem[1] > 0:
                            nEventsPerJob += 1
                        if nEventsPerJob <= 0:
                            nEventsPerJob = 1
                        nRemEvents = nEventsPerJob
                    # LB boundaries
                    if respectLB:
                        if lumiBlockNr is None or lumiBlockNr != fileSpec.lumiBlockNr:
                            lumiBlockNr = fileSpec.lumiBlockNr
                            nRemEvents = nEventsPerJob
                    # make file specs
                    while nRemEvents > 0:
                        splitFileSpec = copy.copy(fileSpec)
                        if tmpStartEvent + nRemEvents >= splitFileSpec.nEvents:
                            splitFileSpec.startEvent = tmpStartEvent
                            splitFileSpec.endEvent = splitFileSpec.nEvents - 1
                            nRemEvents -= splitFileSpec.nEvents - tmpStartEvent
                            if nRemEvents == 0:
                                nRemEvents = nEventsPerJob
                            if firstEventNumber is not None and (nEventsPerFile is not None or useRealNumEvents):
                                splitFileSpec.firstEvent = totalEventNumber
                                totalEventNumber += splitFileSpec.endEvent - splitFileSpec.startEvent + 1
                            tmpFileSpecList.append(splitFileSpec)
                            break
                        else:
                            splitFileSpec.startEvent = tmpStartEvent
                            splitFileSpec.endEvent = tmpStartEvent + nRemEvents - 1
                            tmpStartEvent += nRemEvents
                            nRemEvents = nEventsPerJob
                            if firstEventNumber is not None and (nEventsPerFile is not None or useRealNumEvents):
                                splitFileSpec.firstEvent = totalEventNumber
                                totalEventNumber += splitFileSpec.endEvent - splitFileSpec.startEvent + 1
                            tmpFileSpecList.append(splitFileSpec)
                        if len(tmpFileSpecList) >= maxFileRecords:
                            break
                # append
                for fileSpec in tmpFileSpecList:
                    # check if to skip
                    tmpID = f"{fileSpec.lfn}.{fileSpec.startEvent}.{fileSpec.endEvent}"
                    if tmpID in usedFilesToSkip:
                        continue
                    # append
                    uniqueFileKey = f"{fileSpec.lfn}.{fileSpec.startEvent}.{fileSpec.endEvent}.{fileSpec.boundaryID}"
                    uniqueFileKeyList.append(uniqueFileKey)
                    fileSpecMap[uniqueFileKey] = fileSpec
                # check if number of events is enough
                if fileSpec.nEvents is not None:
                    totalNumEventsF += fileSpec.nEvents
                if nMaxEvents is not None and totalNumEventsF >= nMaxEvents:
                    break
                # too long list
                if len(uniqueFileKeyList) > maxFileRecords:
                    if len(fileMap) > maxFileRecords and nMaxFiles is None:
                        diagMap["errMsg"] = f"Input dataset contains too many files >{maxFileRecords}. Split the dataset or set nFiles properly"
                    elif nEventsPerJob is not None:
                        diagMap[
                            "errMsg"
                        ] = f"SUM(nEventsInEachFile/nEventsPerJob) >{maxFileRecords}. Split the dataset, set nFiles properly, or increase nEventsPerJob"
                    else:
                        diagMap["errMsg"] = f"Too many file record >{maxFileRecords}"
                    tmpLog.error(diagMap["errMsg"])
                    return failedRet
            missingFileList = []
            tmpLog.debug(f"{len(missingFileList)} files missing")
            # sql to check if task is locked
            sqlTL = f"SELECT status,lockedBy FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE NOWAIT "
            # sql to check dataset status
            sqlDs = f"SELECT status,nFilesToBeUsed-nFilesUsed,state,nFilesToBeUsed,nFilesUsed FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlDs += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID FOR UPDATE "
            # sql to get existing files
            sqlCh = "SELECT fileID,lfn,status,startEvent,endEvent,boundaryID,nEvents,lumiBlockNr,attemptNr,maxAttempt,failedAttempt,maxFailure FROM {0}.JEDI_Dataset_Contents ".format(
                jedi_config.db.schemaJEDI
            )
            sqlCh += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID FOR UPDATE "
            # sql to count existing files
            sqlCo = f"SELECT count(*) FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlCo += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql for insert
            sqlIn = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlIn += JediFileSpec.bindValuesExpression(useSeq=False)
            # sql to get fileID
            sqlFID = f"SELECT {jedi_config.db.schemaJEDI}.JEDI_DATASET_CONT_FILEID_SEQ.nextval FROM "
            sqlFID += "(SELECT level FROM dual CONNECT BY level<=:nIDs) "
            # sql to update file status
            sqlFU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents SET status=:status "
            sqlFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # sql to get master status
            sqlMS = f"SELECT status FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlMS += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update dataset
            sqlDU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlDU += "SET status=:status,state=:state,stateCheckTime=:stateUpdateTime,"
            sqlDU += "nFiles=:nFiles,nFilesTobeUsed=:nFilesTobeUsed,nEvents=:nEvents," "nFilesMissing=:nFilesMissing "
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update dataset including nFilesUsed
            sqlDUx = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlDUx += "SET status=:status,state=:state,stateCheckTime=:stateUpdateTime,"
            sqlDUx += "nFiles=:nFiles,nFilesTobeUsed=:nFilesTobeUsed,nEvents=:nEvents," "nFilesUsed=:nFilesUsed,nFilesMissing=:nFilesMissing "
            sqlDUx += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to propagate number of input events to DEFT
            sqlCE = f"UPDATE {jedi_config.db.schemaDEFT}.T_TASK "
            sqlCE += "SET total_input_events=LEAST(9999999999,("
            sqlCE += f"SELECT SUM(nEvents) FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlCE += "WHERE jediTaskID=:jediTaskID AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlCE += f"{mapKey},"
            sqlCE = sqlCE[:-1]
            sqlCE += ") AND masterID IS NULL)) "
            sqlCE += "WHERE taskID=:jediTaskID "
            nInsert = 0
            nReady = 0
            nPending = 0
            nUsed = 0
            nLost = 0
            nStaging = 0
            nFailed = 0
            pendingFID = []
            oldDsStatus = None
            newDsStatus = None
            nActivatedPending = 0
            nEventsToUseEventSplit = 0
            nFilesToUseEventSplit = 0
            nFilesUnprocessed = 0
            nEventsInsert = 0
            nEventsLost = 0
            nEventsExist = 0
            stagingLB = set()
            retVal = None, missingFileList, None, diagMap
            # begin transaction
            self.conn.begin()
            # check task
            try:
                varMap = {}
                varMap[":jediTaskID"] = datasetSpec.jediTaskID
                self.cur.execute(sqlTL + comment, varMap)
                resTask = self.cur.fetchone()
            except Exception:
                errType, errValue = sys.exc_info()[:2]
                if self.isNoWaitException(errValue):
                    # resource busy and acquire with NOWAIT specified
                    tmpLog.debug(f"skip locked jediTaskID={datasetSpec.jediTaskID}")
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    return retVal
                else:
                    # failed with something else
                    raise errType(errValue)
            if resTask is None:
                tmpLog.debug("task not found in Task table")
            else:
                taskStatus, taskLockedBy = resTask
                if taskLockedBy != pid:
                    # task is locked
                    tmpLog.debug(f"task is locked by {taskLockedBy}")
                elif not (
                    taskStatus in JediTaskSpec.statusToUpdateContents()
                    or (
                        taskStatus in ["running", "ready", "scouting", "assigning", "pending"]
                        and taskSpec.oldStatus not in ["defined"]
                        and (datasetState == "mutable" or datasetSpec.state == "mutable" or datasetSpec.isSeqNumber())
                    )
                ):
                    # task status is irrelevant
                    tmpLog.debug(f"task.status={taskStatus} taskSpec.oldStatus={taskSpec.oldStatus} is not for contents update")
                else:
                    tmpLog.debug(f"task.status={taskStatus} task.oldStatus={taskSpec.oldStatus}")
                    # running task
                    if taskStatus in ["running", "assigning", "ready", "scouting", "pending"]:
                        diagMap["isRunningTask"] = True
                    # size of pending input chunk to be activated
                    sizePendingEventChunk = None
                    strSizePendingEventChunk = ""
                    if (set([taskStatus, taskSpec.oldStatus]) & set(["defined", "ready", "scouting", "assigning"])) and useScout:
                        nChunks = nChunksForScout
                        # number of files for scout
                        sizePendingFileChunk = nChunksForScout
                        strSizePendingFileChunk = f"{sizePendingFileChunk}"
                        # number of files per job is specified
                        if nFilesPerJob not in [None, 0]:
                            sizePendingFileChunk *= nFilesPerJob
                            strSizePendingFileChunk = f"{nFilesPerJob}*" + strSizePendingFileChunk
                        strSizePendingFileChunk += " files required for scout"
                        # number of events for scout
                        if isEventSplit:
                            sizePendingEventChunk = nChunksForScout * nEventsPerJob
                            strSizePendingEventChunk = f"{nEventsPerJob}*{nChunksForScout} events required for scout"
                    else:
                        # the number of chunks in one bunch
                        if taskSpec.nChunksToWait() is not None:
                            nChunkInBunch = taskSpec.nChunksToWait()
                        elif taskSpec.noInputPooling():
                            nChunkInBunch = 1
                        else:
                            nChunkInBunch = 20
                        nChunks = nChunkInBunch
                        # number of files to be activated
                        sizePendingFileChunk = nChunkInBunch
                        strSizePendingFileChunk = f"{sizePendingFileChunk}"
                        # number of files per job is specified
                        if nFilesPerJob not in [None, 0]:
                            sizePendingFileChunk *= nFilesPerJob
                            strSizePendingFileChunk = f"{nFilesPerJob}*" + strSizePendingFileChunk
                        strSizePendingFileChunk += " files required"
                        # number of events to be activated
                        if isEventSplit:
                            sizePendingEventChunk = nChunkInBunch * nEventsPerJob
                            strSizePendingEventChunk = f"{nEventsPerJob}*{nChunkInBunch} events required"
                    # check dataset status
                    varMap = {}
                    varMap[":jediTaskID"] = datasetSpec.jediTaskID
                    varMap[":datasetID"] = datasetSpec.datasetID
                    self.cur.execute(sqlDs + comment, varMap)
                    resDs = self.cur.fetchone()
                    if resDs is None:
                        tmpLog.debug("dataset not found in Datasets table")
                    elif resDs[2] != datasetSpec.state:
                        tmpLog.debug(f"dataset.state changed from {datasetSpec.state} to {resDs[2]} in DB")
                    elif not (
                        resDs[0] in JediDatasetSpec.statusToUpdateContents()
                        or (
                            taskStatus in ["running", "assigning", "ready", "scouting", "pending"]
                            and (datasetState == "mutable" or datasetSpec.state == "mutable")
                            or (taskStatus in ["running", "defined", "ready", "scouting", "assigning", "pending"] and datasetSpec.isSeqNumber())
                        )
                    ):
                        tmpLog.debug(f"ds.status={resDs[0]} is not for contents update")
                        oldDsStatus = resDs[0]
                        nFilesUnprocessed = resDs[1]
                        # count existing files
                        if resDs[0] == "ready":
                            varMap = {}
                            varMap[":jediTaskID"] = datasetSpec.jediTaskID
                            varMap[":datasetID"] = datasetSpec.datasetID
                            self.cur.execute(sqlCo + comment, varMap)
                            resCo = self.cur.fetchone()
                            numUniqueLfn = resCo[0]
                            retVal = True, missingFileList, numUniqueLfn, diagMap
                    else:
                        oldDsStatus, nFilesUnprocessed, dsStateInDB, nFilesToUseDS, nFilesUsedInDS = resDs
                        tmpLog.debug(f"ds.state={dsStateInDB} in DB")
                        if not nFilesUsedInDS:
                            nFilesUsedInDS = 0
                        # get existing file list
                        varMap = {}
                        varMap[":jediTaskID"] = datasetSpec.jediTaskID
                        varMap[":datasetID"] = datasetSpec.datasetID
                        self.cur.execute(sqlCh + comment, varMap)
                        tmpRes = self.cur.fetchall()
                        tmpLog.debug(f"{len(tmpRes)} file records in DB")
                        existingFiles = {}
                        statusMap = {}
                        for (
                            fileID,
                            lfn,
                            status,
                            startEvent,
                            endEvent,
                            boundaryID,
                            nEventsInDS,
                            lumiBlockNr,
                            attemptNr,
                            maxAttempt,
                            failedAttempt,
                            maxFailure,
                        ) in tmpRes:
                            statusMap.setdefault(status, 0)
                            statusMap[status] += 1
                            uniqueFileKey = f"{lfn}.{startEvent}.{endEvent}.{boundaryID}"
                            existingFiles[uniqueFileKey] = {"fileID": fileID, "status": status}
                            if startEvent is not None and endEvent is not None:
                                existingFiles[uniqueFileKey]["nevents"] = endEvent - startEvent + 1
                            elif nEventsInDS is not None:
                                existingFiles[uniqueFileKey]["nevents"] = nEventsInDS
                            else:
                                existingFiles[uniqueFileKey]["nevents"] = None
                            existingFiles[uniqueFileKey]["is_failed"] = False
                            lostFlag = False
                            if status == "ready":
                                if (maxAttempt is not None and attemptNr is not None and attemptNr >= maxAttempt) or (
                                    failedAttempt is not None and maxFailure is not None and failedAttempt >= maxFailure
                                ):
                                    nUsed += 1
                                    existingFiles[uniqueFileKey]["is_failed"] = True
                                    nFailed += 1
                                else:
                                    nReady += 1
                            elif status == "pending":
                                nPending += 1
                                pendingFID.append(fileID)
                                # count number of events for scouts with event-level splitting
                                if isEventSplit:
                                    try:
                                        if nEventsToUseEventSplit < sizePendingEventChunk:
                                            nEventsToUseEventSplit += endEvent - startEvent + 1
                                            nFilesToUseEventSplit += 1
                                    except Exception:
                                        pass
                            elif status == "staging":
                                nStaging += 1
                                stagingLB.add(lumiBlockNr)
                            elif status not in ["lost", "missing"]:
                                nUsed += 1
                            elif status in ["lost", "missing"]:
                                nLost += 1
                                lostFlag = True
                            if existingFiles[uniqueFileKey]["nevents"] is not None:
                                if lostFlag:
                                    nEventsLost += existingFiles[uniqueFileKey]["nevents"]
                                else:
                                    nEventsExist += existingFiles[uniqueFileKey]["nevents"]
                        tmStr = "inDB nReady={} nPending={} nUsed={} nUsedInDB={} nLost={} nStaging={} nFailed={}"
                        tmpLog.debug(tmStr.format(nReady, nPending, nUsed, nFilesUsedInDS, nLost, nStaging, nFailed))
                        tmpLog.debug(f"inDB {str(statusMap)}")
                        # insert files
                        uniqueLfnList = {}
                        totalNumEventsF = 0
                        totalNumEventsE = 0
                        escapeNextFile = False
                        numUniqueLfn = 0
                        fileSpecsForInsert = []
                        for uniqueFileKey in uniqueFileKeyList:
                            fileSpec = fileSpecMap[uniqueFileKey]
                            # count number of files
                            if fileSpec.lfn not in uniqueLfnList:
                                # the limit is reached at the previous file
                                if escapeNextFile:
                                    break
                                uniqueLfnList[fileSpec.lfn] = None
                                # maximum number of files to be processed
                                if nMaxFiles is not None and len(uniqueLfnList) > nMaxFiles:
                                    break
                                # counts number of events for non event-level splitting
                                if fileSpec.nEvents is not None:
                                    totalNumEventsF += fileSpec.nEvents
                                    # maximum number of events to be processed
                                    if nMaxEvents is not None and totalNumEventsF >= nMaxEvents:
                                        escapeNextFile = True
                                # count number of unique LFNs
                                numUniqueLfn += 1
                            # count number of events for event-level splitting
                            if fileSpec.startEvent is not None and fileSpec.endEvent is not None:
                                totalNumEventsE += fileSpec.endEvent - fileSpec.startEvent + 1
                                if nMaxEvents is not None and totalNumEventsE > nMaxEvents:
                                    break
                            # avoid duplication
                            if uniqueFileKey in existingFiles:
                                continue
                            if inputPreStaging:
                                # go to staging
                                fileSpec.status = "staging"
                                nStaging += 1
                                stagingLB.add(fileSpec.lumiBlockNr)
                            elif isMutableDataset:
                                # go pending if no wait
                                fileSpec.status = "pending"
                                nPending += 1
                            nInsert += 1
                            if fileSpec.startEvent is not None and fileSpec.endEvent is not None:
                                nEventsInsert += fileSpec.endEvent - fileSpec.startEvent + 1
                            elif fileSpec.nEvents is not None:
                                nEventsInsert += fileSpec.nEvents
                            # count number of events for scouts with event-level splitting
                            if isEventSplit:
                                try:
                                    if nEventsToUseEventSplit < sizePendingEventChunk:
                                        nEventsToUseEventSplit += fileSpec.endEvent - fileSpec.startEvent + 1
                                        nFilesToUseEventSplit += 1
                                except Exception:
                                    pass
                            fileSpecsForInsert.append(fileSpec)
                        # get fileID
                        tmpLog.debug(f"get fileIDs for {nInsert} inputs")
                        newFileIDs = []
                        if nInsert > 0:
                            varMap = {}
                            varMap[":nIDs"] = nInsert
                            self.cur.execute(sqlFID, varMap)
                            resFID = self.cur.fetchall()
                            for (fileID,) in resFID:
                                newFileIDs.append(fileID)
                        if not inputPreStaging and isMutableDataset:
                            pendingFID += newFileIDs
                        # sort fileID
                        tmpLog.debug("sort fileIDs")
                        newFileIDs.sort()
                        # set fileID
                        tmpLog.debug("set fileIDs")
                        varMaps = []
                        for fileID, fileSpec in zip(newFileIDs, fileSpecsForInsert):
                            fileSpec.fileID = fileID
                            # make vars
                            varMap = fileSpec.valuesMap()
                            varMaps.append(varMap)
                        # bulk insert
                        tmpLog.debug(f"bulk insert {len(varMaps)} files")
                        self.cur.executemany(sqlIn + comment, varMaps)
                        # keep original pendingFID
                        orig_pendingFID = set(pendingFID)
                        # respect split rule
                        enoughPendingWithSL = False
                        numFilesWithSL = 0
                        init_num_files_with_sl = None
                        if datasetSpec.isMaster() and taskSpec.respectSplitRule() and (useScout or isMutableDataset or datasetSpec.state == "mutable"):
                            tmpDatasetSpecMap = {}
                            # read files
                            sqlFR = f"SELECT {JediFileSpec.columnNames()} "
                            sqlFR += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents WHERE "
                            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
                            sqlFR += "ORDER BY lfn, startEvent "
                            varMap = {}
                            varMap[":datasetID"] = datasetSpec.datasetID
                            varMap[":jediTaskID"] = datasetSpec.jediTaskID
                            if isMutableDataset or datasetSpec.state == "mutable":
                                varMap[":status"] = "pending"
                            else:
                                varMap[":status"] = "ready"
                            self.cur.execute(sqlFR + comment, varMap)
                            resFileList = self.cur.fetchall()
                            for resFile in resFileList:
                                # make FileSpec
                                tmpFileSpec = JediFileSpec()
                                tmpFileSpec.pack(resFile)
                                # make a list per LB
                                if taskSpec.releasePerLumiblock():
                                    tmpLumiBlockNr = tmpFileSpec.lumiBlockNr
                                else:
                                    tmpLumiBlockNr = None
                                tmpDatasetSpecMap.setdefault(tmpLumiBlockNr, {"datasetSpec": copy.deepcopy(datasetSpec), "newPandingFID": []})
                                tmpDatasetSpecMap[tmpLumiBlockNr]["newPandingFID"].append(tmpFileSpec.fileID)
                                tmpDatasetSpecMap[tmpLumiBlockNr]["datasetSpec"].addFile(tmpFileSpec)
                            if not isMutableDataset and datasetSpec.state == "mutable":
                                for tmpFileSpec in fileSpecsForInsert:
                                    # make a list per LB
                                    if taskSpec.releasePerLumiblock():
                                        tmpLumiBlockNr = tmpFileSpec.lumiBlockNr
                                    else:
                                        tmpLumiBlockNr = None
                                    tmpDatasetSpecMap.setdefault(tmpLumiBlockNr, {"datasetSpec": copy.deepcopy(datasetSpec), "newPandingFID": []})
                                    tmpDatasetSpecMap[tmpLumiBlockNr]["newPandingFID"].append(tmpFileSpec.fileID)
                                    tmpDatasetSpecMap[tmpLumiBlockNr]["datasetSpec"].addFile(tmpFileSpec)
                            # make sub chunks
                            if fake_mutable_for_skip_short_output:
                                # use # of files as max # of chunks for skip_short_output to activate all files in closed datasets
                                maxNumChunks = max(len(uniqueFileKeyList), 100)
                            elif taskSpec.status == "running":
                                maxNumChunks = 100
                            else:
                                maxNumChunks = 1
                            if taskSpec.useHS06():
                                walltimeGradient = taskSpec.getCpuTime()
                            else:
                                walltimeGradient = None
                            maxWalltime = taskSpec.getMaxWalltime()
                            if maxWalltime is None:
                                maxWalltime = 345600
                            corePower = 10
                            maxSizePerJob = None
                            tmpInputChunk = None
                            newPendingFID = []
                            tmpDatasetSpecMapIdxList = list(tmpDatasetSpecMap.keys())
                            for ii in range(maxNumChunks):
                                tmp_nChunks = 0
                                tmp_enoughPendingWithSL = False
                                tmp_numFilesWithSL = 0
                                while tmp_nChunks < nChunks:
                                    # make input chunk
                                    if tmpInputChunk is None:
                                        if not tmpDatasetSpecMapIdxList:
                                            break
                                        tmpLumiBlockNr = tmpDatasetSpecMapIdxList.pop()
                                        tmpInputChunk = InputChunk(taskSpec)
                                        tmpInputChunk.addMasterDS(tmpDatasetSpecMap[tmpLumiBlockNr]["datasetSpec"])
                                        maxSizePerJob = taskSpec.getMaxSizePerJob()
                                        if maxSizePerJob is not None:
                                            maxSizePerJob += InputChunk.defaultOutputSize
                                            maxSizePerJob += taskSpec.getWorkDiskSize()
                                        else:
                                            if useScout:
                                                maxSizePerJob = InputChunk.maxInputSizeScouts * 1024 * 1024
                                            else:
                                                maxSizePerJob = InputChunk.maxInputSizeAvalanche * 1024 * 1024
                                        tmp_nChunksLB = 0
                                    # get a chunk
                                    tmp_sub_chunk = tmpInputChunk.getSubChunk(
                                        None,
                                        maxNumFiles=taskSpec.getMaxNumFilesPerJob(),
                                        nFilesPerJob=taskSpec.getNumFilesPerJob(),
                                        walltimeGradient=walltimeGradient,
                                        maxWalltime=maxWalltime,
                                        sizeGradients=taskSpec.getOutDiskSize(),
                                        sizeIntercepts=taskSpec.getWorkDiskSize(),
                                        maxSize=maxSizePerJob,
                                        nEventsPerJob=taskSpec.getNumEventsPerJob(),
                                        coreCount=taskSpec.coreCount,
                                        corePower=corePower,
                                        respectLB=taskSpec.respectLumiblock(),
                                        skip_short_output=skip_short_output,
                                    )
                                    tmp_enoughPendingWithSL = tmpInputChunk.checkUnused()
                                    if not tmp_enoughPendingWithSL:
                                        if (
                                            (not isMutableDataset)
                                            or (taskSpec.releasePerLumiblock() and tmpLumiBlockNr not in stagingLB)
                                            or (skip_short_output and tmp_sub_chunk)
                                        ):
                                            tmp_nChunksLB += 1
                                            tmp_nChunks += 1
                                            tmp_numFilesWithSL = tmpInputChunk.getMasterUsedIndex()
                                        if tmp_nChunksLB > 0:
                                            numFilesWithSL += tmp_numFilesWithSL
                                            newPendingFID += tmpDatasetSpecMap[tmpLumiBlockNr]["newPandingFID"][:tmp_numFilesWithSL]
                                        tmpInputChunk = None
                                    else:
                                        tmp_nChunksLB += 1
                                        tmp_nChunks += 1
                                        tmp_numFilesWithSL = tmpInputChunk.getMasterUsedIndex()
                                if init_num_files_with_sl is None:
                                    init_num_files_with_sl = tmp_numFilesWithSL
                                if tmp_enoughPendingWithSL:
                                    enoughPendingWithSL = True
                                else:
                                    # one set of sub chunks is at least available
                                    if ii > 0 or tmp_nChunks >= nChunks:
                                        enoughPendingWithSL = True
                                    # terminate lookup for skip short output
                                    if skip_short_output and datasetState == "closed":
                                        enoughPendingWithSL = True
                                    break
                            if tmpInputChunk:
                                numFilesWithSL += tmpInputChunk.getMasterUsedIndex()
                                newPendingFID += tmpDatasetSpecMap[tmpLumiBlockNr]["newPandingFID"][: tmpInputChunk.getMasterUsedIndex()]
                            pendingFID = newPendingFID
                            tmpLog.debug(
                                ("respecting SR nFiles={0} isEnough={1} " "nFilesPerJob={2} maxSize={3} maxNumChunks={4}").format(
                                    numFilesWithSL, enoughPendingWithSL, taskSpec.getNumFilesPerJob(), maxSizePerJob, maxNumChunks
                                )
                            )
                        if init_num_files_with_sl is None:
                            init_num_files_with_sl = 0
                        # activate pending
                        tmpLog.debug("activate pending")
                        toActivateFID = []
                        if isMutableDataset:
                            if not datasetSpec.isMaster():
                                # activate all files except master dataset
                                toActivateFID = pendingFID
                            elif inputPreStaging and nStaging == 0:
                                # all files are staged
                                toActivateFID = pendingFID
                            else:
                                if datasetSpec.isMaster() and taskSpec.respectSplitRule() and (useScout or isMutableDataset):
                                    # enough pending
                                    if enoughPendingWithSL:
                                        toActivateFID = pendingFID[:numFilesWithSL]
                                    else:
                                        diagMap["errMsg"] = "not enough files"
                                elif isEventSplit:
                                    # enough events are pending
                                    if nEventsToUseEventSplit >= sizePendingEventChunk and nFilesToUseEventSplit > 0:
                                        toActivateFID = pendingFID[: (int(nPending / nFilesToUseEventSplit) * nFilesToUseEventSplit)]
                                    else:
                                        diagMap["errMsg"] = f"{nEventsToUseEventSplit} events ({nPending} files) available, {strSizePendingEventChunk}"
                                else:
                                    # enough files are pending
                                    if nPending >= sizePendingFileChunk and sizePendingFileChunk > 0:
                                        toActivateFID = pendingFID[: (int(nPending / sizePendingFileChunk) * sizePendingFileChunk)]
                                    else:
                                        diagMap["errMsg"] = f"{nPending} files available, {strSizePendingFileChunk}"
                        else:
                            nReady += nInsert
                            toActivateFID = orig_pendingFID
                        tmpLog.debug(f"length of pendingFID {len(orig_pendingFID)} -> {len(toActivateFID)}")
                        for tmpFileID in toActivateFID:
                            if tmpFileID in orig_pendingFID:
                                varMap = {}
                                varMap[":status"] = "ready"
                                varMap[":jediTaskID"] = datasetSpec.jediTaskID
                                varMap[":datasetID"] = datasetSpec.datasetID
                                varMap[":fileID"] = tmpFileID
                                self.cur.execute(sqlFU + comment, varMap)
                                nActivatedPending += 1
                                nReady += 1
                        tmpLog.debug(f"nReady={nReady} nPending={nPending} nActivatedPending={nActivatedPending} after activation")
                        # lost or recovered files
                        tmpLog.debug("lost or recovered files")
                        uniqueFileKeySet = set(uniqueFileKeyList)
                        for uniqueFileKey, fileVarMap in existingFiles.items():
                            varMap = {}
                            varMap[":jediTaskID"] = datasetSpec.jediTaskID
                            varMap[":datasetID"] = datasetSpec.datasetID
                            varMap[":fileID"] = fileVarMap["fileID"]
                            lostInPending = False
                            if uniqueFileKey not in uniqueFileKeySet:
                                if fileVarMap["status"] == "lost":
                                    continue
                                if fileVarMap["status"] not in ["ready", "pending", "staging"]:
                                    continue
                                elif fileVarMap["status"] != "ready":
                                    lostInPending = True
                                varMap["status"] = "lost"
                                tmpLog.debug(f"{uniqueFileKey} was lost from {str(uniqueFileKeySet)}")
                            else:
                                continue
                            if varMap["status"] == "ready":
                                nLost -= 1
                                nReady += 1
                                if fileVarMap["nevents"] is not None:
                                    nEventsExist += fileVarMap["nevents"]
                            if varMap["status"] in ["lost", "missing"]:
                                nLost += 1
                                if not lostInPending:
                                    nReady -= 1
                                if fileVarMap["nevents"] is not None:
                                    nEventsExist -= fileVarMap["nevents"]
                                if fileVarMap["is_failed"]:
                                    nUsed -= 1
                            self.cur.execute(sqlFU + comment, varMap)
                        tmpLog.debug(
                            "nReady={} nLost={} nUsed={} nUsedInDB={} nUsedConsistent={} after lost/recovery check".format(
                                nReady, nLost, nUsed, nFilesUsedInDS, nUsed == nFilesUsedInDS
                            )
                        )
                        # get master status
                        masterStatus = None
                        if not datasetSpec.isMaster():
                            varMap = {}
                            varMap[":jediTaskID"] = datasetSpec.jediTaskID
                            varMap[":datasetID"] = datasetSpec.masterID
                            self.cur.execute(sqlMS + comment, varMap)
                            resMS = self.cur.fetchone()
                            (masterStatus,) = resMS
                        tmpLog.debug(f"masterStatus={masterStatus}")
                        tmpLog.debug(f"nFilesToUseDS={nFilesToUseDS}")
                        if nFilesToUseDS is None:
                            nFilesToUseDS = 0
                        # updata dataset
                        varMap = {}
                        varMap[":jediTaskID"] = datasetSpec.jediTaskID
                        varMap[":datasetID"] = datasetSpec.datasetID
                        varMap[":nFiles"] = nInsert + len(existingFiles) - nLost
                        if skip_short_output:
                            # remove pending files to avoid wrong task transition due to nFiles>nFilesTobeUsed
                            varMap[":nFiles"] -= nPending - nActivatedPending
                        varMap[":nEvents"] = nEventsInsert + nEventsExist
                        varMap[":nFilesMissing"] = nLost
                        if datasetSpec.isMaster() and taskSpec.respectSplitRule() and useScout:
                            if set([taskStatus, taskSpec.oldStatus]) & set(["scouting", "ready", "assigning"]):
                                varMap[":nFilesTobeUsed"] = nFilesToUseDS
                            else:
                                if fake_mutable_for_skip_short_output:
                                    # use num_files_with_sl in the first iteration since numFilesWithSL is too big for scouts
                                    varMap[":nFilesTobeUsed"] = init_num_files_with_sl + nUsed
                                elif isMutableDataset:
                                    varMap[":nFilesTobeUsed"] = nReady + nUsed
                                else:
                                    varMap[":nFilesTobeUsed"] = numFilesWithSL + nUsed
                        elif datasetSpec.isMaster() and useScout and (set([taskStatus, taskSpec.oldStatus]) & set(["scouting", "ready", "assigning"])):
                            varMap[":nFilesTobeUsed"] = nFilesToUseDS
                        elif xmlConfig is not None:
                            # disable scout for --loadXML
                            varMap[":nFilesTobeUsed"] = nReady + nUsed
                        elif (
                            (set([taskStatus, taskSpec.oldStatus]) & set(["defined", "ready", "scouting", "assigning"]))
                            and useScout
                            and not isEventSplit
                            and nChunksForScout is not None
                            and nReady > sizePendingFileChunk
                        ):
                            # set a fewer number for scout for file level splitting
                            varMap[":nFilesTobeUsed"] = sizePendingFileChunk
                        elif (
                            [1 for tmpStat in [taskStatus, taskSpec.oldStatus] if tmpStat in ["defined", "ready", "scouting", "assigning"]]
                            and useScout
                            and isEventSplit
                            and nReady > max(nFilesToUseEventSplit, nFilesToUseDS)
                        ):
                            # set a fewer number for scout for event level splitting
                            varMap[":nFilesTobeUsed"] = max(nFilesToUseEventSplit, nFilesToUseDS)
                        else:
                            varMap[":nFilesTobeUsed"] = nReady + nUsed
                        if useScout:
                            if not isEventSplit:
                                # file level splitting
                                if nFilesPerJob in [None, 0]:
                                    # number of files per job is not specified
                                    diagMap["nChunksForScout"] = nChunksForScout - varMap[":nFilesTobeUsed"]
                                else:
                                    tmpQ, tmpR = divmod(varMap[":nFilesTobeUsed"], nFilesPerJob)
                                    diagMap["nChunksForScout"] = nChunksForScout - tmpQ
                                    if tmpR > 0:
                                        diagMap["nChunksForScout"] -= 1
                            else:
                                # event level splitting
                                if varMap[":nFilesTobeUsed"] > 0:
                                    tmpQ, tmpR = divmod(nEventsToUseEventSplit, nEventsPerJob)
                                    diagMap["nChunksForScout"] = nChunksForScout - tmpQ
                                    if tmpR > 0:
                                        diagMap["nChunksForScout"] -= 1
                        if missingFileList != [] or (isMutableDataset and nActivatedPending == 0 and nFilesUnprocessed in [0, None]):
                            if datasetSpec.isMaster() or masterStatus is None:
                                # don't change status when some files are missing or no pending inputs are activated
                                tmpLog.debug(f"using datasetSpec.status={datasetSpec.status}")
                                varMap[":status"] = datasetSpec.status
                            else:
                                # use master status
                                tmpLog.debug(f"using masterStatus={masterStatus}")
                                varMap[":status"] = masterStatus
                        else:
                            varMap[":status"] = "ready"
                        # no more inputs are required even if parent is still running
                        numReqFileRecords = nMaxFiles
                        try:
                            if nEventsPerFile > nEventsPerJob:
                                numReqFileRecords = numReqFileRecords * nEventsPerFile // nEventsPerJob
                        except Exception:
                            pass
                        tmpLog.debug(f"the number of requested file records : {numReqFileRecords}")
                        if isMutableDataset and numReqFileRecords is not None and varMap[":nFilesTobeUsed"] >= numReqFileRecords:
                            varMap[":state"] = "open"
                        elif inputPreStaging and nStaging == 0 and datasetSpec.isMaster() and nPending == nActivatedPending:
                            varMap[":state"] = "closed"
                        else:
                            varMap[":state"] = datasetState
                        varMap[":stateUpdateTime"] = stateUpdateTime
                        newDsStatus = varMap[":status"]
                        if nUsed != nFilesUsedInDS:
                            varMap[":nFilesUsed"] = nUsed
                            tmpLog.debug(sqlDUx + comment + str(varMap))
                            self.cur.execute(sqlDUx + comment, varMap)
                        else:
                            tmpLog.debug(sqlDU + comment + str(varMap))
                            self.cur.execute(sqlDU + comment, varMap)
                        # propagate number of input events to DEFT
                        if datasetSpec.isMaster():
                            varMap = {}
                            varMap[":jediTaskID"] = datasetSpec.jediTaskID
                            for tmpType in JediDatasetSpec.getInputTypes():
                                mapKey = ":type_" + tmpType
                                varMap[mapKey] = tmpType
                            tmpLog.debug(sqlCE + comment + str(varMap))
                            self.cur.execute(sqlCE + comment, varMap)
                        # return number of activated pending inputs
                        diagMap["nActivatedPending"] = nActivatedPending
                        if nFilesUnprocessed not in [0, None]:
                            diagMap["nActivatedPending"] += nFilesUnprocessed
                        # set return value
                        retVal = True, missingFileList, numUniqueLfn, diagMap
            # fix secondary files in staging
            if inputPreStaging and datasetSpec.isSeqNumber():
                self.fix_associated_files_in_staging(datasetSpec.jediTaskID, secondary_id=datasetSpec.datasetID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(
                ("inserted rows={0} with activated={1}, pending={2}, ready={3}, " "unprocessed={4}, staging={5} status={6}->{7}").format(
                    nInsert, nActivatedPending, nPending - nActivatedPending, nReady, nStaging, nFilesUnprocessed, oldDsStatus, newDsStatus
                )
            )
            regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - regStart
            tmpLog.debug("took %s.%03d sec" % (regTime.seconds, regTime.microseconds / 1000))
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            is_fatal, tmp_diag = self.dumpErrorMessage(tmpLog, check_fatal=True)
            regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - regStart
            tmpLog.debug("took %s.%03d sec" % (regTime.seconds, regTime.microseconds / 1000))
            if is_fatal:
                diagMap["errMsg"] = tmp_diag
                return failedRet
            else:
                return harmlessRet

    # get files from the JEDI contents table with jediTaskID and/or datasetID
    def getFilesInDatasetWithID_JEDI(self, jediTaskID, datasetID, nFiles, status):
        comment = " /* JediDBProxy.getFilesInDataset_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} datasetID={datasetID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start nFiles={nFiles} status={status}")
        # return value for failure
        failedRet = False, 0
        if jediTaskID is None and datasetID is None:
            tmpLog.error("either jediTaskID or datasetID is not defined")
            return failedRet
        try:
            # sql
            varMap = {}
            sql = f"SELECT * FROM (SELECT {JediFileSpec.columnNames()} "
            sql += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            useAND = False
            if jediTaskID is not None:
                sql += "jediTaskID=:jediTaskID "
                varMap[":jediTaskID"] = jediTaskID
                useAND = True
            if datasetID is not None:
                if useAND:
                    sql += "AND "
                sql += "datasetID=:datasetID "
                varMap[":datasetID"] = datasetID
                useAND = True
            if status is not None:
                if useAND:
                    sql += "AND "
                sql += "status=:status "
                varMap[":status"] = status
                useAND = True
            sql += " ORDER BY fileID) "
            if nFiles is not None:
                sql += f"WHERE rownum <= {nFiles}"
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            # get existing file list
            self.cur.execute(sql + comment, varMap)
            tmpResList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # make file specs
            fileSpecList = []
            for tmpRes in tmpResList:
                fileSpec = JediFileSpec()
                fileSpec.pack(tmpRes)
                fileSpecList.append(fileSpec)
            tmpLog.debug(f"got {len(fileSpecList)} files")
            return True, fileSpecList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # insert dataset to the JEDI datasets table
    def insertDataset_JEDI(self, datasetSpec):
        comment = " /* JediDBProxy.insertDataset_JEDI */"
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # set attributes
            timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            datasetSpec.creationTime = timeNow
            datasetSpec.modificationTime = timeNow
            # sql
            sql = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Datasets ({JediDatasetSpec.columnNames()}) "
            sql += JediDatasetSpec.bindValuesExpression()
            sql += " RETURNING datasetID INTO :newDatasetID"
            varMap = datasetSpec.valuesMap(useSeq=True)
            varMap[":newDatasetID"] = self.cur.var(varNUMBER)
            # begin transaction
            self.conn.begin()
            # insert dataset
            self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
            return True, int(val)
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, None

    # update JEDI dataset
    def updateDataset_JEDI(self, datasetSpec, criteria, lockTask):
        comment = " /* JediDBProxy.updateDataset_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <datasetID={datasetSpec.datasetID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # return value for failure
        failedRet = False, 0
        # no criteria
        if criteria == {}:
            tmpLog.error("no selection criteria")
            return failedRet
        # check criteria
        for tmpKey in criteria.keys():
            if not hasattr(datasetSpec, tmpKey):
                tmpLog.error(f"unknown attribute {tmpKey} is used in criteria")
                return failedRet
        try:
            # set attributes
            timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            datasetSpec.modificationTime = timeNow
            # values for UPDATE
            varMap = datasetSpec.valuesMap(useSeq=False, onlyChanged=True)
            # sql for update
            sql = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET {datasetSpec.bindUpdateChangesExpression()} WHERE "
            useAND = False
            for tmpKey, tmpVal in criteria.items():
                crKey = f":cr_{tmpKey}"
                if useAND:
                    sql += " AND"
                else:
                    useAND = True
                sql += f" {tmpKey}={crKey}"
                varMap[crKey] = tmpVal

            # sql for loc
            varMapLock = {}
            varMapLock[":jediTaskID"] = datasetSpec.jediTaskID
            sqlLock = f"SELECT 1 FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE"
            # begin transaction
            self.conn.begin()
            # lock task
            if lockTask:
                self.cur.execute(sqlLock + comment, varMapLock)
            # update dataset
            tmpLog.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            # the number of updated rows
            nRows = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"updated {nRows} rows")
            return True, nRows
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # update JEDI dataset attributes
    def updateDatasetAttributes_JEDI(self, jediTaskID, datasetID, attributes):
        comment = " /* JediDBProxy.updateDatasetAttributes_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} datasetID={datasetID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # return value for failure
        failedRet = False
        try:
            # sql for update
            sql = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET "
            # values for UPDATE
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            for tmpKey, tmpVal in attributes.items():
                crKey = f":{tmpKey}"
                sql += f"{tmpKey}={crKey},"
                varMap[crKey] = tmpVal
            sql = sql[:-1]
            sql += " "
            sql += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # begin transaction
            self.conn.begin()
            # update dataset
            tmpLog.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            # the number of updated rows
            nRows = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"updated {nRows} rows")
            return True, nRows
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # get JEDI dataset attributes
    def getDatasetAttributes_JEDI(self, jediTaskID, datasetID, attributes):
        comment = " /* JediDBProxy.getDatasetAttributes_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} datasetID={datasetID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # return value for failure
        failedRet = {}
        try:
            # sql for get attributes
            sql = "SELECT "
            for tmpKey in attributes:
                sql += f"{tmpKey},"
            sql = sql[:-1] + " "
            sql += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sql += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # values for UPDATE
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            # begin transaction
            self.conn.begin()
            # select
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # make return
            retMap = {}
            if res is not None:
                for tmpIdx, tmpKey in enumerate(attributes):
                    retMap[tmpKey] = res[tmpIdx]
            tmpLog.debug(f"got {str(retMap)}")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # get JEDI dataset attributes with map
    def getDatasetAttributesWithMap_JEDI(self, jediTaskID, criteria, attributes):
        comment = " /* JediDBProxy.getDatasetAttributesWithMap_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} criteria={str(criteria)}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # return value for failure
        failedRet = {}
        try:
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # sql for get attributes
            sql = "SELECT "
            for tmpKey in attributes:
                sql += f"{tmpKey},"
            sql = sql[:-1] + " "
            sql += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sql += "WHERE jediTaskID=:jediTaskID "
            for crKey, crVal in criteria.items():
                sql += "AND {0}=:{0} ".format(crKey)
                varMap[f":{crKey}"] = crVal
            # begin transaction
            self.conn.begin()
            # select
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # make return
            retMap = {}
            if res is not None:
                for tmpIdx, tmpKey in enumerate(attributes):
                    retMap[tmpKey] = res[tmpIdx]
            tmpLog.debug(f"got {str(retMap)}")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # get JEDI dataset with datasetID
    def getDatasetWithID_JEDI(self, jediTaskID, datasetID):
        comment = " /* JediDBProxy.getDatasetWithID_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} datasetID={datasetID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # return value for failure
        failedRet = False, None
        try:
            # sql
            sql = f"SELECT {JediDatasetSpec.columnNames()} "
            sql += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            # begin transaction
            self.conn.begin()
            # select
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if res is not None:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(res)
            else:
                datasetSpec = None
            tmpLog.debug("done")
            return True, datasetSpec
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # get JEDI datasets with jediTaskID
    def getDatasetsWithJediTaskID_JEDI(self, jediTaskID, datasetTypes=None):
        comment = " /* JediDBProxy.getDatasetsWithJediTaskID_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} datasetTypes={datasetTypes}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # return value for failure
        failedRet = False, None
        try:
            # sql
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sql = f"SELECT {JediDatasetSpec.columnNames()} "
            sql += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID "
            if datasetTypes is not None:
                sql += "AND type IN ("
                for tmpType in datasetTypes:
                    mapKey = ":type_" + tmpType
                    varMap[mapKey] = tmpType
                    sql += f"{mapKey},"
                sql = sql[:-1]
                sql += ") "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # select
            self.cur.execute(sql + comment, varMap)
            tmpResList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # make file specs
            datasetSpecList = []
            for tmpRes in tmpResList:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(tmpRes)
                datasetSpecList.append(datasetSpec)
            tmpLog.debug(f"done with {len(datasetSpecList)} datasets")
            return True, datasetSpecList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # insert task to the JEDI task table
    def insertTask_JEDI(self, taskSpec):
        comment = " /* JediDBProxy.insertTask_JEDI */"
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # set attributes
            timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            taskSpec.creationDate = timeNow
            taskSpec.modificationTime = timeNow
            # sql
            sql = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Tasks ({JediTaskSpec.columnNames()}) "
            sql += JediTaskSpec.bindValuesExpression()
            varMap = taskSpec.valuesMap()
            # begin transaction
            self.conn.begin()
            # insert dataset
            self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # update JEDI task status by ContentsFeeder
    def updateTaskStatusByContFeeder_JEDI(self, jediTaskID, taskSpec=None, getTaskStatus=False, pid=None, setFrozenTime=True, useWorldCloud=False):
        comment = " /* JediDBProxy.updateTaskStatusByContFeeder_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to check status
            sqlS = f"SELECT status,lockedBy,cloud,prodSourceLabel,frozenTime,nucleus FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlS += "WHERE jediTaskID=:jediTaskID FOR UPDATE "
            # sql to get number of unassigned datasets
            sqlD = f"SELECT COUNT(*) FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlD += "WHERE jediTaskID=:jediTaskID AND destination IS NULL AND type IN (:type1,:type2) "
            # sql to update task
            sqlU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlU += "SET status=:status,modificationTime=:updateTime,stateChangeTime=CURRENT_DATE,"
            sqlU += "lockedBy=NULL,lockedTime=NULL,frozenTime=:frozenTime"
            if taskSpec is not None:
                sqlU += ",oldStatus=:oldStatus,errorDialog=:errorDialog,splitRule=:splitRule"
            sqlU += " WHERE jediTaskID=:jediTaskID "
            # sql to unlock task
            sqlL = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlL += "SET lockedBy=NULL,lockedTime=NULL "
            sqlL += "WHERE jediTaskID=:jediTaskID AND status=:status "
            if pid is not None:
                sqlL += "AND lockedBy=:pid "
            # begin transaction
            self.conn.begin()
            # check status
            taskStatus = None
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            tmpLog.debug(sqlS + comment + str(varMap))
            self.cur.execute(sqlS + comment, varMap)
            res = self.cur.fetchone()
            if res is None:
                tmpLog.debug("task is not found in Tasks table")
            else:
                taskStatus, lockedBy, cloudName, prodSourceLabel, frozenTime, nucleus = res
                if lockedBy != pid:
                    # task is locked
                    tmpLog.debug(f"task is locked by {lockedBy}")
                elif taskStatus not in JediTaskSpec.statusToUpdateContents():
                    # task status is irrelevant
                    tmpLog.debug(f"task.status={taskStatus} is not for contents update")
                    # unlock
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":status"] = taskStatus
                    if pid is not None:
                        varMap[":pid"] = pid
                    self.cur.execute(sqlL + comment, varMap)
                    tmpLog.debug("unlocked")
                else:
                    # get number of unassigned datasets
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":type1"] = "output"
                    varMap[":type2"] = "log"
                    self.cur.execute(sqlD + comment, varMap)
                    (nUnassignedDSs,) = self.cur.fetchone()
                    # update task
                    timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":updateTime"] = timeNow
                    if taskSpec is not None:
                        # new task status is specified
                        varMap[":status"] = taskSpec.status
                        varMap[":oldStatus"] = taskSpec.oldStatus
                        varMap[":errorDialog"] = taskSpec.errorDialog
                        varMap[":splitRule"] = taskSpec.splitRule
                        # set/unset frozen time
                        if taskSpec.status == "pending" and setFrozenTime:
                            if frozenTime is None:
                                varMap[":frozenTime"] = timeNow
                            else:
                                varMap[":frozenTime"] = frozenTime
                        else:
                            varMap[":frozenTime"] = None
                    elif (cloudName is None or (useWorldCloud and (nUnassignedDSs > 0 or nucleus in ["", None]))) and prodSourceLabel in ["managed", "test"]:
                        # set assigning for TaskBrokerage
                        varMap[":status"] = "assigning"
                        varMap[":frozenTime"] = timeNow
                        # set old update time to trigger TaskBrokerage immediately
                        varMap[":updateTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=6)
                    else:
                        # skip task brokerage since cloud is preassigned
                        varMap[":status"] = "ready"
                        varMap[":frozenTime"] = None
                        # set old update time to trigger JG immediately
                        varMap[":updateTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=6)
                    tmpLog.debug(sqlU + comment + str(varMap))
                    self.cur.execute(sqlU + comment, varMap)
                    # update DEFT task status
                    taskStatus = varMap[":status"]
                    if taskStatus in ["broken", "assigning"]:
                        self.setDeftStatus_JEDI(jediTaskID, taskStatus)
                        self.setSuperStatus_JEDI(jediTaskID, taskStatus)
                    # task status logging
                    self.record_task_status_change(jediTaskID)
                    self.push_task_status_message(taskSpec, jediTaskID, taskStatus)
                    tmpLog.debug(f"set to {taskStatus}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if not getTaskStatus:
                return True
            else:
                return True, taskStatus
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            if not getTaskStatus:
                return False
            else:
                return False, None

    # update JEDI task
    def updateTask_JEDI(self, taskSpec, criteria, oldStatus=None, updateDEFT=True, insertUnknown=None, setFrozenTime=True, setOldModTime=False):
        comment = " /* JediDBProxy.updateTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={taskSpec.jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # return value for failure
        failedRet = False, 0
        # no criteria
        if criteria == {}:
            tmpLog.error("no selection criteria")
            return failedRet
        # check criteria
        for tmpKey in criteria.keys():
            if not hasattr(taskSpec, tmpKey):
                tmpLog.error(f"unknown attribute {tmpKey} is used in criteria")
                return failedRet
        try:
            # set attributes
            timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            taskSpec.resetChangedAttr("jediTaskID")
            if setOldModTime:
                taskSpec.modificationTime = timeNow - datetime.timedelta(hours=1)
            else:
                taskSpec.modificationTime = timeNow
            # sql to get old status
            sqlS = f"SELECT status,frozenTime FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sql = "WHERE "
            varMap = {}
            for tmpKey, tmpVal in criteria.items():
                crKey = f":cr_{tmpKey}"
                sql += f"{tmpKey}={crKey} AND "
                varMap[crKey] = tmpVal
            if oldStatus is not None:
                sql += "status IN ("
                for tmpStat in oldStatus:
                    crKey = f":old_{tmpStat}"
                    sql += f"{crKey},"
                    varMap[crKey] = tmpStat
                sql = sql[:-1]
                sql += ") AND "
            sql = sql[:-4]
            # begin transaction
            self.conn.begin()
            # get old status
            frozenTime = None
            statusUpdated = False
            self.cur.execute(sqlS + sql + comment, varMap)
            res = self.cur.fetchone()
            if res is not None:
                statusInDB, frozenTime = res
                if statusInDB != taskSpec.status:
                    taskSpec.stateChangeTime = timeNow
                    statusUpdated = True
            # set/unset frozen time
            if taskSpec.status == "pending" and setFrozenTime:
                if frozenTime is None:
                    taskSpec.frozenTime = timeNow
            elif taskSpec.status == "assigning":
                # keep original frozen time for assigning tasks
                pass
            else:
                if frozenTime is not None:
                    taskSpec.frozenTime = None
            # update task
            sqlU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET {taskSpec.bindUpdateChangesExpression()} "
            for tmpKey, tmpVal in taskSpec.valuesMap(useSeq=False, onlyChanged=True).items():
                varMap[tmpKey] = tmpVal
            tmpLog.debug(sqlU + sql + comment + str(varMap))
            self.cur.execute(sqlU + sql + comment, varMap)
            # the number of updated rows
            nRows = self.cur.rowcount
            # insert unknown datasets
            if nRows > 0 and insertUnknown is not None:
                # sql to check
                sqlUC = f"SELECT datasetID FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
                sqlUC += "WHERE jediTaskID=:jediTaskID AND type=:type AND datasetName=:datasetName "
                # sql to insert dataset
                sqlUI = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Datasets ({JediDatasetSpec.columnNames()}) "
                sqlUI += JediDatasetSpec.bindValuesExpression()
                # loop over all datasets
                for tmpUnknownDataset in insertUnknown:
                    # check if already in DB
                    varMap = {}
                    varMap[":type"] = JediDatasetSpec.getUnknownInputType()
                    varMap[":jediTaskID"] = taskSpec.jediTaskID
                    varMap[":datasetName"] = tmpUnknownDataset
                    self.cur.execute(sqlUC + comment, varMap)
                    resUC = self.cur.fetchone()
                    if resUC is None:
                        # insert dataset
                        datasetSpec = JediDatasetSpec()
                        datasetSpec.jediTaskID = taskSpec.jediTaskID
                        datasetSpec.datasetName = tmpUnknownDataset
                        datasetSpec.creationTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                        datasetSpec.modificationTime = datasetSpec.creationTime
                        datasetSpec.type = JediDatasetSpec.getUnknownInputType()
                        varMap = datasetSpec.valuesMap(useSeq=True)
                        self.cur.execute(sqlUI + comment, varMap)
            # update DEFT
            if nRows > 0:
                if updateDEFT:
                    # count number of finished jobs
                    sqlC = "SELECT count(distinct pandaID) "
                    sqlC += "FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(jedi_config.db.schemaJEDI)
                    sqlC += "WHERE tabD.jediTaskID=tabC.jediTaskID AND tabD.jediTaskID=:jediTaskID "
                    sqlC += "AND tabC.datasetID=tabD.datasetID "
                    sqlC += "AND tabC.status=:status "
                    sqlC += "AND masterID IS NULL AND pandaID IS NOT NULL "
                    varMap = {}
                    varMap[":jediTaskID"] = taskSpec.jediTaskID
                    varMap[":status"] = "finished"
                    self.cur.execute(sqlC + comment, varMap)
                    res = self.cur.fetchone()
                    if res is None:
                        tmpLog.debug("failed to count # of finished jobs when updating DEFT table")
                    else:
                        (nDone,) = res
                        sqlD = f"UPDATE {jedi_config.db.schemaDEFT}.T_TASK "
                        sqlD += "SET status=:status,total_done_jobs=:nDone,timeStamp=CURRENT_DATE "
                        sqlD += "WHERE taskID=:jediTaskID "
                        varMap = {}
                        varMap[":status"] = taskSpec.status
                        varMap[":jediTaskID"] = taskSpec.jediTaskID
                        varMap[":nDone"] = nDone
                        tmpLog.debug(sqlD + comment + str(varMap))
                        self.cur.execute(sqlD + comment, varMap)
                        self.setSuperStatus_JEDI(taskSpec.jediTaskID, taskSpec.status)
                elif taskSpec.status in ["running", "broken", "assigning", "scouting", "aborted", "aborting", "exhausted", "staging"]:
                    # update DEFT task status
                    if taskSpec.status == "scouting":
                        deftStatus = "submitting"
                    else:
                        deftStatus = taskSpec.status
                    sqlD = f"UPDATE {jedi_config.db.schemaDEFT}.T_TASK "
                    sqlD += "SET status=:status,timeStamp=CURRENT_DATE"
                    if taskSpec.status == "scouting":
                        sqlD += ",start_time=CURRENT_DATE"
                    sqlD += " WHERE taskID=:jediTaskID "
                    varMap = {}
                    varMap[":status"] = deftStatus
                    varMap[":jediTaskID"] = taskSpec.jediTaskID
                    tmpLog.debug(sqlD + comment + str(varMap))
                    self.cur.execute(sqlD + comment, varMap)
                    self.setSuperStatus_JEDI(taskSpec.jediTaskID, deftStatus)
                    if taskSpec.status == "running":
                        varMap = {}
                        varMap[":jediTaskID"] = taskSpec.jediTaskID
                        sqlDS = f"UPDATE {jedi_config.db.schemaDEFT}.T_TASK "
                        sqlDS += "SET start_time=timeStamp "
                        sqlDS += "WHERE taskID=:jediTaskID AND start_time IS NULL "
                        tmpLog.debug(sqlDS + comment + str(varMap))
                        self.cur.execute(sqlDS + comment, varMap)
                # status change logging
                if statusUpdated:
                    self.record_task_status_change(taskSpec.jediTaskID)
                    self.push_task_status_message(taskSpec, taskSpec.jediTaskID, taskSpec.status)
                    # task attempt end log
                    if taskSpec.status in ["done", "finished", "failed", "broken", "aborted", "exhausted"]:
                        self.log_task_attempt_end(taskSpec.jediTaskID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"updated {nRows} rows")
            return True, nRows
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # update JEDI task lock
    def updateTaskLock_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.updateTaskLock_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # return value for failure
        failedRet = False
        try:
            # sql to update lock
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sqlS = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlS += "SET lockedTime=CURRENT_DATE "
            sqlS += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # get old status
            self.cur.execute(sqlS + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # get JEDI task with ID
    def getTaskWithID_JEDI(self, jediTaskID, fullFlag, lockTask=False, pid=None, lockInterval=None, clearError=False):
        comment = " /* JediDBProxy.getTaskWithID_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start lockTask={lockTask}")
        # return value for failure
        failedRet = False, None
        try:
            # sql
            sql = f"SELECT {JediTaskSpec.columnNames()} "
            sql += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            if lockInterval is not None:
                sql += "AND (lockedTime IS NULL OR lockedTime<:timeLimit) "
            if lockTask:
                sql += "AND lockedBy IS NULL FOR UPDATE NOWAIT"
            sqlLock = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE"
            if clearError:
                sqlLock += ",errorDialog=NULL"
            sqlLock += " WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            if lockInterval is not None:
                varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=lockInterval)
            # begin transaction
            self.conn.begin()
            # select
            res = None
            try:
                self.cur.execute(sql + comment, varMap)
                res = self.cur.fetchone()
                if res is not None:
                    # template to generate job parameters
                    jobParamsTemplate = None
                    if fullFlag:
                        # sql to read template
                        sqlJobP = f"SELECT jobParamsTemplate FROM {jedi_config.db.schemaJEDI}.JEDI_JobParams_Template "
                        sqlJobP += "WHERE jediTaskID=:jediTaskID "
                        self.cur.execute(sqlJobP + comment, varMap)
                        for (clobJobP,) in self.cur:
                            if clobJobP is not None:
                                jobParamsTemplate = clobJobP
                                break
                    if lockTask:
                        varMap = {}
                        varMap[":lockedBy"] = pid
                        varMap[":jediTaskID"] = jediTaskID
                        self.cur.execute(sqlLock + comment, varMap)
            except Exception:
                errType, errValue = sys.exc_info()[:2]
                if self.isNoWaitException(errValue):
                    # resource busy and acquire with NOWAIT specified
                    tmpLog.debug("skip locked")
                else:
                    # failed with something else
                    raise errType(errValue)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if res is not None:
                taskSpec = JediTaskSpec()
                taskSpec.pack(res)
                if jobParamsTemplate is not None:
                    taskSpec.jobParamsTemplate = jobParamsTemplate
            else:
                taskSpec = None
            if taskSpec is None:
                tmpLog.debug("done with skip")
            else:
                tmpLog.debug("done with got")
            return True, taskSpec
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # get JEDI task and datasets with ID and lock it
    def getTaskDatasetsWithID_JEDI(self, jediTaskID, pid, lockTask=True):
        comment = " /* JediDBProxy.getTaskDatasetsWithID_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start pid={pid}")
        # return value for failure
        failedRet = False, None
        try:
            # sql
            sql = f"SELECT {JediTaskSpec.columnNames()} "
            sql += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            if lockTask:
                sql += "AND lockedBy IS NULL FOR UPDATE NOWAIT"
            sqlLK = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE "
            sqlLK += "WHERE jediTaskID=:jediTaskID "
            sqlDS = f"SELECT {JediDatasetSpec.columnNames()} "
            sqlDS += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # select
            res = None
            try:
                # read task
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sql + comment, varMap)
                res = self.cur.fetchone()
                if res is None:
                    taskSpec = None
                else:
                    taskSpec = JediTaskSpec()
                    taskSpec.pack(res)
                    # lock task
                    if lockTask:
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        self.cur.execute(sqlLK + comment, varMap)
                    # read datasets
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    self.cur.execute(sqlDS + comment, varMap)
                    resList = self.cur.fetchall()
                    for res in resList:
                        datasetSpec = JediDatasetSpec()
                        datasetSpec.pack(res)
                        taskSpec.datasetSpecList.append(datasetSpec)
            except Exception:
                errType, errValue = sys.exc_info()[:2]
                if self.isNoWaitException(errValue):
                    # resource busy and acquire with NOWAIT specified
                    tmpLog.debug("skip locked")
                else:
                    # failed with something else
                    raise errType(errValue)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if taskSpec is None:
                tmpLog.debug("done with None")
            else:
                tmpLog.debug("done with OK")
            return True, taskSpec
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # get JEDI tasks with selection criteria
    def getTaskIDsWithCriteria_JEDI(self, criteria, nTasks=50):
        comment = " /* JediDBProxy.getTaskIDsWithCriteria_JEDI */"
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # return value for failure
        failedRet = None
        # no criteria
        if criteria == {}:
            tmpLog.error("no selection criteria")
            return failedRet
        # check criteria
        for tmpKey in criteria.keys():
            if tmpKey not in JediTaskSpec.attributes:
                tmpLog.error(f"unknown attribute {tmpKey} is used in criteria")
                return failedRet
        varMap = {}
        try:
            # sql
            sql = "SELECT jediTaskID "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            isFirst = True
            for tmpKey, tmpVal in criteria.items():
                if not isFirst:
                    sql += "AND "
                else:
                    isFirst = False
                if tmpVal in ["NULL", "NOT NULL"]:
                    sql += f"{tmpKey} IS {tmpVal} "
                elif tmpVal is None:
                    sql += f"{tmpKey} IS NULL "
                else:
                    crKey = f":cr_{tmpKey}"
                    sql += f"{tmpKey}={crKey} "
                    varMap[crKey] = tmpVal
            sql += f"AND rownum<={nTasks}"
            # begin transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10000
            tmpLog.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # collect jediTaskIDs
            retTaskIDs = []
            for (jediTaskID,) in resList:
                retTaskIDs.append(jediTaskID)
            retTaskIDs.sort()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"got {len(retTaskIDs)} tasks")
            return retTaskIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # get JEDI tasks to be finished
    def getTasksToBeFinished_JEDI(self, vo, prodSourceLabel, pid, nTasks=50, target_tasks=None):
        comment = " /* JediDBProxy.getTasksToBeFinished_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel} pid={pid}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # return value for failure
        failedRet = None
        try:
            # sql
            varMap = {}
            varMap[":status1"] = "prepared"
            varMap[":status2"] = "scouted"
            varMap[":status3"] = "tobroken"
            varMap[":status4"] = "toabort"
            varMap[":status5"] = "passed"
            sqlRT = "SELECT tabT.jediTaskID,tabT.status,tabT.eventService,tabT.site,tabT.useJumbo,tabT.splitRule "
            sqlRT += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlRT += "WHERE tabT.status=tabA.status "
            or_taskids_sql = ""
            if target_tasks:
                taskids_params_key_list = []
                for tmpTaskIdx, tmpTaskID in enumerate(target_tasks):
                    tmpKey = f":jediTaskID{tmpTaskIdx}"
                    taskids_params_key_list.append(tmpKey)
                    varMap[tmpKey] = tmpTaskID
                taskids_params_key_str = ",".join(taskids_params_key_list)
                or_taskids_sql = f"OR tabT.jediTaskID IN ({taskids_params_key_str})"
            sqlRT += f"AND (tabT.jediTaskID>=tabA.min_jediTaskID {or_taskids_sql}) "
            sqlRT += "AND tabT.status IN (:status1,:status2,:status3,:status4,:status5) "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlRT += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlRT += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sqlRT += "AND (lockedBy IS NULL OR lockedTime<:timeLimit) "
            sqlRT += f"AND rownum<{nTasks} "
            sqlNW = f"SELECT jediTaskID FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlNW += "WHERE jediTaskID=:jediTaskID FOR UPDATE NOWAIT"
            sqlLK = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE "
            sqlLK += "WHERE jediTaskID=:jediTaskID AND (lockedBy IS NULL OR lockedTime<:timeLimit) AND status=:status "
            sqlTS = f"SELECT {JediTaskSpec.columnNames()} "
            sqlTS += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTS += "WHERE jediTaskID=:jediTaskID "
            sqlDS = f"SELECT {JediDatasetSpec.columnNames()} "
            sqlDS += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID "
            sqlSC = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET status=:newStatus,modificationTime=:updateTime,stateChangeTime=CURRENT_DATE "
            sqlSC += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=10)
            varMap[":timeLimit"] = timeLimit
            tmpLog.debug(sqlRT + comment + str(varMap))
            self.cur.execute(sqlRT + comment, varMap)
            resList = self.cur.fetchall()
            retTasks = []
            allTasks = []
            taskStatList = []
            for jediTaskID, taskStatus, eventService, site, useJumbo, splitRule in resList:
                taskStatList.append((jediTaskID, taskStatus, eventService, site, useJumbo, splitRule))
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # get tasks and datasets
            for jediTaskID, taskStatus, eventService, site, useJumbo, splitRule in taskStatList:
                # begin transaction
                self.conn.begin()
                # check task
                try:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    self.cur.execute(sqlNW + comment, varMap)
                except Exception:
                    tmpLog.debug(f"skip locked jediTaskID={jediTaskID}")
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    continue
                # special action for scouted
                if taskStatus == "scouted":
                    # make avalanche
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":newStatus"] = "running"
                    varMap[":oldStatus"] = taskStatus
                    # set old update time to trigger JG immediately
                    varMap[":updateTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=6)
                    self.cur.execute(sqlSC + comment, varMap)
                    nRows = self.cur.rowcount
                    tmpLog.debug(f"changed status to {varMap[':newStatus']} for jediTaskID={jediTaskID} with {nRows}")
                    if nRows > 0:
                        self.setSuperStatus_JEDI(jediTaskID, "running")
                        self.record_task_status_change(jediTaskID)
                        self.push_task_status_message(None, jediTaskID, varMap[":newStatus"], splitRule)
                        # enable jumbo
                        self.enableJumboInTask_JEDI(jediTaskID, eventService, site, useJumbo, splitRule)
                else:
                    # lock task
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":lockedBy"] = pid
                    varMap[":status"] = taskStatus
                    varMap[":timeLimit"] = timeLimit
                    self.cur.execute(sqlLK + comment, varMap)
                    nRows = self.cur.rowcount
                    if nRows == 1:
                        # read task
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        self.cur.execute(sqlTS + comment, varMap)
                        resTS = self.cur.fetchone()
                        if resTS is not None:
                            taskSpec = JediTaskSpec()
                            taskSpec.pack(resTS)
                            retTasks.append(taskSpec)
                            # read datasets
                            varMap = {}
                            varMap[":jediTaskID"] = taskSpec.jediTaskID
                            self.cur.execute(sqlDS + comment, varMap)
                            resList = self.cur.fetchall()
                            for resDS in resList:
                                datasetSpec = JediDatasetSpec()
                                datasetSpec.pack(resDS)
                                taskSpec.datasetSpecList.append(datasetSpec)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmpLog.debug(f"got {len(retTasks)} tasks")
            return retTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # get job statistics with work queue
    def getJobStatisticsWithWorkQueue_JEDI(self, vo, prodSourceLabel, minPriority=None, cloud=None):
        comment = " /* DBProxy.getJobStatisticsWithWorkQueue_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel} cloud={cloud}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start minPriority={minPriority}")
        sql0 = "SELECT computingSite,cloud,jobStatus,workQueue_ID,COUNT(*) FROM %s "
        sql0 += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
        if cloud is not None:
            sql0 += "AND cloud=:cloud "
        tmpPrioMap = {}
        if minPriority is not None:
            sql0 += "AND currentPriority>=:minPriority "
            tmpPrioMap[":minPriority"] = minPriority
        sql0 += "GROUP BY computingSite,cloud,prodSourceLabel,jobStatus,workQueue_ID "
        sqlMV = sql0
        sqlMV = re.sub("COUNT\(\*\)", "SUM(num_of_jobs)", sqlMV)
        sqlMV = re.sub("SELECT ", "SELECT /*+ RESULT_CACHE */ ", sqlMV)
        tables = [f"{jedi_config.db.schemaPANDA}.jobsActive4", f"{jedi_config.db.schemaPANDA}.jobsDefined4"]
        if minPriority is not None:
            # read the number of running jobs with prio<=MIN
            tables.append(f"{jedi_config.db.schemaPANDA}.jobsActive4")
            sqlMVforRun = re.sub("currentPriority>=", "currentPriority<=", sqlMV)
        varMap = {}
        varMap[":vo"] = vo
        varMap[":prodSourceLabel"] = prodSourceLabel
        if cloud is not None:
            varMap[":cloud"] = cloud
        for tmpPrio in tmpPrioMap.keys():
            varMap[tmpPrio] = tmpPrioMap[tmpPrio]
        returnMap = {}
        try:
            iActive = 0
            for table in tables:
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000
                useRunning = None
                if table == f"{jedi_config.db.schemaPANDA}.jobsActive4":
                    mvTableName = f"{jedi_config.db.schemaPANDA}.MV_JOBSACTIVE4_STATS"
                    # first count non-running and then running if minPriority is specified
                    if minPriority is not None:
                        if iActive == 0:
                            useRunning = False
                        else:
                            useRunning = True
                        iActive += 1
                    if useRunning in [None, False]:
                        sqlExeTmp = (sqlMV + comment) % mvTableName
                    else:
                        sqlExeTmp = (sqlMVforRun + comment) % mvTableName
                else:
                    sqlExeTmp = (sql0 + comment) % table
                self.cur.execute(sqlExeTmp, varMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # create map
                for computingSite, cloud, jobStatus, workQueue_ID, nCount in res:
                    # count the number of non-running with prio>=MIN
                    if useRunning is True and jobStatus != "running":
                        continue
                    # count the number of running with prio<=MIN
                    if useRunning is False and jobStatus == "running":
                        continue
                    # add site
                    if computingSite not in returnMap:
                        returnMap[computingSite] = {}
                    # add workQueue
                    if workQueue_ID not in returnMap[computingSite]:
                        returnMap[computingSite][workQueue_ID] = {}
                    # add jobstatus
                    if jobStatus not in returnMap[computingSite][workQueue_ID]:
                        returnMap[computingSite][workQueue_ID][jobStatus] = 0
                    # add
                    returnMap[computingSite][workQueue_ID][jobStatus] += nCount
            # return
            tmpLog.debug("done")
            return True, returnMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, {}

    # get job statistics by global share
    def getJobStatisticsByGlobalShare(self, vo, exclude_rwq):
        """
        :param vo: Virtual Organization
        :param exclude_rwq: True/False. Indicates whether we want to indicate special workqueues from the statistics
        """
        comment = " /* DBProxy.getJobStatisticsByGlobalShare */"
        methodName = self.getMethodName(comment)
        methodName += f" < vo={vo} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")

        # define the var map of query parameters
        var_map = {":vo": vo}

        # sql to query on pre-cached job statistics tables (JOBS_SHARE_STATS and JOBSDEFINED_SHARE_STATS)
        sql_jt = """
               SELECT /*+ RESULT_CACHE */ computingSite, jobStatus, gShare, SUM(njobs) FROM %s
               WHERE vo=:vo
               """

        if exclude_rwq:
            sql_jt += f"""
               AND workqueue_id NOT IN
               (SELECT queue_id FROM {jedi_config.db.schemaPANDA}.jedi_work_queue WHERE queue_function = 'Resource')
               """

        sql_jt += """
               GROUP BY computingSite, jobStatus, gshare
               """

        tables = [f"{jedi_config.db.schemaPANDA}.JOBS_SHARE_STATS", f"{jedi_config.db.schemaPANDA}.JOBSDEFINED_SHARE_STATS"]

        return_map = {}
        try:
            for table in tables:
                self.cur.arraysize = 10000
                sql_exe = (sql_jt + comment) % table
                self.cur.execute(sql_exe, var_map)
                res = self.cur.fetchall()

                # create map
                for panda_site, status, gshare, n_count in res:
                    # add site
                    return_map.setdefault(panda_site, {})
                    # add global share
                    return_map[panda_site].setdefault(gshare, {})
                    # add job status
                    return_map[panda_site][gshare].setdefault(status, 0)
                    # increase count
                    return_map[panda_site][gshare][status] += n_count

            tmpLog.debug("done")
            return True, return_map
        except Exception:
            self.dumpErrorMessage(tmpLog)
            return False, {}

    def getJobStatisticsByResourceType(self, workqueue):
        """
        This function will return the job statistics for a particular workqueue, broken down by resource type
        (SCORE, MCORE, etc.)
        :param workqueue: workqueue object
        """
        comment = " /* DBProxy.getJobStatisticsByResourceType */"
        methodName = self.getMethodName(comment)
        methodName += f" < workqueue={workqueue} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")

        # define the var map of query parameters
        var_map = {":vo": workqueue.VO}

        # sql to query on pre-cached job statistics tables (JOBS_SHARE_STATS and JOBSDEFINED_SHARE_STATS)
        sql_jt = "SELECT /*+ RESULT_CACHE */ jobstatus, resource_type, SUM(njobs) FROM %s WHERE vo=:vo "

        if workqueue.is_global_share:
            sql_jt += "AND gshare=:gshare "
            sql_jt += f"AND workqueue_id NOT IN (SELECT queue_id FROM {jedi_config.db.schemaPANDA}.jedi_work_queue WHERE queue_function = 'Resource') "
            var_map[":gshare"] = workqueue.queue_name
        else:
            sql_jt += "AND workqueue_id=:workqueue_id "
            var_map[":workqueue_id"] = workqueue.queue_id

        sql_jt += "GROUP BY jobstatus, resource_type "

        tables = [f"{jedi_config.db.schemaPANDA}.JOBS_SHARE_STATS", f"{jedi_config.db.schemaPANDA}.JOBSDEFINED_SHARE_STATS"]

        return_map = {}
        try:
            for table in tables:
                self.cur.arraysize = 10000
                sql_exe = (sql_jt + comment) % table
                self.cur.execute(sql_exe, var_map)
                res = self.cur.fetchall()

                # create map
                for status, resource_type, n_count in res:
                    return_map.setdefault(status, {})
                    return_map[status][resource_type] = n_count

            tmpLog.debug("done")
            return True, return_map
        except Exception:
            self.dumpErrorMessage(tmpLog)
            return False, {}

    def getJobStatisticsByResourceTypeSite(self, workqueue):
        """
        This function will return the job statistics per site for a particular workqueue, broken down by resource type
        (SCORE, MCORE, etc.)
        :param workqueue: workqueue object
        """
        comment = " /* DBProxy.getJobStatisticsByResourceTypeSite */"
        methodName = self.getMethodName(comment)
        methodName += f" < workqueue={workqueue} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")

        # define the var map of query parameters
        var_map = {":vo": workqueue.VO}

        # sql to query on pre-cached job statistics tables (JOBS_SHARE_STATS and JOBSDEFINED_SHARE_STATS)
        sql_jt = "SELECT /*+ RESULT_CACHE */ jobstatus, resource_type, computingSite, SUM(njobs) FROM %s WHERE vo=:vo "

        if workqueue.is_global_share:
            sql_jt += "AND gshare=:gshare "
            sql_jt += f"AND workqueue_id NOT IN (SELECT queue_id FROM {jedi_config.db.schemaPANDA}.jedi_work_queue WHERE queue_function = 'Resource') "
            var_map[":gshare"] = workqueue.queue_name
        else:
            sql_jt += "AND workqueue_id=:workqueue_id "
            var_map[":workqueue_id"] = workqueue.queue_id

        sql_jt += "GROUP BY jobstatus, resource_type, computingSite "

        tables = [f"{jedi_config.db.schemaPANDA}.JOBS_SHARE_STATS", f"{jedi_config.db.schemaPANDA}.JOBSDEFINED_SHARE_STATS"]

        return_map = {}
        try:
            for table in tables:
                self.cur.arraysize = 10000
                sql_exe = (sql_jt + comment) % table
                self.cur.execute(sql_exe, var_map)
                res = self.cur.fetchall()

                # create map
                for status, resource_type, computingSite, n_count in res:
                    return_map.setdefault(computingSite, {})
                    return_map[computingSite].setdefault(resource_type, {})
                    return_map[computingSite][resource_type][status] = n_count

            tmpLog.debug("done")
            return True, return_map
        except Exception:
            self.dumpErrorMessage(tmpLog)
            return False, {}

    # generate output files for task, and instantiate template datasets if necessary
    def getOutputFiles_JEDI(
        self,
        jediTaskID,
        provenanceID,
        simul,
        instantiateTmpl,
        instantiatedSites,
        isUnMerging,
        isPrePro,
        xmlConfigJob,
        siteDsMap,
        middleName,
        registerDatasets,
        parallelOutMap,
        fileIDPool,
        n_files_per_chunk=1,
    ):
        comment = " /* JediDBProxy.getOutputFiles_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start with simul={simul} instantiateTmpl={instantiateTmpl} instantiatedSites={instantiatedSites}")
        tmpLog.debug(f"isUnMerging={isUnMerging} isPrePro={isPrePro} xmlConfigJob={type(xmlConfigJob)}")
        tmpLog.debug(f"middleName={middleName} registerDatasets={registerDatasets} idPool={len(fileIDPool)}")
        tmpLog.debug(f"n_files_per_chunk={n_files_per_chunk}")
        try:
            if instantiatedSites is None:
                instantiatedSites = ""
            if siteDsMap is None:
                siteDsMap = {}
            if parallelOutMap is None:
                parallelOutMap = {}
            outMap = {}
            datasetToRegister = []
            indexFileID = 0
            maxSerialNr = None
            # sql to get dataset
            sqlD = "SELECT "
            sqlD += f"datasetID,datasetName,vo,masterID,status,type FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlD += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            if provenanceID is not None:
                sqlD += "AND (provenanceID IS NULL OR provenanceID=:provenanceID) "
            # sql to read template
            sqlR = "SELECT outTempID,datasetID,fileNameTemplate,serialNr,outType,streamName "
            sqlR += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Output_Template "
            sqlR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID FOR UPDATE"
            # sql to insert files
            sqlI = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlI += JediFileSpec.bindValuesExpression()
            sqlI += " RETURNING fileID INTO :newFileID"
            # sql to insert files without fileID
            sqlII = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlII += JediFileSpec.bindValuesExpression(useSeq=False)
            # sql to increment SN
            sqlU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Output_Template SET serialNr=serialNr+:diff "
            sqlU += "WHERE jediTaskID=:jediTaskID AND outTempID=:outTempID "
            # sql to instantiate template dataset
            sqlT1 = f"SELECT {JediDatasetSpec.columnNames()} FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlT1 += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlT2 = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Datasets ({JediDatasetSpec.columnNames()}) "
            sqlT2 += JediDatasetSpec.bindValuesExpression()
            sqlT2 += "RETURNING datasetID INTO :newDatasetID "
            # sql to change concrete dataset name
            sqlCN = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlCN += "SET site=:site,datasetName=:datasetName,destination=:destination "
            sqlCN += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to set masterID to concrete datasets
            sqlMC = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlMC += "SET masterID=:masterID "
            sqlMC += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # current current date
            timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 100
            # get datasets
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type1"] = "output"
            varMap[":type2"] = "log"
            # unmerged datasets
            if isUnMerging:
                varMap[":type1"] = "trn_" + varMap[":type1"]
                varMap[":type2"] = "trn_" + varMap[":type2"]
            elif isPrePro:
                varMap[":type1"] = "pp_" + varMap[":type1"]
                varMap[":type2"] = "pp_" + varMap[":type2"]
            # template datasets
            if instantiateTmpl:
                varMap[":type1"] = "tmpl_" + varMap[":type1"]
                varMap[":type2"] = "tmpl_" + varMap[":type2"]
            # keep dataset types
            tmpl_VarMap = {}
            tmpl_VarMap[":type1"] = varMap[":type1"]
            tmpl_VarMap[":type2"] = varMap[":type2"]
            if provenanceID is not None:
                varMap[":provenanceID"] = provenanceID
            self.cur.execute(sqlD + comment, varMap)
            resList = self.cur.fetchall()
            tmpl_RelationMap = {}
            mstr_RelationMap = {}
            varMapsForInsert = []
            varMapsForSN = []
            for datasetID, datasetName, vo, masterID, datsetStatus, datasetType in resList:
                fileDatasetIDs = []
                for instantiatedSite in instantiatedSites.split(","):
                    fileDatasetID = datasetID
                    if registerDatasets and datasetType in ["output", "log"] and fileDatasetID not in datasetToRegister:
                        datasetToRegister.append(fileDatasetID)
                    # instantiate template datasets
                    if instantiateTmpl:
                        doInstantiate = False
                        if isUnMerging:
                            # instantiate new datasets in each submission for premerged
                            if datasetID in siteDsMap and instantiatedSite in siteDsMap[datasetID]:
                                fileDatasetID = siteDsMap[datasetID][instantiatedSite]
                                tmpLog.debug(f"found concrete premerged datasetID={fileDatasetID}")
                            else:
                                doInstantiate = True
                        else:
                            # check if concrete dataset is already there
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":type1"] = re.sub("^tmpl_", "", tmpl_VarMap[":type1"])
                            varMap[":type2"] = re.sub("^tmpl_", "", tmpl_VarMap[":type2"])
                            varMap[":templateID"] = datasetID
                            varMap[":closedState"] = "closed"
                            if provenanceID is not None:
                                varMap[":provenanceID"] = provenanceID
                            if instantiatedSite is not None:
                                sqlDT = sqlD + "AND site=:site "
                                varMap[":site"] = instantiatedSite
                            else:
                                sqlDT = sqlD
                            sqlDT += "AND (state IS NULL OR state<>:closedState) "
                            sqlDT += "AND templateID=:templateID "
                            self.cur.execute(sqlDT + comment, varMap)
                            resDT = self.cur.fetchone()
                            if resDT is not None:
                                fileDatasetID = resDT[0]
                                # collect ID of dataset to be registered
                                if resDT[-1] == "defined":
                                    datasetToRegister.append(fileDatasetID)
                                tmpLog.debug(f"found concrete datasetID={fileDatasetID}")
                            else:
                                doInstantiate = True
                        if doInstantiate:
                            # read dataset template
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = datasetID
                            self.cur.execute(sqlT1 + comment, varMap)
                            resT1 = self.cur.fetchone()
                            cDatasetSpec = JediDatasetSpec()
                            cDatasetSpec.pack(resT1)
                            # instantiate template dataset
                            cDatasetSpec.type = re.sub("^tmpl_", "", cDatasetSpec.type)
                            cDatasetSpec.templateID = datasetID
                            cDatasetSpec.creationTime = timeNow
                            cDatasetSpec.modificationTime = timeNow
                            varMap = cDatasetSpec.valuesMap(useSeq=True)
                            varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                            self.cur.execute(sqlT2 + comment, varMap)
                            val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                            fileDatasetID = int(val)
                            if instantiatedSite is not None:
                                # set concreate name
                                cDatasetSpec.site = instantiatedSite
                                cDatasetSpec.datasetName = re.sub("/*$", f".{fileDatasetID}", datasetName)
                                # set destination
                                if cDatasetSpec.destination in [None, ""]:
                                    cDatasetSpec.destination = cDatasetSpec.site
                                varMap = {}
                                varMap[":datasetName"] = cDatasetSpec.datasetName
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = fileDatasetID
                                varMap[":site"] = cDatasetSpec.site
                                varMap[":destination"] = cDatasetSpec.destination
                                self.cur.execute(sqlCN + comment, varMap)
                            tmpLog.debug(f"instantiated {cDatasetSpec.datasetName} datasetID={fileDatasetID}")
                            if masterID is not None:
                                mstr_RelationMap[fileDatasetID] = (masterID, instantiatedSite)
                            # collect ID of dataset to be registered
                            if fileDatasetID not in datasetToRegister:
                                datasetToRegister.append(fileDatasetID)
                            # collect IDs for pre-merging
                            if isUnMerging:
                                if datasetID not in siteDsMap:
                                    siteDsMap[datasetID] = {}
                                if instantiatedSite not in siteDsMap[datasetID]:
                                    siteDsMap[datasetID][instantiatedSite] = fileDatasetID
                        # keep relation between template and concrete
                        if datasetID not in tmpl_RelationMap:
                            tmpl_RelationMap[datasetID] = {}
                        tmpl_RelationMap[datasetID][instantiatedSite] = fileDatasetID
                    fileDatasetIDs.append(fileDatasetID)
                # get output templates
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                self.cur.execute(sqlR + comment, varMap)
                resTmpList = self.cur.fetchall()
                maxSerialNr = None
                for resR in resTmpList:
                    # make FileSpec
                    outTempID, datasetID, fileNameTemplate, serialNr, outType, streamName = resR
                    if xmlConfigJob is None or outType.endswith("log"):
                        fileNameTemplateList = [(fileNameTemplate, streamName)]
                    else:
                        fileNameTemplateList = []
                        # get output filenames from XML config
                        for tmpFileName in xmlConfigJob.outputs().split(","):
                            # ignore empty
                            if tmpFileName == "":
                                continue
                            newStreamName = tmpFileName
                            newFileNameTemplate = fileNameTemplate + "." + xmlConfigJob.prepend_string() + "." + newStreamName
                            fileNameTemplateList.append((newFileNameTemplate, newStreamName))
                    if outType.endswith("log"):
                        nFileLoop = 1
                    else:
                        nFileLoop = n_files_per_chunk
                    # loop over all filename templates
                    for fileNameTemplate, streamName in fileNameTemplateList:
                        firstFileID = None
                        for fileDatasetID in fileDatasetIDs:
                            for iFileLoop in range(nFileLoop):
                                fileSpec = JediFileSpec()
                                fileSpec.jediTaskID = jediTaskID
                                fileSpec.datasetID = fileDatasetID
                                nameTemplate = fileNameTemplate.replace("${SN}", "{SN:06d}")
                                nameTemplate = nameTemplate.replace("${SN/P}", "{SN:06d}")
                                nameTemplate = nameTemplate.replace("${SN", "{SN")
                                nameTemplate = nameTemplate.replace("${MIDDLENAME}", middleName)
                                fileSpec.lfn = nameTemplate.format(SN=serialNr)
                                fileSpec.status = "defined"
                                fileSpec.creationDate = timeNow
                                fileSpec.type = outType
                                fileSpec.keepTrack = 1
                                if maxSerialNr is None or maxSerialNr < serialNr:
                                    maxSerialNr = serialNr
                                serialNr += 1
                                # scope
                                if vo in jedi_config.ddm.voWithScope.split(","):
                                    fileSpec.scope = self.extractScope(datasetName)
                                if not simul:
                                    # insert
                                    if indexFileID < len(fileIDPool):
                                        fileSpec.fileID = fileIDPool[indexFileID]
                                        varMap = fileSpec.valuesMap()
                                        varMapsForInsert.append(varMap)
                                        indexFileID += 1
                                    else:
                                        varMap = fileSpec.valuesMap(useSeq=True)
                                        varMap[":newFileID"] = self.cur.var(varNUMBER)
                                        self.cur.execute(sqlI + comment, varMap)
                                        val = self.getvalue_corrector(self.cur.getvalue(varMap[":newFileID"]))
                                        fileSpec.fileID = int(val)
                                else:
                                    # set dummy for simulation
                                    fileSpec.fileID = fileSpec.datasetID
                                # append
                                if firstFileID is None:
                                    outMap[streamName] = fileSpec
                                    firstFileID = fileSpec.fileID
                                    parallelOutMap[firstFileID] = []
                                if iFileLoop > 0:
                                    outMap[streamName + f"|{iFileLoop}"] = fileSpec
                                    continue
                                parallelOutMap[firstFileID].append(fileSpec)
                            # increment SN
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":outTempID"] = outTempID
                            varMap[":diff"] = nFileLoop
                            varMapsForSN.append(varMap)
            # bulk increment
            if len(varMapsForSN) > 0 and not simul:
                tmpLog.debug(f"bulk increment {len(varMapsForSN)} SNs")
                self.cur.executemany(sqlU + comment, varMapsForSN)
            # bulk insert
            if len(varMapsForInsert) > 0 and not simul:
                tmpLog.debug(f"bulk insert {len(varMapsForInsert)} files")
                self.cur.executemany(sqlII + comment, varMapsForInsert)
            # set masterID to concrete datasets
            for fileDatasetID, (masterID, instantiatedSite) in mstr_RelationMap.items():
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = fileDatasetID
                if masterID in tmpl_RelationMap and instantiatedSite in tmpl_RelationMap[masterID]:
                    varMap[":masterID"] = tmpl_RelationMap[masterID][instantiatedSite]
                else:
                    varMap[":masterID"] = masterID
                self.cur.execute(sqlMC + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done indexFileID={indexFileID}")
            return outMap, maxSerialNr, datasetToRegister, siteDsMap, parallelOutMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None, None, None, siteDsMap, parallelOutMap

    # insert output file templates
    def insertOutputTemplate_JEDI(self, templates):
        comment = " /* JediDBProxy.insertOutputTemplate_JEDI */"
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # begin transaction
            self.conn.begin()
            # loop over all templates
            for template in templates:
                # make sql
                varMap = {}
                sqlH = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Output_Template (outTempID,"
                sqlL = f"VALUES({jedi_config.db.schemaJEDI}.JEDI_OUTPUT_TEMPLATE_ID_SEQ.nextval,"
                for tmpAttr, tmpVal in template.items():
                    tmpKey = ":" + tmpAttr
                    sqlH += f"{tmpAttr},"
                    sqlL += f"{tmpKey},"
                    varMap[tmpKey] = tmpVal
                sqlH = sqlH[:-1] + ") "
                sqlL = sqlL[:-1] + ") "
                sql = sqlH + sqlL
                self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # get tasks to be processed
    def getTasksToBeProcessed_JEDI(
        self,
        pid,
        vo,
        workQueue,
        prodSourceLabel,
        cloudName,
        nTasks=50,
        nFiles=100,
        isPeeking=False,
        simTasks=None,
        minPriority=None,
        maxNumJobs=None,
        typicalNumFilesMap=None,
        fullSimulation=False,
        simDatasets=None,
        mergeUnThrottled=None,
        readMinFiles=False,
        numNewTaskWithJumbo=0,
        resource_name=None,
        ignore_lock=False,
        target_tasks=None,
    ):
        comment = " /* JediDBProxy.getTasksToBeProcessed_JEDI */"
        methodName = self.getMethodName(comment)
        timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).strftime("%Y/%m/%d %H:%M:%S")
        if simTasks is not None:
            methodName += f" <jediTasks={str(simTasks)}>"
        elif target_tasks:
            methodName += f" <jediTasks={str(target_tasks)}>"
        elif workQueue is None:
            methodName += f" <vo={vo} queue={None} cloud={cloudName} pid={pid} {timeNow}>"
        else:
            methodName += f" <vo={vo} queue={workQueue.queue_name} cloud={cloudName} pid={pid} {timeNow}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start label={prodSourceLabel} nTasks={nTasks} nFiles={nFiles} minPriority={minPriority}")
        tmpLog.debug(f"maxNumJobs={maxNumJobs} typicalNumFilesMap={str(typicalNumFilesMap)}")
        tmpLog.debug(f"simTasks={str(simTasks)} mergeUnThrottled={str(mergeUnThrottled)}")
        tmpLog.debug(f"numNewTaskWithJumbo={numNewTaskWithJumbo}")

        memStart = JediCoreUtils.getMemoryUsage()
        tmpLog.debug(f"memUsage start {memStart} MB pid={os.getpid()}")
        # return value for failure
        failedRet = None
        # set max number of jobs if undefined
        if maxNumJobs is None:
            tmpLog.debug(f"set maxNumJobs={maxNumJobs} since undefined ")
        superHighPrioTaskRatio = self.getConfigValue("dbproxy", "SUPER_HIGH_PRIO_TASK_RATIO", "jedi")
        if superHighPrioTaskRatio is None:
            superHighPrioTaskRatio = 30
        # time limit to avoid duplication
        if hasattr(jedi_config.jobgen, "lockInterval"):
            lockInterval = jedi_config.jobgen.lockInterval
        else:
            lockInterval = 10
        timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=lockInterval)
        try:
            # attribute for GROUP BY
            if workQueue is not None:
                attrNameForGroupBy = self.getConfigValue("jobgen", f"GROUPBYATTR_{workQueue.queue_name}", "jedi")
            else:
                attrNameForGroupBy = None
            if attrNameForGroupBy is None or attrNameForGroupBy not in JediTaskSpec.attributes:
                attrNameForGroupBy = "userName"
                setGroupByAttr = False
            else:
                setGroupByAttr = True
            # sql to get tasks/datasets
            if not simTasks and not target_tasks:
                varMap = {}
                varMap[":vo"] = vo
                if prodSourceLabel not in [None, "", "any"]:
                    varMap[":prodSourceLabel"] = prodSourceLabel
                if cloudName not in [None, "", "any"]:
                    varMap[":cloud"] = cloudName
                varMap[":dsStatus1"] = "ready"
                varMap[":dsStatus2"] = "done"
                varMap[":dsOKStatus1"] = "ready"
                varMap[":dsOKStatus2"] = "done"
                varMap[":dsOKStatus3"] = "defined"
                varMap[":dsOKStatus4"] = "registered"
                varMap[":dsOKStatus5"] = "failed"
                varMap[":dsOKStatus6"] = "finished"
                varMap[":timeLimit"] = timeLimit
                varMap[":useJumboLack"] = JediTaskSpec.enum_useJumbo["lack"]
                sql = "SELECT tabT.jediTaskID,datasetID,currentPriority,nFilesToBeUsed-nFilesUsed,tabD.type,tabT.status,"
                sql += f"tabT.{attrNameForGroupBy},nFiles,nEvents,nFilesWaiting,tabT.useJumbo "
                sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
                sql += "AND tabT.vo=:vo "
                if workQueue.is_global_share:
                    sql += "AND gshare=:wq_name "
                    sql += f"AND workqueue_id NOT IN (SELECT queue_id FROM {jedi_config.db.schemaJEDI}.jedi_work_queue WHERE queue_function = 'Resource') "
                    varMap[":wq_name"] = workQueue.queue_name
                else:
                    sql += "AND workQueue_ID=:wq_id "
                    varMap[":wq_id"] = workQueue.queue_id
                if resource_name:
                    sql += "AND resource_type=:resource_name "
                    varMap[":resource_name"] = resource_name
                if prodSourceLabel not in [None, "", "any"]:
                    sql += "AND prodSourceLabel=:prodSourceLabel "
                if cloudName not in [None, "", "any"]:
                    sql += "AND tabT.cloud=:cloud "
                sql += "AND tabT.status IN ("
                for tmpStat in JediTaskSpec.statusForJobGenerator():
                    tmpKey = f":tstat_{tmpStat}"
                    varMap[tmpKey] = tmpStat
                    sql += f"{tmpKey},"
                sql = sql[:-1]
                sql += ") "
                sql += "AND tabT.lockedBy IS NULL "
                sql += "AND tabT.modificationTime<:timeLimit "
                sql += "AND "
                sql += "(tabT.useJumbo=:useJumboLack "
                sql += "OR (nFilesToBeUsed > nFilesUsed AND type IN ("
                if mergeUnThrottled is True:
                    for tmpType in JediDatasetSpec.getMergeProcessTypes():
                        mapKey = ":type_" + tmpType
                        sql += f"{mapKey},"
                        varMap[mapKey] = tmpType
                else:
                    for tmpType in JediDatasetSpec.getProcessTypes():
                        mapKey = ":type_" + tmpType
                        sql += f"{mapKey},"
                        varMap[mapKey] = tmpType
                sql = sql[:-1]
                sql += "))"
                if mergeUnThrottled is True:
                    sql += "OR (tabT.useJumbo IS NOT NULL AND nFilesWaiting IS NOT NULL AND nFilesToBeUsed>(nFilesUsed+nFilesWaiting) AND type IN ("
                    for tmpType in JediDatasetSpec.getInputTypes():
                        mapKey = ":type_" + tmpType
                        sql += f"{mapKey},"
                        varMap[mapKey] = tmpType
                    sql = sql[:-1]
                    sql += "))"
                sql += ") "
                sql += "AND tabD.status IN (:dsStatus1,:dsStatus2) "
                sql += "AND masterID IS NULL "
                if minPriority is not None:
                    varMap[":minPriority"] = minPriority
                    sql += "AND currentPriority>=:minPriority "
                sql += "AND NOT EXISTS "
                sql += f"(SELECT 1 FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
                sql += f"WHERE {jedi_config.db.schemaJEDI}.JEDI_Datasets.jediTaskID=tabT.jediTaskID "
                sql += "AND type IN ("
                if mergeUnThrottled is True:
                    for tmpType in JediDatasetSpec.getMergeProcessTypes():
                        mapKey = ":type_" + tmpType
                        sql += f"{mapKey},"
                else:
                    for tmpType in JediDatasetSpec.getProcessTypes():
                        mapKey = ":type_" + tmpType
                        sql += f"{mapKey},"
                sql = sql[:-1]
                sql += ") AND NOT status IN (:dsOKStatus1,:dsOKStatus2,:dsOKStatus3,:dsOKStatus4,:dsOKStatus5,:dsOKStatus6)) "
                sql += "ORDER BY currentPriority DESC,jediTaskID "
            else:
                varMap = {}
                if not fullSimulation:
                    sql = "SELECT tabT.jediTaskID,datasetID,currentPriority,nFilesToBeUsed-nFilesUsed,tabD.type,tabT.status,"
                    sql += f"tabT.{attrNameForGroupBy},nFiles,nEvents,nFilesWaiting,tabT.useJumbo "
                else:
                    sql = "SELECT tabT.jediTaskID,datasetID,currentPriority,nFilesToBeUsed,tabD.type,tabT.status,"
                    sql += f"tabT.{attrNameForGroupBy},nFiles,nEvents,nFilesWaiting,tabT.useJumbo "
                sql += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks tabT,{jedi_config.db.schemaJEDI}.JEDI_Datasets tabD "
                sql += "WHERE tabT.jediTaskID=tabD.jediTaskID AND tabT.jediTaskID IN ("
                if simTasks:
                    tasks_to_loop = simTasks
                else:
                    tasks_to_loop = target_tasks
                for tmpTaskIdx, tmpTaskID in enumerate(tasks_to_loop):
                    tmpKey = f":jediTaskID{tmpTaskIdx}"
                    varMap[tmpKey] = tmpTaskID
                    sql += f"{tmpKey},"
                sql = sql[:-1]
                sql += ") AND type IN ("
                for tmpType in JediDatasetSpec.getProcessTypes():
                    mapKey = ":type_" + tmpType
                    sql += f"{mapKey},"
                    varMap[mapKey] = tmpType
                sql = sql[:-1]
                sql += ") AND masterID IS NULL "
                if simDatasets is not None:
                    sql += "AND tabD.datasetID IN ("
                    for tmpDsIdx, tmpDatasetID in enumerate(simDatasets):
                        tmpKey = f":datasetID{tmpDsIdx}"
                        varMap[tmpKey] = tmpDatasetID
                        sql += f"{tmpKey},"
                    sql = sql[:-1]
                    sql += ") "
                if not fullSimulation:
                    sql += "AND nFilesToBeUsed > nFilesUsed "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            # select
            tmpLog.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # no tasks
            if resList == [] and isPeeking:
                return 0

            # make return
            returnMap = {}
            taskDatasetMap = {}
            taskStatusMap = {}
            jediTaskIDList = []
            taskAvalancheMap = {}
            taskUserPrioMap = {}
            taskPrioMap = {}
            taskUseJumboMap = {}
            taskUserMap = {}
            expressAttr = "express_group_by"
            taskMergeMap = {}
            for (
                jediTaskID,
                datasetID,
                currentPriority,
                tmpNumFiles,
                datasetType,
                taskStatus,
                groupByAttr,
                tmpNumInputFiles,
                tmpNumInputEvents,
                tmpNumFilesWaiting,
                useJumbo,
            ) in resList:
                tmpLog.debug(
                    "jediTaskID={0} datasetID={1} tmpNumFiles={2} type={3} prio={4} useJumbo={5} nFilesWaiting={6}".format(
                        jediTaskID, datasetID, tmpNumFiles, datasetType, currentPriority, useJumbo, tmpNumFilesWaiting
                    )
                )

                # just return the max priority
                if isPeeking:
                    return currentPriority
                # make task-status mapping
                taskStatusMap[jediTaskID] = taskStatus
                # make task-useJumbo mapping
                taskUseJumboMap[jediTaskID] = useJumbo
                # task and usermap
                taskUserMap[jediTaskID] = groupByAttr
                # make task-dataset mapping
                if jediTaskID not in taskDatasetMap:
                    taskDatasetMap[jediTaskID] = []
                data = (datasetID, tmpNumFiles, datasetType, tmpNumInputFiles, tmpNumInputEvents, tmpNumFilesWaiting, useJumbo)
                if datasetType in JediDatasetSpec.getMergeProcessTypes():
                    taskDatasetMap[jediTaskID].insert(0, data)
                else:
                    taskDatasetMap[jediTaskID].append(data)
                # use single value if WQ has a share
                if workQueue is not None and workQueue.queue_share is not None and not setGroupByAttr:
                    groupByAttr = ""
                elif currentPriority >= JobUtils.priorityTasksToJumpOver:
                    # use special name for super high prio tasks
                    groupByAttr = expressAttr
                # increase priority so that scouts do not wait behind the bulk
                if taskStatus in ["scouting"]:
                    currentPriority += 1
                # make task-prio mapping
                taskPrioMap[jediTaskID] = currentPriority
                if groupByAttr not in taskUserPrioMap:
                    taskUserPrioMap[groupByAttr] = {}
                if currentPriority not in taskUserPrioMap[groupByAttr]:
                    taskUserPrioMap[groupByAttr][currentPriority] = []
                if jediTaskID not in taskUserPrioMap[groupByAttr][currentPriority]:
                    taskUserPrioMap[groupByAttr][currentPriority].append(jediTaskID)
                taskMergeMap.setdefault(jediTaskID, True)
                if datasetType not in JediDatasetSpec.getMergeProcessTypes():
                    taskMergeMap[jediTaskID] = False
            # make user-task mapping
            userTaskMap = {}
            for groupByAttr in taskUserPrioMap.keys():
                # use high priority tasks first
                priorityList = sorted(taskUserPrioMap[groupByAttr].keys())
                priorityList.reverse()
                for currentPriority in priorityList:
                    tmpMergeTasks = []
                    userTaskMap.setdefault(groupByAttr, [])
                    # randomize super high prio tasks to avoid that multiple threads try to get the same tasks
                    if groupByAttr == expressAttr:
                        random.shuffle(taskUserPrioMap[groupByAttr][currentPriority])
                    for jediTaskID in taskUserPrioMap[groupByAttr][currentPriority]:
                        if taskMergeMap[jediTaskID]:
                            tmpMergeTasks.append(jediTaskID)
                        else:
                            userTaskMap[groupByAttr].append(jediTaskID)
                    userTaskMap[groupByAttr] = tmpMergeTasks + userTaskMap[groupByAttr]
            # make list
            groupByAttrList = list(userTaskMap.keys())
            random.shuffle(groupByAttrList)
            tmpLog.debug(f"{len(groupByAttrList)} groupBy values for {len(taskDatasetMap)} tasks")
            if expressAttr in userTaskMap:
                useSuperHigh = True
            else:
                useSuperHigh = False
            nPickUp = 10
            while groupByAttrList != []:
                # pickup one task from each groupByAttr
                for groupByAttr in groupByAttrList:
                    if userTaskMap[groupByAttr] == []:
                        groupByAttrList.remove(groupByAttr)
                    else:
                        # add high prio tasks first
                        if useSuperHigh and expressAttr in userTaskMap and random.randint(1, 100) <= superHighPrioTaskRatio:
                            tmpGroupByAttrList = [expressAttr]
                        else:
                            tmpGroupByAttrList = []
                        # add normal tasks
                        tmpGroupByAttrList.append(groupByAttr)
                        for tmpGroupByAttr in tmpGroupByAttrList:
                            for iPickUp in range(nPickUp):
                                if len(userTaskMap[tmpGroupByAttr]) > 0:
                                    jediTaskID = userTaskMap[tmpGroupByAttr].pop(0)
                                    jediTaskIDList.append(jediTaskID)
                                    # add next task if only pmerge
                                    if not taskMergeMap[jediTaskID]:
                                        break
                                else:
                                    break
            # sql to read task
            sqlRT = f"SELECT {JediTaskSpec.columnNames()} "
            sqlRT += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlRT += "WHERE jediTaskID=:jediTaskID AND status=:statusInDB "
            if not ignore_lock:
                sqlRT += "AND lockedBy IS NULL "
            if simTasks is None:
                sqlRT += "FOR UPDATE NOWAIT "
            # sql to read locked task
            sqlRL = f"SELECT {JediTaskSpec.columnNames()} "
            sqlRL += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlRL += "WHERE jediTaskID=:jediTaskID AND status=:statusInDB AND lockedBy=:newLockedBy "
            if simTasks is None:
                sqlRL += "FOR UPDATE NOWAIT "
            # sql to lock task
            sqlLock = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks  "
            sqlLock += "SET lockedBy=:newLockedBy,lockedTime=CURRENT_DATE,modificationTime=CURRENT_DATE "
            sqlLock += "WHERE jediTaskID=:jediTaskID AND status=:status AND lockedBy IS NULL AND modificationTime<:timeLimit "
            # sql to put the task in pending
            sqlPDG = ("UPDATE {0}.JEDI_Tasks " "SET lockedBy=NULL,lockedTime=NULL,status=:status,errorDialog=:err " "WHERE jediTaskID=:jediTaskID ").format(
                jedi_config.db.schemaJEDI
            )
            # sql to read template
            sqlJobP = f"SELECT jobParamsTemplate FROM {jedi_config.db.schemaJEDI}.JEDI_JobParams_Template WHERE jediTaskID=:jediTaskID "
            # sql to read datasets
            sqlRD = f"SELECT {JediDatasetSpec.columnNames()} "
            sqlRD += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlRD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            if simTasks is None:
                sqlRD += "FOR UPDATE NOWAIT "
            # sql to read files
            sqlFR = f"SELECT * FROM (SELECT {JediFileSpec.columnNames()} "
            sqlFR += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID "
            if not fullSimulation:
                sqlFR += "AND status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt) "
                sqlFR += "AND (maxFailure IS NULL OR failedAttempt<maxFailure) "
                sqlFR += "AND ramCount=:ramCount "
            sqlFR += "ORDER BY {0}) "
            sqlFR += "WHERE rownum <= {1}"

            # sql to read files for fake co-jumbo
            sqlCJ_FR = re.sub(
                "jediTaskID=:jediTaskID AND datasetID=:datasetID ", "jediTaskID=:jediTaskID AND datasetID=:datasetID AND is_waiting IS NULL ", sqlFR
            )
            # For the cases where the ram count is not set
            sqlFR_RCNull = f"SELECT * FROM (SELECT {JediFileSpec.columnNames()} "
            sqlFR_RCNull += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            sqlFR_RCNull += "jediTaskID=:jediTaskID AND datasetID=:datasetID "
            if not fullSimulation:
                sqlFR_RCNull += "AND status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt) "
                sqlFR_RCNull += "AND (maxFailure IS NULL OR failedAttempt<maxFailure) "
                sqlFR_RCNull += "AND (ramCount IS NULL OR ramCount=0) "
            sqlFR_RCNull += "ORDER BY {0}) "
            sqlFR_RCNull += "WHERE rownum <= {1}"

            # sql to read files for fake co-jumbo for the cases where the ram count is not set
            sqlCJ_FR_RCNull = re.sub(
                "jediTaskID=:jediTaskID AND datasetID=:datasetID ", "jediTaskID=:jediTaskID AND datasetID=:datasetID AND is_waiting IS NULL ", sqlFR_RCNull
            )

            # sql to read files without ramcount
            sqlFRNR = f"SELECT * FROM (SELECT {JediFileSpec.columnNames()} "
            sqlFRNR += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            sqlFRNR += "jediTaskID=:jediTaskID AND datasetID=:datasetID "
            if not fullSimulation:
                sqlFRNR += "AND status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt) "
                sqlFRNR += "AND (maxFailure IS NULL OR failedAttempt<maxFailure) "
            sqlFRNR += "ORDER BY {0}) "
            sqlFRNR += "WHERE rownum <= {1}"

            # sql to read files for fake co-jumbo without ramcount
            sqlCJ_FRNR = re.sub(
                "jediTaskID=:jediTaskID AND datasetID=:datasetID ", "jediTaskID=:jediTaskID AND datasetID=:datasetID AND is_waiting IS NULL ", sqlFRNR
            )
            # sql to read memory requirements of files in dataset
            sqlRM = f"""SELECT ramCount FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents
                       WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID """
            if not fullSimulation:
                sqlRM += """AND status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt)
                            AND (maxFailure IS NULL OR failedAttempt<maxFailure) """
            sqlRM += "GROUP BY ramCount "
            # sql to read memory requirements for fake co-jumbo
            sqlCJ_RM = re.sub(
                "jediTaskID=:jediTaskID AND datasetID=:datasetID ", "jediTaskID=:jediTaskID AND datasetID=:datasetID AND is_waiting IS NULL ", sqlRM
            )
            # sql to update file status
            sqlFU = "UPDATE /*+ INDEX_RS_ASC(JEDI_DATASET_CONTENTS (JEDI_DATASET_CONTENTS.JEDITASKID JEDI_DATASET_CONTENTS.DATASETID JEDI_DATASET_CONTENTS.FILEID)) */ {0}.JEDI_Dataset_Contents SET status=:nStatus ".format(
                jedi_config.db.schemaJEDI
            )
            sqlFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND status=:oStatus "
            # sql to update file usage info in dataset
            sqlDU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET nFilesUsed=:nFilesUsed "

            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlDU += "RETURNING nFilesUsed,nFilesTobeUsed INTO :newnFilesUsed,:newnFilesTobeUsed "
            # sql to read DN
            sqlDN = f"SELECT dn FROM {jedi_config.db.schemaMETA}.users WHERE name=:name "
            # sql to count the number of files for avalanche
            sqlAV = f"SELECT SUM(nFiles-nFilesToBeUsed) FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlAV += "WHERE jediTaskID=:jediTaskID AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlAV += f"{mapKey},"
            sqlAV = sqlAV[:-1]
            sqlAV += ") AND masterID IS NULL "
            # sql to check datasets with empty requirements
            sqlCER = f"SELECT status,attemptNr,maxAttempt,failedAttempt,maxFailure FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlCER += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlCDD = f"SELECT nFilesUsed,nFilesToBeUsed,nFilesFinished,nFilesFailed FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlCDD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update datasets with empty requirements
            sqlUER = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET status=:status "
            sqlUER += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update datasets with empty requirements
            sqlUFU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET nFilesUsed=:nFilesUsed "
            sqlUFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlUFB = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET nFilesToBeUsed=:nFilesToBeUsed "
            sqlUFB += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to get number of events
            sqlGNE = ("SELECT COUNT(*),datasetID FROM {0}.JEDI_Events " "WHERE jediTaskID=:jediTaskID AND status=:eventStatus " "GROUP BY datasetID ").format(
                jedi_config.db.schemaJEDI
            )
            # sql to get number of ready HPO workers
            sqlNRH = (
                "SELECT COUNT(*),datasetID FROM ("
                "(SELECT j.PandaID,f.datasetID FROM {0}.jobsDefined4 j, {0}.filesTable4 f "
                "WHERE j.jediTaskID=:jediTaskID AND f.PandaID=j.PandaID AND f.type=:f_type "
                "UNION "
                "SELECT j.PandaID,f.datasetID FROM {0}.jobsWaiting4 j, {0}.filesTable4 f "
                "WHERE j.jediTaskID=:jediTaskID AND f.PandaID=j.PandaID AND f.type=:f_type "
                "UNION "
                "SELECT j.PandaID,f.datasetID FROM {0}.jobsActive4 j, {0}.filesTable4 f "
                "WHERE j.jediTaskID=:jediTaskID AND f.PandaID=j.PandaID AND f.type=:f_type) "
                "MINUS "
                "SELECT PandaID,datasetID FROM {1}.JEDI_Events "
                "WHERE  jediTaskID=:jediTaskID AND "
                "status IN (:esSent,:esRunning)"
                ") GROUP BY datasetID"
            ).format(jedi_config.db.schemaPANDA, jedi_config.db.schemaJEDI)
            # sql to set frozenTime
            sqlFZT = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET frozenTime=:frozenTime WHERE jediTaskID=:jediTaskID "
            # sql to check files
            selCKF = f"SELECT nFilesToBeUsed-nFilesUsed FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # loop over all tasks
            iTasks = 0
            lockedTasks = []
            lockedByAnother = []
            memoryExceed = False
            for tmpIdxTask, jediTaskID in enumerate(jediTaskIDList):
                # process only merging if enough jobs are already generated
                dsWithfakeCoJumbo = set()
                containMerging = False
                if (maxNumJobs is not None and maxNumJobs <= 0) or taskUseJumboMap[jediTaskID] == JediTaskSpec.enum_useJumbo["pending"] or mergeUnThrottled:
                    for datasetID, tmpNumFiles, datasetType, tmpNumInputFiles, tmpNumInputEvents, tmpNumFilesWaiting, useJumbo in taskDatasetMap[jediTaskID]:
                        if datasetType in JediDatasetSpec.getMergeProcessTypes():
                            # pmerge
                            containMerging = True
                            if useJumbo is None or useJumbo == JediTaskSpec.enum_useJumbo["disabled"]:
                                break
                        elif (
                            useJumbo is None
                            or useJumbo == JediTaskSpec.enum_useJumbo["disabled"]
                            or (tmpNumFiles - tmpNumFilesWaiting <= 0 and useJumbo != JediTaskSpec.enum_useJumbo["lack"])
                        ):
                            # no jumbo or no more co-jumbo
                            pass
                        elif useJumbo in [JediTaskSpec.enum_useJumbo["running"], JediTaskSpec.enum_useJumbo["pending"], JediTaskSpec.enum_useJumbo["lack"]] or (
                            useJumbo == JediTaskSpec.enum_useJumbo["waiting"] and numNewTaskWithJumbo > 0
                        ):
                            # jumbo with fake co-jumbo
                            dsWithfakeCoJumbo.add(datasetID)
                    if not containMerging and len(dsWithfakeCoJumbo) == 0:
                        tmpLog.debug(
                            f"skipping no pmerge or jumbo jediTaskID={jediTaskID} {tmpIdxTask}/{len(jediTaskIDList)}/{iTasks} prio={taskPrioMap[jediTaskID]}"
                        )

                        continue
                tmpLog.debug(
                    f"getting jediTaskID={jediTaskID} {tmpIdxTask}/{len(jediTaskIDList)}/{iTasks} prio={taskPrioMap[jediTaskID]} by={taskUserMap[jediTaskID]}"
                )
                # locked by another
                if jediTaskID in lockedByAnother:
                    tmpLog.debug(f"skip locked by another jediTaskID={jediTaskID}")
                    continue
                # begin transaction
                self.conn.begin()
                # read task
                toSkip = False
                try:
                    # read task
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":statusInDB"] = taskStatusMap[jediTaskID]
                    if jediTaskID not in lockedTasks:
                        tmpLog.debug(sqlRT + comment + str(varMap))
                        self.cur.execute(sqlRT + comment, varMap)
                    else:
                        varMap[":newLockedBy"] = pid
                        tmpLog.debug(sqlRL + comment + str(varMap))
                        self.cur.execute(sqlRL + comment, varMap)
                    resRT = self.cur.fetchone()
                    # locked by another
                    if resRT is None:
                        toSkip = True
                        tmpLog.debug(f"skip locked jediTaskID={jediTaskID}")
                        lockedByAnother.append(jediTaskID)
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        continue
                    else:
                        origTaskSpec = JediTaskSpec()
                        origTaskSpec.pack(resRT)
                    # check nFiles in datasets
                    if simTasks is None and not ignore_lock and not target_tasks:
                        toSkip = False
                        for tmp_item in taskDatasetMap[jediTaskID]:
                            datasetID, tmpNumFiles = tmp_item[:2]
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = datasetID
                            self.cur.execute(selCKF + comment, varMap)
                            (newNumFiles,) = self.cur.fetchone()
                            tmpLog.debug(f"jediTaskID={jediTaskID} datasetID={datasetID} nFilesToBeUsed-nFilesUsed old:{tmpNumFiles} new:{newNumFiles}")
                            if tmpNumFiles > newNumFiles:
                                tmpLog.debug(f"skip jediTaskID={jediTaskID} since nFilesToBeUsed-nFilesUsed decreased")
                                lockedByAnother.append(jediTaskID)
                                toSkip = True
                                break
                        if toSkip:
                            if not self._commit():
                                raise RuntimeError("Commit error")
                            continue
                    # skip fake co-jumbo for scouting
                    if not containMerging and len(dsWithfakeCoJumbo) > 0 and origTaskSpec.useScout() and not origTaskSpec.isPostScout():
                        toSkip = True
                        tmpLog.debug(f"skip scouting jumbo jediTaskID={jediTaskID}")
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        continue
                    # lock task
                    if simTasks is None and jediTaskID not in lockedTasks:
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":newLockedBy"] = pid
                        varMap[":status"] = taskStatusMap[jediTaskID]
                        varMap[":timeLimit"] = timeLimit
                        tmpLog.debug(sqlLock + comment + str(varMap))
                        self.cur.execute(sqlLock + comment, varMap)
                        nRow = self.cur.rowcount
                        if nRow != 1:
                            tmpLog.debug(f"failed to lock jediTaskID={jediTaskID}")
                            lockedByAnother.append(jediTaskID)
                            toSkip = True
                            if not self._commit():
                                raise RuntimeError("Commit error")
                            continue
                        # list of locked tasks
                        if jediTaskID not in lockedTasks:
                            lockedTasks.append(jediTaskID)
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        # resource busy and acquire with NOWAIT specified
                        toSkip = True
                        tmpLog.debug(f"skip locked with NOWAIT jediTaskID={jediTaskID}")
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        continue
                    else:
                        # failed with something else
                        raise errType(errValue)
                # count the number of files for avalanche
                if not toSkip:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    for tmpType in JediDatasetSpec.getInputTypes():
                        mapKey = ":type_" + tmpType
                        varMap[mapKey] = tmpType
                    tmpLog.debug(sqlAV + comment + str(varMap))
                    self.cur.execute(sqlAV + comment, varMap)
                    resAV = self.cur.fetchone()
                    tmpLog.debug(str(resAV))
                    if resAV is None:
                        # no file info
                        toSkip = True
                        tmpLog.error("skipped since failed to get number of files for avalanche")
                    else:
                        (numAvalanche,) = resAV
                # change userName for analysis
                if not toSkip:
                    # for analysis use DN as userName
                    if origTaskSpec.prodSourceLabel in ["user"]:
                        varMap = {}
                        varMap[":name"] = origTaskSpec.userName
                        tmpLog.debug(sqlDN + comment + str(varMap))
                        self.cur.execute(sqlDN + comment, varMap)
                        resDN = self.cur.fetchone()
                        tmpLog.debug(resDN)
                        if resDN is None:
                            # no user info
                            toSkip = True
                            tmpLog.error(f"skipped since failed to get DN for {origTaskSpec.userName} jediTaskID={jediTaskID}")
                        else:
                            origTaskSpec.origUserName = origTaskSpec.userName
                            (origTaskSpec.userName,) = resDN
                            if origTaskSpec.userName in ["", None]:
                                # DN is empty
                                toSkip = True
                                tmpLog.error(f"skipped since DN is empty for {origTaskSpec.userName} jediTaskID={jediTaskID}")
                            else:
                                # reset change to not update userName
                                origTaskSpec.resetChangedAttr("userName")
                # checks for HPO
                numEventsHPO = None
                if not toSkip and simTasks is None:
                    if origTaskSpec.is_hpo_workflow():
                        # number of jobs
                        numMaxHpoJobs = origTaskSpec.get_max_num_jobs()
                        if numMaxHpoJobs is not None:
                            sqlNTJ = f"SELECT total_req_jobs FROM {jedi_config.db.schemaDEFT}.T_TASK "
                            sqlNTJ += "WHERE taskid=:taskid "
                            varMap = {}
                            varMap[":taskID"] = jediTaskID
                            self.cur.execute(sqlNTJ + comment, varMap)
                            (tmpNumHpoJobs,) = self.cur.fetchone()
                            if tmpNumHpoJobs >= numMaxHpoJobs:
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":status"] = origTaskSpec.status
                                varMap[":err"] = "skipped max number of HPO jobs reached"
                                self.cur.execute(sqlPDG + comment, varMap)
                                tmpLog.debug(f"jediTaskID={jediTaskID} to finish due to maxNumHpoJobs={numMaxHpoJobs} numHpoJobs={tmpNumHpoJobs}")
                                if not self._commit():
                                    raise RuntimeError("Commit error")
                                # send finish command
                                self.sendCommandTaskPanda(jediTaskID, "HPO task finished since maxNumJobs reached", True, "finish", comQualifier="soft")
                                continue
                        # get number of active samples
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":eventStatus"] = EventServiceUtils.ST_ready
                        self.cur.execute(sqlGNE + comment, varMap)
                        resGNE = self.cur.fetchall()
                        numEventsHPO = {}
                        totalNumEventsHPO = 0
                        for tmpNumEventsHPO, datasetIdHPO in resGNE:
                            numEventsHPO[datasetIdHPO] = tmpNumEventsHPO
                            totalNumEventsHPO += tmpNumEventsHPO
                        # subtract ready workers
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":esSent"] = EventServiceUtils.ST_sent
                        varMap[":esRunning"] = EventServiceUtils.ST_running
                        varMap[":f_type"] = "pseudo_input"
                        self.cur.execute(sqlNRH + comment, varMap)
                        resNRH = self.cur.fetchall()
                        numWorkersHPO = {}
                        totalNumWorkersHPO = 0
                        for tmpNumWorkersHPO, datasetIdHPO in resNRH:
                            totalNumWorkersHPO += tmpNumWorkersHPO
                            if datasetIdHPO in numEventsHPO:
                                numEventsHPO[datasetIdHPO] -= tmpNumWorkersHPO
                        # go to pending if no events (samples)
                        if not [i for i in numEventsHPO.values() if i > 0]:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":status"] = origTaskSpec.status
                            if not numEventsHPO:
                                varMap[":err"] = "skipped since no HP points to evaluate"
                            else:
                                varMap[":err"] = "skipped since enough HPO jobs are running or scheduled"
                            self.cur.execute(sqlPDG + comment, varMap)
                            # set frozenTime
                            if totalNumEventsHPO + totalNumWorkersHPO == 0 and origTaskSpec.frozenTime is None:
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":frozenTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                                self.cur.execute(sqlFZT + comment, varMap)
                            elif totalNumEventsHPO + totalNumWorkersHPO > 0 and origTaskSpec.frozenTime is not None:
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":frozenTime"] = None
                                self.cur.execute(sqlFZT + comment, varMap)
                            tmpLog.debug(
                                f"HPO jediTaskID={jediTaskID} skipped due to nSamplesToEvaluate={totalNumEventsHPO} nReadyWorkers={totalNumWorkersHPO}"
                            )
                            if not self._commit():
                                raise RuntimeError("Commit error")
                            # terminate if inactive for long time
                            waitInterval = 24
                            if (
                                totalNumEventsHPO + totalNumWorkersHPO == 0
                                and origTaskSpec.frozenTime is not None
                                and datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - origTaskSpec.frozenTime
                                > datetime.timedelta(hours=waitInterval)
                            ):
                                # send finish command
                                self.sendCommandTaskPanda(jediTaskID, "HPO task finished since inactive for one day", True, "finish", comQualifier="soft")
                            continue
                # read datasets
                if not toSkip:
                    iDsPerTask = 0
                    nDsPerTask = 10
                    taskWithNewJumbo = False
                    for datasetID, tmpNumFiles, datasetType, tmpNumInputFiles, tmpNumInputEvents, tmpNumFilesWaiting, useJumbo in taskDatasetMap[jediTaskID]:
                        primaryDatasetID = datasetID
                        datasetIDs = [datasetID]
                        taskSpec = copy.copy(origTaskSpec)
                        origTmpNumFiles = tmpNumFiles
                        # reduce NumInputFiles for HPO to avoid redundant workers
                        if numEventsHPO is not None:
                            if datasetID not in numEventsHPO or numEventsHPO[datasetID] <= 0:
                                continue
                            if tmpNumFiles > numEventsHPO[datasetID]:
                                tmpNumFiles = numEventsHPO[datasetID]
                        # See if there are different memory requirements that need to be mapped to different chuncks
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = datasetID
                        if not fullSimulation:
                            if useJumbo == JediTaskSpec.enum_useJumbo["lack"] and origTmpNumFiles == 0:
                                varMap[":status"] = "running"
                            else:
                                varMap[":status"] = "ready"
                        self.cur.arraysize = 100000
                        # figure out if there are different memory requirements in the dataset
                        if datasetID not in dsWithfakeCoJumbo or useJumbo == JediTaskSpec.enum_useJumbo["lack"]:
                            self.cur.execute(sqlRM + comment, varMap)
                        else:
                            self.cur.execute(sqlCJ_RM + comment, varMap)
                        memReqs = [req[0] for req in self.cur.fetchall()]  # Unpack resultset

                        # Group 0 and NULL memReqs
                        if 0 in memReqs and None in memReqs:
                            memReqs.remove(None)

                        tmpLog.debug(f"memory requirements for files in jediTaskID={jediTaskID} datasetID={datasetID} type={datasetType} are: {memReqs}")
                        if not memReqs:
                            tmpLog.debug(f"skip jediTaskID={jediTaskID} datasetID={primaryDatasetID} since memory requirements are empty")
                            varMap = dict()
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = primaryDatasetID
                            self.cur.execute(sqlCDD + comment, varMap)
                            cdd_nFilesUsed, cdd_nFilesToBeUsed, cdd_nFilesFinished, cdd_nFilesFailed = self.cur.fetchone()
                            varMap = dict()
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = primaryDatasetID
                            self.cur.execute(sqlCER + comment, varMap)
                            resCER = self.cur.fetchall()
                            nDone = 0
                            nFinished = 0
                            nFailed = 0
                            nActive = 0
                            nRunning = 0
                            nReady = 0
                            nUnknown = 0
                            nLost = 0
                            for cer_status, cer_attemptNr, cer_maxAttempt, cer_failedAttempt, cer_maxFailure in resCER:
                                if cer_status in ["missing", "lost"]:
                                    nLost += 1
                                elif cer_status in ["finished", "failed", "cancelled"] or (
                                    cer_status == "ready" and (cer_attemptNr >= cer_maxAttempt or (cer_maxFailure and cer_failedAttempt >= cer_maxFailure))
                                ):
                                    nDone += 1
                                    if cer_status == "finished":
                                        nFinished += 1
                                    else:
                                        nFailed += 1
                                else:
                                    nActive += 1
                                    if cer_status in ["running", "merging", "picked"]:
                                        nRunning += 1
                                    elif cer_status == "ready":
                                        nReady += 1
                                    else:
                                        nUnknown += 1
                            tmpMsg = "jediTaskID={} datasetID={} to check due to empty memory requirements :" " nDone={} nActive={} nReady={} ".format(
                                jediTaskID, primaryDatasetID, nDone, nActive, nReady
                            )
                            tmpMsg += f"nRunning={nRunning} nFinished={nFinished} nFailed={nFailed} nUnknown={nUnknown} nLost={nLost} "
                            tmpMsg += "ds.nFilesUsed={} nFilesToBeUsed={} ds.nFilesFinished={} " "ds.nFilesFailed={}".format(
                                cdd_nFilesUsed, cdd_nFilesToBeUsed, cdd_nFilesFinished, cdd_nFilesFailed
                            )
                            tmpLog.debug(tmpMsg)
                            if cdd_nFilesUsed < cdd_nFilesToBeUsed and cdd_nFilesToBeUsed > 0 and nUnknown == 0:
                                varMap = dict()
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = primaryDatasetID
                                varMap[":nFilesUsed"] = nDone + nActive
                                self.cur.execute(sqlUFU + comment, varMap)
                                tmpLog.debug(
                                    "jediTaskID={} datasetID={} set nFilesUsed={} from {} "
                                    "to fix empty memory req".format(jediTaskID, primaryDatasetID, varMap[":nFilesUsed"], cdd_nFilesUsed)
                                )
                            if cdd_nFilesToBeUsed > nDone + nRunning + nReady:
                                varMap = dict()
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = primaryDatasetID
                                varMap[":nFilesToBeUsed"] = nDone + nRunning + nReady
                                self.cur.execute(sqlUFB + comment, varMap)
                                tmpLog.debug(
                                    "jediTaskID={} datasetID={} set nFilesToBeUsed={} from {} "
                                    "to fix empty memory req ".format(jediTaskID, primaryDatasetID, varMap[":nFilesToBeUsed"], cdd_nFilesToBeUsed)
                                )
                            if nActive == 0:
                                varMap = dict()
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = primaryDatasetID
                                varMap[":status"] = "finished"
                                self.cur.execute(sqlUER + comment, varMap)
                                tmpLog.debug(f"jediTaskID={jediTaskID} datasetID={primaryDatasetID} set status=finished to fix empty memory requirements")
                            continue
                        else:
                            # make InputChunks by ram count
                            inputChunks = []
                            for memReq in memReqs:
                                inputChunks.append(InputChunk(taskSpec, ramCount=memReq))
                            # merging
                            if datasetType in JediDatasetSpec.getMergeProcessTypes():
                                for inputChunk in inputChunks:
                                    inputChunk.isMerging = True
                            elif (
                                useJumbo in [JediTaskSpec.enum_useJumbo["running"], JediTaskSpec.enum_useJumbo["pending"]]
                                or (useJumbo == JediTaskSpec.enum_useJumbo["waiting"] and numNewTaskWithJumbo > 0)
                            ) and tmpNumFiles > tmpNumFilesWaiting:
                                # set jumbo flag only to the first chunk
                                if datasetID in dsWithfakeCoJumbo:
                                    if origTaskSpec.useScout() and not origTaskSpec.isPostScout():
                                        tmpLog.debug(f"skip jediTaskID={jediTaskID} datasetID={primaryDatasetID} due to jumbo for scouting")
                                        continue
                                    inputChunks[0].useJumbo = "fake"
                                else:
                                    inputChunks[0].useJumbo = "full"
                                # overwrite tmpNumFiles
                                tmpNumFiles -= tmpNumFilesWaiting
                                if useJumbo == JediTaskSpec.enum_useJumbo["waiting"]:
                                    taskWithNewJumbo = True
                            elif useJumbo == JediTaskSpec.enum_useJumbo["lack"]:
                                inputChunks[0].useJumbo = "only"
                                tmpNumFiles = 1
                            else:
                                # only process merging or jumbo if enough jobs are already generated
                                if maxNumJobs is not None and maxNumJobs <= 0:
                                    tmpLog.debug(f"skip jediTaskID={jediTaskID} datasetID={primaryDatasetID} due to non-merge + enough jobs")
                                    continue
                        # read secondary dataset IDs
                        if not toSkip:
                            # sql to get seconday dataset list
                            sqlDS = f"SELECT datasetID FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID "
                            if not fullSimulation:
                                sqlDS += "AND nFilesToBeUsed >= nFilesUsed AND type IN ("
                            else:
                                sqlDS += "AND type IN ("
                            varMap = {}
                            if datasetType not in JediDatasetSpec.getMergeProcessTypes():
                                # for normal process
                                for tmpType in JediDatasetSpec.getInputTypes():
                                    mapKey = ":type_" + tmpType
                                    varMap[mapKey] = tmpType
                                    sqlDS += f"{mapKey},"
                            else:
                                # for merge process
                                for tmpType in JediDatasetSpec.getMergeProcessTypes():
                                    mapKey = ":type_" + tmpType
                                    varMap[mapKey] = tmpType
                                    sqlDS += f"{mapKey},"
                            sqlDS = sqlDS[:-1]
                            if simTasks is None:
                                sqlDS += ") AND status=:dsStatus "
                                varMap[":dsStatus"] = "ready"
                            else:
                                sqlDS += ") "
                            sqlDS += "AND masterID=:masterID "
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":masterID"] = datasetID
                            # select
                            self.cur.execute(sqlDS + comment, varMap)
                            resSecDsList = self.cur.fetchall()
                            for (tmpDatasetID,) in resSecDsList:
                                datasetIDs.append(tmpDatasetID)
                        # read dataset
                        if not toSkip:
                            for datasetID in datasetIDs:
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                try:
                                    for inputChunk in inputChunks:
                                        # select
                                        self.cur.execute(sqlRD + comment, varMap)
                                        resRD = self.cur.fetchone()
                                        datasetSpec = JediDatasetSpec()
                                        datasetSpec.pack(resRD)
                                        # change stream name for merging
                                        if datasetSpec.type in JediDatasetSpec.getMergeProcessTypes():
                                            # change OUTPUT to IN
                                            datasetSpec.streamName = re.sub("^OUTPUT", "TRN_OUTPUT", datasetSpec.streamName)
                                            # change LOG to INLOG
                                            datasetSpec.streamName = re.sub("^LOG", "TRN_LOG", datasetSpec.streamName)
                                        # add to InputChunk
                                        if datasetSpec.isMaster():
                                            inputChunk.addMasterDS(datasetSpec)
                                        else:
                                            inputChunk.addSecondaryDS(datasetSpec)
                                except Exception:
                                    errType, errValue = sys.exc_info()[:2]
                                    if self.isNoWaitException(errValue):
                                        # resource busy and acquire with NOWAIT specified
                                        toSkip = True
                                        tmpLog.debug(f"skip locked jediTaskID={jediTaskID} datasetID={datasetID}")
                                    else:
                                        # failed with something else
                                        raise errType(errValue)
                            # set useScout
                            if (numAvalanche == 0 and not inputChunks[0].isMutableMaster()) or not taskSpec.useScout() or readMinFiles:
                                for inputChunk in inputChunks:
                                    inputChunk.setUseScout(False)
                            else:
                                for inputChunk in inputChunks:
                                    inputChunk.setUseScout(True)
                        # read job params and files
                        if not toSkip:
                            # read template to generate job parameters
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            self.cur.execute(sqlJobP + comment, varMap)
                            for (clobJobP,) in self.cur:
                                if clobJobP is not None:
                                    taskSpec.jobParamsTemplate = clobJobP
                                break
                            # typical number of files
                            typicalNumFilesPerJob = 5
                            if taskSpec.getNumFilesPerJob() is not None:
                                # the number of files is specified
                                typicalNumFilesPerJob = taskSpec.getNumFilesPerJob()
                            elif taskSpec.getNumEventsPerJob() is not None:
                                typicalNumFilesPerJob = 1
                                try:
                                    if taskSpec.getNumEventsPerJob() > (tmpNumInputEvents // tmpNumInputFiles):
                                        typicalNumFilesPerJob = taskSpec.getNumEventsPerJob() * tmpNumInputFiles // tmpNumInputEvents
                                except Exception:
                                    pass
                                if typicalNumFilesPerJob < 1:
                                    typicalNumFilesPerJob = 1
                            elif (
                                typicalNumFilesMap is not None
                                and taskSpec.processingType in typicalNumFilesMap
                                and typicalNumFilesMap[taskSpec.processingType] > 0
                            ):
                                # typical usage
                                typicalNumFilesPerJob = typicalNumFilesMap[taskSpec.processingType]
                            tmpLog.debug(f"jediTaskID={jediTaskID} typicalNumFilesPerJob={typicalNumFilesPerJob}")
                            # max number of files based on typical usage
                            if maxNumJobs is not None and not inputChunks[0].isMerging and not inputChunks[0].useScout():
                                maxNumFiles = min(nFiles, typicalNumFilesPerJob * maxNumJobs + 10)
                            else:
                                maxNumFiles = nFiles
                            # set lower limit to avoid too fine slashing
                            lowerLimitOnMaxNumFiles = 100
                            if maxNumFiles < lowerLimitOnMaxNumFiles and simTasks is None:
                                maxNumFiles = lowerLimitOnMaxNumFiles
                            # read files
                            readBlock = False
                            if maxNumFiles > tmpNumFiles:
                                maxMasterFilesTobeRead = tmpNumFiles
                            else:
                                # reading with a fix size of block
                                readBlock = True
                                maxMasterFilesTobeRead = maxNumFiles
                            iFiles = {}
                            totalEvents = {}
                            maxFilesTobeReadWithEventRatio = 10000
                            for inputChunk in inputChunks:
                                for datasetID in datasetIDs:
                                    iFiles.setdefault(datasetID, 0)
                                    totalEvents.setdefault(datasetID, [])
                                    # get DatasetSpec
                                    tmpDatasetSpec = inputChunk.getDatasetWithID(datasetID)
                                    # the number of files to be read
                                    if tmpDatasetSpec.isMaster():
                                        maxFilesTobeRead = maxMasterFilesTobeRead
                                    else:
                                        # for secondaries
                                        if taskSpec.useLoadXML() or tmpDatasetSpec.isNoSplit() or tmpDatasetSpec.getEventRatio() is not None:
                                            maxFilesTobeRead = maxFilesTobeReadWithEventRatio
                                        elif tmpDatasetSpec.getNumFilesPerJob() is not None:
                                            maxFilesTobeRead = maxMasterFilesTobeRead * tmpDatasetSpec.getNumFilesPerJob()
                                        else:
                                            maxFilesTobeRead = tmpDatasetSpec.getNumMultByRatio(maxMasterFilesTobeRead)
                                    # minimum read
                                    if readMinFiles:
                                        maxFilesForMinRead = 10
                                        if maxFilesTobeRead > maxFilesForMinRead:
                                            maxFilesTobeRead = maxFilesForMinRead
                                    # number of files to read in this cycle
                                    if tmpDatasetSpec.isMaster():
                                        numFilesTobeReadInCycle = maxFilesTobeRead - iFiles[datasetID]
                                    elif inputChunk.isEmpty:
                                        numFilesTobeReadInCycle = 0
                                    else:
                                        numFilesTobeReadInCycle = maxFilesTobeRead
                                    if tmpDatasetSpec.isSeqNumber():
                                        orderBy = "fileID"
                                    elif not tmpDatasetSpec.isMaster() and taskSpec.reuseSecOnDemand() and not inputChunk.isMerging:
                                        orderBy = "fileID"
                                    elif taskSpec.respectLumiblock() or taskSpec.orderByLB():
                                        orderBy = "lumiBlockNr,lfn"
                                    elif not taskSpec.useLoadXML():
                                        orderBy = "fileID"
                                    else:
                                        orderBy = "boundaryID"
                                    # read files to make FileSpec
                                    iFiles_tmp = 0
                                    iFilesWaiting = 0
                                    for iDup in range(100):  # avoid infinite loop just in case
                                        tmpLog.debug(
                                            "jediTaskID={} to read {} files from datasetID={} in attmpt={} with ramCount={} orderBy={}".format(
                                                jediTaskID, numFilesTobeReadInCycle, datasetID, iDup + 1, inputChunk.ramCount, orderBy
                                            )
                                        )
                                        varMap = {}
                                        varMap[":datasetID"] = datasetID
                                        varMap[":jediTaskID"] = jediTaskID
                                        if not tmpDatasetSpec.toKeepTrack():
                                            if not fullSimulation:
                                                varMap[":status"] = "ready"
                                            if primaryDatasetID not in dsWithfakeCoJumbo:
                                                self.cur.execute(sqlFRNR.format(orderBy, numFilesTobeReadInCycle - iFiles_tmp) + comment, varMap)
                                            else:
                                                self.cur.execute(sqlCJ_FRNR.format(orderBy, numFilesTobeReadInCycle - iFiles_tmp) + comment, varMap)
                                        else:
                                            if not fullSimulation:
                                                if useJumbo == JediTaskSpec.enum_useJumbo["lack"] and origTmpNumFiles == 0:
                                                    varMap[":status"] = "running"
                                                else:
                                                    varMap[":status"] = "ready"
                                                if inputChunk.ramCount not in (None, 0):
                                                    varMap[":ramCount"] = inputChunk.ramCount
                                            if inputChunk.ramCount not in (None, 0):
                                                if primaryDatasetID not in dsWithfakeCoJumbo or useJumbo == JediTaskSpec.enum_useJumbo["lack"]:
                                                    self.cur.execute(sqlFR.format(orderBy, numFilesTobeReadInCycle - iFiles_tmp) + comment, varMap)
                                                else:
                                                    self.cur.execute(sqlCJ_FR.format(orderBy, numFilesTobeReadInCycle - iFiles_tmp) + comment, varMap)
                                            else:  # We group inputChunk.ramCount None and 0 together
                                                if primaryDatasetID not in dsWithfakeCoJumbo or useJumbo == JediTaskSpec.enum_useJumbo["lack"]:
                                                    self.cur.execute(sqlFR_RCNull.format(orderBy, numFilesTobeReadInCycle - iFiles_tmp) + comment, varMap)
                                                else:
                                                    self.cur.execute(sqlCJ_FR_RCNull.format(orderBy, numFilesTobeReadInCycle - iFiles_tmp) + comment, varMap)

                                        resFileList = self.cur.fetchall()
                                        for resFile in resFileList:
                                            # make FileSpec
                                            tmpFileSpec = JediFileSpec()
                                            tmpFileSpec.pack(resFile)
                                            # update file status
                                            if simTasks is None and tmpDatasetSpec.toKeepTrack():
                                                varMap = {}
                                                varMap[":jediTaskID"] = tmpFileSpec.jediTaskID
                                                varMap[":datasetID"] = tmpFileSpec.datasetID
                                                varMap[":fileID"] = tmpFileSpec.fileID
                                                varMap[":nStatus"] = "picked"
                                                varMap[":oStatus"] = "ready"
                                                self.cur.execute(sqlFU + comment, varMap)
                                                nFileRow = self.cur.rowcount
                                                if nFileRow != 1 and not (useJumbo == JediTaskSpec.enum_useJumbo["lack"] and origTmpNumFiles == 0):
                                                    tmpLog.debug(f"skip fileID={tmpFileSpec.fileID} already used by another")
                                                    continue
                                            # add to InputChunk
                                            tmpDatasetSpec.addFile(tmpFileSpec)
                                            iFiles[datasetID] += 1
                                            iFiles_tmp += 1
                                            totalEvents[datasetID].append(tmpFileSpec.getEffectiveNumEvents())
                                            if tmpFileSpec.is_waiting == "Y":
                                                iFilesWaiting += 1

                                        # no reuse
                                        if (
                                            not taskSpec.reuseSecOnDemand()
                                            or tmpDatasetSpec.isMaster()
                                            or taskSpec.useLoadXML()
                                            or tmpDatasetSpec.isNoSplit()
                                            or tmpDatasetSpec.toMerge()
                                            or inputChunk.ramCount not in (None, 0)
                                        ):
                                            break
                                        # enough files were read
                                        if iFiles_tmp >= numFilesTobeReadInCycle and tmpDatasetSpec.getEventRatio() is None:
                                            break
                                        # check if enough events were read
                                        totEvtSecond = 0
                                        secIndex = 0
                                        enoughSecondary = False
                                        if tmpDatasetSpec.getEventRatio() is not None:
                                            enoughSecondary = True
                                            for evtMaster in totalEvents[inputChunk.masterDataset.datasetID]:
                                                targetEvents = evtMaster * tmpDatasetSpec.getEventRatio()
                                                targetEvents = int(math.ceil(targetEvents))
                                                if targetEvents <= 0:
                                                    targetEvents = 1
                                                # count number of secondary events per master file
                                                subTotEvtSecond = 0
                                                for evtSecond in totalEvents[datasetID][secIndex:]:
                                                    subTotEvtSecond += evtSecond
                                                    secIndex += 1
                                                    if subTotEvtSecond >= targetEvents:
                                                        totEvtSecond += targetEvents
                                                        break
                                                if subTotEvtSecond < targetEvents:
                                                    enoughSecondary = False
                                                    break
                                            if not enoughSecondary:
                                                # read more files without making duplication
                                                if iFiles_tmp >= numFilesTobeReadInCycle:
                                                    numFilesTobeReadInCycle += maxFilesTobeReadWithEventRatio
                                                    continue
                                        if enoughSecondary:
                                            break
                                        # duplicate files for reuse
                                        tmpStr = f"jediTaskID={jediTaskID} try to increase files for datasetID={tmpDatasetSpec.datasetID} "
                                        tmpStr += f"since only {iFiles_tmp}/{numFilesTobeReadInCycle} files were read "
                                        if tmpDatasetSpec.getEventRatio() is not None:
                                            tmpStr += "or {0} events is less than {1}*{2} ".format(
                                                totEvtSecond, tmpDatasetSpec.getEventRatio(), sum(totalEvents[inputChunk.masterDataset.datasetID])
                                            )
                                        tmpLog.debug(tmpStr)
                                        if not tmpDatasetSpec.isSeqNumber():
                                            nNewRec = self.duplicateFilesForReuse_JEDI(tmpDatasetSpec)
                                            tmpLog.debug(f"jediTaskID={jediTaskID} {nNewRec} files were duplicated")
                                        else:
                                            nNewRec = self.increaseSeqNumber_JEDI(tmpDatasetSpec, numFilesTobeReadInCycle - iFiles_tmp)
                                            tmpLog.debug(f"jediTaskID={jediTaskID} {nNewRec} seq nums were added")
                                        if nNewRec == 0:
                                            break

                                    if tmpDatasetSpec.isMaster() and iFiles_tmp == 0:
                                        inputChunk.isEmpty = True

                                    if iFiles[datasetID] == 0:
                                        # no input files
                                        if not readMinFiles or not tmpDatasetSpec.isPseudo():
                                            tmpLog.debug(f"jediTaskID={jediTaskID} datasetID={datasetID} has no files to be processed")
                                            # toSkip = True
                                            break
                                    elif (
                                        simTasks is None
                                        and tmpDatasetSpec.toKeepTrack()
                                        and iFiles_tmp != 0
                                        and not (useJumbo == JediTaskSpec.enum_useJumbo["lack"] and origTmpNumFiles == 0)
                                    ):
                                        # update nFilesUsed in DatasetSpec
                                        nFilesUsed = tmpDatasetSpec.nFilesUsed + iFiles[datasetID]
                                        tmpDatasetSpec.nFilesUsed = nFilesUsed
                                        varMap = {}
                                        varMap[":jediTaskID"] = jediTaskID
                                        varMap[":datasetID"] = datasetID
                                        varMap[":nFilesUsed"] = nFilesUsed
                                        varMap[":newnFilesUsed"] = self.cur.var(varNUMBER)
                                        varMap[":newnFilesTobeUsed"] = self.cur.var(varNUMBER)
                                        self.cur.execute(sqlDU + comment, varMap)
                                        # newnFilesUsed = int(varMap[':newnFilesUsed'].getvalue())
                                        # newnFilesTobeUsed = int(varMap[':newnFilesTobeUsed'].getvalue())
                                    tmpLog.debug(
                                        "jediTaskID={2} datasetID={0} has {1} files to be processed for ramCount={3}".format(
                                            datasetID, iFiles_tmp, jediTaskID, inputChunk.ramCount
                                        )
                                    )
                                    # set flag if it is a block read
                                    if tmpDatasetSpec.isMaster():
                                        if readBlock and iFiles[datasetID] == maxFilesTobeRead:
                                            inputChunk.readBlock = True
                                        else:
                                            inputChunk.readBlock = False
                        # add to return
                        if not toSkip:
                            if jediTaskID not in returnMap:
                                returnMap[jediTaskID] = []
                                iTasks += 1
                            for inputChunk in inputChunks:
                                if not inputChunk.isEmpty:
                                    returnMap[jediTaskID].append((taskSpec, cloudName, inputChunk))
                                    iDsPerTask += 1
                                # reduce the number of jobs
                                if maxNumJobs is not None and not inputChunk.isMerging:
                                    maxNumJobs -= int(math.ceil(float(len(inputChunk.masterDataset.Files)) / float(typicalNumFilesPerJob)))
                        else:
                            tmpLog.debug(f"escape due to toSkip for jediTaskID={jediTaskID} datasetID={primaryDatasetID}")
                            break
                        if iDsPerTask > nDsPerTask:
                            break
                        if maxNumJobs is not None and maxNumJobs <= 0:
                            pass
                        # memory check
                        try:
                            memLimit = 1 * 1024
                            memNow = JediCoreUtils.getMemoryUsage()
                            tmpLog.debug(f"memUsage now {memNow} MB pid={os.getpid()}")
                            if memNow - memStart > memLimit:
                                tmpLog.warning(f"memory limit exceeds {memNow}-{memStart} > {memLimit} MB : jediTaskID={jediTaskID}")
                                memoryExceed = True
                                break
                        except Exception:
                            pass
                    if taskWithNewJumbo:
                        numNewTaskWithJumbo -= 1
                if not toSkip:
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                else:
                    tmpLog.debug(f"rollback for jediTaskID={jediTaskID}")
                    # roll back
                    self._rollback()
                # enough tasks
                if iTasks >= nTasks:
                    break
                # already read enough files to generate jobs
                if maxNumJobs is not None and maxNumJobs <= 0:
                    pass
                # memory limit exceeds
                if memoryExceed:
                    break
            tmpLog.debug(f"returning {iTasks} tasks")
            # change map to list
            returnList = []
            for tmpJediTaskID, tmpTaskDsList in returnMap.items():
                returnList.append((tmpJediTaskID, tmpTaskDsList))
            tmpLog.debug(f"memUsage end {JediCoreUtils.getMemoryUsage()} MB pid={os.getpid()}")
            return returnList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # insert JobParamsTemplate
    def insertJobParamsTemplate_JEDI(self, jediTaskID, templ):
        comment = " /* JediDBProxy.insertJobParamsTemplate_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # SQL
            sql = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_JobParams_Template (jediTaskID,jobParamsTemplate) VALUES (:jediTaskID,:templ) "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":templ"] = templ
            # begin transaction
            self.conn.begin()
            # insert
            self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # insert TaskParams
    def insertTaskParams_JEDI(self, vo, prodSourceLabel, userName, taskName, taskParams, parent_tid=None):
        comment = " /* JediDBProxy.insertTaskParams_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f"<userName={userName} taskName={taskName}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to insert task parameters
            sqlT = f"INSERT INTO {jedi_config.db.schemaDEFT}.T_TASK "
            sqlT += "(taskid,status,submit_time,vo,prodSourceLabel,userName,taskName,jedi_task_parameters,parent_tid) VALUES "
            sqlT += f"({jedi_config.db.schemaDEFT}.PRODSYS2_TASK_ID_SEQ.nextval,"
            sqlT += ":status,CURRENT_DATE,:vo,:prodSourceLabel,:userName,:taskName,:param,"
            if parent_tid is None:
                sqlT += f"{jedi_config.db.schemaDEFT}.PRODSYS2_TASK_ID_SEQ.currval) "
            else:
                sqlT += ":parent_tid) "
            sqlT += "RETURNING taskid INTO :jediTaskID"
            # begin transaction
            self.conn.begin()
            # insert task parameters
            varMap = {}
            varMap[":vo"] = vo
            varMap[":param"] = taskParams
            varMap[":status"] = "waiting"
            varMap[":userName"] = userName
            varMap[":taskName"] = taskName
            if parent_tid is not None:
                varMap[":parent_tid"] = parent_tid
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":jediTaskID"] = self.cur.var(varNUMBER)
            self.cur.execute(sqlT + comment, varMap)
            val = self.getvalue_corrector(self.cur.getvalue(varMap[":jediTaskID"]))
            jediTaskID = int(val)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmpLog.debug(f"done new jediTaskID={jediTaskID}")
            return True, jediTaskID
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, None

    # insert new TaskParams and update parent TaskParams. mainly used by TaskGenerator
    def insertUpdateTaskParams_JEDI(self, jediTaskID, vo, prodSourceLabel, updateTaskParams, insertTaskParamsList):
        comment = " /* JediDBProxy.insertUpdateTaskParams_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f"<jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to insert new task parameters
            sqlIT = f"INSERT INTO {jedi_config.db.schemaDEFT}.T_TASK "
            sqlIT += "(taskid,status,submit_time,vo,prodSourceLabel,jedi_task_parameters,parent_tid) VALUES "
            sqlIT += f"({jedi_config.db.schemaDEFT}.PRODSYS2_TASK_ID_SEQ.nextval,"
            sqlIT += ":status,CURRENT_DATE,:vo,:prodSourceLabel,:param,:parent_tid) "
            sqlIT += "RETURNING taskid INTO :jediTaskID"
            # sql to update parent task parameters
            sqlUT = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_TaskParams SET taskParams=:taskParams "
            sqlUT += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # insert task parameters
            newJediTaskIDs = []
            for taskParams in insertTaskParamsList:
                varMap = {}
                varMap[":vo"] = vo
                varMap[":param"] = taskParams
                varMap[":status"] = "waiting"
                varMap[":parent_tid"] = jediTaskID
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":jediTaskID"] = self.cur.var(varNUMBER)
                self.cur.execute(sqlIT + comment, varMap)
                val = self.getvalue_corrector(self.cur.getvalue(varMap[":jediTaskID"]))
                newJediTaskID = int(val)
                newJediTaskIDs.append(newJediTaskID)
            # update task parameters
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":taskParams"] = updateTaskParams
            self.cur.execute(sqlUT + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done new jediTaskIDs={str(newJediTaskIDs)}")
            return True, newJediTaskIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, None

    # reset unused files
    def resetUnusedFiles_JEDI(self, jediTaskID, inputChunk):
        comment = " /* JediDBProxy.resetUnusedFiles_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            nFileRowMaster = 0
            # sql to rollback files
            sql = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents SET status=:nStatus "
            sql += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:oStatus "
            if inputChunk.ramCount in (None, 0):
                sql += "AND (ramCount IS NULL OR ramCount=:ramCount) "
            else:
                sql += "AND ramCount=:ramCount "
            # sql to reset nFilesUsed
            sqlD = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET nFilesUsed=nFilesUsed-:nFileRow "
            sqlD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # begin transaction
            self.conn.begin()
            for datasetSpec in inputChunk.getDatasets(includePseudo=True):
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetSpec.datasetID
                varMap[":nStatus"] = "ready"
                varMap[":oStatus"] = "picked"
                varMap[":ramCount"] = inputChunk.ramCount
                # update contents
                self.cur.execute(sql + comment, varMap)
                nFileRow = self.cur.rowcount
                tmpLog.debug(f"reset {nFileRow} rows for datasetID={datasetSpec.datasetID}")
                if nFileRow > 0:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetSpec.datasetID
                    varMap[":nFileRow"] = nFileRow
                    # update dataset
                    self.cur.execute(sqlD + comment, varMap)
                    if datasetSpec.isMaster():
                        nFileRowMaster = nFileRow
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return nFileRowMaster
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return 0

    # set missing files
    def setMissingFiles_JEDI(self, jediTaskID, datasetID, fileIDs):
        comment = " /* JediDBProxy.setMissingFiles_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} datasetID={datasetID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to set missing files
            sqlF = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents SET status=:nStatus "
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID and status<>:nStatus"
            # sql to set nFilesFailed
            sqlD = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET nFilesFailed=nFilesFailed+:nFileRow "
            sqlD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # begin transaction
            self.conn.begin()
            nFileRow = 0
            # update contents
            for fileID in fileIDs:
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                varMap[":nStatus"] = "missing"
                self.cur.execute(sqlF + comment, varMap)
                nRow = self.cur.rowcount
                nFileRow += nRow
            # update dataset
            if nFileRow > 0:
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":nFileRow"] = nFileRow
                self.cur.execute(sqlD + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done set {nFileRow} missing files")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # rescue picked files
    def rescuePickedFiles_JEDI(self, vo, prodSourceLabel, waitTime):
        comment = " /* JediDBProxy.rescuePickedFiles_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get orphaned tasks
            sqlTR = "SELECT jediTaskID,lockedBy "
            sqlTR += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTR += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTR += "AND tabT.status IN (:status1,:status2,:status3,:status4) AND lockedBy IS NOT NULL AND lockedTime<:timeLimit "
            if vo not in [None, "any"]:
                sqlTR += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                sqlTR += "AND prodSourceLabel=:prodSourceLabel "
            # sql to get picked datasets
            sqlDP = f"SELECT datasetID FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlDP += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2,:type3,:type4,:type5) "
            # sql to rollback files
            sqlF = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents SET status=:nStatus "
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:oStatus AND keepTrack=:keepTrack "
            # sql to reset nFilesUsed
            sqlDU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET nFilesUsed=nFilesUsed-:nFileRow "
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to unlock task
            sqlTU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET lockedBy=NULL,lockedTime=NULL "
            sqlTU += "WHERE jediTaskID=:jediTaskID "
            # sql to re-lock task
            sqlRL = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET lockedTime=CURRENT_DATE "
            sqlRL += "WHERE jediTaskID=:jediTaskID AND lockedBy=:lockedBy AND lockedTime<:timeLimit "
            # sql to re-lock task with nowait
            sqlNW = f"SELECT jediTaskID FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlNW += "WHERE jediTaskID=:jediTaskID AND lockedBy=:lockedBy AND lockedTime<:timeLimit "
            sqlNW += "FOR UPDATE NOWAIT "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=waitTime)
            # get orphaned tasks
            varMap = {}
            varMap[":status1"] = "ready"
            varMap[":status2"] = "scouting"
            varMap[":status3"] = "running"
            varMap[":status4"] = "merging"
            varMap[":timeLimit"] = timeLimit
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
            self.cur.execute(sqlTR + comment, varMap)
            resTaskList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all tasks
            nTasks = 0
            for jediTaskID, lockedBy in resTaskList:
                tmpLog.debug(f"[jediTaskID={jediTaskID}] rescue")
                self.conn.begin()
                # re-lock the task with NOWAIT
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":lockedBy"] = lockedBy
                varMap[":timeLimit"] = timeLimit
                toSkip = False
                try:
                    self.cur.execute(sqlNW + comment, varMap)
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        tmpLog.debug(f"[jediTaskID={jediTaskID}] skip to rescue since locked by another")
                        toSkip = True
                    else:
                        # failed with something else
                        raise errType(errValue)
                if not toSkip:
                    # re-lock the task
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":lockedBy"] = lockedBy
                    varMap[":timeLimit"] = timeLimit
                    self.cur.execute(sqlRL + comment, varMap)
                    nRow = self.cur.rowcount
                    if nRow == 0:
                        tmpLog.debug(f"[jediTaskID={jediTaskID}] skip to rescue since failed to re-lock")
                    else:
                        # get input datasets
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":type1"] = "input"
                        varMap[":type2"] = "trn_log"
                        varMap[":type3"] = "trn_output"
                        varMap[":type4"] = "pseudo_input"
                        varMap[":type5"] = "random_seed"
                        self.cur.execute(sqlDP + comment, varMap)
                        resDatasetList = self.cur.fetchall()
                        # loop over all input datasets
                        for (datasetID,) in resDatasetList:
                            # update contents
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = datasetID
                            varMap[":nStatus"] = "ready"
                            varMap[":oStatus"] = "picked"
                            varMap[":keepTrack"] = 1
                            self.cur.execute(sqlF + comment, varMap)
                            nFileRow = self.cur.rowcount
                            tmpLog.debug(f"[jediTaskID={jediTaskID}] reset {nFileRow} rows for datasetID={datasetID}")
                            if nFileRow > 0:
                                # reset nFilesUsed
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":nFileRow"] = nFileRow
                                self.cur.execute(sqlDU + comment, varMap)
                        # unlock task
                        tmpLog.debug(f"[jediTaskID={jediTaskID}] unlock")
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        self.cur.execute(sqlTU + comment, varMap)
                        nRows = self.cur.rowcount
                        tmpLog.debug(f"[jediTaskID={jediTaskID}] done with nRows={nRows}")
                        if nRows == 1:
                            nTasks += 1
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return nTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # rescue unlocked tasks with picked files
    def rescueUnLockedTasksWithPicked_JEDI(self, vo, prodSourceLabel, waitTime, pid):
        comment = " /* JediDBProxy.rescueUnLockedTasksWithPicked_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            timeToCheck = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=waitTime)
            varMap = {}
            varMap[":taskstatus1"] = "running"
            varMap[":taskstatus2"] = "scouting"
            varMap[":taskstatus3"] = "ready"
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":timeLimit"] = timeToCheck
            # sql to get tasks and datasetsto be checked
            sqlRL = "SELECT tabT.jediTaskID,tabD.datasetID "
            sqlRL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA,{0}.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI)
            sqlRL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlRL += "AND tabT.jediTaskID=tabD.jediTaskID "
            sqlRL += "AND tabT.status IN (:taskstatus1,:taskstatus2,:taskstatus3) AND prodSourceLabel=:prodSourceLabel "
            sqlRL += "AND tabT.lockedBy IS NULL AND tabT.lockedTime IS NULL "
            sqlRL += "AND tabT.modificationTime<:timeLimit "
            sqlRL += "AND (tabT.rescueTime IS NULL OR tabT.rescueTime<:timeLimit) "
            if vo is not None:
                sqlRL += "AND tabT.vo=:vo "
                varMap[":vo"] = vo
            sqlRL += "AND tabT.lockedBy IS NULL "
            sqlRL += "AND tabD.masterID IS NULL AND tabD.nFilesTobeUsed=tabD.nFilesUsed "
            sqlRL += "AND tabD.nFilesTobeUsed>0 AND tabD.nFilesTobeUsed>(tabD.nFilesFinished+tabD.nFilesFailed) "
            sqlRL += "AND tabD.type IN ("
            for tmpType in JediDatasetSpec.getProcessTypes():
                mapKey = ":type_" + tmpType
                sqlRL += f"{mapKey},"
                varMap[mapKey] = tmpType
            sqlRL = sqlRL[:-1]
            sqlRL += ") "
            # sql to check if there is picked file
            sqlDP = f"SELECT * FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlDP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:fileStatus AND rownum<2 "
            # sql to set dummy lock to task
            sqlTU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET lockedBy=:lockedBy,lockedTime=:lockedTime,rescueTime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL AND lockedTime IS NULL "
            sqlTU += "AND modificationTime<:timeLimit "
            # sql to lock task with nowait
            sqlNW = f"SELECT jediTaskID FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlNW += "WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL AND lockedTime IS NULL "
            sqlNW += "AND (rescueTime IS NULL OR rescueTime<:timeLimit) "
            sqlNW += "FOR UPDATE NOWAIT "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            self.cur.execute(sqlRL + comment, varMap)
            resTaskList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            taskDsMap = dict()
            for jediTaskID, datasetID in resTaskList:
                if jediTaskID not in taskDsMap:
                    taskDsMap[jediTaskID] = []
                taskDsMap[jediTaskID].append(datasetID)
            tmpLog.debug(f"got {len(taskDsMap)} tasks")
            # loop over all tasks
            ngTasks = set()
            for jediTaskID, datasetIDs in taskDsMap.items():
                self.conn.begin()
                # lock task
                toSkip = False
                try:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":timeLimit"] = timeToCheck
                    self.cur.execute(sqlNW + comment, varMap)
                    resNW = self.cur.fetchone()
                    if resNW is None:
                        tmpLog.debug(f"[jediTaskID={jediTaskID} datasetID={datasetID}] skip since checked by another")
                        toSkip = True
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        tmpLog.debug(f"[jediTaskID={jediTaskID} datasetID={datasetID}] skip since locked by another")
                        toSkip = True
                    else:
                        # failed with something else
                        raise errType(errValue)
                if not toSkip:
                    # loop over all datasets
                    allOK = True
                    for datasetID in datasetIDs:
                        tmpLog.debug(f"[jediTaskID={jediTaskID} datasetID={datasetID}] to check")
                        # check if there is picked file
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":fileStatus"] = "picked"
                        self.cur.execute(sqlDP + comment, varMap)
                        resDP = self.cur.fetchone()
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":timeLimit"] = timeToCheck
                        if resDP is not None:
                            allOK = False
                            break
                    # set lock
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":timeLimit"] = timeToCheck
                    if allOK:
                        # OK
                        varMap[":lockedBy"] = None
                        varMap[":lockedTime"] = None
                    else:
                        varMap[":lockedBy"] = pid
                        varMap[":lockedTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=24)
                        tmpLog.debug(f"[jediTaskID={jediTaskID} datasetID={datasetID}] set dummy lock to trigger rescue")
                        ngTasks.add(jediTaskID)
                    self.cur.execute(sqlTU + comment, varMap)
                    nRow = self.cur.rowcount
                    tmpLog.debug(f"[jediTaskID={jediTaskID}] done with {nRow}")
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            nTasks = len(ngTasks)
            tmpLog.debug(f"done {nTasks} stuck tasks")
            return nTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # unlock tasks
    def unlockTasks_JEDI(self, vo, prodSourceLabel, waitTime, hostName, pgid):
        comment = " /* JediDBProxy.unlockTasks_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel} host={hostName} pgid={pgid}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to look for locked tasks
            sqlTR = f"SELECT jediTaskID,lockedBy,lockedTime FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTR += "WHERE lockedBy IS NOT NULL AND lockedTime<:timeLimit "
            if vo not in [None, "", "any"]:
                sqlTR += "AND vo=:vo "
            if prodSourceLabel not in [None, "", "any"]:
                sqlTR += "AND prodSourceLabel=:prodSourceLabel "
            if hostName is not None:
                sqlTR += "AND lockedBy LIKE :patt "
            # sql to unlock
            sqlTU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET lockedBy=NULL,lockedTime=NULL "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND lockedBy=:lockedBy AND lockedTime<:timeLimit "
            timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            # sql to get picked datasets
            sqlDP = f"SELECT datasetID FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlDP += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2,:type3,:type4,:type5) "
            # sql to rollback files
            sqlF = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents SET status=:nStatus "
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:oStatus AND keepTrack=:keepTrack "
            # sql to reset nFilesUsed
            sqlDU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET nFilesUsed=nFilesUsed-:nFileRow "
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 1000
            # get locked task list
            timeLimit = timeNow - datetime.timedelta(minutes=waitTime)
            varMap = {}
            varMap[":timeLimit"] = timeLimit
            if vo not in [None, "", "any"]:
                varMap[":vo"] = vo
            if prodSourceLabel not in [None, "", "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
            if hostName is not None:
                varMap[":patt"] = f"{hostName}-%"
            self.cur.execute(sqlTR + comment, varMap)
            taskList = self.cur.fetchall()
            # unlock tasks
            nTasks = 0
            for jediTaskID, lockedBy, lockedTime in taskList:
                # extract PID
                if hostName is not None:
                    # hostname mismatch
                    if not lockedBy.startswith(hostName):
                        continue
                    tmpMatch = re.search(f"^{hostName}-\\d+_(\\d+)-", lockedBy)
                    # no PGID
                    if tmpMatch is None:
                        continue
                    tmpPGID = int(tmpMatch.group(1))
                    # active process
                    if tmpPGID == pgid:
                        continue
                varMap = {}
                varMap[":lockedBy"] = lockedBy
                varMap[":timeLimit"] = timeLimit
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlTU + comment, varMap)
                iTasks = self.cur.rowcount
                if iTasks == 1:
                    tmpLog.debug(f"unlocked jediTaskID={jediTaskID} lockedBy={lockedBy} lockedTime={lockedTime}")
                    # get input datasets
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":type1"] = "input"
                    varMap[":type2"] = "trn_log"
                    varMap[":type3"] = "trn_output"
                    varMap[":type4"] = "pseudo_input"
                    varMap[":type5"] = "random_seed"
                    self.cur.execute(sqlDP + comment, varMap)
                    resDatasetList = self.cur.fetchall()
                    # loop over all input datasets
                    for (datasetID,) in resDatasetList:
                        # update contents
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":nStatus"] = "ready"
                        varMap[":oStatus"] = "picked"
                        varMap[":keepTrack"] = 1
                        self.cur.execute(sqlF + comment, varMap)
                        nFileRow = self.cur.rowcount
                        tmpLog.debug(f"unlocked jediTaskID={jediTaskID} released {nFileRow} rows for datasetID={datasetID}")
                        if nFileRow > 0:
                            # reset nFilesUsed
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = datasetID
                            varMap[":nFileRow"] = nFileRow
                            self.cur.execute(sqlDU + comment, varMap)
                nTasks += iTasks
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {nTasks} tasks")
            return nTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get the size of input files which will be copied to the site
    def getMovingInputSize_JEDI(self, siteName):
        comment = " /* JediDBProxy.getMovingInputSize_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" site={siteName}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get size
            sql = f"SELECT SUM(inputFileBytes)/1024/1024/1024 FROM {jedi_config.db.schemaPANDA}.jobsDefined4 "
            sql += "WHERE computingSite=:computingSite "
            # begin transaction
            self.conn.begin()
            varMap = {}
            varMap[":computingSite"] = siteName
            # exec
            self.cur.execute(sql + comment, varMap)
            resSum = self.cur.fetchone()
            retVal = 0
            if resSum is not None:
                (retVal,) = resSum
            if retVal is None:
                retVal = 0
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get typical number of input files for each gshare+processingType
    def getTypicalNumInput_JEDI(self, vo, prodSourceLabel, workQueue):
        comment = " /* JediDBProxy.getTypicalNumInput_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" vo={vo} label={prodSourceLabel} queue={workQueue.queue_name}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")

        try:
            # sql to get size
            var_map = {}
            var_map[":vo"] = vo
            var_map[":prodSourceLabel"] = prodSourceLabel
            sql = f"SELECT processingtype, nInputDataFiles FROM {jedi_config.db.schemaPANDA}.typical_num_input "
            sql += "WHERE vo=:vo AND agg_type=:agg_type AND agg_key=:agg_key AND prodSourceLabel=:prodSourceLabel "

            if workQueue.is_global_share:
                var_map[":agg_type"] = "gshare"
                var_map[":agg_key"] = workQueue.queue_name
            else:
                var_map[":agg_type"] = "workqueue"
                var_map[":agg_key"] = str(workQueue.queue_id)

            # sql to get config
            sqlC = "SELECT key,value FROM ATLAS_PANDA.CONFIG "
            sqlC += "WHERE app=:app AND component=:component AND vo=:vo AND key LIKE :patt "

            # begin transaction
            self.conn.begin()

            # get values from cache
            self.cur.execute(sql + comment, var_map)
            resList = self.cur.fetchall()
            retMap = {}
            for processingType, numFile in resList:
                if numFile is None:
                    numFile = 0
                retMap[processingType] = int(math.ceil(numFile))

            # get from DB config
            var_map = {}
            var_map[":vo"] = vo
            var_map[":app"] = "jedi"
            var_map[":component"] = "jobgen"
            var_map[":patt"] = f"TYPNFILES_{prodSourceLabel}_%"
            self.cur.execute(sqlC + comment, var_map)
            resC = self.cur.fetchall()
            for tmpKey, tmpVal in resC:
                tmpItems = tmpKey.split("_")
                if len(tmpItems) != 4:
                    continue
                confWorkQueue = tmpItems[2]
                confProcessingType = tmpItems[3]
                if confWorkQueue != "" and confWorkQueue != workQueue.queue_name:
                    continue
                retMap[confProcessingType] = int(tmpVal)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            # use predefined values from config file
            tmpLog.debug(hasattr(jedi_config.jobgen, "typicalNumFile"))
            try:
                if hasattr(jedi_config.jobgen, "typicalNumFile"):
                    for tmpItem in jedi_config.jobgen.typicalNumFile.split(","):
                        confVo, confProdSourceLabel, confWorkQueue, confProcessingType, confNumFiles = tmpItem.split(":")
                        if vo != confVo and confVo not in [None, "", "any"]:
                            continue
                        if prodSourceLabel != confProdSourceLabel and confProdSourceLabel not in [None, "", "any"]:
                            continue
                        if workQueue != confWorkQueue and confWorkQueue not in [None, "", "any"]:
                            continue
                        retMap[confProcessingType] = int(confNumFiles)
            except Exception:
                pass
            tmpLog.debug(f"done -> {retMap}")

            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get highest prio jobs with workQueueID
    def getHighestPrioJobStat_JEDI_OLD(self, prodSourceLabel, cloudName, workQueue, resource_name=None):
        comment = " /* JediDBProxy.getHighestPrioJobStat_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <cloud={cloudName} queue={workQueue.queue_name} resource_name={resource_name}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        varMapO = {}
        varMapO[":cloud"] = cloudName
        varMapO[":prodSourceLabel"] = prodSourceLabel

        # sqlS has the where conditions
        sqlS = "WHERE prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) "
        sqlS += "AND processingType<>:pmerge AND (eventService IS NULL OR eventService<>:esmerge) "
        sqlS += "AND cloud=:cloud "
        if resource_name:
            sqlS += "AND resource_type=:resource_type "
            varMapO[":resource_type"] = resource_name
        if workQueue.is_global_share:
            sqlS += "AND gshare=:wq_name "
            sqlS += "AND workqueue_id IN ("
            sqlS += "SELECT UNIQUE workqueue_id FROM {0} "
            sqlS += "MINUS "
            sqlS += "SELECT queue_id FROM atlas_panda.jedi_work_queue WHERE queue_function = 'Resource') "
            varMapO[":wq_name"] = workQueue.queue_name
        else:
            sqlS += "AND workQueue_ID=:wq_id "
            varMapO[":wq_id"] = workQueue.queue_id

        # sql for highest current priority
        sql0 = "SELECT max(currentPriority) FROM {0} "
        sql0 += sqlS

        # sqlC counts the jobs with highest priority
        sqlC = "SELECT COUNT(*) FROM {0} "
        sqlC += sqlS
        sqlC += "AND currentPriority=:currentPriority"

        tables = [f"{jedi_config.db.schemaPANDA}.jobsActive4", f"{jedi_config.db.schemaPANDA}.jobsDefined4"]

        # make return map
        prioKey = "highestPrio"
        nNotRunKey = "nNotRun"
        retMap = {prioKey: 0, nNotRunKey: 0}
        try:
            for table in tables:
                # start transaction
                self.conn.begin()
                varMap = copy.copy(varMapO)
                # select
                if table == f"{jedi_config.db.schemaPANDA}.jobsActive4":
                    varMap[":jobStatus1"] = "activated"
                    varMap[":jobStatus2"] = "dummy"
                else:
                    varMap[":jobStatus1"] = "defined"
                    varMap[":jobStatus2"] = "assigned"
                varMap[":pmerge"] = "pmerge"
                varMap[":esmerge"] = EventServiceUtils.esMergeJobFlagNumber
                self.cur.arraysize = 100
                tmpLog.debug((sql0 + comment).format(table) + str(varMap))
                self.cur.execute((sql0 + comment).format(table), varMap)
                res = self.cur.fetchone()
                # if there is a job
                if res is not None and res[0] is not None:
                    maxPriority = res[0]
                    getNumber = False
                    if retMap[prioKey] < maxPriority:
                        retMap[prioKey] = maxPriority
                        # reset
                        retMap[nNotRunKey] = 0
                        getNumber = True
                    elif retMap[prioKey] == maxPriority:
                        getNumber = True
                    # get number of jobs with highest prio
                    if getNumber:
                        varMap[":currentPriority"] = maxPriority
                        self.cur.arraysize = 10
                        tmpLog.debug((sqlC + comment).format(table))
                        self.cur.execute((sqlC + comment).format(table), varMap)
                        resC = self.cur.fetchone()
                        retMap[nNotRunKey] += resC[0]
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            # return
            tmpLog.debug(str(retMap))
            return True, retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, None

    # get highest prio jobs with workQueueID
    def getHighestPrioJobStat_JEDI(self, prodSourceLabel, cloudName, workQueue, resource_name=None):
        comment = " /* JediDBProxy.getHighestPrioJobStat_JEDI */"
        method_name = self.getMethodName(comment)
        method_name += f" <cloud={cloudName} queue={workQueue.queue_name} resource_type={resource_name}>"
        tmp_log = MsgWrapper(logger, method_name)
        tmp_log.debug("start")
        var_map = {}
        var_map[":cloud"] = cloudName
        var_map[":prodSourceLabel"] = prodSourceLabel

        sql_sum = f"SELECT MAX_PRIORITY, SUM(MAX_PRIORITY_COUNT) FROM {jedi_config.db.schemaPANDA}.JOB_STATS_HP "
        sql_max = f"SELECT MAX(MAX_PRIORITY) FROM {jedi_config.db.schemaPANDA}.JOB_STATS_HP "

        sql_where = "WHERE prodSourceLabel=:prodSourceLabel AND cloud=:cloud "

        if resource_name:
            sql_where += "AND resource_type=:resource_type "
            var_map[":resource_type"] = resource_name

        if workQueue.is_global_share:
            sql_where += "AND gshare=:wq_name "
            sql_where += "AND workqueue_id IN ("
            sql_where += f"SELECT UNIQUE workqueue_id FROM {jedi_config.db.schemaPANDA}.JOB_STATS_HP "
            sql_where += "MINUS "
            sql_where += f"SELECT queue_id FROM {jedi_config.db.schemaPANDA}.jedi_work_queue WHERE queue_function = 'Resource') "
            var_map[":wq_name"] = workQueue.queue_name
        else:
            sql_where += "AND workQueue_ID=:wq_id "
            var_map[":wq_id"] = workQueue.queue_id

        sql_max += sql_where
        sql_where += f"AND MAX_PRIORITY=({sql_max}) "
        sql_where += "GROUP BY MAX_PRIORITY"
        sql_sum += sql_where

        # make return map
        max_priority_tag = "highestPrio"
        max_priority_count_tag = "nNotRun"
        ret_map = {max_priority_tag: 0, max_priority_count_tag: 0}

        try:
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 100

            tmp_log.debug((sql_sum + comment) + str(var_map))
            self.cur.execute((sql_sum + comment), var_map)
            res = self.cur.fetchone()
            if res:
                max_priority, count = res
                if max_priority and count:  # otherwise leave it to 0
                    ret_map[max_priority_tag] = max_priority
                    ret_map[max_priority_count_tag] = count

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(str(ret_map))
            return True, ret_map
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmp_log)
            return False, None

    # get the list of tasks to refine
    def getTasksToRefine_JEDI(self, vo=None, prodSourceLabel=None):
        comment = " /* JediDBProxy.getTasksToRefine_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retTaskIDs = []
        try:
            # sql to get jediTaskIDs to refine from the command table
            sqlC = f"SELECT taskid,parent_tid FROM {jedi_config.db.schemaDEFT}.T_TASK "
            sqlC += "WHERE status=:status "
            varMap = {}
            varMap[":status"] = "waiting"
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlC += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlC += "AND prodSourceLabel=:prodSourceLabel "
            sqlC += "ORDER BY submit_time "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            tmpLog.debug(sqlC + comment + str(varMap))
            self.cur.execute(sqlC + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"got {len(resList)} tasks")
            for jediTaskID, parent_tid in resList:
                tmpLog.debug(f"start jediTaskID={jediTaskID}")
                # start transaction
                self.conn.begin()
                # lock
                varMap = {}
                varMap[":taskid"] = jediTaskID
                varMap[":status"] = "waiting"
                sqlLock = f"SELECT taskid FROM {jedi_config.db.schemaDEFT}.T_TASK WHERE taskid=:taskid AND status=:status "
                sqlLock += "FOR UPDATE "
                toSkip = False
                try:
                    tmpLog.debug(sqlLock + comment + str(varMap))
                    self.cur.execute(sqlLock + comment, varMap)
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        # resource busy and acquire with NOWAIT specified
                        toSkip = True
                        tmpLog.debug(f"skip locked jediTaskID={jediTaskID}")
                    else:
                        # failed with something else
                        raise errType(errValue)
                if not toSkip:
                    resLock = self.cur.fetchone()
                    if resLock is None:
                        # already processed
                        toSkip = True
                        tmpLog.debug(f"skip jediTaskID={jediTaskID} already processed")
                isOK = True
                if not toSkip:
                    if isOK:
                        # insert task to JEDI
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        import uuid

                        varMap[":taskName"] = str(uuid.uuid4())
                        varMap[":status"] = "registered"
                        varMap[":userName"] = "tobeset"
                        varMap[":parent_tid"] = parent_tid
                        sqlIT = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Tasks "
                        sqlIT += "(jediTaskID,taskName,status,userName,creationDate,modificationTime,parent_tid,stateChangeTime"
                        if vo is not None:
                            sqlIT += ",vo"
                        if prodSourceLabel is not None:
                            sqlIT += ",prodSourceLabel"
                        sqlIT += ") "
                        sqlIT += "VALUES(:jediTaskID,:taskName,:status,:userName,CURRENT_DATE,CURRENT_DATE,:parent_tid,CURRENT_DATE"
                        if vo is not None:
                            sqlIT += ",:vo"
                            varMap[":vo"] = vo
                        if prodSourceLabel is not None:
                            sqlIT += ",:prodSourceLabel"
                            varMap[":prodSourceLabel"] = prodSourceLabel
                        sqlIT += ") "
                        try:
                            tmpLog.debug(sqlIT + comment + str(varMap))
                            self.cur.execute(sqlIT + comment, varMap)
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            tmpLog.error(f"failed to insert jediTaskID={jediTaskID} with {errtype} {errvalue}")
                            isOK = False
                            try:
                                # delete task and param until DEFT bug is fixed
                                tmpLog.debug(f"trying to delete jediTaskID={jediTaskID}")
                                # check status
                                sqlDelCK = f"SELECT status FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
                                sqlDelCK += "WHERE jediTaskID=:jediTaskID "
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                self.cur.execute(sqlDelCK + comment, varMap)
                                resDelCK = self.cur.fetchone()
                                if resDelCK is not None:
                                    (delStatus,) = resDelCK
                                else:
                                    delStatus = None
                                # get size of DEFT param
                                sqlDelDZ = f"SELECT LENGTH(jedi_task_parameters) FROM {jedi_config.db.schemaDEFT}.T_TASK "
                                sqlDelDZ += "WHERE taskid=:jediTaskID "
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                self.cur.execute(sqlDelDZ + comment, varMap)
                                resDelDZ = self.cur.fetchone()
                                if resDelDZ is not None:
                                    (delDeftSize,) = resDelDZ
                                else:
                                    delDeftSize = None
                                # get size of JEDI param
                                sqlDelJZ = f"SELECT LENGTH(taskParams) FROM {jedi_config.db.schemaJEDI}.JEDI_TaskParams "
                                sqlDelJZ += "WHERE jediTaskID=:jediTaskID "
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                self.cur.execute(sqlDelJZ + comment, varMap)
                                resDelJZ = self.cur.fetchone()
                                if resDelJZ is not None:
                                    (delJediSize,) = resDelJZ
                                else:
                                    delJediSize = None
                                tmpLog.debug(f"jediTaskID={jediTaskID} has status={delStatus} param size in DEFT {delDeftSize} vs in JEDI {delJediSize}")
                                # delete
                                if delStatus == "registered" and delDeftSize != delJediSize and delJediSize == 2000:
                                    sqlDelJP = f"DELETE FROM {jedi_config.db.schemaJEDI}.JEDI_TaskParams "
                                    sqlDelJP += "WHERE jediTaskID=:jediTaskID "
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    self.cur.execute(sqlDelJP + comment, varMap)
                                    nRowP = self.cur.rowcount
                                    tmpLog.debug(f"deleted param for jediTaskID={jediTaskID} with {nRowP}")
                                    sqlDelJT = f"DELETE FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
                                    sqlDelJT += "WHERE jediTaskID=:jediTaskID ".format(jedi_config.db.schemaJEDI)
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    self.cur.execute(sqlDelJT + comment, varMap)
                                    nRowT = self.cur.rowcount
                                    tmpLog.debug(f"deleted task for jediTaskID={jediTaskID} with {nRowT}")
                                    if nRowP == 1 and nRowT == 1:
                                        # commit
                                        if not self._commit():
                                            raise RuntimeError("Commit error")
                                        # continue to skip subsequent rollback
                                        continue
                            except Exception:
                                errtype, errvalue = sys.exc_info()[:2]
                                tmpLog.error(f"failed to delete jediTaskID={jediTaskID} with {errtype} {errvalue}")
                    if isOK:
                        # check task parameters
                        varMap = {}
                        varMap[":taskid"] = jediTaskID
                        sqlTC = f"SELECT taskid FROM {jedi_config.db.schemaDEFT}.T_TASK WHERE taskid=:taskid "
                        tmpLog.debug(sqlTC + comment + str(varMap))
                        self.cur.execute(sqlTC + comment, varMap)
                        resTC = self.cur.fetchone()
                        if resTC is None or resTC[0] is None:
                            tmpLog.error("task parameters not found in T_TASK")
                            isOK = False
                    if isOK:
                        # copy task parameters
                        varMap = {}
                        varMap[":taskid"] = jediTaskID
                        sqlPaste = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_TaskParams (jediTaskID,taskParams) "
                        sqlPaste += "VALUES(:taskid,:taskParams) "
                        sqlSize = f"SELECT LENGTH(jedi_task_parameters) FROM {jedi_config.db.schemaDEFT}.T_TASK "
                        sqlSize += "WHERE taskid=:taskid "
                        sqlCopy = f"SELECT jedi_task_parameters FROM {jedi_config.db.schemaDEFT}.T_TASK "
                        sqlCopy += "WHERE taskid=:taskid "
                        try:
                            # get size
                            self.cur.execute(sqlSize + comment, varMap)
                            (totalSize,) = self.cur.fetchone()
                            # decomposed to SELECT and INSERT since sometimes oracle truncated params
                            tmpLog.debug(sqlCopy + comment + str(varMap))
                            self.cur.execute(sqlCopy + comment, varMap)
                            retStr = ""
                            for (tmpItem,) in self.cur:
                                retStr = tmpItem
                                break
                            # check size
                            if len(retStr) != totalSize:
                                raise RuntimeError(f"taskParams was truncated {len(retStr)}/{totalSize} bytes")
                            varMap = {}
                            varMap[":taskid"] = jediTaskID
                            varMap[":taskParams"] = retStr
                            self.cur.execute(sqlPaste + comment, varMap)
                            tmpLog.debug(f"inserted taskParams for jediTaskID={jediTaskID} {len(retStr)}/{totalSize}")
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            tmpLog.error(f"failed to insert param for jediTaskID={jediTaskID} with {errtype} {errvalue}")
                            isOK = False
                    # update
                    if isOK:
                        deftStatus = "registered"
                        varMap = {}
                        varMap[":taskid"] = jediTaskID
                        varMap[":status"] = deftStatus
                        varMap[":ndone"] = 0
                        varMap[":nreq"] = 0
                        varMap[":tevts"] = 0
                        sqlUC = f"UPDATE {jedi_config.db.schemaDEFT}.T_TASK "
                        sqlUC += "SET status=:status,timestamp=CURRENT_DATE,total_done_jobs=:ndone,total_req_jobs=:nreq,total_events=:tevts "
                        sqlUC += "WHERE taskid=:taskid "
                        tmpLog.debug(sqlUC + comment + str(varMap))
                        self.cur.execute(sqlUC + comment, varMap)
                        self.setSuperStatus_JEDI(jediTaskID, deftStatus)
                    # append
                    if isOK:
                        retTaskIDs.append((jediTaskID, None, "registered", parent_tid))
                # commit
                if isOK:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                else:
                    # roll back
                    self._rollback()
            # find orphaned tasks to rescue
            self.conn.begin()
            varMap = {}
            varMap[":status1"] = "registered"
            varMap[":status2"] = JediTaskSpec.commandStatusMap()["incexec"]["done"]
            varMap[":status3"] = "staged"
            varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=10)
            sqlOrpS = "SELECT tabT.jediTaskID,tabT.splitRule,tabT.status,tabT.parent_tid "
            sqlOrpS += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlOrpS += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlOrpS += "AND tabT.status IN (:status1,:status2,:status3) AND tabT.modificationtime<:timeLimit "
            if vo is not None:
                sqlOrpS += "AND vo=:vo "
                varMap[":vo"] = vo
            if prodSourceLabel is not None:
                sqlOrpS += "AND prodSourceLabel=:prodSourceLabel "
                varMap[":prodSourceLabel"] = prodSourceLabel
            sqlOrpS += "FOR UPDATE "
            tmpLog.debug(sqlOrpS + comment + str(varMap))
            self.cur.execute(sqlOrpS + comment, varMap)
            resList = self.cur.fetchall()
            # update modtime to avoid immediate reattempts
            sqlOrpU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET modificationtime=CURRENT_DATE "
            sqlOrpU += "WHERE jediTaskID=:jediTaskID "
            for jediTaskID, splitRule, taskStatus, parent_tid in resList:
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                tmpLog.debug(sqlOrpU + comment + str(varMap))
                self.cur.execute(sqlOrpU + comment, varMap)
                nRow = self.cur.rowcount
                if nRow == 1 and jediTaskID not in retTaskIDs:
                    retTaskIDs.append((jediTaskID, splitRule, taskStatus, parent_tid))
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"return {len(retTaskIDs)} tasks")
            return retTaskIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get task parameters with jediTaskID
    def getTaskParamsWithID_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getTaskParamsWithID_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql
            sql = f"SELECT taskParams FROM {jedi_config.db.schemaJEDI}.JEDI_TaskParams WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 100
            self.cur.execute(sql + comment, varMap)
            retStr = ""
            totalSize = 0
            for (tmpItem,) in self.cur:
                retStr = tmpItem
                totalSize += len(tmpItem)
                break
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"read {len(retStr)}/{totalSize} bytes")
            return retStr
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # register task/dataset/templ/param in a single transaction
    def registerTaskInOneShot_JEDI(
        self,
        jediTaskID,
        taskSpec,
        inMasterDatasetSpecList,
        inSecDatasetSpecList,
        outDatasetSpecList,
        outputTemplateMap,
        jobParamsTemplate,
        taskParams,
        unmergeMasterDatasetSpec,
        unmergeDatasetSpecMap,
        uniqueTaskName,
        oldTaskStatus,
    ):
        comment = " /* JediDBProxy.registerTaskInOneShot_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            # set attributes
            if taskSpec.status not in ["topreprocess"]:
                taskSpec.status = "defined"
            tmpLog.debug(f"taskStatus={taskSpec.status}")
            taskSpec.modificationTime = timeNow
            taskSpec.resetChangedAttr("jediTaskID")
            # begin transaction
            self.conn.begin()
            # check duplication
            duplicatedFlag = False
            if uniqueTaskName is True:
                sqlDup = f"SELECT jediTaskID FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
                sqlDup += "WHERE userName=:userName AND taskName=:taskName AND jediTaskID<>:jediTaskID FOR UPDATE "
                varMap = {}
                varMap[":userName"] = taskSpec.userName
                varMap[":taskName"] = taskSpec.taskName
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlDup + comment, varMap)
                resDupList = self.cur.fetchall()
                tmpErrStr = ""
                for (tmpJediTaskID,) in resDupList:
                    duplicatedFlag = True
                    tmpErrStr += f"{tmpJediTaskID},"
                if duplicatedFlag:
                    taskSpec.status = "toabort"
                    tmpErrStr = tmpErrStr[:-1]
                    tmpErrStr = f"{taskSpec.status} since there is duplicated task -> jediTaskID={tmpErrStr}"
                    taskSpec.setErrDiag(tmpErrStr)
                    # reset task name
                    taskSpec.taskName = None
                    tmpLog.debug(tmpErrStr)
            # update task
            varMap = taskSpec.valuesMap(useSeq=False, onlyChanged=True)
            varMap[":jediTaskID"] = jediTaskID
            varMap[":preStatus"] = oldTaskStatus
            sql = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET {taskSpec.bindUpdateChangesExpression()} WHERE "
            sql += "jediTaskID=:jediTaskID AND status=:preStatus "
            self.cur.execute(sql + comment, varMap)
            nRow = self.cur.rowcount
            tmpLog.debug(f"update {nRow} row in task table")
            if nRow != 1:
                tmpLog.error("the task not found in task table or already registered")
            elif duplicatedFlag:
                pass
            else:
                # delete unknown datasets
                tmpLog.debug("deleting unknown datasets")
                sql = f"DELETE FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
                sql += "WHERE jediTaskID=:jediTaskID AND type=:type "
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":type"] = JediDatasetSpec.getUnknownInputType()
                self.cur.execute(sql + comment, varMap)
                tmpLog.debug("inserting datasets")
                # sql to insert datasets
                sql = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Datasets ({JediDatasetSpec.columnNames()}) "
                sql += JediDatasetSpec.bindValuesExpression()
                sql += " RETURNING datasetID INTO :newDatasetID"
                # sql to insert files
                sqlI = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
                sqlI += JediFileSpec.bindValuesExpression()
                # insert master dataset
                masterID = -1
                datasetIdMap = {}
                for datasetSpec in inMasterDatasetSpecList:
                    if datasetSpec is not None:
                        datasetSpec.creationTime = timeNow
                        datasetSpec.modificationTime = timeNow
                        varMap = datasetSpec.valuesMap(useSeq=True)
                        varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                        # insert dataset
                        self.cur.execute(sql + comment, varMap)
                        val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                        datasetID = int(val)
                        masterID = datasetID
                        datasetIdMap[datasetSpec.uniqueMapKey()] = datasetID
                        datasetSpec.datasetID = datasetID
                        # insert files
                        for fileSpec in datasetSpec.Files:
                            fileSpec.datasetID = datasetID
                            fileSpec.creationDate = timeNow
                            varMap = fileSpec.valuesMap(useSeq=True)
                            self.cur.execute(sqlI + comment, varMap)
                    # insert secondary datasets
                    for datasetSpec in inSecDatasetSpecList:
                        datasetSpec.creationTime = timeNow
                        datasetSpec.modificationTime = timeNow
                        datasetSpec.masterID = masterID
                        varMap = datasetSpec.valuesMap(useSeq=True)
                        varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                        # insert dataset
                        self.cur.execute(sql + comment, varMap)
                        val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                        datasetID = int(val)
                        datasetIdMap[datasetSpec.uniqueMapKey()] = datasetID
                        datasetSpec.datasetID = datasetID
                        # insert files
                        for fileSpec in datasetSpec.Files:
                            fileSpec.datasetID = datasetID
                            fileSpec.creationDate = timeNow
                            varMap = fileSpec.valuesMap(useSeq=True)
                            self.cur.execute(sqlI + comment, varMap)
                # insert unmerged master dataset
                unmergeMasterID = -1
                for datasetSpec in unmergeMasterDatasetSpec.values():
                    datasetSpec.creationTime = timeNow
                    datasetSpec.modificationTime = timeNow
                    varMap = datasetSpec.valuesMap(useSeq=True)
                    varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                    # insert dataset
                    self.cur.execute(sql + comment, varMap)
                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                    datasetID = int(val)
                    datasetIdMap[datasetSpec.outputMapKey()] = datasetID
                    datasetSpec.datasetID = datasetID
                    unmergeMasterID = datasetID
                # insert unmerged output datasets
                for datasetSpec in unmergeDatasetSpecMap.values():
                    datasetSpec.creationTime = timeNow
                    datasetSpec.modificationTime = timeNow
                    datasetSpec.masterID = unmergeMasterID
                    varMap = datasetSpec.valuesMap(useSeq=True)
                    varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                    # insert dataset
                    self.cur.execute(sql + comment, varMap)
                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                    datasetID = int(val)
                    datasetIdMap[datasetSpec.outputMapKey()] = datasetID
                    datasetSpec.datasetID = datasetID
                # insert output datasets
                for datasetSpec in outDatasetSpecList:
                    datasetSpec.creationTime = timeNow
                    datasetSpec.modificationTime = timeNow
                    # keep original outputMapKey since provenanceID may change
                    outputMapKey = datasetSpec.outputMapKey()
                    # associate to unmerged dataset
                    if datasetSpec.outputMapKey() in unmergeMasterDatasetSpec:
                        datasetSpec.provenanceID = unmergeMasterDatasetSpec[datasetSpec.outputMapKey()].datasetID
                    elif datasetSpec.outputMapKey() in unmergeDatasetSpecMap:
                        datasetSpec.provenanceID = unmergeDatasetSpecMap[datasetSpec.outputMapKey()].datasetID
                    varMap = datasetSpec.valuesMap(useSeq=True)
                    varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                    # insert dataset
                    self.cur.execute(sql + comment, varMap)
                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                    datasetID = int(val)
                    datasetIdMap[outputMapKey] = datasetID
                    datasetSpec.datasetID = datasetID
                # insert outputTemplates
                tmpLog.debug("inserting outTmpl")
                for outputMapKey, outputTemplateList in outputTemplateMap.items():
                    if outputMapKey not in datasetIdMap:
                        raise RuntimeError(f"datasetID is not defined for {outputMapKey}")
                    for outputTemplate in outputTemplateList:
                        sqlH = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Output_Template (outTempID,datasetID,"
                        sqlL = f"VALUES({jedi_config.db.schemaJEDI}.JEDI_OUTPUT_TEMPLATE_ID_SEQ.nextval,:datasetID,"
                        varMap = {}
                        varMap[":datasetID"] = datasetIdMap[outputMapKey]
                        for tmpAttr, tmpVal in outputTemplate.items():
                            tmpKey = ":" + tmpAttr
                            sqlH += f"{tmpAttr},"
                            sqlL += f"{tmpKey},"
                            varMap[tmpKey] = tmpVal
                        sqlH = sqlH[:-1] + ") "
                        sqlL = sqlL[:-1] + ") "
                        sql = sqlH + sqlL
                        self.cur.execute(sql + comment, varMap)
                # check if jobParams is already there
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                sql = f"SELECT jediTaskID FROM {jedi_config.db.schemaJEDI}.JEDI_JobParams_Template "
                sql += "WHERE jediTaskID=:jediTaskID "
                self.cur.execute(sql + comment, varMap)
                resPar = self.cur.fetchone()
                if resPar is None:
                    # insert job parameters
                    tmpLog.debug("inserting jobParamsTmpl")
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":templ"] = jobParamsTemplate
                    sql = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_JobParams_Template "
                    sql += "(jediTaskID,jobParamsTemplate) VALUES (:jediTaskID,:templ) "
                else:
                    tmpLog.debug("replacing jobParamsTmpl")
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":templ"] = jobParamsTemplate
                    sql = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_JobParams_Template "
                    sql += "SET jobParamsTemplate=:templ WHERE jediTaskID=:jediTaskID"
                self.cur.execute(sql + comment, varMap)
                # update task parameters
                if taskParams is not None:
                    tmpLog.debug("updating taskParams")
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":taskParams"] = taskParams
                    sql = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_TaskParams SET taskParams=:taskParams "
                    sql += "WHERE jediTaskID=:jediTaskID "
                    self.cur.execute(sql + comment, varMap)
            # task status logging
            self.record_task_status_change(taskSpec.jediTaskID)
            self.push_task_status_message(taskSpec, taskSpec.jediTaskID, taskSpec.status)
            # task attempt start log
            self.log_task_attempt_start(taskSpec.jediTaskID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return True, taskSpec.status
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, "tobroken"

    # update jobMetrics
    def updateJobMetrics_JEDI(self, jediTaskID, pandaID, jobMetrics, tags):
        comment = " /* JediDBProxy.updateJobMetrics_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskid={jediTaskID} PandaID={pandaID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start tags={','.join(tags)}")
        # set new jobMetrics
        tagStr = "scout=" + "|".join(tags)
        if jobMetrics is None:
            newSH = tagStr
        else:
            items = jobMetrics.split(" ")
            items.append(tagStr)
            newSH = " ".join(items)
        # cap
        newSH = newSH[:500]
        # update live table
        sqlL = f"UPDATE {jedi_config.db.schemaPANDA}.jobsArchived4 "
        sqlL += "SET jobMetrics=:newStr WHERE PandaID=:PandaID "
        varMap = {}
        varMap[":PandaID"] = pandaID
        varMap[":newStr"] = newSH
        self.cur.execute(sqlL + comment, varMap)
        nRow = self.cur.rowcount
        if nRow != 1:
            # update archive table
            sqlA = f"UPDATE {jedi_config.db.schemaPANDAARCH}.jobsArchived "
            sqlA += "SET jobMetrics=:newStr WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-30) "
            varMap = {}
            varMap[":PandaID"] = pandaID
            varMap[":newStr"] = newSH
            self.cur.execute(sqlA + comment, varMap)
            nRow = self.cur.rowcount
        tmpLog.debug(f"done with {nRow}")
        return

    # get scout job data
    def getScoutJobData_JEDI(
        self, jediTaskID, useTransaction=False, scoutSuccessRate=None, mergeScout=False, flagJob=False, setPandaID=None, site_mapper=None, task_spec=None
    ):
        comment = " /* JediDBProxy.getScoutJobData_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start mergeScout={mergeScout}")
        returnMap = {}
        extraInfo = {}

        # get percentile rank and margin for memory
        ramCountRank = self.getConfigValue("dbproxy", "SCOUT_RAMCOUNT_RANK", "jedi")
        if ramCountRank is None:
            ramCountRank = 75
        ramCountMargin = self.getConfigValue("dbproxy", "SCOUT_RAMCOUNT_MARGIN", "jedi")
        if ramCountMargin is None:
            ramCountMargin = 10
        # get percentile rank for cpuTime
        cpuTimeRank = self.getConfigValue("dbproxy", "SCOUT_CPUTIME_RANK", "jedi")
        if cpuTimeRank is None:
            cpuTimeRank = 95

        # sql to get preset values
        if not mergeScout:
            sqlGPV = "SELECT "
            sqlGPV += "prodSourceLabel, outDiskCount, outDiskUnit, walltime, ramCount, ramUnit, baseRamCount, "
            sqlGPV += "workDiskCount, cpuTime, cpuEfficiency, baseWalltime, splitRule, cpuTimeUnit, "
            sqlGPV += "memory_leak_core, memory_leak_x2 "
        else:
            sqlGPV = "SELECT "
            sqlGPV += "prodSourceLabel, outDiskCount, outDiskUnit, mergeWalltime, mergeRamCount, ramUnit, baseRamCount, "
            sqlGPV += "workDiskCount, cpuTime, cpuEfficiency, baseWalltime, splitRule, cpuTimeUnit, "
            sqlGPV += "memory_leak_core, memory_leak_x2 "
        sqlGPV += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
        sqlGPV += "WHERE jediTaskID=:jediTaskID "

        # sql to get scout job data from JEDI
        sqlSCF = "SELECT tabF.PandaID,tabF.fsize,tabF.startEvent,tabF.endEvent,tabF.nEvents,tabF.type "
        sqlSCF += "FROM {0}.JEDI_Datasets tabD, {0}.JEDI_Dataset_Contents tabF WHERE ".format(jedi_config.db.schemaJEDI)
        sqlSCF += "tabD.jediTaskID=tabF.jediTaskID AND tabD.jediTaskID=:jediTaskID AND tabF.status=:status "
        sqlSCF += "AND tabD.datasetID=tabF.datasetID "
        if not mergeScout:
            sqlSCF += "AND tabF.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlSCF += f"{mapKey},"
        else:
            sqlSCF += "AND tabD.type IN ("
            for tmpType in JediDatasetSpec.getMergeProcessTypes():
                mapKey = ":type_" + tmpType
                sqlSCF += f"{mapKey},"
        sqlSCF = sqlSCF[:-1]
        sqlSCF += ") AND tabD.masterID IS NULL "
        if setPandaID is not None:
            sqlSCF += "AND tabF.PandaID=:usePandaID "

        # sql to check scout success rate
        sqlCSSR = "SELECT COUNT(*),SUM(is_finished),SUM(is_failed) FROM "
        sqlCSSR += (
            "(SELECT DISTINCT tabF.PandaID,CASE WHEN tabF.status='finished' THEN 1 ELSE 0 END is_finished,"
            "CASE WHEN tabF.status='ready' AND "
            "(tabF.maxAttempt<=tabF.attemptNr OR "
            "(tabF.maxfailure IS NOT NULL AND tabF.maxFailure<=tabF.failedAttempt)) THEN 1 ELSE 0 END "
            "is_failed "
        )
        sqlCSSR += "FROM {0}.JEDI_Datasets tabD, {0}.JEDI_Dataset_Contents tabF WHERE ".format(jedi_config.db.schemaJEDI)
        sqlCSSR += "tabD.jediTaskID=tabF.jediTaskID AND tabD.jediTaskID=:jediTaskID AND tabF.PandaID IS NOT NULL "
        sqlCSSR += "AND tabD.datasetID=tabF.datasetID "
        sqlCSSR += "AND tabF.type IN ("
        for tmpType in JediDatasetSpec.getInputTypes():
            mapKey = ":type_" + tmpType
            sqlCSSR += f"{mapKey},"
        sqlCSSR = sqlCSSR[:-1]
        sqlCSSR += ") AND tabD.masterID IS NULL "
        sqlCSSR += ") tmp_sub "

        # sql to get normal scout job data from Panda
        sqlSCDN = "SELECT eventService, jobsetID, PandaID, jobStatus, outputFileBytes, jobMetrics, cpuConsumptionTime, "
        sqlSCDN += "actualCoreCount, coreCount, startTime, endTime, computingSite, maxPSS, jobMetrics, nEvents, "
        sqlSCDN += "totRBYTES, totWBYTES, inputFileBytes, memory_leak, memory_leak_x2 "
        sqlSCDN += f"FROM {jedi_config.db.schemaPANDA}.jobsArchived4 "
        sqlSCDN += "WHERE PandaID=:pandaID AND jobStatus=:jobStatus AND jediTaskID=:jediTaskID "
        sqlSCDN += "UNION "
        sqlSCDN += "SELECT eventService, jobsetID, PandaID, jobStatus, outputFileBytes, jobMetrics, cpuConsumptionTime, "
        sqlSCDN += "actualCoreCount, coreCount, startTime, endTime, computingSite, maxPSS, jobMetrics, nEvents, "
        sqlSCDN += "totRBYTES, totWBYTES, inputFileBytes, memory_leak, memory_leak_x2 "
        sqlSCDN += f"FROM {jedi_config.db.schemaPANDAARCH}.jobsArchived "
        sqlSCDN += "WHERE PandaID=:pandaID AND jobStatus=:jobStatus AND jediTaskID=:jediTaskID "
        sqlSCDN += "AND modificationTime>(CURRENT_DATE-30) "

        # sql to get ES scout job data from Panda
        sqlSCDE = "SELECT eventService, jobsetID, PandaID, jobStatus, outputFileBytes, jobMetrics, cpuConsumptionTime, "
        sqlSCDE += "actualCoreCount, coreCount, startTime, endTime, computingSite, maxPSS, jobMetrics, nEvents, "
        sqlSCDE += "totRBYTES, totWBYTES, inputFileBytes, memory_leak, memory_leak_x2 "
        sqlSCDE += f"FROM {jedi_config.db.schemaPANDA}.jobsArchived4 "
        sqlSCDE += "WHERE jobsetID=:pandaID AND jobStatus=:jobStatus AND jediTaskID=:jediTaskID "
        sqlSCDE += "UNION "
        sqlSCDE += "SELECT eventService, jobsetID, PandaID, jobStatus, outputFileBytes, jobMetrics, cpuConsumptionTime, "
        sqlSCDE += "actualCoreCount, coreCount, startTime, endTime, computingSite, maxPSS, jobMetrics, nEvents, "
        sqlSCDE += "totRBYTES, totWBYTES, inputFileBytes, memory_leak, memory_leak_x2 "
        sqlSCDE += f"FROM {jedi_config.db.schemaPANDAARCH}.jobsArchived "
        sqlSCDE += "WHERE jobsetID=:pandaID AND jobStatus=:jobStatus AND jediTaskID=:jediTaskID "
        sqlSCDE += "AND modificationTime>(CURRENT_DATE-14) "

        # get size of lib
        sqlLIB = "SELECT MAX(fsize) "
        sqlLIB += "FROM {0}.JEDI_Datasets tabD, {0}.JEDI_Dataset_Contents tabF WHERE ".format(jedi_config.db.schemaJEDI)
        sqlLIB += "tabD.jediTaskID=tabF.jediTaskID AND tabD.jediTaskID=:jediTaskID AND tabF.status=:status AND "
        sqlLIB += "tabD.type=:type AND tabF.type=:type "

        # get core power
        sqlCore = f"SELECT /* use_json_type */ scj.data.corepower FROM {jedi_config.db.schemaJEDI}.schedconfig_json scj "
        sqlCore += "WHERE panda_queue=:site "

        # get num of new jobs
        sqlNumJobs = f"SELECT SUM(nFiles),SUM(nFilesFinished),SUM(nFilesUsed) FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
        sqlNumJobs += "WHERE jediTaskID=:jediTaskID AND type IN ("
        for tmpType in JediDatasetSpec.getInputTypes():
            mapKey = ":type_" + tmpType
            sqlNumJobs += f"{mapKey},"
        sqlNumJobs = sqlNumJobs[:-1]
        sqlNumJobs += ") AND masterID IS NULL "

        # get num of new jobs with event
        sql_num_jobs_event = (
            "SELECT SUM(n_events), SUM(CASE WHEN status='finished' THEN n_events ELSE 0 END) FROM ("
            "SELECT (CASE WHEN tabF.endEvent IS NULL THEN tabF.nEvents ELSE tabF.endEvent-tabF.startEvent+1 END) n_events,tabF.status "
            f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets tabD, "
            f"{jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents tabF "
            "WHERE tabD.jediTaskID=:jediTaskID AND tabF.jediTaskID=tabD.jediTaskID "
            "AND tabF.datasetID=tabD.datasetID AND tabD.type IN ("
        )
        for tmpType in JediDatasetSpec.getInputTypes():
            mapKey = ":type_" + tmpType
            sql_num_jobs_event += f"{mapKey},"
        sql_num_jobs_event = sql_num_jobs_event[:-1]
        sql_num_jobs_event += ") AND tabD.masterID IS NULL) tmp_tab "

        if useTransaction:
            # begin transaction
            self.conn.begin()
        self.cur.arraysize = 100000
        # get preset values
        varMap = {}
        varMap[":jediTaskID"] = jediTaskID
        self.cur.execute(sqlGPV + comment, varMap)
        resGPV = self.cur.fetchone()
        if resGPV is not None:
            (
                prodSourceLabel,
                preOutDiskCount,
                preOutDiskUnit,
                preWalltime,
                preRamCount,
                preRamUnit,
                preBaseRamCount,
                preWorkDiskCount,
                preCpuTime,
                preCpuEfficiency,
                preBaseWalltime,
                splitRule,
                preCpuTimeUnit,
                memory_leak_core,
                memory_leak_x2,
            ) = resGPV
            # get preOutDiskCount in kB
            if preOutDiskCount not in [0, None]:
                if preOutDiskUnit is not None:
                    if preOutDiskUnit.startswith("GB"):
                        preOutDiskCount = preOutDiskCount * 1024 * 1024
                    elif preOutDiskUnit.startswith("MB"):
                        preOutDiskCount = preOutDiskCount * 1024
                    elif preOutDiskUnit.startswith("kB"):
                        pass
                    else:
                        preOutDiskCount = preOutDiskCount // 1024
            # get preCpuTime in sec
            try:
                if preCpuTimeUnit.startswith("m"):
                    preCpuTime = float(preCpuTime) / 1000.0
            except Exception:
                pass
        else:
            prodSourceLabel = None
            preOutDiskCount = 0
            preOutDiskUnit = None
            preWalltime = 0
            preRamCount = 0
            preRamUnit = None
            preBaseRamCount = 0
            preWorkDiskCount = 0
            preCpuTime = 0
            preCpuEfficiency = None
            preBaseWalltime = None
            splitRule = None
            preCpuTimeUnit = None
        if preOutDiskUnit is not None and preOutDiskUnit.endswith("PerEvent"):
            preOutputScaleWithEvents = True
        else:
            preOutputScaleWithEvents = False
        if preCpuEfficiency is None:
            preCpuEfficiency = 100
        if preBaseWalltime is None:
            preBaseWalltime = 0
        # don't use baseRamCount for pmerge
        if mergeScout:
            preBaseRamCount = 0
        extraInfo["oldCpuTime"] = preCpuTime
        extraInfo["oldRamCount"] = preRamCount
        # get minimum ram count
        minRamCount = self.getConfigValue("dbproxy", "SCOUT_RAMCOUNT_MIN", "jedi")
        # get limit for short jobs
        shortExecTime = self.getConfigValue("dbproxy", f"SCOUT_SHORT_EXECTIME_{prodSourceLabel}", "jedi")
        if shortExecTime is None:
            shortExecTime = 0
        # get limit for cpu-inefficient jobs
        lowCpuEff = self.getConfigValue("dbproxy", f"SCOUT_LOW_CPU_EFFICIENCY_{prodSourceLabel}", "jedi")
        if lowCpuEff is None:
            lowCpuEff = 0
        # cap on diskIO
        capOnDiskIO = self.getConfigValue("dbproxy", "SCOUT_DISK_IO_CAP", "jedi")
        extraInfo["shortExecTime"] = shortExecTime
        extraInfo["cpuEfficiencyCap"] = lowCpuEff
        # get the size of lib
        varMap = {}
        varMap[":jediTaskID"] = jediTaskID
        varMap[":type"] = "lib"
        varMap[":status"] = "finished"
        self.cur.execute(sqlLIB + comment, varMap)
        resLIB = self.cur.fetchone()
        libSize = None
        if resLIB is not None:
            try:
                (libSize,) = resLIB
                libSize /= 1024 * 1024
            except Exception:
                pass
        # get files
        varMap = {}
        varMap[":jediTaskID"] = jediTaskID
        varMap[":status"] = "finished"
        if not mergeScout:
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                varMap[mapKey] = tmpType
        else:
            for tmpType in JediDatasetSpec.getMergeProcessTypes():
                mapKey = ":type_" + tmpType
                varMap[mapKey] = tmpType
        if setPandaID is not None:
            varMap[":usePandaID"] = setPandaID
        self.cur.execute(sqlSCF + comment, varMap)
        resList = self.cur.fetchall()
        # scout succeeded or not
        scoutSucceeded = True
        if not resList:
            scoutSucceeded = False
            tmpLog.debug("no scouts succeeded")
            extraInfo["successRate"] = 0
        else:
            if not mergeScout and task_spec and task_spec.useScout():
                # check scout success rate
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                for tmpType in JediDatasetSpec.getInputTypes():
                    mapKey = ":type_" + tmpType
                    varMap[mapKey] = tmpType
                self.cur.execute(sqlCSSR + comment, varMap)
                scTotal, scOK, scNG = self.cur.fetchone()

                if scTotal > 0 and scOK + scNG > 0:
                    extraInfo["successRate"] = scOK / (scOK + scNG)
                else:
                    extraInfo["successRate"] = 0
                tmpLog.debug(
                    f"scout total={scTotal} finished={scOK} failed={scNG} target_rate={scoutSuccessRate/10} " f"""actual_rate={extraInfo["successRate"]}"""
                )
                if scoutSuccessRate and scTotal and extraInfo["successRate"] < scoutSuccessRate / 10:
                    tmpLog.debug("not enough scouts succeeded")
                    scoutSucceeded = False
        # upper limit
        limitWallTime = 999999999
        # loop over all files
        outSizeList = []
        outSizeDict = {}
        walltimeList = []
        walltimeDict = {}
        memSizeList = []
        memSizeDict = {}
        leak_list = []
        leak_dict = {}
        leak_x2_list = []
        leak_x2_dict = {}
        workSizeList = []
        cpuTimeList = []
        cpuTimeDict = {}
        ioIntentList = []
        ioIntentDict = {}
        cpuEffList = []
        cpuEffDict = {}
        cpuEffMap = {}
        finishedJobs = []
        inFSizeList = []
        inFSizeMap = {}
        inEventsMap = {}
        corePowerMap = {}
        jMetricsMap = {}
        execTimeMap = {}
        siteMap = {}
        diskIoList = []
        pandaIDList = set()
        totInSizeMap = {}
        masterInSize = {}
        coreCountMap = {}
        pseudoInput = set()
        for pandaID, fsize, startEvent, endEvent, nEvents, fType in resList:
            pandaIDList.add(pandaID)
            if pandaID not in inFSizeMap:
                inFSizeMap[pandaID] = 0
            # get effective file size
            effectiveFsize = JediCoreUtils.getEffectiveFileSize(fsize, startEvent, endEvent, nEvents)
            inFSizeMap[pandaID] += effectiveFsize
            # events
            if pandaID not in inEventsMap:
                inEventsMap[pandaID] = 0
            inEventsMap[pandaID] += JediCoreUtils.getEffectiveNumEvents(startEvent, endEvent, nEvents)
            # master input size
            if pandaID not in masterInSize:
                masterInSize[pandaID] = 0
            masterInSize[pandaID] += fsize
            if fType == "pseudo_input":
                pseudoInput.add(pandaID)
        # get nFiles
        totalJobs = 0
        totFiles = 0
        totFinished = 0
        nNewJobs = 0
        total_jobs_with_event = 0
        if not mergeScout:
            # estimate the number of new jobs with the number of files
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                varMap[mapKey] = tmpType
            self.cur.execute(sqlNumJobs + comment, varMap)
            resNumJobs = self.cur.fetchone()
            if resNumJobs is not None:
                totFiles, totFinished, totUsed = resNumJobs
                if totFinished > 0:
                    totalJobs = int(totFiles * len(pandaIDList) // totFinished)
                    nNewJobs = int((totFiles - totUsed) * len(pandaIDList) // totFinished)
                    # estimate the number of new jobs with size
                    var_map = dict()
                    var_map[":jediTaskID"] = jediTaskID
                    for tmp_type in JediDatasetSpec.getInputTypes():
                        var_map[":type_" + tmp_type] = tmp_type
                    self.cur.execute(sql_num_jobs_event + comment, var_map)
                    res_num_jobs_event = self.cur.fetchone()
                    if res_num_jobs_event:
                        total_in_event, total_finished_event = res_num_jobs_event
                        if total_finished_event is not None and total_finished_event > 0:
                            total_jobs_with_event = int(total_in_event * len(pandaIDList) // total_finished_event)
        extraInfo["expectedNumJobs"] = totalJobs
        extraInfo["numFinishedJobs"] = len(pandaIDList)
        extraInfo["nFiles"] = totFiles
        extraInfo["nFilesFinished"] = totFinished
        extraInfo["nNewJobs"] = nNewJobs
        extraInfo["expectedNumJobsWithEvent"] = total_jobs_with_event
        # loop over all jobs
        loopPandaIDs = list(inFSizeMap.keys())
        random.shuffle(loopPandaIDs)
        loopPandaIDs = loopPandaIDs[:1000]
        for loopPandaID in loopPandaIDs:
            totalFSize = inFSizeMap[loopPandaID]
            # get job data
            varMap = {}
            varMap[":pandaID"] = loopPandaID
            varMap[":jobStatus"] = "finished"
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlSCDN + comment, varMap)
            resData = self.cur.fetchone()
            if resData is not None:
                eventServiceJob = resData[0]
                jobsetID = resData[1]
                resDataList = [resData]
                # get all ES jobs since input is associated to only one consumer
                if eventServiceJob == EventServiceUtils.esJobFlagNumber:
                    varMap = {}
                    varMap[":pandaID"] = jobsetID
                    varMap[":jobStatus"] = "finished"
                    varMap[":jediTaskID"] = jediTaskID
                    self.cur.execute(sqlSCDE + comment, varMap)
                    resDataList = self.cur.fetchall()
                # loop over all jobs
                for oneResData in resDataList:
                    (
                        eventServiceJob,
                        jobsetID,
                        pandaID,
                        jobStatus,
                        outputFileBytes,
                        jobMetrics,
                        cpuConsumptionTime,
                        actualCoreCount,
                        defCoreCount,
                        startTime,
                        endTime,
                        computingSite,
                        maxPSS,
                        jobMetrics,
                        nEvents,
                        totRBYTES,
                        totWBYTES,
                        inputFileByte,
                        memory_leak,
                        memory_leak_x2,
                    ) = oneResData

                    # add inputSize and nEvents
                    if pandaID not in inFSizeMap:
                        inFSizeMap[pandaID] = totalFSize
                    if pandaID not in inEventsMap or eventServiceJob == EventServiceUtils.esJobFlagNumber:
                        inEventsMap[pandaID] = nEvents
                    totInSizeMap[pandaID] = inputFileByte

                    siteMap[pandaID] = computingSite

                    # get core power
                    if computingSite not in corePowerMap:
                        varMap = {}
                        varMap[":site"] = computingSite
                        self.cur.execute(sqlCore + comment, varMap)
                        resCore = self.cur.fetchone()
                        if resCore is not None:
                            (corePower,) = resCore
                            corePower = float(corePower)
                        else:
                            corePower = None
                        corePowerMap[computingSite] = corePower
                    corePower = corePowerMap[computingSite]
                    if corePower in [0, None]:
                        corePower = 10
                    finishedJobs.append(pandaID)
                    inFSizeList.append(totalFSize)
                    jMetricsMap[pandaID] = jobMetrics

                    # core count
                    coreCount = JobUtils.getCoreCount(actualCoreCount, defCoreCount, jobMetrics)
                    coreCountMap[pandaID] = coreCount

                    # output size
                    tmpWorkSize = 0
                    if eventServiceJob != EventServiceUtils.esJobFlagNumber:
                        try:
                            try:
                                # add size of intermediate files
                                if jobMetrics is not None:
                                    tmpMatch = re.search("workDirSize=(\d+)", jobMetrics)
                                    tmpWorkSize = int(tmpMatch.group(1))
                                    tmpWorkSize /= 1024 * 1024
                            except Exception:
                                pass
                            if preOutDiskUnit is None or "Fixed" not in preOutDiskUnit:
                                if preOutputScaleWithEvents:
                                    # scale with events
                                    if pandaID in inEventsMap and inEventsMap[pandaID] > 0:
                                        tmpVal = int(math.ceil(float(outputFileBytes) / inEventsMap[pandaID]))
                                    if pandaID not in inEventsMap or inEventsMap[pandaID] >= 10:
                                        outSizeList.append(tmpVal)
                                        outSizeDict[tmpVal] = pandaID
                                else:
                                    # scale with input size
                                    tmpVal = int(math.ceil(float(outputFileBytes) / totalFSize))
                                    if pandaID not in inEventsMap or inEventsMap[pandaID] >= 10:
                                        outSizeList.append(tmpVal)
                                        outSizeDict[tmpVal] = pandaID
                        except Exception:
                            pass

                    # execution time
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            tmpVal = cpuConsumptionTime
                            walltimeList.append(tmpVal)
                            walltimeDict[tmpVal] = pandaID
                        except Exception:
                            pass
                        try:
                            execTimeMap[pandaID] = endTime - startTime
                        except Exception:
                            pass

                    # CPU time
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            if preCpuTimeUnit in ["HS06sPerEventFixed", "mHS06sPerEventFixed"]:
                                pass
                            else:
                                tmpVal = JobUtils.getHS06sec(
                                    startTime, endTime, corePower, coreCount, baseWalltime=preBaseWalltime, cpuEfficiency=preCpuEfficiency
                                )
                                if pandaID in inEventsMap and inEventsMap[pandaID] > 0:
                                    tmpVal /= float(inEventsMap[pandaID])
                                if (
                                    pandaID not in inEventsMap
                                    or inEventsMap[pandaID] >= (10 * coreCount)
                                    or pandaID in pseudoInput
                                    or (
                                        inEventsMap[pandaID] < (10 * coreCount)
                                        and pandaID in execTimeMap
                                        and execTimeMap[pandaID] > datetime.timedelta(seconds=6 * 3600)
                                    )
                                ):
                                    cpuTimeList.append(tmpVal)
                                    cpuTimeDict[tmpVal] = pandaID
                        except Exception:
                            pass

                    # IO
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            tmpTimeDelta = endTime - startTime
                            tmpVal = totalFSize * 1024.0 + float(outputFileBytes) / 1024.0
                            tmpVal = tmpVal / float(tmpTimeDelta.seconds + tmpTimeDelta.days * 24 * 3600)
                            tmpVal /= float(coreCount)
                            ioIntentList.append(tmpVal)
                            ioIntentDict[tmpVal] = pandaID
                        except Exception:
                            pass

                    # disk IO
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            tmpTimeDelta = endTime - startTime
                            tmpVal = totRBYTES + totWBYTES
                            tmpVal /= float(tmpTimeDelta.seconds + tmpTimeDelta.days * 24 * 3600)
                            diskIoList.append(tmpVal)
                        except Exception:
                            pass

                    # memory leak
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            memory_leak_core_tmp = float(memory_leak) / float(coreCount)
                            memory_leak_core_tmp = int(math.ceil(memory_leak_core_tmp))
                            leak_list.append(memory_leak_core_tmp)
                            leak_dict[memory_leak_core_tmp] = pandaID
                        except Exception:
                            pass
                        # memory leak chi2 measurement
                        try:
                            memory_leak_x2_tmp = int(memory_leak_x2)
                            leak_x2_list.append(memory_leak_x2_tmp)
                            leak_x2_dict[memory_leak_x2_tmp] = pandaID
                        except Exception:
                            pass

                    # RAM size
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            if preRamUnit == "MBPerCoreFixed":
                                pass
                            elif preRamUnit == "MBPerCore":
                                if maxPSS > 0:
                                    tmpPSS = maxPSS
                                    if preBaseRamCount not in [0, None]:
                                        tmpPSS -= preBaseRamCount * 1024
                                    tmpPSS = float(tmpPSS) / float(coreCount)
                                    tmpPSS = int(math.ceil(tmpPSS))
                                    memSizeList.append(tmpPSS)
                                    memSizeDict[tmpPSS] = pandaID
                            else:
                                if maxPSS > 0:
                                    tmpMEM = maxPSS
                                else:
                                    tmpMatch = re.search("vmPeakMax=(\d+)", jobMetrics)
                                    tmpMEM = int(tmpMatch.group(1))
                                memSizeList.append(tmpMEM)
                                memSizeDict[tmpMEM] = pandaID
                        except Exception:
                            pass

                    # use lib size as workdir size
                    if tmpWorkSize is None or (libSize is not None and libSize > tmpWorkSize):
                        tmpWorkSize = libSize
                    if tmpWorkSize is not None:
                        workSizeList.append(tmpWorkSize)

                    # CPU efficiency
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            tmpTimeDelta = endTime - startTime
                            float(tmpTimeDelta.seconds + tmpTimeDelta.days * 24 * 3600)
                            tmpCpuEff = int(
                                math.ceil(float(cpuConsumptionTime) / (coreCount * float(tmpTimeDelta.seconds + tmpTimeDelta.days * 24 * 3600)) * 100)
                            )
                            cpuEffList.append(tmpCpuEff)
                            cpuEffDict[tmpCpuEff] = pandaID
                            cpuEffMap[pandaID] = tmpCpuEff
                        except Exception:
                            pass

        # add tags
        def addTag(jobTagMap, idDict, value, tagStr):
            if value in idDict:
                tmpPandaID = idDict[value]
                if tmpPandaID not in jobTagMap:
                    jobTagMap[tmpPandaID] = []
                if tagStr not in jobTagMap[tmpPandaID]:
                    jobTagMap[tmpPandaID].append(tagStr)

        # calculate values
        jobTagMap = {}
        if outSizeList != []:
            median, origValues = JediCoreUtils.percentile(outSizeList, 75, outSizeDict)
            for origValue in origValues:
                addTag(jobTagMap, outSizeDict, origValue, "outDiskCount")
            median /= 1024
            # upper limit 10MB output per 1MB input
            upperLimit = 10 * 1024
            if median > upperLimit:
                median = upperLimit
            returnMap["outDiskCount"] = int(median)
            if preOutputScaleWithEvents:
                returnMap["outDiskUnit"] = "kBPerEvent"
            else:
                returnMap["outDiskUnit"] = "kB"
        if walltimeList != []:
            maxWallTime = max(walltimeList)
            extraInfo["maxCpuConsumptionTime"] = maxWallTime
            extraInfo["maxExecTime"] = execTimeMap[walltimeDict[maxWallTime]]
            extraInfo["defCoreCount"] = coreCountMap[walltimeDict[maxWallTime]]
            addTag(jobTagMap, walltimeDict, maxWallTime, "walltime")
            # cut off of 60min
            if maxWallTime < 60 * 60:
                maxWallTime = 0
            median = float(maxWallTime) / float(max(inFSizeList)) * 1.5
            median = math.ceil(median)
            returnMap["walltime"] = int(median)
            # use preset value if larger
            if preWalltime is not None and (preWalltime > returnMap["walltime"] or preWalltime < 0):
                returnMap["walltime"] = preWalltime
            # upper limit
            if returnMap["walltime"] > limitWallTime:
                returnMap["walltime"] = limitWallTime
        returnMap["walltimeUnit"] = "kSI2kseconds"
        if cpuTimeList != []:
            maxCpuTime, origValues = JediCoreUtils.percentile(cpuTimeList, cpuTimeRank, cpuTimeDict)
            for origValue in origValues:
                addTag(jobTagMap, cpuTimeDict, origValue, "cpuTime")
                try:
                    extraInfo["execTime"] = execTimeMap[cpuTimeDict[origValue]]
                except Exception:
                    pass
            maxCpuTime *= 1.5
            if maxCpuTime < 10:
                maxCpuTime *= 1000
                returnMap["cpuTimeUnit"] = "mHS06sPerEvent"
                if extraInfo["oldCpuTime"]:
                    extraInfo["oldCpuTime"] = int(extraInfo["oldCpuTime"] * 1000)
            elif preCpuTimeUnit is not None:
                # for mHS06sPerEvent -> HS06sPerEvent
                returnMap["cpuTimeUnit"] = "HS06sPerEvent"
            maxCpuTime = int(math.ceil(maxCpuTime))
            returnMap["cpuTime"] = maxCpuTime
        if ioIntentList != []:
            maxIoIntent = max(ioIntentList)
            addTag(jobTagMap, ioIntentDict, maxIoIntent, "ioIntensity")
            maxIoIntent = int(math.ceil(maxIoIntent))
            returnMap["ioIntensity"] = maxIoIntent
            returnMap["ioIntensityUnit"] = "kBPerS"
        if diskIoList != []:
            aveDiskIo = sum(diskIoList) // len(diskIoList)
            aveDiskIo = int(math.ceil(aveDiskIo))
            if capOnDiskIO is not None:
                aveDiskIo = min(aveDiskIo, capOnDiskIO)
            returnMap["diskIO"] = aveDiskIo
            returnMap["diskIOUnit"] = "kBPerS"
        if leak_list:
            ave_leak = int(math.ceil(sum(leak_list) / len(leak_list)))
            returnMap["memory_leak_core"] = ave_leak
        if leak_x2_list:
            ave_leak_x2 = int(math.ceil(sum(leak_x2_list) / len(leak_x2_list)))
            returnMap["memory_leak_x2"] = ave_leak_x2
        if memSizeList != []:
            memVal, origValues = JediCoreUtils.percentile(memSizeList, ramCountRank, memSizeDict)
            for origValue in origValues:
                addTag(jobTagMap, memSizeDict, origValue, "ramCount")
            memVal = memVal * (100 + ramCountMargin) // 100
            memVal /= 1024
            memVal = int(memVal)
            if memVal < 0:
                memVal = 1
            if minRamCount is not None and minRamCount > memVal:
                memVal = minRamCount
            if preRamUnit == "MBPerCore":
                returnMap["ramUnit"] = preRamUnit
                returnMap["ramCount"] = memVal
            elif preRamUnit == "MBPerCoreFixed":
                returnMap["ramUnit"] = preRamUnit
            else:
                returnMap["ramUnit"] = "MB"
                returnMap["ramCount"] = memVal
        if workSizeList != []:
            median = max(workSizeList)
            returnMap["workDiskCount"] = int(median)
            returnMap["workDiskUnit"] = "MB"
            # use preset value if larger
            if preWorkDiskCount is not None and preWorkDiskCount > returnMap["workDiskCount"]:
                returnMap["workDiskCount"] = preWorkDiskCount
        if cpuEffList != []:
            minCpuEfficiency = int(numpy.median(cpuEffList))
            addTag(jobTagMap, cpuEffDict, minCpuEfficiency, "cpuEfficiency")
            extraInfo["minCpuEfficiency"] = minCpuEfficiency
        nShortJobs = 0
        nShortJobsWithCtoS = 0
        nTotalForShort = 0
        longestShortExecTime = 0
        for tmpPandaID, tmpExecTime in execTimeMap.items():
            if tmpExecTime <= datetime.timedelta(minutes=shortExecTime):
                longestShortExecTime = max(longestShortExecTime, tmpExecTime.total_seconds())
                if site_mapper and task_spec:
                    # ignore if the site enforces to use copy-to-scratch
                    tmpSiteSpec = site_mapper.getSite(siteMap[tmpPandaID])
                    if not task_spec.useLocalIO() and not JediCoreUtils.use_direct_io_for_job(task_spec, tmpSiteSpec, None):
                        nShortJobsWithCtoS += 1
                        continue
                nShortJobs += 1
            nTotalForShort += 1
        extraInfo["nShortJobs"] = nShortJobs
        extraInfo["nShortJobsWithCtoS"] = nShortJobsWithCtoS
        extraInfo["nTotalForShort"] = nTotalForShort
        extraInfo["longestShortExecTime"] = longestShortExecTime
        nInefficientJobs = 0
        for tmpPandaID, tmpCpuEff in cpuEffMap.items():
            if tmpCpuEff < lowCpuEff:
                nInefficientJobs += 1
        extraInfo["nInefficientJobs"] = nInefficientJobs
        extraInfo["nTotalForIneff"] = len(cpuEffMap)
        # tag jobs
        if flagJob:
            for tmpPandaID, tmpTags in jobTagMap.items():
                self.updateJobMetrics_JEDI(jediTaskID, tmpPandaID, jMetricsMap[tmpPandaID], tmpTags)
        # reset NG
        taskSpec = JediTaskSpec()
        taskSpec.splitRule = splitRule
        if not mergeScout and taskSpec.getTgtMaxOutputForNG() is not None and "outDiskCount" in returnMap:
            # look for PandaID for outDiskCount
            for tmpPandaID, tmpTags in jobTagMap.items():
                if "outDiskCount" in tmpTags:
                    # get total and the largest output fsize
                    sqlBig = f"SELECT SUM(fsize) FROM {jedi_config.db.schemaPANDA}.filesTable4 WHERE PandaID=:PandaID AND type=:type GROUP BY dataset "
                    varMap = dict()
                    varMap[":PandaID"] = tmpPandaID
                    varMap[":type"] = "output"
                    self.cur.execute(sqlBig + comment, varMap)
                    resBig = self.cur.fetchall()
                    outTotal = 0
                    outBig = 0
                    for (tmpFsize,) in resBig:
                        outTotal += tmpFsize
                        if tmpFsize > outBig:
                            outBig = tmpFsize
                    if outTotal * outBig > 0:
                        # get NG
                        taskSpec.outDiskCount = returnMap["outDiskCount"]
                        taskSpec.outDiskUnit = returnMap["outDiskUnit"]
                        expectedOutSize = outTotal * taskSpec.getTgtMaxOutputForNG() * 1024 * 1024 * 1024 // outBig
                        outDiskCount = taskSpec.getOutDiskSize()
                        if "workDiskCount" in returnMap:
                            taskSpec.workDiskCount = returnMap["workDiskCount"]
                        else:
                            taskSpec.workDiskCount = preWorkDiskCount
                        taskSpec.workDiskUnit = "MB"
                        workDiskCount = taskSpec.getWorkDiskSize()
                        if outDiskCount == 0:
                            # to avoid zero-division
                            outDiskCount = 1
                        scaleFactor = expectedOutSize // outDiskCount
                        if preOutputScaleWithEvents:
                            # scaleFactor is num of events
                            try:
                                expectedInSize = (
                                    (inFSizeMap[tmpPandaID] + totInSizeMap[tmpPandaID] - masterInSize[tmpPandaID]) * scaleFactor // inEventsMap[tmpPandaID]
                                )
                                newNG = expectedOutSize + expectedInSize + workDiskCount - InputChunk.defaultOutputSize
                            except Exception:
                                newNG = None
                        else:
                            # scaleFactor is input size
                            newNG = expectedOutSize + scaleFactor * (1024 * 1024) - InputChunk.defaultOutputSize
                        if newNG is not None:
                            newNG /= 1024 * 1024 * 1024
                            if newNG <= 0:
                                newNG = 1
                            maxNG = 100
                            if newNG > maxNG:
                                newNG = maxNG
                            returnMap["newNG"] = int(newNG)
        if useTransaction:
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
        # filtered dump
        if scoutSucceeded and not mergeScout and len(returnMap) > 0:
            tmpMsg = f"scouts got for jediTaskID={jediTaskID} "
            tmpKeys = sorted(returnMap.keys())
            for tmpKey in tmpKeys:
                tmpMsg += f"{tmpKey}={returnMap[tmpKey]} "
            for tmpPandaID, tmpTags in jobTagMap.items():
                for tmpTag in tmpTags:
                    tmpMsg += f"{tmpTag}_by={tmpPandaID} "
            tmpLog.info(tmpMsg[:-1])
        # return
        tmpLog.debug(f"succeeded={scoutSucceeded} data={str(returnMap)} extra={str(extraInfo)} tag={jobTagMap}")
        return scoutSucceeded, returnMap, extraInfo

    # set scout job data
    def setScoutJobData_JEDI(self, taskSpec, useCommit, useExhausted, site_mapper):
        comment = " /* JediDBProxy.setScoutJobData_JEDI */"
        methodName = self.getMethodName(comment)
        jediTaskID = taskSpec.jediTaskID
        methodName += f" < jediTaskID={jediTaskID} label={taskSpec.prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # get thresholds for exausted
        ramThr = self.getConfigValue("dbproxy", "RAM_THR_EXAUSTED", "jedi")
        if ramThr is None:
            ramThr = 4
        ramThr *= 1024
        # send tasks to exhausted when task.successRate > rate >= thr
        minNumOkScoutsForExhausted = self.getConfigValue("dbproxy", f"SCOUT_MIN_OK_RATE_EXHAUSTED_{taskSpec.prodSourceLabel}", "jedi")
        scoutSuccessRate = taskSpec.getScoutSuccessRate()
        if scoutSuccessRate and minNumOkScoutsForExhausted:
            if scoutSuccessRate > minNumOkScoutsForExhausted * 10:
                scoutSuccessRate = minNumOkScoutsForExhausted * 10
            else:
                minNumOkScoutsForExhausted = None
        if useCommit:
            # begin transaction
            self.conn.begin()
        # set average job data
        scoutSucceeded, scoutData, extraInfo = self.getScoutJobData_JEDI(
            jediTaskID, scoutSuccessRate=scoutSuccessRate, flagJob=True, site_mapper=site_mapper, task_spec=taskSpec
        )
        # sql to update task data
        if scoutData != {}:
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sqlTSD = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET "
            for scoutKey, scoutVal in scoutData.items():
                # skip new NG
                if scoutKey in ["newNG"]:
                    continue
                tmpScoutKey = f":{scoutKey}"
                varMap[tmpScoutKey] = scoutVal
                sqlTSD += f"{scoutKey}={tmpScoutKey},"
            sqlTSD = sqlTSD[:-1]
            sqlTSD += " WHERE jediTaskID=:jediTaskID "
            tmpLog.debug(sqlTSD + comment + str(varMap))
            self.cur.execute(sqlTSD + comment, varMap)
            # update NG
            if "newNG" in scoutData:
                taskSpec.setSplitRule("nGBPerJob", str(scoutData["newNG"]))
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":splitRule"] = taskSpec.splitRule
                sqlTSL = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET splitRule=:splitRule "
                sqlTSL += " WHERE jediTaskID=:jediTaskID "
                tmpLog.debug(sqlTSL + comment + str(varMap))
                self.cur.execute(sqlTSL + comment, varMap)
        # set average merge job data
        mergeScoutSucceeded = None
        if taskSpec.mergeOutput():
            mergeScoutSucceeded, mergeScoutData, mergeExtraInfo = self.getScoutJobData_JEDI(jediTaskID, mergeScout=True)
            if mergeScoutData != {}:
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                sqlTSD = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET "
                for mergeScoutKey, mergeScoutVal in mergeScoutData.items():
                    # only walltime and ramCount
                    if not mergeScoutKey.startswith("walltime") and not mergeScoutKey.startswith("ram"):
                        continue
                    tmpScoutKey = f":{mergeScoutKey}"
                    varMap[tmpScoutKey] = mergeScoutVal
                    sqlTSD += f"merge{mergeScoutKey}={tmpScoutKey},"
                sqlTSD = sqlTSD[:-1]
                sqlTSD += " WHERE jediTaskID=:jediTaskID "
                tmpLog.debug(sqlTSD + comment + str(varMap))
                self.cur.execute(sqlTSD + comment, varMap)
        # go to exhausted if necessary
        nNewJobsCutoff = 20
        if useExhausted and scoutSucceeded and extraInfo["nNewJobs"] > nNewJobsCutoff:
            # check cpuTime
            if taskSpec.useHS06() and "cpuTime" in scoutData and "execTime" in extraInfo:
                minExecTime = 24
                if (
                    extraInfo["oldCpuTime"] not in [0, None]
                    and scoutData["cpuTime"] > 2 * extraInfo["oldCpuTime"]
                    and extraInfo["execTime"] > datetime.timedelta(hours=minExecTime)
                ):
                    errMsg = "#KV #ATM action=set_exhausted reason=scout_cpuTime ({0}) is larger than 2*task_cpuTime ({1})".format(
                        scoutData["cpuTime"], extraInfo["oldCpuTime"]
                    )
                    tmpLog.info(errMsg)
                    taskSpec.setErrDiag(errMsg)
                    taskSpec.status = "exhausted"

            # check ramCount
            if taskSpec.status != "exhausted":
                if (
                    taskSpec.ramPerCore()
                    and "ramCount" in scoutData
                    and extraInfo["oldRamCount"] is not None
                    and extraInfo["oldRamCount"] < ramThr < scoutData["ramCount"]
                ):
                    errMsg = f"#KV #ATM action=set_exhausted reason=scout_ramCount {scoutData['ramCount']} MB is larger than {ramThr} MB "
                    errMsg += f"while task_ramCount {extraInfo['oldRamCount']} MB is less than {ramThr} MB"
                    tmpLog.info(errMsg)
                    taskSpec.setErrDiag(errMsg)
                    taskSpec.status = "exhausted"

            # check memory leak
            if taskSpec.status != "exhausted":
                memory_leak_core_max = self.getConfigValue("dbproxy", f"SCOUT_MEM_LEAK_PER_CORE_{taskSpec.prodSourceLabel}", "jedi")
                memory_leak_core = scoutData.get("memory_leak_core")
                memory_leak_x2 = scoutData.get("memory_leak_x2")  # TODO: decide what to do with it
                if memory_leak_core and memory_leak_core_max and memory_leak_core > memory_leak_core_max:
                    errMsg = f"#ATM #KV action=set_exhausted since reason=scout_memory_leak {memory_leak_core} is larger than {memory_leak_core_max}"
                    tmpLog.info(errMsg)
                    taskSpec.setErrDiag(errMsg)
                    # taskSpec.status = 'exhausted'

            # short job check
            sl_changed = False
            if taskSpec.status != "exhausted":
                # get exectime threshold for exhausted
                maxShortJobs = self.getConfigValue("dbproxy", f"SCOUT_NUM_SHORT_{taskSpec.prodSourceLabel}", "jedi")
                shortJobCutoff = self.getConfigValue("dbproxy", f"SCOUT_THR_SHORT_{taskSpec.prodSourceLabel}", "jedi")
                if maxShortJobs and shortJobCutoff:
                    # many short jobs w/o copy-to-scratch
                    manyShortJobs = extraInfo["nTotalForShort"] > 0 and extraInfo["nShortJobs"] / extraInfo["nTotalForShort"] >= maxShortJobs / 10
                    if manyShortJobs:
                        toExhausted = True
                        # check expected number of jobs
                        if shortJobCutoff and max(extraInfo["expectedNumJobs"], extraInfo["expectedNumJobsWithEvent"]) < shortJobCutoff:
                            tmpLog.debug(
                                "not to set exhausted or change split rule since expect num of jobs "
                                "max({} file-based est., {} event-based est.) is less than {}".format(
                                    extraInfo["expectedNumJobs"], extraInfo["expectedNumJobsWithEvent"], shortJobCutoff
                                )
                            )
                            toExhausted = False
                        # remove wrong rules
                        if toExhausted and self.getConfigValue("dbproxy", f"SCOUT_CHANGE_SR_{taskSpec.prodSourceLabel}", "jedi"):
                            updateSL = []
                            removeSL = []
                            if taskSpec.getNumFilesPerJob() is not None:
                                taskSpec.removeNumFilesPerJob()
                                removeSL.append("nFilesPerJob")
                            if taskSpec.getMaxSizePerJob() is not None:
                                taskSpec.removeMaxSizePerJob()
                                removeSL.append("nGBPerJob")
                            MAX_NUM_FILES = 200
                            if taskSpec.getMaxNumFilesPerJob() is not None and taskSpec.getMaxNumFilesPerJob() < MAX_NUM_FILES:
                                taskSpec.setMaxNumFilesPerJob(str(MAX_NUM_FILES))
                                updateSL.append("MF")
                            if updateSL or removeSL:
                                sl_changed = True
                                tmpMsg = "#KV #ATM action=change_split_rule reason=many_shorter_jobs"
                                if removeSL:
                                    tmpMsg += f" removed {','.join(removeSL)},"
                                if updateSL:
                                    tmpMsg += f" changed {','.join(updateSL)},"
                                tmpMsg = tmpMsg[:-1]
                                tmpLog.info(tmpMsg)
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":splitRule"] = taskSpec.splitRule
                                sqlTSL = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET splitRule=:splitRule "
                                sqlTSL += " WHERE jediTaskID=:jediTaskID "
                                tmpLog.debug(sqlTSL + comment + str(varMap))
                                self.cur.execute(sqlTSL + comment, varMap)
                                toExhausted = False
                        # check scaled walltime
                        if toExhausted:
                            scMsg = ""
                            if taskSpec.useScout():
                                scaled_max_walltime = extraInfo["longestShortExecTime"]
                                scaled_max_walltime *= InputChunk.maxInputSizeAvalanche / InputChunk.maxInputSizeScouts
                                scaled_max_walltime = int(scaled_max_walltime / 60)
                                if scaled_max_walltime > extraInfo["shortExecTime"]:
                                    tmpLog.debug(
                                        "not to set exhausted since scaled execution time ({}) is longer "
                                        "than {} min".format(scaled_max_walltime, extraInfo["shortExecTime"])
                                    )
                                    toExhausted = False
                                else:
                                    scMsg = " and scaled execution time ({} = walltime * {}/{}) less than {} min".format(
                                        scaled_max_walltime, InputChunk.maxInputSizeAvalanche, InputChunk.maxInputSizeScouts, extraInfo["shortExecTime"]
                                    )
                        # go to exhausted
                        if toExhausted:
                            errMsg = "#ATM #KV action=set_exhausted since reason=many_shorter_jobs "
                            errMsg += (
                                "{}/{} jobs (greater than {}/10, excluding {} jobs that the site "
                                "config enforced "
                                "to run with copy-to-scratch) had shorter execution time than {} min "
                                "and the expected num of jobs max({} file-based est., {} event-based est.) is larger than {} {}".format(
                                    extraInfo["nShortJobs"],
                                    extraInfo["nTotalForShort"],
                                    maxShortJobs,
                                    extraInfo["nShortJobsWithCtoS"],
                                    extraInfo["shortExecTime"],
                                    extraInfo["expectedNumJobs"],
                                    extraInfo["expectedNumJobsWithEvent"],
                                    shortJobCutoff,
                                    scMsg,
                                )
                            )
                            tmpLog.info(errMsg)
                            taskSpec.setErrDiag(errMsg)
                            taskSpec.status = "exhausted"

            # CPU efficiency
            if taskSpec.status != "exhausted" and not sl_changed:
                # OK if minCpuEfficiency is satisfied
                if taskSpec.getMinCpuEfficiency() and extraInfo["minCpuEfficiency"] >= taskSpec.getMinScoutEfficiency():
                    pass
                else:
                    # get inefficiency threshold for exhausted
                    maxIneffJobs = self.getConfigValue("dbproxy", f"SCOUT_NUM_CPU_INEFFICIENT_{taskSpec.prodSourceLabel}", "jedi")
                    ineffJobCutoff = self.getConfigValue("dbproxy", f"SCOUT_THR_CPU_INEFFICIENT_{taskSpec.prodSourceLabel}", "jedi")
                    tmp_skip = False
                    errMsg = "#ATM #KV action=set_exhausted since reason=low_efficiency "
                    if taskSpec.getMinCpuEfficiency() and extraInfo["minCpuEfficiency"] < taskSpec.getMinCpuEfficiency():
                        tmp_skip = True
                        errMsg += f"lowest CPU efficiency {extraInfo['minCpuEfficiency']} is less than getMinCpuEfficiency={taskSpec.getMinCpuEfficiency()}"
                    elif (
                        maxIneffJobs
                        and extraInfo["nTotalForIneff"] > 0
                        and extraInfo["nInefficientJobs"] / extraInfo["nTotalForIneff"] >= maxIneffJobs / 10
                        and ineffJobCutoff
                        and max(extraInfo["expectedNumJobs"], extraInfo["expectedNumJobsWithEvent"]) > ineffJobCutoff
                    ):
                        tmp_skip = True
                        errMsg += (
                            "{}/{} jobs (greater than {}/10) had lower CPU efficiencies than {} "
                            "and expected num of jobs max({} file-based est, {} event-based est) is larger than {}".format(
                                extraInfo["nInefficientJobs"],
                                extraInfo["nTotalForIneff"],
                                maxIneffJobs,
                                extraInfo["cpuEfficiencyCap"],
                                extraInfo["expectedNumJobs"],
                                extraInfo["expectedNumJobsWithEvent"],
                                ineffJobCutoff,
                            )
                        )
                    if tmp_skip:
                        tmpLog.info(errMsg)
                        taskSpec.setErrDiag(errMsg)
                        taskSpec.status = "exhausted"

            # cpu abuse
            if taskSpec.status != "exhausted":
                try:
                    abuseOffset = 2
                    if extraInfo["maxCpuConsumptionTime"] > extraInfo["maxExecTime"].total_seconds() * extraInfo["defCoreCount"] * abuseOffset:
                        errMsg = f"#ATM #KV action=set_exhausted since reason=over_cpu_consumption {extraInfo['maxCpuConsumptionTime']} sec "
                        errMsg += "is larger than jobDuration*coreCount*safety ({0}*{1}*{2}). ".format(
                            extraInfo["maxExecTime"].total_seconds(), extraInfo["defCoreCount"], abuseOffset
                        )
                        errMsg += "Running multi-core payload on single core queues? #ATM"
                        tmpLog.info(errMsg)
                        taskSpec.setErrDiag(errMsg)
                        taskSpec.status = "exhausted"
                except Exception:
                    tmpLog.error("failed to check CPU abuse")
                    pass

            # low success rate
            if taskSpec.status != "exhausted" and minNumOkScoutsForExhausted:
                if taskSpec.getScoutSuccessRate() and extraInfo["successRate"] < taskSpec.getScoutSuccessRate() / 10:
                    errMsg = "#ATM #KV action=set_exhausted since reason=low_success_rate between {} and {} ".format(
                        minNumOkScoutsForExhausted, taskSpec.getScoutSuccessRate() / 10
                    )
                    tmpLog.info(errMsg)
                    taskSpec.setErrDiag(errMsg)
                    taskSpec.status = "exhausted"

        if useCommit:
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
        # reset the task resource type
        try:
            self.reset_resource_type_task(jediTaskID, useCommit)
        except Exception:
            tmpLog.error(f"reset_resource_type_task excepted with: {traceback.format_exc()}")

        return scoutSucceeded, mergeScoutSucceeded

    # set scout job data to tasks
    def setScoutJobDataToTasks_JEDI(self, vo, prodSourceLabel, site_mapper):
        comment = " /* JediDBProxy.setScoutJobDataToTasks_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retJediTaskIDs = []
        try:
            # sql to get tasks to set scout job data
            varMap = {}
            varMap[":status"] = "running"
            varMap[":minJobs"] = 5
            varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=24)
            sqlSCF = "SELECT tabT.jediTaskID "
            sqlSCF += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA,{1}.T_TASK tabD ".format(
                jedi_config.db.schemaJEDI, jedi_config.db.schemaDEFT
            )
            sqlSCF += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlSCF += "AND tabT.jediTaskID=tabD.taskID AND tabT.modificationTime>:timeLimit "
            sqlSCF += "AND tabT.status=:status AND tabT.walltimeUnit IS NULL "
            sqlSCF += "AND tabD.total_done_jobs>=:minJobs "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlSCF += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlSCF += "AND tabT.prodSourceLabel=:prodSourceLabel "
            # sql to update task status
            sqlTU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET status=:newStatus,modificationTime=CURRENT_DATE,"
            sqlTU += "errorDialog=:errorDialog,stateChangeTime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # begin transaction
            self.conn.begin()
            # get tasks
            tmpLog.debug(sqlSCF + comment + str(varMap))
            self.cur.execute(sqlSCF + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            nTasks = 0
            for (jediTaskID,) in resList:
                # get task
                tmpStat, taskSpec = self.getTaskWithID_JEDI(jediTaskID, False)
                if tmpStat:
                    tmpLog.debug(f"set jediTaskID={jediTaskID}")
                    self.setScoutJobData_JEDI(taskSpec, True, True, site_mapper)
                    # update exhausted task status
                    if taskSpec.status == "exhausted":
                        # begin transaction
                        self.conn.begin()
                        # update task status
                        varMap = {}
                        varMap[":jediTaskID"] = taskSpec.jediTaskID
                        varMap[":newStatus"] = taskSpec.status
                        varMap[":oldStatus"] = "running"
                        varMap[":errorDialog"] = taskSpec.errorDialog
                        self.cur.execute(sqlTU + comment, varMap)
                        nRow = self.cur.rowcount
                        # update DEFT task
                        if nRow > 0:
                            self.setDeftStatus_JEDI(taskSpec.jediTaskID, taskSpec.status)
                            self.setSuperStatus_JEDI(taskSpec.jediTaskID, taskSpec.status)
                            self.record_task_status_change(taskSpec.jediTaskID)
                            self.push_task_status_message(taskSpec, taskSpec.jediTaskID, taskSpec.status)
                        # commit
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        tmpLog.debug(f"set status={taskSpec.status} to jediTaskID={taskSpec.jediTaskID} with {nRow} since {taskSpec.errorDialog}")
                    nTasks += 1
            # return
            tmpLog.debug(f"done with {nTasks} tasks")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # prepare tasks to be finished
    def prepareTasksToBeFinished_JEDI(self, vo, prodSourceLabel, nTasks=50, simTasks=None, pid="lock", noBroken=False, site_mapper=None):
        comment = " /* JediDBProxy.prepareTasksToBeFinished_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # return value for failure
        failedRet = None
        # return list of taskids
        ret_list = []
        try:
            # sql to get tasks/datasets
            if simTasks is None:
                varMap = {}
                varMap[":taskstatus1"] = "running"
                varMap[":taskstatus2"] = "scouting"
                varMap[":taskstatus3"] = "merging"
                varMap[":taskstatus4"] = "preprocessing"
                varMap[":taskstatus5"] = "ready"
                varMap[":dsEndStatus1"] = "finished"
                varMap[":dsEndStatus2"] = "done"
                varMap[":dsEndStatus3"] = "failed"
                if vo is not None:
                    varMap[":vo"] = vo
                if prodSourceLabel is not None:
                    varMap[":prodSourceLabel"] = prodSourceLabel
                sql = "SELECT tabT.jediTaskID,tabT.status "
                sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
                sql += "AND tabT.status IN (:taskstatus1,:taskstatus2,:taskstatus3,:taskstatus4,:taskstatus5) "
                if vo is not None:
                    sql += "AND tabT.vo=:vo "
                if prodSourceLabel is not None:
                    sql += "AND prodSourceLabel=:prodSourceLabel "
                sql += "AND tabT.lockedBy IS NULL AND NOT EXISTS "
                sql += f"(SELECT 1 FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets tabD "
                sql += "WHERE tabD.jediTaskID=tabT.jediTaskID AND masterID IS NULL "
                sql += "AND type IN ("
                for tmpType in JediDatasetSpec.getProcessTypes():
                    mapKey = ":type_" + tmpType
                    sql += f"{mapKey},"
                    varMap[mapKey] = tmpType
                sql = sql[:-1]
                sql += ") AND NOT status IN (:dsEndStatus1,:dsEndStatus2,:dsEndStatus3) AND ("
                sql += "nFilesToBeUsed>nFilesFinished+nFilesFailed "
                sql += "OR (nFilesUsed=0 AND nFilesToBeUsed IS NOT NULL AND nFilesToBeUsed>0) "
                sql += "OR (nFilesToBeUsed IS NOT NULL AND nFilesToBeUsed>nFilesFinished+nFilesFailed)) "
                sql += f") AND rownum<={nTasks}"
            else:
                varMap = {}
                sql = "SELECT tabT.jediTaskID,tabT.status "
                sql += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks tabT "
                sql += "WHERE jediTaskID IN ("
                for tmpTaskIdx, tmpTaskID in enumerate(simTasks):
                    tmpKey = f":jediTaskID{tmpTaskIdx}"
                    varMap[tmpKey] = tmpTaskID
                    sql += f"{tmpKey},"
                sql = sql[:-1]
                sql += ") "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            tmpLog.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # make list
            jediTaskIDstatusMap = {}
            set_scout_data_only = set()
            for jediTaskID, taskStatus in resList:
                jediTaskIDstatusMap[jediTaskID] = taskStatus
            # tasks to force avalanche
            toAvalancheTasks = set()
            # get tasks for early avalanche
            if simTasks is None:
                minSuccessScouts = 5
                timeToCheck = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=10)
                varMap = {}
                varMap[":scouting"] = "scouting"
                varMap[":running"] = "running"
                if prodSourceLabel:
                    varMap[":prodSourceLabel"] = prodSourceLabel
                else:
                    varMap[":prodSourceLabel"] = "managed"
                varMap[":timeLimit"] = timeToCheck
                if vo is not None:
                    varMap[":vo"] = vo
                sqlEA = "SELECT jediTaskID,t_status,COUNT(*),SUM(CASE WHEN f_status='finished' THEN 1 ELSE 0 END) FROM "
                sqlEA += "(SELECT DISTINCT tabT.jediTaskID,tabT.status as t_status,tabF.PandaID,tabF.status as f_status "
                sqlEA += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA,{0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabF ".format(
                    jedi_config.db.schemaJEDI
                )
                sqlEA += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
                sqlEA += "AND tabT.jediTaskID=tabD.jediTaskID "
                sqlEA += "AND tabD.jediTaskID=tabF.jediTaskID AND tabD.datasetID=tabF.datasetID "
                sqlEA += "AND (tabT.status=:scouting OR (tabT.status=:running AND tabT.walltimeUnit IS NULL)) "
                sqlEA += "AND tabT.prodSourceLabel=:prodSourceLabel "
                sqlEA += "AND (tabT.assessmentTime IS NULL OR tabT.assessmentTime<:timeLimit) "
                if vo is not None:
                    sqlEA += "AND tabT.vo=:vo "
                sqlEA += "AND tabT.lockedBy IS NULL "
                sqlEA += "AND tabD.masterID IS NULL AND tabD.nFilesToBeUsed>0 "
                sqlEA += "AND tabD.type IN ("
                for tmpType in JediDatasetSpec.getInputTypes():
                    mapKey = ":type_" + tmpType
                    sqlEA += f"{mapKey},"
                    varMap[mapKey] = tmpType
                sqlEA = sqlEA[:-1]
                sqlEA += ") "
                sqlEA += "AND tabF.PandaID IS NOT NULL "
                sqlEA += ") "
                sqlEA += "GROUP BY jediTaskID,t_status "
                # get tasks
                tmpLog.debug(sqlEA + comment + str(varMap))
                self.cur.execute(sqlEA + comment, varMap)
                resList = self.cur.fetchall()
                # update assessmentTime
                sqlLK = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET assessmentTime=CURRENT_DATE "
                sqlLK += "WHERE jediTaskID=:jediTaskID AND (assessmentTime IS NULL OR assessmentTime<:timeLimit) "
                sqlLK += "AND (status=:scouting OR (status=:running AND walltimeUnit IS NULL)) "
                # append to list
                for jediTaskID, taskstatus, totJobs, totFinished in resList:
                    # update assessmentTime to avoid frequent check
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":timeLimit"] = timeToCheck
                    varMap[":scouting"] = "scouting"
                    varMap[":running"] = "running"
                    self.cur.execute(sqlLK + comment, varMap)
                    nRow = self.cur.rowcount
                    if nRow and totFinished and totFinished >= totJobs * minSuccessScouts / 10:
                        if jediTaskID not in jediTaskIDstatusMap:
                            if taskstatus == "running":
                                set_scout_data_only.add(jediTaskID)
                                msg_piece = "reset in running"
                            else:
                                msg_piece = "early avalanche"
                            jediTaskIDstatusMap[jediTaskID] = taskstatus
                            tmpLog.debug(f"got jediTaskID={jediTaskID} {totFinished}/{totJobs} finished for {msg_piece}")

            # get tasks to force avalanche
            if simTasks is None:
                taskstatus = "scouting"
                varMap = {}
                varMap[":taskstatus"] = taskstatus
                sqlFA = "SELECT jediTaskID "
                sqlFA += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
                sqlFA += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
                sqlFA += "AND tabT.status=:taskstatus "
                if prodSourceLabel is not None:
                    sqlFA += "AND prodSourceLabel=:prodSourceLabel "
                    varMap[":prodSourceLabel"] = prodSourceLabel
                if vo is not None:
                    sqlFA += "AND tabT.vo=:vo "
                    varMap[":vo"] = vo
                sqlFA += "AND tabT.walltimeUnit IS NOT NULL "
                # get tasks
                tmpLog.debug(sqlFA + comment + str(varMap))
                self.cur.execute(sqlFA + comment, varMap)
                resList = self.cur.fetchall()
                # append to list
                for (jediTaskID,) in resList:
                    if jediTaskID not in jediTaskIDstatusMap:
                        jediTaskIDstatusMap[jediTaskID] = taskstatus
                        toAvalancheTasks.add(jediTaskID)
                        tmpLog.debug(f"got jediTaskID={jediTaskID} to force avalanche")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            jediTaskIDList = list(jediTaskIDstatusMap.keys())
            random.shuffle(jediTaskIDList)
            tmpLog.debug(f"got {len(jediTaskIDList)} tasks")
            # sql to read task
            sqlRT = f"SELECT {JediTaskSpec.columnNames()} "
            sqlRT += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlRT += "WHERE jediTaskID=:jediTaskID AND status=:statusInDB AND lockedBy IS NULL FOR UPDATE NOWAIT "
            # sql to lock task
            sqlLK = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET lockedBy=:newLockedBy "
            sqlLK += "WHERE jediTaskID=:jediTaskID AND status=:status AND lockedBy IS NULL "
            # sql to read dataset status
            sqlRD = "SELECT datasetID,status,nFiles,nFilesFinished,nFilesFailed,masterID,state "
            sqlRD += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND status=:status AND type IN ("
            for tmpType in JediDatasetSpec.getProcessTypes():
                mapKey = ":type_" + tmpType
                sqlRD += f"{mapKey},"
            sqlRD = sqlRD[:-1]
            sqlRD += ") "
            # sql to check if there is mutable dataset
            sqlMTC = f"SELECT COUNT(*) FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlMTC += "WHERE jediTaskID=:jediTaskID AND state=:state AND masterID IS NULL AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlMTC += f"{mapKey},"
            sqlMTC = sqlMTC[:-1]
            sqlMTC += ") "
            # sql to update input dataset status
            sqlDIU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET status=:status,modificationTime=CURRENT_DATE "
            sqlDIU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update output/log dataset status
            sqlDOU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET status=:status,modificationTime=CURRENT_DATE "
            sqlDOU += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # sql to update status of mutable dataset
            sqlMUT = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET status=:status,modificationTime=CURRENT_DATE "
            sqlMUT += "WHERE jediTaskID=:jediTaskID AND state=:state "
            # sql to get nFilesToBeUsed of dataset
            sqlFUD = "SELECT tabD.datasetID,COUNT(*) FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(jedi_config.db.schemaJEDI)
            sqlFUD += "WHERE tabD.jediTaskID=tabC.jediTaskID AND tabD.datasetID=tabC.datasetID AND tabD.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlFUD += f"{mapKey},"
            sqlFUD = sqlFUD[:-1]
            sqlFUD += ") AND tabD.jediTaskID=:jediTaskID AND tabD.masterID IS NULL "
            sqlFUD += "AND NOT tabC.status IN (:status1,:status2,:status3,:status4) "
            sqlFUD += "GROUP BY tabD.datasetID "
            # sql to update nFiles of dataset
            sqlFUU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets SET nFilesToBeUsed=:nFilesToBeUsed,modificationTime=CURRENT_DATE "
            sqlFUU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update task status
            sqlTU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET status=:status,modificationTime=CURRENT_DATE,lockedBy=NULL,lockedTime=NULL,"
            sqlTU += "errorDialog=:errorDialog,splitRule=:splitRule,stateChangeTime=CURRENT_DATE,oldStatus=:oldStatus "
            sqlTU += "WHERE jediTaskID=:jediTaskID "
            # sql to unlock task
            sqlTUU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTUU += "SET lockedBy=NULL,lockedTime=NULL "
            sqlTUU += "WHERE jediTaskID=:jediTaskID AND status=:status "
            # sql to update split rule
            sqlUSL = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlUSL += "SET splitRule=:splitRule WHERE jediTaskID=:jediTaskID "
            # sql to reset walltimeUnit
            sqlRWU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET walltimeUnit=NULL "
            sqlRWU += "WHERE jediTaskID=:jediTaskID AND status=:status AND walltimeUnit IS NOT NULL "
            # loop over all tasks
            iTasks = 1
            for jediTaskID in jediTaskIDList:
                taskStatus = jediTaskIDstatusMap[jediTaskID]
                tmpLog.debug(f"start {iTasks}/{len(jediTaskIDList)} jediTaskID={jediTaskID} status={taskStatus}")
                iTasks += 1
                # begin transaction
                self.conn.begin()
                # read task
                toSkip = False
                errorDialog = None
                oldStatus = None
                try:
                    # read task
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":statusInDB"] = taskStatus
                    self.cur.execute(sqlRT + comment, varMap)
                    resRT = self.cur.fetchone()
                    # locked by another
                    if resRT is None:
                        tmpLog.debug(f"skip jediTaskID={jediTaskID} since status has changed")
                        toSkip = True
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        continue
                    else:
                        taskSpec = JediTaskSpec()
                        taskSpec.pack(resRT)
                        taskSpec.lockedBy = None
                        taskSpec.lockedTime = None
                    # lock
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":newLockedBy"] = pid
                    varMap[":status"] = taskStatus
                    self.cur.execute(sqlLK + comment, varMap)
                    nRow = self.cur.rowcount
                    if nRow != 1:
                        tmpLog.debug(f"failed to lock jediTaskID={jediTaskID}")
                        toSkip = True
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        continue
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        # resource busy and acquire with NOWAIT specified
                        toSkip = True
                        tmpLog.debug(f"skip locked jediTaskID={jediTaskID}")
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        continue
                    else:
                        # failed with something else
                        raise errType(errValue)
                # update dataset
                if not toSkip:
                    tmpLog.debug(
                        "jediTaskID={} status={} useScout={} isPostScout={} onlyData={}".format(
                            jediTaskID, taskSpec.status, taskSpec.useScout(), taskSpec.isPostScout(), jediTaskID in set_scout_data_only
                        )
                    )
                    if (
                        taskSpec.status == "scouting"
                        or jediTaskID in set_scout_data_only
                        or (taskSpec.status == "ready" and taskSpec.useScout() and not taskSpec.isPostScout())
                    ):
                        # reset walltimeUnit
                        if jediTaskID in toAvalancheTasks:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":status"] = taskSpec.status
                            self.cur.execute(sqlRWU + comment, varMap)
                        # set average job data
                        if jediTaskID in set_scout_data_only:
                            use_exhausted = False
                        else:
                            use_exhausted = True
                        scoutSucceeded, mergeScoutSucceeded = self.setScoutJobData_JEDI(taskSpec, False, use_exhausted, site_mapper)
                        if jediTaskID in set_scout_data_only:
                            toSkip = True
                            tmpLog.debug(f"done set only scout data for jediTaskID={jediTaskID} in status={taskSpec.status}")
                        else:
                            # get nFiles to be used
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":status1"] = "pending"
                            varMap[":status2"] = "lost"
                            varMap[":status3"] = "missing"
                            varMap[":status4"] = "staging"
                            for tmpType in JediDatasetSpec.getInputTypes():
                                mapKey = ":type_" + tmpType
                                varMap[mapKey] = tmpType
                            self.cur.execute(sqlFUD + comment, varMap)
                            resFUD = self.cur.fetchall()
                            # update nFiles to be used
                            for datasetID, nReadyFiles in resFUD:
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":nFilesToBeUsed"] = nReadyFiles
                                tmpLog.debug(f"jediTaskID={jediTaskID} datasetID={datasetID} set nFilesToBeUsed={nReadyFiles}")
                                self.cur.execute(sqlFUU + comment, varMap)
                            # new task status
                            if scoutSucceeded or noBroken or jediTaskID in toAvalancheTasks:
                                if taskSpec.status == "exhausted":
                                    # went to exhausted since real cpuTime etc is too large
                                    newTaskStatus = "exhausted"
                                    errorDialog = taskSpec.errorDialog
                                    oldStatus = taskStatus
                                else:
                                    newTaskStatus = "scouted"
                                taskSpec.setPostScout()
                            else:
                                newTaskStatus = "tobroken"
                                if taskSpec.getScoutSuccessRate() is None:
                                    errorDialog = "no scout jobs succeeded"
                                else:
                                    errorDialog = "not enough scout jobs succeeded"
                    elif taskSpec.status in ["running", "merging", "preprocessing", "ready"]:
                        # get input datasets
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":status"] = "ready"
                        for tmpType in JediDatasetSpec.getProcessTypes():
                            mapKey = ":type_" + tmpType
                            varMap[mapKey] = tmpType
                        self.cur.execute(sqlRD + comment, varMap)
                        resRD = self.cur.fetchall()
                        varMapList = []
                        mutableFlag = False
                        preprocessedFlag = False
                        for datasetID, dsStatus, nFiles, nFilesFinished, nFilesFailed, masterID, dsState in resRD:
                            # parent could be still running
                            if dsState == "mutable" and masterID is None:
                                mutableFlag = True
                                break
                            # check if there are unprocessed files
                            if masterID is None and nFiles and nFiles > nFilesFinished + nFilesFailed:
                                tmpLog.debug(f"skip jediTaskID={jediTaskID} datasetID={datasetID} has unprocessed files")
                                toSkip = True
                                break
                            # set status for input datasets
                            varMap = {}
                            varMap[":datasetID"] = datasetID
                            varMap[":jediTaskID"] = jediTaskID
                            if masterID is not None:
                                # seconday dataset, this will be reset in post-processor
                                varMap[":status"] = "done"
                            else:
                                # master dataset
                                if nFiles == nFilesFinished:
                                    # all succeeded
                                    varMap[":status"] = "done"
                                    preprocessedFlag = True
                                elif nFilesFinished == 0:
                                    # all failed
                                    varMap[":status"] = "failed"
                                else:
                                    # partially succeeded
                                    varMap[":status"] = "finished"
                            varMapList.append(varMap)
                        if not toSkip:
                            # check just in case if there is mutable dataset
                            if not mutableFlag:
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":state"] = "mutable"
                                for tmpType in JediDatasetSpec.getInputTypes():
                                    mapKey = ":type_" + tmpType
                                    varMap[mapKey] = tmpType
                                self.cur.execute(sqlMTC + comment, varMap)
                                resMTC = self.cur.fetchone()
                                (numMutable,) = resMTC
                                tmpLog.debug(f"jediTaskID={jediTaskID} has {numMutable} mutable datasets")
                                if numMutable > 0:
                                    mutableFlag = True
                            if mutableFlag:
                                # go to defined to trigger CF
                                newTaskStatus = "defined"
                                # change status of mutable datasets to trigger CF
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":state"] = "mutable"
                                varMap[":status"] = "toupdate"
                                self.cur.execute(sqlMUT + comment, varMap)
                                nRow = self.cur.rowcount
                                tmpLog.debug(f"jediTaskID={jediTaskID} updated {nRow} mutable datasets")
                            else:
                                # update input datasets
                                for varMap in varMapList:
                                    self.cur.execute(sqlDIU + comment, varMap)
                                # update output datasets
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":type1"] = "log"
                                varMap[":type2"] = "output"
                                varMap[":status"] = "prepared"
                                self.cur.execute(sqlDOU + comment, varMap)
                                # new task status
                                if taskSpec.status == "preprocessing" and preprocessedFlag:
                                    # failed preprocess goes to prepared to terminate the task
                                    newTaskStatus = "registered"
                                    # update split rule
                                    taskSpec.setPreProcessed()
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":splitRule"] = taskSpec.splitRule
                                    self.cur.execute(sqlUSL + comment, varMap)
                                else:
                                    newTaskStatus = "prepared"
                    else:
                        toSkip = True
                        tmpLog.debug(f"skip jediTaskID={jediTaskID} due to status={taskSpec.status}")
                    # update tasks
                    if not toSkip:
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":status"] = newTaskStatus
                        varMap[":oldStatus"] = oldStatus
                        varMap[":errorDialog"] = errorDialog
                        varMap[":splitRule"] = taskSpec.splitRule
                        self.cur.execute(sqlTU + comment, varMap)
                        tmpLog.debug(f"done new status={newTaskStatus} for jediTaskID={jediTaskID}{f' since {errorDialog}' if errorDialog else ''}")
                        if newTaskStatus == "exhausted":
                            self.setDeftStatus_JEDI(jediTaskID, newTaskStatus)
                            self.setSuperStatus_JEDI(jediTaskID, newTaskStatus)
                        self.record_task_status_change(jediTaskID)
                        self.push_task_status_message(taskSpec, jediTaskID, newTaskStatus)
                        ret_list.append(jediTaskID)
                    else:
                        # unlock
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":status"] = taskSpec.status
                        self.cur.execute(sqlTUU + comment, varMap)
                        nRow = self.cur.rowcount
                        tmpLog.debug(f"unlock jediTaskID={jediTaskID} in status={taskSpec.status} with {nRow}")
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return ret_list
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # get tasks to be assigned
    def getTasksToAssign_JEDI(self, vo, prodSourceLabel, workQueue, resource_name):
        comment = " /* JediDBProxy.getTasksToAssign_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < vo={vo} label={prodSourceLabel} queue={workQueue.queue_name} resource_name={resource_name} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retJediTaskIDs = []
        try:
            # sql to get tasks to assign
            varMap = {}
            varMap[":status"] = "assigning"
            varMap[":worldCloud"] = JediTaskSpec.worldCloudName
            varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=30)
            sqlSCF = "SELECT jediTaskID "
            sqlSCF += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlSCF += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlSCF += "AND tabT.status=:status AND tabT.modificationTime<:timeLimit "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlSCF += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlSCF += "AND prodSourceLabel=:prodSourceLabel "
            sqlSCF += "AND (cloud IS NULL OR "
            sqlSCF += "(cloud=:worldCloud AND (nucleus IS NULL OR EXISTS "
            sqlSCF += f"(SELECT 1 FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlSCF += f"WHERE {jedi_config.db.schemaJEDI}.JEDI_Datasets.jediTaskID=tabT.jediTaskID "
            sqlSCF += "AND type IN (:dsType1,:dsType2) AND destination IS NULL) "
            sqlSCF += "))) "
            varMap[":dsType1"] = "output"
            varMap[":dsType2"] = "log"
            if workQueue.is_global_share:
                sqlSCF += "AND gshare=:wq_name AND resource_type=:resource_name "
                sqlSCF += f"AND tabT.workqueue_id NOT IN (SELECT queue_id FROM {jedi_config.db.schemaJEDI}.jedi_work_queue WHERE queue_function = 'Resource') "
                varMap[":wq_name"] = workQueue.queue_name
                varMap[":resource_name"] = resource_name
            else:
                sqlSCF += "AND workQueue_ID=:wq_id "
                varMap[":wq_id"] = workQueue.queue_id
            sqlSCF += "ORDER BY currentPriority DESC,jediTaskID FOR UPDATE"
            sqlSPC = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET modificationTime=CURRENT_DATE,errorDialog=NULL "
            sqlSPC += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # get tasks
            tmpLog.debug(sqlSCF + comment + str(varMap))
            self.cur.execute(sqlSCF + comment, varMap)
            resList = self.cur.fetchall()
            for (jediTaskID,) in resList:
                # update modificationTime
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlSPC + comment, varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    retJediTaskIDs.append(jediTaskID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got {len(retJediTaskIDs)} tasks")
            return retJediTaskIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get tasks to check task assignment
    def getTasksToCheckAssignment_JEDI(self, vo, prodSourceLabel, workQueue, resource_name):
        comment = " /* JediDBProxy.getTasksToCheckAssignment_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel} queue={workQueue.queue_name}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retJediTaskIDs = []
        try:
            # sql to get tasks to assign
            varMap = {}
            varMap[":status"] = "assigning"
            varMap[":worldCloud"] = JediTaskSpec.worldCloudName
            sqlSCF = "SELECT jediTaskID "
            sqlSCF += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlSCF += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlSCF += "AND tabT.status=:status "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlSCF += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlSCF += "AND prodSourceLabel=:prodSourceLabel "
            sqlSCF += "AND (cloud IS NULL OR "
            sqlSCF += "(cloud=:worldCloud AND EXISTS "
            sqlSCF += f"(SELECT 1 FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlSCF += f"WHERE {jedi_config.db.schemaJEDI}.JEDI_Datasets.jediTaskID=tabT.jediTaskID "
            sqlSCF += "AND type IN (:dsType1,:dsType2) AND destination IS NULL) "
            sqlSCF += ")) "
            varMap[":dsType1"] = "output"
            varMap[":dsType2"] = "log"
            if workQueue.is_global_share:
                sqlSCF += "AND gshare=:wq_name AND resource_type=:resource_name "
                sqlSCF += f"AND tabT.workqueue_id NOT IN (SELECT queue_id FROM {jedi_config.db.schemaJEDI}.jedi_work_queue WHERE queue_function = 'Resource') "
                varMap[":wq_name"] = workQueue.queue_name
                varMap[":resource_name"] = resource_name
            else:
                sqlSCF += "AND workQueue_ID=:wq_id "
                varMap[":wq_id"] = workQueue.queue_id

            # begin transaction
            self.conn.begin()
            # get tasks
            tmpLog.debug(sqlSCF + comment + str(varMap))
            self.cur.execute(sqlSCF + comment, varMap)
            resList = self.cur.fetchall()
            for (jediTaskID,) in resList:
                retJediTaskIDs.append(jediTaskID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got {len(retJediTaskIDs)} tasks")
            return retJediTaskIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # set cloud to tasks
    def setCloudToTasks_JEDI(self, taskCloudMap):
        comment = " /* JediDBProxy.setCloudToTasks_JEDI */"
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            if taskCloudMap != {}:
                for jediTaskID, tmpVal in taskCloudMap.items():
                    # begin transaction
                    self.conn.begin()
                    if isinstance(tmpVal, str):
                        # sql to set cloud
                        sql = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
                        sql += "SET cloud=:cloud,status=:status,oldStatus=NULL,stateChangeTime=CURRENT_DATE "
                        sql += "WHERE jediTaskID=:jediTaskID AND cloud IS NULL "
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":status"] = "ready"
                        varMap[":cloud"] = tmpVal
                        # set cloud
                        self.cur.execute(sql + comment, varMap)
                        nRow = self.cur.rowcount
                        tmpLog.debug(f"set cloud={tmpVal} for jediTaskID={jediTaskID} with {nRow}")
                    else:
                        # sql to set destinations for WORLD cloud
                        sql = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
                        sql += "SET storageToken=:token,destination=:destination "
                        sql += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                        for tmpItem in tmpVal["datasets"]:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = tmpItem["datasetID"]
                            varMap[":token"] = tmpItem["token"]
                            varMap[":destination"] = tmpItem["destination"]
                            self.cur.execute(sql + comment, varMap)
                            tmpLog.debug(f"set token={tmpItem['token']} for jediTaskID={jediTaskID} datasetID={tmpItem['datasetID']}")
                        # sql to set ready
                        sql = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
                        sql += "SET nucleus=:nucleus,status=:newStatus,oldStatus=NULL,stateChangeTime=CURRENT_DATE,modificationTime=CURRENT_DATE-1/24 "
                        sql += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":nucleus"] = tmpVal["nucleus"]
                        varMap[":newStatus"] = "ready"
                        varMap[":oldStatus"] = "assigning"
                        self.cur.execute(sql + comment, varMap)
                        nRow = self.cur.rowcount
                        tmpLog.debug(f"set nucleus={tmpVal['nucleus']} for jediTaskID={jediTaskID} with {nRow}")
                        newStatus = varMap[":newStatus"]
                    # update DEFT
                    if nRow > 0:
                        deftStatus = "ready"
                        self.setDeftStatus_JEDI(jediTaskID, deftStatus)
                        self.setSuperStatus_JEDI(jediTaskID, deftStatus)
                        # get parameters to enable jumbo
                        sqlRT = f"SELECT eventService,site,useJumbo,splitRule FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
                        sqlRT += "WHERE jediTaskID=:jediTaskID "
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        self.cur.execute(sqlRT + comment, varMap)
                        resRT = self.cur.fetchone()
                        if resRT is not None:
                            eventService, site, useJumbo, splitRule = resRT
                            # enable jumbo
                            self.enableJumboInTask_JEDI(jediTaskID, eventService, site, useJumbo, splitRule)
                        # task status logging
                        self.record_task_status_change(jediTaskID)
                        try:
                            (newStatus, splitRule)
                        except NameError:
                            pass
                        else:
                            self.push_task_status_message(None, jediTaskID, newStatus, splitRule)
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # calculate RW for tasks
    def calculateTaskRW_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.calculateTaskRW_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get RW
            sql = "SELECT ROUND(SUM((nFiles-nFilesFinished-nFilesFailed-nFilesOnHold)*walltime)/24/3600) "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
            sql += "AND tabT.jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # begin transaction
            self.conn.begin()
            # get
            self.cur.execute(sql + comment, varMap)
            resRT = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # locked by another
            if resRT is None:
                retVal = None
            else:
                retVal = resRT[0]
            tmpLog.debug(f"RW={retVal}")
            # return
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # calculate RW with a priority
    def calculateRWwithPrio_JEDI(self, vo, prodSourceLabel, workQueue, priority):
        comment = " /* JediDBProxy.calculateRWwithPrio_JEDI */"
        methodName = self.getMethodName(comment)
        if workQueue is None:
            methodName += f" <vo={vo} label={prodSourceLabel} queue={None} prio={priority}>"
        else:
            methodName += f" <vo={vo} label={prodSourceLabel} queue={workQueue.queue_name} prio={priority}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get RW
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            if priority is not None:
                varMap[":priority"] = priority
            sql = "SELECT tabT.jediTaskID,tabT.cloud,tabD.datasetID,nFiles-nFilesFinished-nFilesFailed,walltime "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sql += "AND tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
            sql += "AND (nFiles-nFilesFinished-nFilesFailed)>0 "
            sql += "AND tabT.vo=:vo AND prodSourceLabel=:prodSourceLabel "

            if priority is not None:
                sql += "AND currentPriority>=:priority "

            if workQueue is not None:
                if workQueue.is_global_share:
                    sql += "AND gshare=:wq_name "
                    sql += f"AND tabT.workqueue_id NOT IN (SELECT queue_id FROM {jedi_config.db.schemaJEDI}.jedi_work_queue WHERE queue_function = 'Resource') "
                    varMap[":wq_name"] = workQueue.queue_name
                else:
                    sql += "AND workQueue_ID=:wq_id "
                    varMap[":wq_id"] = workQueue.queue_id

            sql += "AND tabT.status IN (:status1,:status2,:status3,:status4) "
            sql += "AND tabD.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sql += f"{mapKey},"
                varMap[mapKey] = tmpType
            sql = sql[:-1]
            sql += ") "
            varMap[":status1"] = "ready"
            varMap[":status2"] = "scouting"
            varMap[":status3"] = "running"
            varMap[":status4"] = "pending"
            sql += "AND tabT.cloud IS NOT NULL "
            # begin transaction
            self.conn.begin()
            # set cloud
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all tasks
            retMap = {}
            sqlF = "SELECT fsize,startEvent,endEvent,nEvents "
            sqlF += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND rownum<=1"
            for jediTaskID, cloud, datasetID, nRem, walltime in resList:
                # get effective size
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                # begin transaction
                self.conn.begin()
                # get file
                self.cur.execute(sqlF + comment, varMap)
                resFile = self.cur.fetchone()
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                if resFile is not None:
                    # calculate RW using effective size
                    fsize, startEvent, endEvent, nEvents = resFile
                    effectiveFsize = JediCoreUtils.getEffectiveFileSize(fsize, startEvent, endEvent, nEvents)
                    tmpRW = nRem * effectiveFsize * walltime
                    if cloud not in retMap:
                        retMap[cloud] = 0
                    retMap[cloud] += tmpRW
            for cloudName, rwValue in retMap.items():
                retMap[cloudName] = int(rwValue / 24 / 3600)
            tmpLog.debug(f"RW={str(retMap)}")
            # return
            tmpLog.debug("done")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # calculate WORLD RW with a priority
    def calculateWorldRWwithPrio_JEDI(self, vo, prodSourceLabel, workQueue, priority):
        comment = " /* JediDBProxy.calculateWorldRWwithPrio_JEDI */"
        methodName = self.getMethodName(comment)
        if workQueue is None:
            methodName += f" <vo={vo} label={prodSourceLabel} queue={None} prio={priority}>"
        else:
            methodName += f" <vo={vo} label={prodSourceLabel} queue={workQueue.queue_name} prio={priority}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get RW
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":worldCloud"] = JediTaskSpec.worldCloudName
            if priority is not None:
                varMap[":priority"] = priority
            sql = "SELECT tabT.nucleus,SUM((nEvents-nEventsUsed)*(CASE WHEN cpuTime IS NULL THEN 300 ELSE cpuTime END)) "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sql += "AND tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
            sql += "AND (nFiles-nFilesFinished-nFilesFailed)>0 "
            sql += "AND tabT.vo=:vo AND prodSourceLabel=:prodSourceLabel "
            sql += "AND tabT.cloud=:worldCloud "

            if priority is not None:
                sql += "AND currentPriority>=:priority "

            if workQueue is not None:
                if workQueue.is_global_share:
                    sql += "AND gshare=:wq_name "
                    sql += f"AND tabT.workqueue_id NOT IN (SELECT queue_id FROM {jedi_config.db.schemaJEDI}.jedi_work_queue WHERE queue_function = 'Resource') "
                    varMap[":wq_name"] = workQueue.queue_name
                else:
                    sql += "AND workQueue_ID=:wq_id "
                    varMap[":wq_id"] = workQueue.queue_id

            sql += "AND tabT.status IN (:status1,:status2,:status3,:status4) "
            sql += "AND tabD.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sql += f"{mapKey},"
                varMap[mapKey] = tmpType
            sql = sql[:-1]
            sql += ") "
            varMap[":status1"] = "ready"
            varMap[":status2"] = "scouting"
            varMap[":status3"] = "running"
            varMap[":status4"] = "pending"
            sql += "GROUP BY tabT.nucleus "
            # begin transaction
            self.conn.begin()
            # set cloud
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all nuclei
            retMap = {}
            for nucleus, worldRW in resList:
                retMap[nucleus] = worldRW
            tmpLog.debug(f"RW={str(retMap)}")
            # return
            tmpLog.debug("done")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # calculate WORLD RW for tasks
    def calculateTaskWorldRW_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.calculateTaskWorldRW_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get RW
            sql = (
                "SELECT (nEvents-nEventsUsed)*(CASE "
                "WHEN cpuTime IS NULL THEN 300 "
                "WHEN cpuTimeUnit='mHS06sPerEvent' OR cpuTimeUnit='mHS06sPerEventFixed' THEN cpuTime/1000 "
                "ELSE cpuTime END) "
            )
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
            sql += "AND tabT.jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # begin transaction
            self.conn.begin()
            # get
            self.cur.execute(sql + comment, varMap)
            resRT = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # locked by another
            if resRT is None:
                retVal = None
            else:
                retVal = resRT[0]
            tmpLog.debug(f"RW={retVal}")
            # return
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get the list of tasks to exec command
    def getTasksToExecCommand_JEDI(self, vo, prodSourceLabel):
        comment = " /* JediDBProxy.getTasksToExecCommand_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retTaskIDs = {}
        commandStatusMap = JediTaskSpec.commandStatusMap()
        try:
            # sql to get jediTaskIDs to exec a command from the command table
            varMap = {}
            varMap[":comm_owner"] = "DEFT"
            sqlC = f"SELECT comm_task,comm_cmd,comm_comment FROM {jedi_config.db.schemaDEFT}.PRODSYS_COMM "
            sqlC += "WHERE comm_owner=:comm_owner AND comm_cmd IN ("
            for commandStr, taskStatusMap in commandStatusMap.items():
                tmpKey = f":comm_cmd_{commandStr}"
                varMap[tmpKey] = commandStr
                sqlC += f"{tmpKey},"
            sqlC = sqlC[:-1]
            sqlC += ") "
            if vo not in [None, "any"]:
                varMap[":comm_vo"] = vo
                sqlC += "AND comm_vo=:comm_vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":comm_prodSourceLabel"] = prodSourceLabel
                sqlC += "AND comm_prodSourceLabel=:comm_prodSourceLabel "
            sqlC += "ORDER BY comm_ts "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            tmpLog.debug(sqlC + comment + str(varMap))
            self.cur.execute(sqlC + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"got {len(resList)} tasks")
            for jediTaskID, commandStr, comComment in resList:
                tmpLog.debug(f"start jediTaskID={jediTaskID} command={commandStr}")
                # start transaction
                self.conn.begin()
                # lock
                varMap = {}
                varMap[":comm_task"] = jediTaskID
                sqlLock = f"SELECT comm_cmd FROM {jedi_config.db.schemaDEFT}.PRODSYS_COMM WHERE comm_task=:comm_task "
                sqlLock += "FOR UPDATE "
                toSkip = False
                sync_action_only = False
                resetFrozenTime = False
                try:
                    tmpLog.debug(sqlLock + comment + str(varMap))
                    self.cur.execute(sqlLock + comment, varMap)
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        # resource busy and acquire with NOWAIT specified
                        toSkip = True
                        tmpLog.debug(f"skip locked+nowauit jediTaskID={jediTaskID}")
                    else:
                        # failed with something else
                        raise errType(errValue)
                isOK = True
                update_task = True
                if not toSkip:
                    if isOK:
                        # check task status
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        sqlTC = f"SELECT status,oldStatus,wallTimeUnit FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
                        sqlTC += "WHERE jediTaskID=:jediTaskID FOR UPDATE "
                        self.cur.execute(sqlTC + comment, varMap)
                        resTC = self.cur.fetchone()
                        if resTC is None or resTC[0] is None:
                            tmpLog.error(f"jediTaskID={jediTaskID} is not found in JEDI_Tasks")
                            isOK = False
                        else:
                            taskStatus, taskOldStatus, wallTimeUnit = resTC
                            tmpLog.debug(f"jediTaskID={jediTaskID} in status:{taskStatus} old:{taskOldStatus} com:{commandStr}")
                            if commandStr == "retry":
                                if taskStatus not in JediTaskSpec.statusToRetry():
                                    # task is in a status which rejects retry
                                    tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr}. status={taskStatus} is not for retry")
                                    isOK = False
                            elif commandStr == "incexec":
                                if taskStatus not in JediTaskSpec.statusToIncexec():
                                    # task is in a status which rejects retry
                                    tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr}. status={taskStatus} is not for incexec")
                                    isOK = False
                            elif commandStr == "pause":
                                if taskStatus in JediTaskSpec.statusNotToPause():
                                    # task is in a status which rejects pause
                                    tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr}. status={taskStatus} is not for pause")
                                    isOK = False
                            elif commandStr == "resume":
                                if taskStatus not in ["paused", "throttled", "staging"]:
                                    # task is in a status which rejects resume
                                    tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr}. status={taskStatus} is not for resume")
                                    isOK = False
                            elif commandStr == "avalanche":
                                if taskStatus not in ["scouting"]:
                                    # task is in a status which rejects avalanche
                                    tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr}. status={taskStatus} is not for avalanche")
                                    isOK = False
                            elif commandStr == "release":
                                if taskStatus not in ["scouting", "pending", "running", "ready", "assigning", "defined"]:
                                    # task is in a status which rejects avalanche
                                    tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr}. status={taskStatus} is not applicable")
                                    isOK = False
                                update_task = False
                                sync_action_only = True
                            elif taskStatus in JediTaskSpec.statusToRejectExtChange():
                                # task is in a status which rejects external changes
                                tmpLog.error(f"jediTaskID={jediTaskID} rejected command={commandStr} (due to status={taskStatus})")
                                isOK = False
                            if isOK:
                                # set new task status
                                if commandStr == "retry" and taskStatus == "exhausted" and taskOldStatus in ["running", "scouting"]:
                                    # change task status only since retryTask increments attemptNrs for existing jobs
                                    if taskOldStatus == "scouting" and wallTimeUnit:
                                        # go to running since scouting passed, to avoid being prepared again
                                        newTaskStatus = "running"
                                    else:
                                        newTaskStatus = taskOldStatus
                                    sync_action_only = True
                                    resetFrozenTime = True
                                elif commandStr in ["avalanche"]:
                                    newTaskStatus = "scouting"
                                    sync_action_only = True
                                elif commandStr == "resume" and taskStatus == "staging":
                                    newTaskStatus = "staged"
                                    sync_action_only = True
                                elif commandStr in commandStatusMap:
                                    newTaskStatus = commandStatusMap[commandStr]["doing"]
                                else:
                                    tmpLog.error(f"jediTaskID={jediTaskID} new status is undefined for command={commandStr}")
                                    isOK = False
                    if isOK:
                        # actions in transaction
                        if commandStr == "release":
                            self.updateInputDatasetsStagedAboutIdds_JEDI(jediTaskID, None, use_commit=False)
                    if isOK and update_task:
                        # update task status
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":taskStatus"] = taskStatus
                        if newTaskStatus != "dummy":
                            varMap[":status"] = newTaskStatus
                        varMap[":errDiag"] = comComment
                        sqlTU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
                        if newTaskStatus != "dummy":
                            sqlTU += "SET status=:status,"
                        else:
                            if taskOldStatus is None:
                                tmpLog.error("jediTaskID={0} has oldStatus=None and status={1} for ".format(jediTaskID, taskStatus, commandStr))
                                isOK = False
                            sqlTU += "SET status=oldStatus,"
                        if taskStatus in ["paused"] or sync_action_only:
                            sqlTU += "oldStatus=NULL,"
                        elif taskStatus in ["throttled"] and commandStr in ["pause", "reassign"]:
                            # unchange oldStatus when throttled->paused/toreassign
                            pass
                        elif taskStatus not in ["pending"]:
                            sqlTU += "oldStatus=status,"
                        if commandStr in ["avalanche"]:
                            # set dummy wallTimeUnit to trigger avalanche
                            sqlTU += "wallTimeUnit=:wallTimeUnit,"
                            varMap[":wallTimeUnit"] = "ava"
                        if resetFrozenTime:
                            sqlTU += "frozenTime=NULL,"
                        sqlTU += "modificationTime=CURRENT_DATE,errorDialog=:errDiag,stateChangeTime=CURRENT_DATE "
                        sqlTU += "WHERE jediTaskID=:jediTaskID AND status=:taskStatus "
                        if isOK:
                            tmpLog.debug(sqlTU + comment + str(varMap))
                            self.cur.execute(sqlTU + comment, varMap)
                            nRow = self.cur.rowcount
                        else:
                            nRow = 0
                        if nRow != 1:
                            tmpLog.debug(f"skip updated jediTaskID={jediTaskID}")
                            toSkip = True
                        else:
                            # update T_TASK
                            if (
                                newTaskStatus in ["paused"]
                                or (newTaskStatus in ["running", "ready", "scouting"] and taskStatus in ["paused", "exhausted"])
                                or newTaskStatus in ["staged"]
                            ):
                                if newTaskStatus == "scouting":
                                    deftStatus = "submitting"
                                elif newTaskStatus == "staged":
                                    deftStatus = "registered"
                                else:
                                    deftStatus = newTaskStatus
                                self.setDeftStatus_JEDI(jediTaskID, deftStatus)
                                self.setSuperStatus_JEDI(jediTaskID, deftStatus)
                                # add missing record_task_status_change and push_task_status_message updates
                                self.record_task_status_change(jediTaskID)
                                self.push_task_status_message(None, jediTaskID, newTaskStatus)
                    # update command table
                    if not toSkip:
                        varMap = {}
                        varMap[":comm_task"] = jediTaskID
                        if isOK:
                            varMap[":comm_cmd"] = commandStr + "ing"
                        else:
                            varMap[":comm_cmd"] = commandStr + " failed"
                        sqlUC = f"UPDATE {jedi_config.db.schemaDEFT}.PRODSYS_COMM SET comm_cmd=:comm_cmd WHERE comm_task=:comm_task "
                        self.cur.execute(sqlUC + comment, varMap)
                        # append
                        if isOK:
                            if commandStr not in ["pause", "resume"] and not sync_action_only:
                                retTaskIDs[jediTaskID] = {"command": commandStr, "comment": comComment, "oldStatus": taskStatus}
                                # use old status if pending or throttled
                                if taskStatus in ["pending", "throttled"]:
                                    retTaskIDs[jediTaskID]["oldStatus"] = taskOldStatus
                            # update job table
                            if commandStr in ["pause", "resume"]:
                                sqlJT = f"UPDATE {jedi_config.db.schemaPANDA}.jobsActive4 "
                                sqlJT += "SET jobStatus=:newJobStatus "
                                sqlJT += "WHERE jediTaskID=:jediTaskID AND jobStatus=:oldJobStatus "
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                if commandStr == "resume":
                                    varMap[":newJobStatus"] = "activated"
                                    varMap[":oldJobStatus"] = "throttled"
                                else:
                                    varMap[":newJobStatus"] = "throttled"
                                    varMap[":oldJobStatus"] = "activated"
                                self.cur.execute(sqlJT + comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            # find orphaned tasks to rescue
            for commandStr, taskStatusMap in commandStatusMap.items():
                varMap = {}
                varMap[":status"] = taskStatusMap["doing"]
                # skip dummy status
                if varMap[":status"] in ["dummy", "paused"]:
                    continue
                self.conn.begin()
                # FIXME
                # varMap[':timeLimit'] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=1)
                varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=5)
                sqlOrpS = "SELECT jediTaskID,errorDialog,oldStatus "
                sqlOrpS += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
                sqlOrpS += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
                sqlOrpS += "AND tabT.status=:status AND tabT.modificationtime<:timeLimit "
                if vo not in [None, "any"]:
                    sqlOrpS += "AND vo=:vo "
                    varMap[":vo"] = vo
                if prodSourceLabel not in [None, "any"]:
                    sqlOrpS += "AND prodSourceLabel=:prodSourceLabel "
                    varMap[":prodSourceLabel"] = prodSourceLabel
                sqlOrpS += "FOR UPDATE "
                tmpLog.debug(sqlOrpS + comment + str(varMap))
                self.cur.execute(sqlOrpS + comment, varMap)
                resList = self.cur.fetchall()
                # update modtime to avoid immediate reattempts
                sqlOrpU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET modificationtime=CURRENT_DATE "
                sqlOrpU += "WHERE jediTaskID=:jediTaskID "
                for jediTaskID, comComment, oldStatus in resList:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    tmpLog.debug(sqlOrpU + comment + str(varMap))
                    self.cur.execute(sqlOrpU + comment, varMap)
                    nRow = self.cur.rowcount
                    if nRow == 1 and jediTaskID not in retTaskIDs:
                        retTaskIDs[jediTaskID] = {"command": commandStr, "comment": comComment, "oldStatus": oldStatus}
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            # read clob
            sqlCC = f"SELECT comm_parameters FROM {jedi_config.db.schemaDEFT}.PRODSYS_COMM WHERE comm_task=:comm_task "
            for jediTaskID in retTaskIDs.keys():
                if retTaskIDs[jediTaskID]["command"] in ["incexec"]:
                    # start transaction
                    self.conn.begin()
                    varMap = {}
                    varMap[":comm_task"] = jediTaskID
                    self.cur.execute(sqlCC + comment, varMap)
                    tmpComComment = None
                    for (clobCC,) in self.cur:
                        if clobCC is not None:
                            tmpComComment = clobCC
                        break
                    if tmpComComment not in ["", None]:
                        retTaskIDs[jediTaskID]["comment"] = tmpComComment
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
            # convert to list
            retTaskList = []
            for jediTaskID, varMap in retTaskIDs.items():
                retTaskList.append((jediTaskID, varMap))
            # return
            tmpLog.debug(f"return {len(retTaskList)} tasks")
            return retTaskList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get the list of PandaIDs for a task
    def getPandaIDsWithTask_JEDI(self, jediTaskID, onlyActive):
        comment = " /* JediDBProxy.getPandaIDsWithTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} onlyActive={onlyActive}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retPandaIDs = set()
        try:
            # sql to get PandaIDs
            tables = [
                f"{jedi_config.db.schemaPANDA}.jobsDefined4",
                f"{jedi_config.db.schemaPANDA}.jobsWaiting4",
                f"{jedi_config.db.schemaPANDA}.jobsActive4",
            ]
            if not onlyActive:
                tables += [f"{jedi_config.db.schemaPANDA}.jobsArchived4", f"{jedi_config.db.schemaPANDAARCH}.jobsArchived"]
            sqlP = ""
            for tableName in tables:
                if sqlP != "":
                    sqlP += "UNION ALL "
                sqlP += f"SELECT PandaID FROM {tableName} WHERE jediTaskID=:jediTaskID "
                if tableName.startswith(jedi_config.db.schemaPANDAARCH):
                    sqlP += "AND modificationTime>(CURRENT_DATE-30) "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000000
            self.cur.execute(sqlP + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            for (pandaID,) in resList:
                retPandaIDs.add(pandaID)
            # return
            tmpLog.debug(f"return {len(retPandaIDs)} PandaIDs")
            return list(retPandaIDs)
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get the list of queued PandaIDs for a task
    def getQueuedPandaIDsWithTask_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getQueuedPandaIDsWithTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retPandaIDs = []
        try:
            # sql to get PandaIDs
            tables = [
                f"{jedi_config.db.schemaPANDA}.jobsDefined4",
                f"{jedi_config.db.schemaPANDA}.jobsWaiting4",
                f"{jedi_config.db.schemaPANDA}.jobsActive4",
            ]
            sqlP = ""
            for tableName in tables:
                if sqlP != "":
                    sqlP += "UNION ALL "
                sqlP += f"SELECT PandaID FROM {tableName} WHERE jediTaskID=:jediTaskID "
                sqlP += "AND jobStatus NOT IN (:st1,:st2,:st3) "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":st1"] = "running"
            varMap[":st2"] = "holding"
            varMap[":st3"] = "transferring"
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000000
            self.cur.execute(sqlP + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            for (pandaID,) in resList:
                if pandaID not in retPandaIDs:
                    retPandaIDs.append(pandaID)
            # return
            tmpLog.debug(f"return {len(retPandaIDs)} PandaIDs")
            return retPandaIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get jediTaskID/datasetID/FileID with dataset and file names
    def getIDsWithFileDataset_JEDI(self, datasetName, fileName, fileType):
        comment = " /* JediDBProxy.getIDsWithFileDataset_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <dataset={datasetName} file={fileName} type={fileType}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retPandaIDs = []
        try:
            # sql to get jediTaskID and datasetID
            sqlT = f"SELECT jediTaskID,datasetID FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets WHERE "
            sqlT += "datasetName=:datasetName and type=:type "
            # sql to get fileID
            sqlF = f"SELECT FileID FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            sqlF += "jediTaskID=:jediTaskID AND datasetID=:datasetID and lfn=:lfn "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":datasetName"] = datasetName
            varMap[":type"] = fileType
            self.cur.arraysize = 1000000
            self.cur.execute(sqlT + comment, varMap)
            resList = self.cur.fetchall()
            retMap = None
            for jediTaskID, datasetID in resList:
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":lfn"] = fileName
                self.cur.execute(sqlF + comment, varMap)
                resFileList = self.cur.fetchall()
                if resFileList != []:
                    retMap = {}
                    retMap["jediTaskID"] = jediTaskID
                    retMap["datasetID"] = datasetID
                    retMap["fileID"] = resFileList[0][0]
                    break
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"return {str(retMap)}")
            return True, retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, None

    # get JOBSARCHVIEW corresponding to a timestamp
    def getArchView(self, timeStamp):
        tableList = [
            (7, "JOBSARCHVIEW_7DAYS"),
            (15, "JOBSARCHVIEW_15DAYS"),
            (30, "JOBSARCHVIEW_30DAYS"),
            (60, "JOBSARCHVIEW_60DAYS"),
            (90, "JOBSARCHVIEW_90DAYS"),
            (180, "JOBSARCHVIEW_180DAYS"),
            (365, "JOBSARCHVIEW_365DAYS"),
        ]
        timeDelta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - timeStamp
        for timeLimit, archViewName in tableList:
            # +2 for safety margin
            if timeDelta < datetime.timedelta(days=timeLimit + 2):
                return archViewName
        # range over
        return None

    # get PandaID for a file
    def getPandaIDWithFileID_JEDI(self, jediTaskID, datasetID, fileID):
        comment = " /* JediDBProxy.getPandaIDWithFileID_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} datasetID={datasetID} fileID={fileID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retPandaIDs = []
        try:
            # sql to get PandaID
            sqlP = f"SELECT PandaID FROM {jedi_config.db.schemaPANDA}.filesTable4 WHERE "
            sqlP += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # get creation time of the task
            sqlCT = f"SELECT creationDate FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            varMap[":fileID"] = fileID
            self.cur.arraysize = 100
            self.cur.execute(sqlP + comment, varMap)
            resP = self.cur.fetchone()
            pandaID = None
            if resP is not None:
                # found in live table
                pandaID = resP[0]
            else:
                # get creation time of the task
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlCT + comment, varMap)
                resCT = self.cur.fetchone()
                if resCT is not None:
                    (creationDate,) = resCT
                    archView = self.getArchView(creationDate)
                    if archView is None:
                        tmpLog.debug("no JOBSARCHVIEW since creationDate is too old")
                    else:
                        # sql to get PandaID using JOBSARCHVIEW
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":fileID"] = fileID
                        sqlAP = "SELECT fTab.PandaID "
                        sqlAP += "FROM {0}.filesTable_ARCH fTab,{0}.{1} aTab WHERE ".format(jedi_config.db.schemaPANDAARCH, archView)
                        sqlAP += "fTab.PandaID=aTab.PandaID AND aTab.jediTaskID=:jediTaskID "
                        sqlAP += "AND fTab.jediTaskID=:jediTaskID AND fTab.datasetID=:datasetID "
                        sqlAP += "AND fTab.fileID=:fileID "
                        tmpLog.debug(sqlAP + comment + str(varMap))
                        self.cur.execute(sqlAP + comment, varMap)
                        resAP = self.cur.fetchone()
                        if resAP is not None:
                            pandaID = resAP[0]
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"PandaID -> {pandaID}")
            return True, pandaID
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, None

    # get JEDI files for a job
    def getFilesWithPandaID_JEDI(self, pandaID):
        comment = " /* JediDBProxy.getFilesWithPandaID_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <pandaID={pandaID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retPandaIDs = []
        try:
            # sql to get fileID
            sqlT = f"SELECT jediTaskID,datasetID,fileID FROM {jedi_config.db.schemaPANDA}.filesTable4 WHERE "
            sqlT += "pandaID=:pandaID "
            sqlT += "UNION ALL "
            sqlT += f"SELECT jediTaskID,datasetID,fileID FROM {jedi_config.db.schemaPANDAARCH}.filesTable_ARCH WHERE "
            sqlT += "pandaID=:pandaID "
            sqlT += "AND modificationTime>CURRENT_DATE-180"
            # sql to read files
            sqlFR = f"SELECT {JediFileSpec.columnNames()} "
            sqlFR += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID and fileID=:fileID "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":pandaID"] = pandaID
            self.cur.arraysize = 1000000
            self.cur.execute(sqlT + comment, varMap)
            resTC = self.cur.fetchall()
            fileIDList = []
            fileSpecList = []
            # loop over all fileIDs
            for jediTaskID, datasetID, fileID in resTC:
                # skip duplication
                if fileID in fileIDList:
                    continue
                # read files
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                self.cur.execute(sqlFR + comment, varMap)
                tmpRes = self.cur.fetchone()
                fileSpec = JediFileSpec()
                fileSpec.pack(tmpRes)
                fileSpecList.append(fileSpec)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got {len(fileSpecList)} files")
            return True, fileSpecList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, None

    # update task parameters
    def updateTaskParams_JEDI(self, jediTaskID, taskParams):
        comment = " /* JediDBProxy.updateTaskParams_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retPandaIDs = []
        try:
            # sql to update task params
            sqlT = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_TaskParams SET taskParams=:taskParams "
            sqlT += "WHERE jediTaskID=:jediTaskID "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":taskParams"] = taskParams
            self.cur.execute(sqlT + comment, varMap)
            nRow = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"updated {nRow} rows")
            if nRow == 1:
                return True
            else:
                return False
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # reactivate pending tasks
    def reactivatePendingTasks_JEDI(self, vo, prodSourceLabel, timeLimit, timeoutLimit=None, minPriority=None):
        comment = " /* JediDBProxy.reactivatePendingTasks_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel} limit={timeLimit} min timeout={timeoutLimit}hours minPrio={minPriority}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            timeoutDate = None
            if timeoutLimit is not None:
                timeoutDate = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=timeoutLimit)
            # sql to get pending tasks
            varMap = {}
            varMap[":status"] = "pending"
            varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=timeLimit)
            sqlTL = "SELECT jediTaskID,frozenTime,errorDialog,parent_tid,splitRule,startTime "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += "AND tabT.status=:status AND tabT.modificationTime<:timeLimit AND tabT.oldStatus IS NOT NULL "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlTL += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlTL += "AND prodSourceLabel=:prodSourceLabel "
            if minPriority is not None:
                varMap[":minPriority"] = minPriority
                sqlTL += "AND currentPriority>=:minPriority "
            # sql to update tasks
            sqlTU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET status=oldStatus,oldStatus=NULL,modificationtime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND oldStatus IS NOT NULL AND status=:oldStatus "
            # sql to timeout tasks
            sqlTO = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTO += "SET status=:newStatus,errorDialog=:errorDialog,modificationtime=CURRENT_DATE,stateChangeTime=CURRENT_DATE "
            sqlTO += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # sql to keep pending
            sqlTK = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTK += "SET modificationtime=CURRENT_DATE,frozenTime=CURRENT_DATE "
            sqlTK += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # sql to check the number of finished files
            sqlND = f"SELECT SUM(nFilesFinished) FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlND += "WHERE jediTaskID=:jediTaskID AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlND += f"{mapKey},"
            sqlND = sqlND[:-1]
            sqlND += ") AND masterID IS NULL "
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlTL + comment, varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            nRow = 0
            for jediTaskID, frozenTime, errorDialog, parent_tid, splitRule, startTime in resTL:
                timeoutFlag = False
                keepFlag = False
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":oldStatus"] = "pending"
                # check parent
                parentRunning = False
                if parent_tid not in [None, jediTaskID]:
                    tmpStat = self.checkParentTask_JEDI(parent_tid, useCommit=False)
                    # if parent is running
                    if tmpStat == "running":
                        parentRunning = True
                if not keepFlag:
                    # if timeout
                    if not parentRunning and timeoutDate is not None and frozenTime is not None and frozenTime < timeoutDate:
                        timeoutFlag = True
                        # check the number of finished files
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        for tmpType in JediDatasetSpec.getInputTypes():
                            mapKey = ":type_" + tmpType
                            varMap[mapKey] = tmpType
                        self.cur.execute(sqlND + comment, varMap)
                        tmpND = self.cur.fetchone()
                        if tmpND is not None and tmpND[0] is not None and tmpND[0] > 0:
                            abortingFlag = False
                        else:
                            abortingFlag = True
                        # go to exhausted
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":newStatus"] = "exhausted"
                        varMap[":oldStatus"] = "pending"
                        if errorDialog is None:
                            errorDialog = ""
                        else:
                            errorDialog += ". "
                        errorDialog += f"timeout while in pending since {frozenTime.strftime('%Y/%m/%d %H:%M:%S')}"
                        varMap[":errorDialog"] = errorDialog[: JediTaskSpec._limitLength["errorDialog"]]
                        sql = sqlTO
                    else:
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":oldStatus"] = "pending"
                        sql = sqlTU
                self.cur.execute(sql + comment, varMap)
                tmpRow = self.cur.rowcount
                if tmpRow > 0:
                    if timeoutFlag:
                        tmpLog.info(f"#ATM #KV jediTaskID={jediTaskID} timeout")
                    elif keepFlag:
                        tmpLog.info(f"#ATM #KV jediTaskID={jediTaskID} action=keep_pending")
                    else:
                        tmpLog.info(f"#ATM #KV jediTaskID={jediTaskID} action=reactivate")
                nRow += tmpRow
                if tmpRow > 0 and not keepFlag:
                    self.record_task_status_change(jediTaskID)
                # update DEFT for timeout
                if timeoutFlag:
                    self.push_task_status_message(None, jediTaskID, varMap[":newStatus"], splitRule)
                    deftStatus = varMap[":newStatus"]
                    self.setDeftStatus_JEDI(jediTaskID, deftStatus)
                    self.setSuperStatus_JEDI(jediTaskID, deftStatus)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"updated {nRow} rows")
            return nRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # restart contents update
    def restartTasksForContentsUpdate_JEDI(self, vo, prodSourceLabel, timeLimit):
        comment = " /* JediDBProxy.restartTasksForContentsUpdate_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel} limit={timeLimit}min>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get stalled tasks in defined
            varMap = {}
            varMap[":taskStatus1"] = "defined"
            varMap[":taskStatus2"] = "ready"
            varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=timeLimit)
            varMap[":dsType"] = "input"
            varMap[":dsState"] = "mutable"
            varMap[":dsStatus1"] = "ready"
            varMap[":dsStatus2"] = "toupdate"
            sqlTL = "SELECT distinct tabT.jediTaskID,tabT.status "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
            sqlTL += "AND ((tabT.status=:taskStatus1 AND tabD.status=:dsStatus1) OR (tabT.status=:taskStatus2 AND tabD.status=:dsStatus2)) "
            sqlTL += "AND tabD.type=:dsType AND tabD.state=:dsState AND tabT.modificationTime<:timeLimit "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlTL += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlTL += "AND tabT.prodSourceLabel=:prodSourceLabel "
            # get tasks in defined with only ready datasets
            sqlTR = "SELECT distinct tabT.jediTaskID "
            sqlTR += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTR += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
            sqlTR += "AND tabT.status=:taskStatus1 AND tabD.status=:dsStatus1 "
            sqlTR += "AND tabD.type=:dsType AND tabT.modificationTime<:timeLimit "
            sqlTR += "AND NOT EXISTS "
            sqlTR += f"(SELECT 1 FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=tabT.jediTaskID AND type=:dsType AND status<>:dsStatus1) "
            if vo not in [None, "any"]:
                sqlTR += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                sqlTR += "AND tabT.prodSourceLabel=:prodSourceLabel "
            # get tasks in ready with defined datasets
            sqlTW = "SELECT distinct tabT.jediTaskID "
            sqlTW += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTW += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
            sqlTW += "AND tabT.status=:taskStatus1 AND tabD.status=:dsStatus1 "
            sqlTW += "AND tabD.type=:dsType AND tabT.modificationTime<:timeLimit "
            if vo not in [None, "any"]:
                sqlTW += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                sqlTW += "AND tabT.prodSourceLabel=:prodSourceLabel "
            # sql to update mutable datasets
            sqlTU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlTU += "SET status=:newStatus "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND type=:type AND state=:state AND status=:oldStatus "
            # sql to update ready datasets
            sqlRD = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlRD += "SET status=:newStatus "
            sqlRD += "WHERE jediTaskID=:jediTaskID AND type=:type AND status=:oldStatus "
            # sql to update task
            sqlTD = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTD += "SET status=:newStatus,modificationtime=CURRENT_DATE "
            sqlTD += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # start transaction
            self.conn.begin()
            # get jediTaskIDs
            self.cur.execute(sqlTL + comment, varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            nTasks = 0
            for jediTaskID, taskStatus in resTL:
                if taskStatus == "defined":
                    # update mutable datasets
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":type"] = "input"
                    varMap[":state"] = "mutable"
                    varMap[":oldStatus"] = "ready"
                    varMap[":newStatus"] = "toupdate"
                    self.cur.execute(sqlTU + comment, varMap)
                    nRow = self.cur.rowcount
                    tmpLog.debug(f"jediTaskID={jediTaskID} toupdate {nRow} datasets")
                    if nRow > 0:
                        nTasks += 1
                        # update task
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":oldStatus"] = "defined"
                        varMap[":newStatus"] = "defined"
                        self.cur.execute(sqlTD + comment, varMap)
                else:
                    # update task
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":oldStatus"] = "ready"
                    varMap[":newStatus"] = "defined"
                    self.cur.execute(sqlTD + comment, varMap)
                    nRow = self.cur.rowcount
                    if nRow > 0:
                        tmpLog.debug("jediTaskID={0} back to defined".format(jediTaskID, nRow))
                        nTasks += 1
            # get tasks in defined with only ready datasets
            varMap = {}
            varMap[":taskStatus1"] = "defined"
            varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=timeLimit)
            varMap[":dsType"] = "input"
            varMap[":dsStatus1"] = "ready"
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
            # get jediTaskIDs
            self.cur.execute(sqlTR + comment, varMap)
            resTR = self.cur.fetchall()
            for (jediTaskID,) in resTR:
                # update ready datasets
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":type"] = "input"
                varMap[":oldStatus"] = "ready"
                varMap[":newStatus"] = "defined"
                self.cur.execute(sqlRD + comment, varMap)
                nRow = self.cur.rowcount
                tmpLog.debug(f"jediTaskID={jediTaskID} reset {nRow} datasets in ready")
                if nRow > 0:
                    nTasks += 1
            # get tasks in ready with defined datasets
            varMap = {}
            varMap[":taskStatus1"] = "ready"
            varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=timeLimit)
            varMap[":dsType"] = "input"
            varMap[":dsStatus1"] = "defined"
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
            # get jediTaskIDs
            self.cur.execute(sqlTW + comment, varMap)
            resTW = self.cur.fetchall()
            for (jediTaskID,) in resTW:
                # update task
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":oldStatus"] = "ready"
                varMap[":newStatus"] = "defined"
                self.cur.execute(sqlTD + comment, varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    self.record_task_status_change(jediTaskID)
                    self.push_task_status_message(None, jediTaskID, varMap[":newStatus"])
                    tmpLog.debug(f"jediTaskID={jediTaskID} reset to defined")
                    nTasks += 1
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return nTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # kick exhausted tasks
    def kickExhaustedTasks_JEDI(self, vo, prodSourceLabel, timeLimit):
        comment = " /* JediDBProxy.kickExhaustedTasks_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel} limit={timeLimit}h>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get stalled tasks
            varMap = {}
            varMap[":taskStatus"] = "exhausted"
            varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=timeLimit)
            sqlTL = "SELECT tabT.jediTaskID,tabT.splitRule "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += "AND tabT.status=:taskStatus AND tabT.modificationTime<:timeLimit "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlTL += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlTL += "AND tabT.prodSourceLabel=:prodSourceLabel "
            # sql to timeout tasks
            sqlTO = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTO += "SET status=:newStatus,modificationtime=CURRENT_DATE,stateChangeTime=CURRENT_DATE "
            sqlTO += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # start transaction
            self.conn.begin()
            # get jediTaskIDs
            self.cur.execute(sqlTL + comment, varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            nTasks = 0
            for jediTaskID, splitRule in resTL:
                taskSpec = JediTaskSpec()
                taskSpec.splitRule = splitRule
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":oldStatus"] = "exhausted"
                if taskSpec.disableAutoFinish():
                    # to keep it exhausted since auto finish is disabled
                    varMap[":newStatus"] = "exhausted"
                else:
                    varMap[":newStatus"] = "finishing"
                self.cur.execute(sqlTO + comment, varMap)
                nRow = self.cur.rowcount
                tmpLog.debug(f"jediTaskID={jediTaskID} to {varMap[':newStatus']} with {nRow}")
                if nRow > 0:
                    nTasks += 1
                    # add missing record_task_status_change and push_task_status_message updates
                    self.record_task_status_change(jediTaskID)
                    self.push_task_status_message(taskSpec, jediTaskID, varMap[":newStatus"], splitRule)

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return nTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get file spec of lib.tgz
    def getBuildFileSpec_JEDI(self, jediTaskID, siteName, associatedSites):
        comment = " /* JediDBProxy.getBuildFileSpec_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} siteName={siteName}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        tmpLog.debug(f"associatedSites={str(associatedSites)}")
        try:
            # sql to get dataset
            sqlRD = f"SELECT {JediDatasetSpec.columnNames()} "
            sqlRD += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlRD += "WHERE jediTaskID=:jediTaskID AND type=:type AND site=:site "
            sqlRD += "AND (state IS NULL OR state<>:state) "
            sqlRD += "ORDER BY creationTime DESC "
            # sql to read files
            sqlFR = f"SELECT {JediFileSpec.columnNames()} "
            sqlFR += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND type=:type "
            sqlFR += "AND status IN (:status1) "
            sqlFR += "ORDER BY creationDate DESC "
            # start transaction
            self.conn.begin()
            foundFlag = False
            for tmpSiteName in [siteName] + associatedSites:
                # get dataset
                varMap = {}
                varMap[":type"] = "lib"
                varMap[":site"] = tmpSiteName
                varMap[":state"] = "closed"
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlRD + comment, varMap)
                resList = self.cur.fetchall()
                # loop over all datasets
                fileSpec = None
                datasetSpec = None
                for resItem in resList:
                    datasetSpec = JediDatasetSpec()
                    datasetSpec.pack(resItem)
                    # get file
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetSpec.datasetID
                    varMap[":type"] = "lib"
                    varMap[":status1"] = "finished"
                    self.cur.execute(sqlFR + comment, varMap)
                    resFileList = self.cur.fetchall()
                    for resFile in resFileList:
                        # make FileSpec
                        fileSpec = JediFileSpec()
                        fileSpec.pack(resFile)
                        foundFlag = True
                        break
                    # no more dataset lookup
                    if foundFlag:
                        break
                # no more lookup with other sites
                if foundFlag:
                    break
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            if fileSpec is not None:
                tmpLog.debug(f"got lib.tgz={fileSpec.lfn}")
            else:
                tmpLog.debug("no lib.tgz")
            return True, fileSpec, datasetSpec
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, None

    # get file spec of old lib.tgz
    def getOldBuildFileSpec_JEDI(self, jediTaskID, datasetID, fileID):
        comment = " /* JediDBProxy.getOldBuildFileSpec_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} datasetID={datasetID} fileID={fileID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get dataset
            sqlRD = f"SELECT {JediDatasetSpec.columnNames()} "
            sqlRD += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlRD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to read files
            sqlFR = f"SELECT {JediFileSpec.columnNames()} "
            sqlFR += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # start transaction
            self.conn.begin()
            # get dataset
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            self.cur.execute(sqlRD + comment, varMap)
            tmpRes = self.cur.fetchone()
            datasetSpec = JediDatasetSpec()
            datasetSpec.pack(tmpRes)
            # get file
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            varMap[":fileID"] = fileID
            self.cur.execute(sqlFR + comment, varMap)
            tmpRes = self.cur.fetchone()
            fileSpec = JediFileSpec()
            fileSpec.pack(tmpRes)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            if fileSpec is not None:
                tmpLog.debug(f"got lib.tgz={fileSpec.lfn}")
            else:
                tmpLog.debug("no lib.tgz")
            return True, fileSpec, datasetSpec
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, None, None

    # insert lib dataset and files
    def insertBuildFileSpec_JEDI(self, jobSpec, reusedDatasetID, simul):
        comment = " /* JediDBProxy.insertBuildFileSpec_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jobSpec.jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to insert dataset
            sqlDS = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Datasets ({JediDatasetSpec.columnNames()}) "
            sqlDS += JediDatasetSpec.bindValuesExpression()
            sqlDS += " RETURNING datasetID INTO :newDatasetID"
            # sql to insert file
            sqlFI = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlFI += JediFileSpec.bindValuesExpression()
            sqlFI += " RETURNING fileID INTO :newFileID"
            # sql to update LFN
            sqlFU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFU += "SET lfn=:newLFN "
            sqlFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # make datasetSpec
            pandaFileSpec = jobSpec.Files[0]
            timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            datasetSpec = JediDatasetSpec()
            datasetSpec.jediTaskID = jobSpec.jediTaskID
            datasetSpec.creationTime = timeNow
            datasetSpec.modificationTime = timeNow
            datasetSpec.datasetName = pandaFileSpec.dataset
            datasetSpec.status = "defined"
            datasetSpec.type = "lib"
            datasetSpec.vo = jobSpec.VO
            datasetSpec.cloud = jobSpec.cloud
            datasetSpec.site = jobSpec.computingSite
            # make fileSpec
            fileSpecList = []
            for pandaFileSpec in jobSpec.Files:
                fileSpec = JediFileSpec()
                fileSpec.convertFromJobFileSpec(pandaFileSpec)
                fileSpec.status = "defined"
                fileSpec.creationDate = timeNow
                fileSpec.keepTrack = 1
                # change type to lib
                if fileSpec.type == "output":
                    fileSpec.type = "lib"
                # scope
                if datasetSpec.vo in jedi_config.ddm.voWithScope.split(","):
                    fileSpec.scope = self.extractScope(datasetSpec.datasetName)
                # append
                fileSpecList.append((fileSpec, pandaFileSpec))
            # start transaction
            self.conn.begin()
            varMap = datasetSpec.valuesMap(useSeq=True)
            varMap[":newDatasetID"] = self.cur.var(varNUMBER)
            # insert dataset
            if reusedDatasetID is not None:
                datasetID = reusedDatasetID
            elif not simul:
                self.cur.execute(sqlDS + comment, varMap)
                val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                datasetID = int(val)
            else:
                datasetID = 0
            # insert files
            fileIdMap = {}
            for fileSpec, pandaFileSpec in fileSpecList:
                fileSpec.datasetID = datasetID
                varMap = fileSpec.valuesMap(useSeq=True)
                varMap[":newFileID"] = self.cur.var(varNUMBER)
                if not simul:
                    self.cur.execute(sqlFI + comment, varMap)
                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newFileID"]))
                    fileID = int(val)
                else:
                    fileID = 0
                # change placeholder in filename
                newLFN = fileSpec.lfn.replace("$JEDIFILEID", str(fileID))
                varMap = {}
                varMap[":jediTaskID"] = fileSpec.jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                varMap[":newLFN"] = newLFN
                if not simul:
                    self.cur.execute(sqlFU + comment, varMap)
                # return IDs in a map since changes to jobSpec are not effective
                # since invoked in separate processes
                fileIdMap[fileSpec.lfn] = {"datasetID": datasetID, "fileID": fileID, "newLFN": newLFN, "scope": fileSpec.scope}
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True, fileIdMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, None

    # get sites used by a task
    def getSitesUsedByTask_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getSitesUsedByTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to insert dataset
            sqlDS = f"SELECT distinct site FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlDS += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type1"] = "output"
            varMap[":type2"] = "log"
            # execute
            self.cur.execute(sqlDS + comment, varMap)
            resList = self.cur.fetchall()
            siteList = set()
            for (siteName,) in resList:
                siteList.add(siteName)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"done -> {str(siteList)}")
            return True, siteList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, None

    # get random seed
    def getRandomSeed_JEDI(self, jediTaskID, simul):
        comment = " /* JediDBProxy.getRandomSeed_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get pseudo dataset for random seed
            sqlDS = f"SELECT {JediDatasetSpec.columnNames()} "
            sqlDS += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlDS += "WHERE jediTaskID=:jediTaskID AND type=:type "
            # sql to get min random seed
            sqlFR = f"SELECT {JediFileSpec.columnNames()} "
            sqlFR += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents WHERE "
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlFR += "ORDER BY firstEvent "
            # sql to update file status
            sqlFU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFU += "SET status=:status "
            sqlFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # sql to get max random seed
            sqlLR = f"SELECT MAX(firstEvent) FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlLR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to insert file
            sqlFI = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlFI += JediFileSpec.bindValuesExpression()
            sqlFI += " RETURNING fileID INTO :newFileID"
            # start transaction
            self.conn.begin()
            # get pseudo dataset for random seed
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type"] = "random_seed"
            self.cur.execute(sqlDS + comment, varMap)
            resDS = self.cur.fetchone()
            if resDS is None:
                # no random seed
                retVal = (None, None)
                tmpLog.debug("no random seed")
            else:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(resDS)
                # get min random seed
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetSpec.datasetID
                varMap[":status"] = "ready"
                self.cur.execute(sqlFR + comment, varMap)
                resFR = self.cur.fetchone()
                if resFR is not None:
                    # make FileSpec to reuse the row
                    tmpFileSpec = JediFileSpec()
                    tmpFileSpec.pack(resFR)
                    tmpLog.debug(f"reuse fileID={tmpFileSpec.fileID} datasetID={tmpFileSpec.datasetID} rndmSeed={tmpFileSpec.firstEvent}")
                    # update status
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetSpec.datasetID
                    varMap[":fileID"] = tmpFileSpec.fileID
                    varMap[":status"] = "picked"
                    self.cur.execute(sqlFU + comment, varMap)
                else:
                    # get max random seed
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetSpec.datasetID
                    self.cur.execute(sqlLR + comment, varMap)
                    resLR = self.cur.fetchone()
                    maxRndSeed = None
                    if resLR is not None:
                        (maxRndSeed,) = resLR
                    if maxRndSeed is None:
                        # first row
                        maxRndSeed = 1
                    else:
                        # increment
                        maxRndSeed += 1
                    # insert file
                    tmpFileSpec = JediFileSpec()
                    tmpFileSpec.jediTaskID = jediTaskID
                    tmpFileSpec.datasetID = datasetSpec.datasetID
                    tmpFileSpec.status = "picked"
                    tmpFileSpec.creationDate = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                    tmpFileSpec.keepTrack = 1
                    tmpFileSpec.type = "random_seed"
                    tmpFileSpec.lfn = f"{maxRndSeed}"
                    tmpFileSpec.firstEvent = maxRndSeed
                    if not simul:
                        varMap = tmpFileSpec.valuesMap(useSeq=True)
                        varMap[":newFileID"] = self.cur.var(varNUMBER)
                        self.cur.execute(sqlFI + comment, varMap)
                        val = self.getvalue_corrector(self.cur.getvalue(varMap[":newFileID"]))
                        tmpFileSpec.fileID = int(val)
                        tmpLog.debug(f"insert fileID={tmpFileSpec.fileID} datasetID={tmpFileSpec.datasetID} rndmSeed={tmpFileSpec.firstEvent}")
                    tmpFileSpec.status = "ready"
                # cannot return JobFileSpec due to owner.PandaID
                retVal = (tmpFileSpec, datasetSpec)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True, retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False, (None, None)

    # get preprocess metadata
    def getPreprocessMetadata_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getPreprocessMetadata_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # sql to get jobPrams for runXYZ
        sqlSCF = "SELECT tabF.PandaID "
        sqlSCF += "FROM {0}.JEDI_Datasets tabD, {0}.JEDI_Dataset_Contents tabF WHERE ".format(jedi_config.db.schemaJEDI)
        sqlSCF += "tabD.jediTaskID=tabF.jediTaskID AND tabD.jediTaskID=:jediTaskID AND tabF.status=:status "
        sqlSCF += "AND tabD.datasetID=tabF.datasetID "
        sqlSCF += "AND tabF.type=:type AND tabD.masterID IS NULL "
        sqlSCD = f"SELECT metaData FROM {jedi_config.db.schemaPANDA}.metaTable "
        sqlSCD += "WHERE PandaID=:pandaID "
        failedRet = False, None
        retVal = failedRet
        try:
            # begin transaction
            self.conn.begin()
            # get files
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":status"] = "finished"
            varMap[":type"] = "pp_input"
            self.cur.execute(sqlSCF + comment, varMap)
            tmpRes = self.cur.fetchone()
            if tmpRes is None:
                tmpLog.error("no successful input file")
            else:
                (pandaID,) = tmpRes
                # get metadata
                metaData = None
                varMap = {}
                varMap[":pandaID"] = pandaID
                self.cur.execute(sqlSCD + comment, varMap)
                for (clobMeta,) in self.cur:
                    metaData = clobMeta
                    break
                if metaData is None:
                    tmpLog.error(f"no metaData for PandaID={pandaID}")
                else:
                    retVal = True, metaData
                    tmpLog.debug(f"got metaData from PandaID={pandaID}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # get log dataset for preprocessing
    def getPreproLog_JEDI(self, jediTaskID, simul):
        comment = " /* JediDBProxy.getPreproLog_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # sql to get dataset
        sqlDS = f"SELECT {JediDatasetSpec.columnNames()} "
        sqlDS += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
        sqlDS += "WHERE jediTaskID=:jediTaskID AND type=:type "
        # sql to insert file
        sqlFI = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
        sqlFI += JediFileSpec.bindValuesExpression()
        sqlFI += " RETURNING fileID INTO :newFileID"
        # sql to update dataset
        sqlUD = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
        sqlUD += "SET nFiles=nFiles+1 "
        sqlUD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        failedRet = False, None, None
        retVal = failedRet
        try:
            # begin transaction
            self.conn.begin()
            # get dataset
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type"] = "pp_log"
            self.cur.execute(sqlDS + comment, varMap)
            resDS = self.cur.fetchone()
            if resDS is None:
                tmpLog.error(f"no dataset with type={varMap[':type']}")
            else:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(resDS)
                # make file
                datasetSpec.nFiles = datasetSpec.nFiles + 1
                tmpFileSpec = JediFileSpec()
                tmpFileSpec.jediTaskID = jediTaskID
                tmpFileSpec.datasetID = datasetSpec.datasetID
                tmpFileSpec.status = "defined"
                tmpFileSpec.creationDate = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                tmpFileSpec.keepTrack = 1
                tmpFileSpec.type = "log"
                tmpFileSpec.lfn = f"{datasetSpec.datasetName}._{datasetSpec.nFiles:06d}.log.tgz"
                if not simul:
                    varMap = tmpFileSpec.valuesMap(useSeq=True)
                    varMap[":newFileID"] = self.cur.var(varNUMBER)
                    self.cur.execute(sqlFI + comment, varMap)
                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newFileID"]))
                    tmpFileSpec.fileID = int(val)
                    # increment nFiles
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetSpec.datasetID
                    self.cur.execute(sqlUD + comment, varMap)
                # return value
                retVal = True, datasetSpec, tmpFileSpec
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # retry or incrementally execute a task
    def retryTask_JEDI(
        self, jediTaskID, commStr, maxAttempt=5, useCommit=True, statusCheck=True, retryChildTasks=True, discardEvents=False, release_unstaged=False
    ):
        comment = " /* JediDBProxy.retryTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start command={commStr} retryChildTasks={retryChildTasks}")
        newTaskStatus = None
        # check command
        if commStr not in ["retry", "incexec"]:
            tmpLog.debug(f"unknown command={commStr}")
            return False, None
        try:
            # sql to retry files without maxFailure
            sqlRFO = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlRFO += "SET maxAttempt=maxAttempt+:maxAttempt,proc_status=:proc_status "
            sqlRFO += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlRFO += "AND keepTrack=:keepTrack AND maxAttempt IS NOT NULL AND maxAttempt<=attemptNr AND maxFailure IS NULL "
            # sql to retry files with maxFailure
            sqlRFF = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlRFF += "SET maxAttempt=maxAttempt+:maxAttempt,maxFailure=maxFailure+:maxAttempt,proc_status=:proc_status "
            sqlRFF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlRFF += "AND keepTrack=:keepTrack AND maxAttempt IS NOT NULL AND maxFailure IS NOT NULL AND (maxAttempt<=attemptNr OR maxFailure<=failedAttempt) "
            # sql to reset ramCount
            sqlRRC = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlRRC += "SET ramCount=0 "
            sqlRRC += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlRRC += "AND keepTrack=:keepTrack "
            # sql to count unprocessed files
            sqlCU = f"SELECT COUNT(*) FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlCU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlCU += "AND keepTrack=:keepTrack AND maxAttempt IS NOT NULL AND maxAttempt>attemptNr "
            sqlCU += "AND (maxFailure IS NULL OR maxFailure>failedAttempt) "
            # sql to count failed files
            sqlCF = f"SELECT COUNT(*) FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlCF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlCF += "AND keepTrack=:keepTrack AND ((maxAttempt IS NOT NULL AND maxAttempt<=attemptNr) "
            sqlCF += "OR (maxFailure IS NOT NULL AND maxFailure<=failedAttempt)) "
            # sql to retry/incexecute datasets
            sqlRD = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlRD += (
                "SET status=:status,"
                "nFilesUsed=(CASE WHEN nFilesUsed-:nDiff-:nRun > 0 THEN nFilesUsed-:nDiff-:nRun ELSE 0 END),"
                "nFilesFailed=(CASE WHEN nFilesFailed-:nDiff > 0 THEN nFilesFailed-:nDiff ELSE 0 END) "
            )
            sqlRD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to reset lost files in datasets
            sqlRL = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlRL += "SET nFiles=nFiles+nFilesMissing,nFilesToBeUsed=nFilesToBeUsed+nFilesMissing,nFilesMissing=0 "
            sqlRL += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update task status
            sqlUTB = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlUTB += "SET status=:status,oldStatus=NULL,modificationtime=:updateTime,errorDialog=:errorDialog,stateChangeTime=CURRENT_DATE "
            sqlUTB += "WHERE jediTaskID=:jediTaskID "
            sqlUTN = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlUTN += "SET status=:status,oldStatus=NULL,modificationtime=:updateTime,errorDialog=:errorDialog,"
            sqlUTN += "stateChangeTime=CURRENT_DATE,startTime=NULL,attemptNr=attemptNr+1,frozenTime=NULL "
            sqlUTN += "WHERE jediTaskID=:jediTaskID "
            # sql to update DEFT task status
            sqlTT = f"UPDATE {jedi_config.db.schemaDEFT}.T_TASK "
            sqlTT += "SET status=:status,timeStamp=CURRENT_DATE,start_time=NULL "
            sqlTT += "WHERE taskID=:jediTaskID AND start_time IS NOT NULL "
            # sql to discard events
            sqlDE = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Events "
            sqlDE += "SET status=:newStatus "
            sqlDE += "WHERE jediTaskID=:jediTaskID "
            sqlDE += "AND status IN (:esFinished,:esDone) "
            # sql to reset running files
            sqlRR = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlRR += "SET status=:newStatus,attemptNr=attemptNr+1,maxAttempt=maxAttempt+:maxAttempt,proc_status=:proc_status "
            sqlRR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status IN (:oldStatus1,:oldStatus2) "
            sqlRR += "AND keepTrack=:keepTrack AND maxAttempt IS NOT NULL "
            # sql to update output/lib/log datasets
            sqlUO = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlUO += "SET status=:status "
            sqlUO += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2,:type3,"
            for tmpType in JediDatasetSpec.getProcessTypes():
                if tmpType in JediDatasetSpec.getInputTypes():
                    continue
                mapKey = ":type_" + tmpType
                sqlUO += f"{mapKey},"
            sqlUO = sqlUO[:-1]
            sqlUO += ") "
            # start transaction
            if useCommit:
                self.conn.begin()
            self.cur.arraysize = 100000
            # check task status
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sqlTK = f"SELECT status,oldStatus FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE "
            self.cur.execute(sqlTK + comment, varMap)
            resTK = self.cur.fetchone()
            if resTK is None:
                # task not found
                msgStr = "task not found"
                tmpLog.debug(msgStr)
            else:
                # check task status
                taskStatus, taskOldStatus = resTK
                newTaskStatus = None
                newErrorDialog = None
                if taskOldStatus == "done" and commStr == "retry" and statusCheck:
                    # no retry for finished task
                    msgStr = f"no {commStr} for task in {taskOldStatus} status"
                    tmpLog.debug(msgStr)
                    newTaskStatus = taskOldStatus
                    newErrorDialog = msgStr
                elif taskOldStatus not in JediTaskSpec.statusToIncexec() and statusCheck:
                    # only tasks in a relevant final status
                    msgStr = f"no {commStr} since not in relevant final status ({taskOldStatus})"
                    tmpLog.debug(msgStr)
                    newTaskStatus = taskOldStatus
                    newErrorDialog = msgStr
                else:
                    # check max attempts
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    sqlMAX = "SELECT MAX(c.maxAttempt) "
                    sqlMAX += "FROM {0}.JEDI_Datasets d, {0}.JEDI_Dataset_Contents c ".format(jedi_config.db.schemaJEDI)
                    sqlMAX += "WHERE c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID "
                    sqlMAX += "AND d.jediTaskID=:jediTaskID AND d.type IN ("
                    for tmpType in JediDatasetSpec.getInputTypes():
                        mapKey = ":type_" + tmpType
                        sqlMAX += f"{mapKey},"
                        varMap[mapKey] = tmpType
                    sqlMAX = sqlMAX[:-1]
                    sqlMAX += ") "
                    self.cur.execute(sqlMAX + comment, varMap)
                    resMAX = self.cur.fetchone()
                    maxRetry = 1000
                    if resMAX is not None and resMAX[0] is not None and resMAX[0] + maxAttempt >= maxRetry:
                        # only tasks in a relevant final status
                        msgStr = f"no {commStr} since too many attempts (~{maxRetry}) in the past"
                        tmpLog.debug(msgStr)
                        newTaskStatus = taskOldStatus
                        newErrorDialog = msgStr
                    else:
                        # get input datasets
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        sqlDS = "SELECT datasetID,masterID,nFiles,nFilesFinished,nFilesFailed,nFilesUsed," "status,state,type,datasetName,nFilesMissing "
                        sqlDS += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
                        sqlDS += "WHERE jediTaskID=:jediTaskID AND type IN ("
                        for tmpType in JediDatasetSpec.getInputTypes():
                            mapKey = ":type_" + tmpType
                            sqlDS += f"{mapKey},"
                            varMap[mapKey] = tmpType
                        sqlDS = sqlDS[:-1]
                        sqlDS += ") "
                        self.cur.execute(sqlDS + comment, varMap)
                        resDS = self.cur.fetchall()
                        changedMasterList = []
                        secMap = {}
                        for (
                            datasetID,
                            masterID,
                            nFiles,
                            nFilesFinished,
                            nFilesFailed,
                            nFilesUsed,
                            status,
                            state,
                            datasetType,
                            datasetName,
                            nFilesMissing,
                        ) in resDS:
                            if masterID is not None:
                                if state not in [None, ""]:
                                    # keep secondary dataset info
                                    if masterID not in secMap:
                                        secMap[masterID] = []
                                    secMap[masterID].append((datasetID, nFilesFinished, status, state, datasetType))
                                    # update dataset
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":nDiff"] = 0
                                    varMap[":nRun"] = 0
                                    varMap[":status"] = "ready"
                                    tmpLog.debug(f"set status={varMap[':status']} for 2nd datasetID={datasetID}")
                                    self.cur.execute(sqlRD + comment, varMap)
                                else:
                                    # set dataset status to defined to trigger file lookup when state is not set
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":nDiff"] = 0
                                    varMap[":nRun"] = 0
                                    varMap[":status"] = "defined"
                                    tmpLog.debug(f"set status={varMap[':status']} for 2nd datasetID={datasetID}")
                                    self.cur.execute(sqlRD + comment, varMap)
                            else:
                                # set done if no more try is needed
                                if nFiles == nFilesFinished and status == "failed":
                                    # update dataset
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":nDiff"] = 0
                                    varMap[":nRun"] = 0
                                    varMap[":status"] = "done"
                                    tmpLog.debug(f"set status={varMap[':status']} for datasetID={datasetID}")
                                    self.cur.execute(sqlRD + comment, varMap)
                                # no retry if master dataset successfully finished
                                if commStr == "retry" and nFiles == nFilesFinished:
                                    tmpLog.debug(f"no {commStr} for datasetID={datasetID} : nFiles==nFilesFinished")
                                    continue
                                # count unprocessed files
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":status"] = "ready"
                                varMap[":keepTrack"] = 1
                                self.cur.execute(sqlCU + comment, varMap)
                                (nUnp,) = self.cur.fetchone()
                                # update files
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":status"] = "ready"
                                varMap[":proc_status"] = "ready"
                                varMap[":maxAttempt"] = maxAttempt
                                varMap[":keepTrack"] = 1
                                nDiff = 0
                                self.cur.execute(sqlRFO + comment, varMap)
                                nDiff += self.cur.rowcount
                                self.cur.execute(sqlRFF + comment, varMap)
                                nDiff += self.cur.rowcount
                                # reset running files
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":oldStatus1"] = "picked"
                                if taskOldStatus == "exhausted":
                                    varMap[":oldStatus2"] = "dummy"
                                else:
                                    varMap[":oldStatus2"] = "running"
                                varMap[":newStatus"] = "ready"
                                varMap[":proc_status"] = "ready"
                                varMap[":keepTrack"] = 1
                                varMap[":maxAttempt"] = maxAttempt
                                self.cur.execute(sqlRR + comment, varMap)
                                nRun = self.cur.rowcount
                                # reset ramCount
                                if commStr == "incexec":
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":status"] = "ready"
                                    varMap[":keepTrack"] = 1
                                    self.cur.execute(sqlRRC + comment, varMap)
                                    # reset lost files
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":oldStatus1"] = "lost"
                                    varMap[":oldStatus2"] = "missing"
                                    varMap[":newStatus"] = "ready"
                                    varMap[":proc_status"] = "ready"
                                    varMap[":keepTrack"] = 1
                                    varMap[":maxAttempt"] = maxAttempt
                                    self.cur.execute(sqlRR + comment, varMap)
                                    nLost = self.cur.rowcount
                                    if nLost > 0 and nFilesMissing:
                                        varMap = {}
                                        varMap[":jediTaskID"] = jediTaskID
                                        varMap[":datasetID"] = datasetID
                                        self.cur.execute(sqlRL + comment, varMap)
                                        tmpLog.debug(f"reset nFilesMissing for datasetID={datasetID}")
                                # no retry if no failed files
                                if commStr == "retry" and nDiff == 0 and nUnp == 0 and nRun == 0 and state != "mutable":
                                    tmpLog.debug(f"no {commStr} for datasetID={datasetID} : nDiff/nReady/nRun=0")
                                    continue
                                # count failed files which could be screwed up when files are lost
                                if nDiff == 0 and nRun == 0 and nFilesUsed <= (nFilesFinished + nFilesFailed):
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":status"] = "ready"
                                    varMap[":keepTrack"] = 1
                                    self.cur.execute(sqlCF + comment, varMap)
                                    (newNumFailed,) = self.cur.fetchone()
                                    nDiff = nFilesFailed - newNumFailed
                                    tmpLog.debug(f"got nFilesFailed={newNumFailed} while {nFilesFailed} in DB for datasetID={datasetID}")
                                # update dataset
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":nDiff"] = nDiff
                                varMap[":nRun"] = nRun
                                if commStr == "retry":
                                    varMap[":status"] = "ready"
                                    tmpLog.debug(f"set status={varMap[':status']} for datasetID={datasetID} diff={nDiff}")
                                elif commStr == "incexec":
                                    varMap[":status"] = "toupdate"
                                self.cur.execute(sqlRD + comment, varMap)
                                # collect masterIDs
                                changedMasterList.append(datasetID)
                                # release unstaged
                                if release_unstaged:
                                    self.updateInputDatasetsStagedAboutIdds_JEDI(
                                        jediTaskID, datasetName.split(":")[0], [datasetName.split(":")[-1]], use_commit=False
                                    )
                        # update secondary
                        for masterID in changedMasterList:
                            # no seconday
                            if masterID not in secMap:
                                continue
                            # loop over all datasets
                            for datasetID, nFilesFinished, status, state, datasetType in secMap[masterID]:
                                # update files
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":status"] = "ready"
                                varMap[":proc_status"] = "ready"
                                varMap[":maxAttempt"] = maxAttempt
                                varMap[":keepTrack"] = 1
                                nDiff = 0
                                self.cur.execute(sqlRFO + comment, varMap)
                                nDiff += self.cur.rowcount
                                self.cur.execute(sqlRFF + comment, varMap)
                                nDiff += self.cur.rowcount
                                # reset running files
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":oldStatus1"] = "picked"
                                if taskOldStatus == "exhausted":
                                    varMap[":oldStatus2"] = "dummy"
                                else:
                                    varMap[":oldStatus2"] = "running"
                                varMap[":newStatus"] = "ready"
                                varMap[":proc_status"] = "ready"
                                varMap[":keepTrack"] = 1
                                varMap[":maxAttempt"] = maxAttempt
                                self.cur.execute(sqlRR + comment, varMap)
                                nRun = self.cur.rowcount
                                # reset ramCount
                                if commStr == "incexec":
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":status"] = "ready"
                                    varMap[":keepTrack"] = 1
                                    self.cur.execute(sqlRRC + comment, varMap)
                                    # reset lost files
                                    varMap = {}
                                    varMap[":jediTaskID"] = jediTaskID
                                    varMap[":datasetID"] = datasetID
                                    varMap[":oldStatus1"] = "lost"
                                    varMap[":oldStatus2"] = "missing"
                                    varMap[":newStatus"] = "ready"
                                    varMap[":proc_status"] = "ready"
                                    varMap[":keepTrack"] = 1
                                    varMap[":maxAttempt"] = maxAttempt
                                    self.cur.execute(sqlRR + comment, varMap)
                                    nLost = self.cur.rowcount
                                    if nLost > 0 and nFilesMissing:
                                        varMap = {}
                                        varMap[":jediTaskID"] = jediTaskID
                                        varMap[":datasetID"] = datasetID
                                        self.cur.execute(sqlRL + comment, varMap)
                                        tmpLog.debug(f"reset nFilesMissing for datasetID={datasetID}")
                                # update dataset
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = datasetID
                                varMap[":nDiff"] = nDiff
                                varMap[":nRun"] = nRun
                                if commStr == "incexec" and datasetType == "input":
                                    varMap[":status"] = "toupdate"
                                else:
                                    varMap[":status"] = "ready"
                                tmpLog.debug(f"set status={varMap[':status']} for associated 2nd datasetID={datasetID}")
                                self.cur.execute(sqlRD + comment, varMap)
                        # discard events
                        if discardEvents:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":newStatus"] = EventServiceUtils.ST_discarded
                            varMap[":esDone"] = EventServiceUtils.ST_done
                            varMap[":esFinished"] = EventServiceUtils.ST_finished
                            self.cur.execute(sqlDE + comment, varMap)
                            nDE = self.cur.rowcount
                            tmpLog.debug(f"discarded {nDE} events")
                        # update task
                        if commStr == "retry":
                            if changedMasterList != [] or taskOldStatus == "exhausted":
                                newTaskStatus = JediTaskSpec.commandStatusMap()[commStr]["done"]
                            else:
                                # to finalization since no files left in ready status
                                msgStr = f"no {commStr} since no new/unprocessed files available"
                                tmpLog.debug(msgStr)
                                newTaskStatus = taskOldStatus
                                newErrorDialog = msgStr
                        else:
                            # for incremental execution
                            newTaskStatus = JediTaskSpec.commandStatusMap()[commStr]["done"]
                # update task
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":status"] = newTaskStatus
                varMap[":errorDialog"] = newErrorDialog
                if newTaskStatus != taskOldStatus:
                    tmpLog.debug(f"set taskStatus={newTaskStatus} from {taskStatus} for command={commStr}")
                    # set old update time to trigger subsequent process
                    varMap[":updateTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=6)
                    self.cur.execute(sqlUTN + comment, varMap)
                    deftStatus = "ready"
                    self.setSuperStatus_JEDI(jediTaskID, deftStatus)
                    # update DEFT
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":status"] = deftStatus
                    self.cur.execute(sqlTT + comment, varMap)
                    # task status log
                    self.record_task_status_change(jediTaskID)
                    self.push_task_status_message(None, jediTaskID, newTaskStatus)
                    # task attempt start log
                    self.log_task_attempt_start(jediTaskID)
                else:
                    tmpLog.debug(f"back to taskStatus={newTaskStatus} for command={commStr}")
                    varMap[":updateTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                    self.cur.execute(sqlUTB + comment, varMap)
                # update output/lib/log
                if newTaskStatus != taskOldStatus and taskStatus != "exhausted":
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":type1"] = "output"
                    varMap[":type2"] = "lib"
                    varMap[":type3"] = "log"
                    varMap[":status"] = "done"
                    for tmpType in JediDatasetSpec.getProcessTypes():
                        if tmpType in JediDatasetSpec.getInputTypes():
                            continue
                        mapKey = ":type_" + tmpType
                        varMap[mapKey] = tmpType
                    self.cur.execute(sqlUO + comment, varMap)
                # retry or reactivate child tasks
                if retryChildTasks and newTaskStatus != taskOldStatus:
                    self.retryChildTasks_JEDI(jediTaskID, useCommit=False)
            if useCommit:
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True, newTaskStatus
        except Exception:
            if useCommit:
                # roll back
                self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None, None

    # append input datasets for incremental execution
    def appendDatasets_JEDI(self, jediTaskID, inMasterDatasetSpecList, inSecDatasetSpecList):
        comment = " /* JediDBProxy.appendDatasets_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        goDefined = False
        refreshContents = False
        commandStr = "incexec"
        try:
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            # check task status
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sqlTK = f"SELECT status FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE "
            self.cur.execute(sqlTK + comment, varMap)
            resTK = self.cur.fetchone()
            if resTK is None:
                # task not found
                msgStr = "task not found"
                tmpLog.debug(msgStr)
            else:
                (taskStatus,) = resTK
                # invalid status
                if taskStatus != JediTaskSpec.commandStatusMap()[commandStr]["done"]:
                    msgStr = f"invalid status={taskStatus} for dataset appending"
                    tmpLog.debug(msgStr)
                else:
                    timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                    # get existing input datasets
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    sqlDS = "SELECT datasetName,status,nFilesTobeUsed,nFilesUsed,masterID "
                    sqlDS += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
                    sqlDS += "WHERE jediTaskID=:jediTaskID AND type IN ("
                    for tmpType in JediDatasetSpec.getInputTypes():
                        mapKey = ":type_" + tmpType
                        sqlDS += f"{mapKey},"
                        varMap[mapKey] = tmpType
                    sqlDS = sqlDS[:-1]
                    sqlDS += ") "
                    self.cur.execute(sqlDS + comment, varMap)
                    resDS = self.cur.fetchall()
                    existingDatasets = {}
                    for datasetName, datasetStatus, nFilesTobeUsed, nFilesUsed, masterID in resDS:
                        existingDatasets[datasetName] = datasetStatus
                        # remaining master files
                        try:
                            if masterID is None and (nFilesTobeUsed - nFilesUsed > 0 or datasetStatus in JediDatasetSpec.statusToUpdateContents()):
                                goDefined = True
                                if datasetStatus in JediDatasetSpec.statusToUpdateContents():
                                    refreshContents = True
                        except Exception:
                            pass
                    # insert datasets
                    sqlID = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Datasets ({JediDatasetSpec.columnNames()}) "
                    sqlID += JediDatasetSpec.bindValuesExpression()
                    sqlID += " RETURNING datasetID INTO :newDatasetID"
                    for datasetSpec in inMasterDatasetSpecList:
                        # skip existing datasets
                        if datasetSpec.datasetName in existingDatasets:
                            # check dataset status and remaiing files
                            if existingDatasets[datasetSpec.datasetName] in JediDatasetSpec.statusToUpdateContents():
                                goDefined = True
                            continue
                        datasetSpec.creationTime = timeNow
                        datasetSpec.modificationTime = timeNow
                        varMap = datasetSpec.valuesMap(useSeq=True)
                        varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                        # insert dataset
                        self.cur.execute(sqlID + comment, varMap)
                        val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                        datasetID = int(val)
                        masterID = datasetID
                        datasetSpec.datasetID = datasetID
                        # insert secondary datasets
                        for datasetSpec in inSecDatasetSpecList:
                            datasetSpec.creationTime = timeNow
                            datasetSpec.modificationTime = timeNow
                            datasetSpec.masterID = masterID
                            varMap = datasetSpec.valuesMap(useSeq=True)
                            varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                            # insert dataset
                            self.cur.execute(sqlID + comment, varMap)
                            val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                            datasetID = int(val)
                            datasetSpec.datasetID = datasetID
                        goDefined = True
                    # update task
                    sqlUT = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
                    sqlUT += "SET status=:status,lockedBy=NULL,lockedTime=NULL,modificationtime=:updateTime,stateChangeTime=CURRENT_DATE "
                    sqlUT += "WHERE jediTaskID=:jediTaskID "
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    if goDefined:
                        # no new datasets
                        if inMasterDatasetSpecList == [] and not refreshContents:
                            # pass to JG
                            varMap[":status"] = "ready"
                        else:
                            # pass to ContentsFeeder
                            varMap[":status"] = "defined"
                    else:
                        # go to finalization since no datasets are appended
                        varMap[":status"] = "prepared"
                    # set old update time to trigger subsequent process
                    varMap[":updateTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=6)
                    tmpLog.debug(f"set taskStatus={varMap[':status']}")
                    self.cur.execute(sqlUT + comment, varMap)
                    # add missing record_task_status_change and push_task_status_message updates
                    self.record_task_status_change(jediTaskID)
                    self.push_task_status_message(None, jediTaskID, varMap[":status"])

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # record retry history
    def recordRetryHistory_JEDI(self, jediTaskID, oldNewPandaIDs, relationType):
        comment = " /* JediDBProxy.recordRetryHistory_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            sqlIN = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Job_Retry_History "
            if relationType is None:
                sqlIN += "(jediTaskID,oldPandaID,newPandaID,originPandaID) "
                sqlIN += "VALUES(:jediTaskID,:oldPandaID,:newPandaID,:originPandaID) "
            else:
                sqlIN += "(jediTaskID,oldPandaID,newPandaID,originPandaID,relationType) "
                sqlIN += "VALUES(:jediTaskID,:oldPandaID,:newPandaID,:originPandaID,:relationType) "
            # start transaction
            self.conn.begin()
            for newPandaID, oldPandaIDs in oldNewPandaIDs.items():
                for oldPandaID in oldPandaIDs:
                    # get origin
                    originIDs = self.getOriginPandaIDsJEDI(oldPandaID, jediTaskID, self.cur)
                    for originID in originIDs:
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":oldPandaID"] = oldPandaID
                        varMap[":newPandaID"] = newPandaID
                        varMap[":originPandaID"] = originID
                        if relationType is not None:
                            varMap[":relationType"] = relationType
                        self.cur.execute(sqlIN + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # get JEDI tasks with a selection criteria
    def getTasksWithCriteria_JEDI(
        self, vo, prodSourceLabel, taskStatusList, taskCriteria, datasetCriteria, taskParamList, datasetParamList, taskLockColumn, taskLockInterval
    ):
        comment = " /* JediDBProxy.getTasksWithCriteria_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start with tC={str(taskCriteria)} dC={str(datasetCriteria)}")
        # return value for failure
        failedRet = None
        try:
            # sql
            varMap = {}
            sqlRT = "SELECT tabT.jediTaskID,"
            for tmpPar in taskParamList:
                sqlRT += f"tabT.{tmpPar},"
            for tmpPar in datasetParamList:
                sqlRT += f"tabD.{tmpPar},"
            sqlRT = sqlRT[:-1]
            sqlRT += " "
            sqlRT += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlRT += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
            sqlRT += "AND tabT.status IN ("
            for tmpStatus in taskStatusList:
                tmpKey = f":status_{tmpStatus}"
                varMap[tmpKey] = tmpStatus
                sqlRT += f"{tmpKey},"
            sqlRT = sqlRT[:-1]
            sqlRT += ") "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlRT += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlRT += "AND tabT.prodSourceLabel=:prodSourceLabel "
            for tmpKey, tmpVal in taskCriteria.items():
                if isinstance(tmpVal, list):
                    sqlRT += f"AND tabT.{tmpKey} IN ("
                    for tmpValItem in tmpVal:
                        sqlRT += f":{tmpKey}_{tmpValItem},"
                        varMap[f":{tmpKey}_{tmpValItem}"] = tmpValItem
                    sqlRT = sqlRT[:-1]
                    sqlRT += ") "
                elif tmpVal is not None:
                    sqlRT += "AND tabT.{0}=:{0} ".format(tmpKey)
                    varMap[f":{tmpKey}"] = tmpVal
                else:
                    sqlRT += f"AND tabT.{tmpKey} IS NULL "
            for tmpKey, tmpVal in datasetCriteria.items():
                if isinstance(tmpVal, list):
                    sqlRT += f"AND tabD.{tmpKey} IN ("
                    for tmpValItem in tmpVal:
                        sqlRT += f":{tmpKey}_{tmpValItem},"
                        varMap[f":{tmpKey}_{tmpValItem}"] = tmpValItem
                    sqlRT = sqlRT[:-1]
                    sqlRT += ") "
                elif tmpVal is not None:
                    sqlRT += "AND tabD.{0}=:{0} ".format(tmpKey)
                    varMap[f":{tmpKey}"] = tmpVal
                else:
                    sqlRT += f"AND tabD.{tmpKey} IS NULL "
            timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=taskLockInterval)
            if taskLockColumn is not None:
                sqlRT += "AND (tabT.{0} IS NULL OR tabT.{0}<:lockTimeLimit) ".format(taskLockColumn)
                varMap[":lockTimeLimit"] = timeLimit
            sqlRT += "ORDER BY tabT.jediTaskID "
            # sql to lock
            if taskLockColumn is not None:
                sqlLK = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
                sqlLK += f"SET {taskLockColumn}=CURRENT_DATE "
                sqlLK += "WHERE jediTaskID=:jediTaskID AND ({0} IS NULL OR {0}<:lockTimeLimit) ".format(taskLockColumn)
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            tmpLog.debug(sqlRT + comment + str(varMap))
            self.cur.execute(sqlRT + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retTasks = []
            for resRT in resList:
                jediTaskID = resRT[0]
                taskParMap = {}
                for tmpIdx, tmpPar in enumerate(taskParamList):
                    taskParMap[tmpPar] = resRT[tmpIdx + 1]
                datasetParMap = {}
                for tmpIdx, tmpPar in enumerate(datasetParamList):
                    datasetParMap[tmpPar] = resRT[tmpIdx + 1 + len(taskParamList)]
                # lock
                if taskLockColumn is not None:
                    # begin transaction
                    self.conn.begin()
                    varMap = dict()
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":lockTimeLimit"] = timeLimit
                    self.cur.execute(sqlLK + comment, varMap)
                    nLK = self.cur.rowcount
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    # not locked
                    if nLK == 0:
                        continue
                retTasks.append((taskParMap, datasetParMap))
            tmpLog.debug(f"got {len(retTasks)} tasks")
            return retTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # check parent task status
    def checkParentTask_JEDI(self, jediTaskID, useCommit=True):
        comment = " /* JediDBProxy.checkParentTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retVal = None
            sql = f"SELECT status FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sql += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # start transaction
            if useCommit:
                self.conn.begin()
            self.cur.execute(sql + comment, varMap)
            resTK = self.cur.fetchone()
            if useCommit:
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            if resTK is None:
                tmpLog.error("parent not found")
                retVal = "unknown"
            else:
                # task status
                (taskStatus,) = resTK
                tmpLog.debug(f"parent status = {taskStatus}")
                if taskStatus in ["done", "finished"]:
                    # parent is completed
                    retVal = "completed"
                elif taskStatus in ["broken", "aborted", "failed"]:
                    # parent is corrupted
                    retVal = "corrupted"
                else:
                    # parent is running
                    retVal = "running"
            # return
            tmpLog.debug(f"done with {retVal}")
            return retVal
        except Exception:
            if useCommit:
                # roll back
                self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal

    # get task status
    def getTaskStatus_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getTaskStatus_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retVal = None
            sql = f"SELECT status FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sql += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, varMap)
            resTK = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if resTK is not None:
                (retVal,) = resTK
            # return
            tmpLog.debug(f"done with {retVal}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal

    # get lib.tgz for waiting jobs
    def getLibForWaitingRunJob_JEDI(self, vo, prodSourceLabel, checkInterval):
        comment = " /* JediDBProxy.getLibForWaitingRunJob_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get the list of user/jobIDs
            sqlL = "SELECT prodUserName,jobsetID,jobDefinitionID,MAX(PandaID) "
            sqlL += f"FROM {jedi_config.db.schemaPANDA}.jobsDefined4 "
            sqlL += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
            sqlL += "AND lockedBy=:lockedBy AND modificationTime<:timeLimit "
            sqlL += "GROUP BY prodUserName,jobsetID,jobDefinitionID "
            # sql to get data of lib.tgz
            sqlD = "SELECT lfn,dataset,jediTaskID,datasetID,fileID "
            sqlD += f"FROM {jedi_config.db.schemaPANDA}.filesTable4 "
            sqlD += "WHERE PandaID=:PandaID AND type=:type AND status=:status "
            # sql to read file spec
            sqlF = f"SELECT {JediFileSpec.columnNames()} "
            sqlF += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # sql to update modificationTime
            sqlU = f"UPDATE {jedi_config.db.schemaPANDA}.jobsDefined4 "
            sqlU += "SET modificationTime=CURRENT_DATE "
            sqlU += "WHERE prodUserName=:prodUserName AND jobsetID=:jobsetID AND jobDefinitionID=:jobDefinitionID "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            retList = []
            # get the list of waiting user/jobIDs
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":lockedBy"] = "jedi"
            varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=checkInterval)
            self.cur.execute(sqlL + comment, varMap)
            resL = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all user/jobIDs
            for prodUserName, jobsetID, jobDefinitionID, pandaID in resL:
                self.conn.begin()
                # get data of lib.tgz
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":type"] = "input"
                varMap[":status"] = "unknown"
                self.cur.execute(sqlD + comment, varMap)
                resD = self.cur.fetchall()
                # loop over all files
                for lfn, datasetName, jediTaskID, datasetID, fileID in resD:
                    if re.search("\.lib\.tgz(\.\d+)*$", lfn) is not None:
                        # read file spec
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":fileID"] = fileID
                        self.cur.execute(sqlF + comment, varMap)
                        resF = self.cur.fetchone()
                        # make FileSpec
                        if resF is not None:
                            tmpFileSpec = JediFileSpec()
                            tmpFileSpec.pack(resF)
                            retList.append((prodUserName, datasetName, tmpFileSpec))
                            break
                # update modificationTime
                varMap = {}
                varMap[":prodUserName"] = prodUserName
                varMap[":jobsetID"] = jobsetID
                varMap[":jobDefinitionID"] = jobDefinitionID
                self.cur.execute(sqlU + comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"done with {len(retList)}")
            return retList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return []

    # get tasks to get reassigned
    def getTasksToReassign_JEDI(self, vo=None, prodSourceLabel=None):
        comment = " /* JediDBProxy.getTasksToReassign_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retTasks = []
        try:
            # sql to get tasks to reassign
            varMap = {}
            varMap[":status"] = "reassigning"
            varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=5)
            sqlSCF = f"SELECT {JediTaskSpec.columnNames('tabT')} "
            sqlSCF += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlSCF += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlSCF += "AND tabT.status=:status AND tabT.modificationTime<:timeLimit "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlSCF += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlSCF += "AND prodSourceLabel=:prodSourceLabel "
            sqlSCF += "FOR UPDATE"
            sqlSPC = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET modificationTime=CURRENT_DATE "
            sqlSPC += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # get tasks
            tmpLog.debug(sqlSCF + comment + str(varMap))
            self.cur.execute(sqlSCF + comment, varMap)
            resList = self.cur.fetchall()
            for resRT in resList:
                # make taskSpec
                taskSpec = JediTaskSpec()
                taskSpec.pack(resRT)
                # update modificationTime
                varMap = {}
                varMap[":jediTaskID"] = taskSpec.jediTaskID
                self.cur.execute(sqlSPC + comment, varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    retTasks.append(taskSpec)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got {len(retTasks)} tasks")
            return retTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return []

    # kill child tasks
    def killChildTasks_JEDI(self, jediTaskID, taskStatus, useCommit=True):
        comment = " /* JediDBProxy.killChildTasks_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retTasks = []
        try:
            # sql to get child tasks
            sqlGT = f"SELECT jediTaskID,status FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlGT += "WHERE parent_tid=:jediTaskID AND parent_tid<>jediTaskID "
            # sql to change status
            sqlCT = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlCT += "SET status=:status,errorDialog=:errorDialog,stateChangeTime=CURRENT_DATE "
            sqlCT += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            if useCommit:
                self.conn.begin()
            # get tasks
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlGT + comment, varMap)
            resList = self.cur.fetchall()
            for cJediTaskID, cTaskStatus in resList:
                # no more changes
                if cTaskStatus in JediTaskSpec.statusToRejectExtChange():
                    continue
                # change status
                cTaskStatus = "toabort"
                varMap = {}
                varMap[":jediTaskID"] = cJediTaskID
                varMap[":status"] = cTaskStatus
                varMap[":errorDialog"] = f"parent task is {taskStatus}"
                self.cur.execute(sqlCT + comment, varMap)
                tmpLog.debug(f"set {cTaskStatus} to jediTaskID={cJediTaskID}")
                # add missing record_task_status_change and push_task_status_message updates
                self.record_task_status_change(cJediTaskID)
                self.push_task_status_message(None, cJediTaskID, cTaskStatus)
                # kill child
                tmpStat = self.killChildTasks_JEDI(cJediTaskID, cTaskStatus, useCommit=False)
                if not tmpStat:
                    raise RuntimeError("Failed to kill child tasks")
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # kick child tasks
    def kickChildTasks_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.kickChildTasks_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retTasks = []
        try:
            # sql to get child tasks
            sqlGT = f"SELECT jediTaskID,status FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlGT += "WHERE parent_tid=:jediTaskID AND parent_tid<>jediTaskID "
            # sql to change modification time to the time just before pending tasks are reactivated
            timeLimitT = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=5)
            sqlCT = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlCT += "SET modificationTime=CURRENT_DATE-1 "
            sqlCT += "WHERE jediTaskID=:jediTaskID AND modificationTime<:timeLimit "
            sqlCT += "AND status=:status AND lockedBy IS NULL "
            # sql to change state check time
            timeLimitD = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=5)
            sqlCC = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlCC += "SET stateCheckTime=CURRENT_DATE-1 "
            sqlCC += "WHERE jediTaskID=:jediTaskID AND state=:dsState AND stateCheckTime<:timeLimit "
            # begin transaction
            self.conn.begin()
            # get tasks
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlGT + comment, varMap)
            resList = self.cur.fetchall()
            for cJediTaskID, cTaskStatus in resList:
                # no more changes
                if cTaskStatus in JediTaskSpec.statusToRejectExtChange():
                    continue
                # change modification time for pending task
                varMap = {}
                varMap[":jediTaskID"] = cJediTaskID
                varMap[":status"] = "pending"
                varMap[":timeLimit"] = timeLimitT
                self.cur.execute(sqlCT + comment, varMap)
                nRow = self.cur.rowcount
                # add missing record_task_status_change and push_task_status_message updates
                self.record_task_status_change(cJediTaskID)
                self.push_task_status_message(None, cJediTaskID, varMap[":status"])
                tmpLog.debug(f"kicked jediTaskID={cJediTaskID} with {nRow}")
                # change state check time for mutable datasets
                if cTaskStatus not in ["pending"]:
                    varMap = {}
                    varMap[":jediTaskID"] = cJediTaskID
                    varMap[":dsState"] = "mutable"
                    varMap[":timeLimit"] = timeLimitD
                    self.cur.execute(sqlCC + comment, varMap)
                    nRow = self.cur.rowcount
                    tmpLog.debug(f"kicked {nRow} mutable datasets for jediTaskID={cJediTaskID}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # retry child tasks
    def retryChildTasks_JEDI(self, jediTaskID, useCommit=True):
        comment = " /* JediDBProxy.retryChildTasks_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retTasks = []
        try:
            # sql to get output datasets of parent task
            sqlPD = f"SELECT datasetName,containerName FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlPD += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # sql to get child tasks
            sqlGT = f"SELECT jediTaskID,status FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlGT += "WHERE parent_tid=:jediTaskID AND parent_tid<>jediTaskID "
            # sql to get input datasets of child task
            sqlRD = f"SELECT datasetID,datasetName FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlRD += "WHERE jediTaskID=:jediTaskID AND type IN ("
            for tmpType in JediDatasetSpec.getProcessTypes():
                mapKey = ":type_" + tmpType
                sqlRD += f"{mapKey},"
            sqlRD = sqlRD[:-1]
            sqlRD += ") "
            # sql to change task status
            sqlCT = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlCT += "SET status=:status,errorDialog=NULL,stateChangeTime=CURRENT_DATE "
            sqlCT += "WHERE jediTaskID=:jediTaskID "
            # sql to set mutable to dataset status
            sqlMD = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlMD += "SET state=:state,stateCheckTime=CURRENT_DATE "
            sqlMD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to set dataset status
            sqlCD = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlCD += "SET status=:status "
            sqlCD += "WHERE jediTaskID=:jediTaskID AND type=:type "
            # begin transaction
            if useCommit:
                self.conn.begin()
            # get output datasets of parent task
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type1"] = "output"
            varMap[":type2"] = "log"
            self.cur.execute(sqlPD + comment, varMap)
            resList = self.cur.fetchall()
            parentDatasets = set()
            for tmpDS, tmpDC in resList:
                parentDatasets.add(tmpDS)
                parentDatasets.add(tmpDC)
            # get child tasks
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlGT + comment, varMap)
            resList = self.cur.fetchall()
            for cJediTaskID, cTaskStatus in resList:
                # not to retry if child task is aborted/broken
                if cTaskStatus in ["aborted", "toabort", "aborting", "broken", "tobroken", "failed"]:
                    tmpLog.debug(f"not to retry child jediTaskID={cJediTaskID} in {cTaskStatus}")
                    continue
                # get input datasets of child task
                varMap = {}
                varMap[":jediTaskID"] = cJediTaskID
                for tmpType in JediDatasetSpec.getProcessTypes():
                    mapKey = ":type_" + tmpType
                    varMap[mapKey] = tmpType
                self.cur.execute(sqlRD + comment, varMap)
                dsList = self.cur.fetchall()
                inputReady = False
                for datasetID, datasetName in dsList:
                    # set dataset status to mutable
                    if datasetName in parentDatasets or datasetName.split(":")[-1] in parentDatasets:
                        varMap = {}
                        varMap[":jediTaskID"] = cJediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":state"] = "mutable"
                        self.cur.execute(sqlMD + comment, varMap)
                        inputReady = True
                # set task status
                if not inputReady:
                    # set task status to registered since dataset is not ready
                    varMap = {}
                    varMap[":jediTaskID"] = cJediTaskID
                    varMap[":status"] = "registered"
                    self.cur.execute(sqlCT + comment, varMap)
                    # add missing record_task_status_change and push_task_status_message updates
                    self.record_task_status_change(cJediTaskID)
                    self.push_task_status_message(None, cJediTaskID, varMap[":status"])
                    tmpLog.debug(f"set status of child jediTaskID={cJediTaskID} to {varMap[':status']}")
                elif cTaskStatus not in ["ready", "running", "scouting", "scouted"]:
                    # incexec child task
                    tmpLog.debug(f"incremental execution for child jediTaskID={cJediTaskID}")
                    self.retryTask_JEDI(cJediTaskID, "incexec", useCommit=False, statusCheck=False)
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # set super status
    def setSuperStatus_JEDI(self, jediTaskID, superStatus):
        comment = " /* JediDBProxy.setSuperStatus_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        retTasks = []
        try:
            # sql to set super status
            sqlCT = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlCT += "SET superStatus=:superStatus "
            sqlCT += "WHERE jediTaskID=:jediTaskID "
            # set super status
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":superStatus"] = superStatus
            self.cur.execute(sqlCT + comment, varMap)
            return True
        except Exception:
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # set DEFT status
    def setDeftStatus_JEDI(self, jediTaskID, taskStatus):
        comment = " /* JediDBProxy.setDeftStatus_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        try:
            sqlD = f"UPDATE {jedi_config.db.schemaDEFT}.T_TASK "
            sqlD += "SET status=:status,timeStamp=CURRENT_DATE "
            sqlD += "WHERE taskID=:jediTaskID "
            varMap = {}
            varMap[":status"] = taskStatus
            varMap[":jediTaskID"] = jediTaskID
            tmpLog.debug(sqlD + comment + str(varMap))
            self.cur.execute(sqlD + comment, varMap)
            return True
        except Exception:
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # lock task
    def lockTask_JEDI(self, jediTaskID, pid):
        comment = " /* JediDBProxy.lockTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} pid={pid}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to lock task
            sqlPD = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlPD += "SET lockedTime=CURRENT_DATE,modificationTime=CURRENT_DATE "
            sqlPD += "WHERE jediTaskID=:jediTaskID AND lockedBy=:lockedBy "
            # sql to check lock
            sqlCL = f"SELECT lockedBy,lockedTime FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlCL += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # lock
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":lockedBy"] = pid
            self.cur.execute(sqlPD + comment, varMap)
            nRow = self.cur.rowcount
            if nRow == 1:
                retVal = True
                tmpLog.debug(f"done with {retVal}")
            else:
                retVal = False
                # check lock
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlCL + comment, varMap)
                tmpLockedBy, tmpLockedTime = self.cur.fetchone()
                tmpLog.debug(f"done with {retVal} locked by another {tmpLockedBy} at {tmpLockedTime}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # get successful files
    def getSuccessfulFiles_JEDI(self, jediTaskID, datasetID):
        comment = " /* JediDBProxy.getSuccessfulFiles_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} datasetID={datasetID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get files
            sqlF = f"SELECT lfn FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            # begin transaction
            self.conn.begin()
            # lock
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            varMap[":status"] = "finished"
            self.cur.execute(sqlF + comment, varMap)
            res = self.cur.fetchall()
            lfnList = []
            for (lfn,) in res:
                lfnList.append(lfn)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got {len(lfnList)} files")
            return lfnList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # unlock a single task
    def unlockSingleTask_JEDI(self, jediTaskID, pid):
        comment = " /* JediDBProxy.unlockSingleTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} pid={pid}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to unlock
            sqlTU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET lockedBy=NULL,lockedTime=NULL "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND lockedBy=:pid "
            # begin transaction
            self.conn.begin()
            # unlock
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":pid"] = pid
            self.cur.execute(sqlTU + comment, varMap)
            nRow = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {nRow}")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # get JEDI tasks to be throttled
    def throttleTasks_JEDI(self, vo, prodSourceLabel, waitTime):
        comment = " /* JediDBProxy.throttleTasks_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start waitTime={waitTime}min")
        try:
            # sql
            varMap = {}
            varMap[":taskStatus"] = "running"
            varMap[":fileStat1"] = "ready"
            varMap[":fileStat2"] = "running"
            sqlRT = "SELECT tabT.jediTaskID,tabT.numThrottled,AVG(tabC.failedAttempt) "
            sqlRT += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA,".format(jedi_config.db.schemaJEDI)
            sqlRT += "{0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(jedi_config.db.schemaJEDI)
            sqlRT += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlRT += "AND tabT.jediTaskID=tabD.jediTaskID AND tabT.jediTaskID=tabC.jediTaskID "
            sqlRT += "AND tabD.datasetID=tabC.datasetID "
            sqlRT += "AND tabT.status IN (:taskStatus) "
            sqlRT += "AND tabT.numThrottled IS NOT NULL "
            sqlRT += "AND tabD.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlRT += f"{mapKey},"
                varMap[mapKey] = tmpType
            sqlRT = sqlRT[:-1]
            sqlRT += ") AND tabD.masterID IS NULL "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlRT += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlRT += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sqlRT += "AND tabC.status IN (:fileStat1,:fileStat2) "
            sqlRT += "AND tabT.lockedBy IS NULL "
            sqlRT += "GROUP BY tabT.jediTaskID,tabT.numThrottled "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            tmpLog.debug(sqlRT + comment + str(varMap))
            self.cur.execute(sqlRT + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # sql to throttle tasks
            sqlTH = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTH += "SET throttledTime=:releaseTime,modificationTime=CURRENT_DATE,"
            sqlTH += "oldStatus=status,status=:newStatus,errorDialog=:errorDialog,"
            sqlTH += "numThrottled=:numThrottled "
            sqlTH += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            sqlTH += "AND lockedBy IS NULL "
            attemptInterval = 5
            nTasks = 0
            for jediTaskID, numThrottled, largestAttemptNr in resList:
                # check threshold
                if int(largestAttemptNr / attemptInterval) <= numThrottled:
                    continue
                # begin transaction
                self.conn.begin()
                # check task
                try:
                    numThrottled += 1
                    throttledTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                    releaseTime = throttledTime + datetime.timedelta(minutes=waitTime * numThrottled * numThrottled)
                    errorDialog = "#ATM #KV action=throttle jediTaskID={0} due to reason=many_attempts {0} > {1}x{2} ".format(
                        jediTaskID, largestAttemptNr, numThrottled, attemptInterval
                    )
                    errorDialog += f"from {throttledTime.strftime('%Y/%m/%d %H:%M:%S')} "
                    errorDialog += f"till {releaseTime.strftime('%Y/%m/%d %H:%M:%S')}"
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":newStatus"] = "throttled"
                    varMap[":oldStatus"] = "running"
                    varMap[":releaseTime"] = releaseTime
                    varMap[":numThrottled"] = numThrottled
                    varMap[":errorDialog"] = errorDialog
                    tmpLog.debug(sqlTH + comment + str(varMap))
                    self.cur.execute(sqlTH + comment, varMap)
                    if self.cur.rowcount > 0:
                        tmpLog.info(errorDialog)
                        nTasks += 1
                        self.record_task_status_change(jediTaskID)
                        self.push_task_status_message(None, jediTaskID, varMap[":newStatus"])
                except Exception:
                    tmpLog.debug(f"skip locked jediTaskID={jediTaskID}")
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmpLog.debug("done")
            return nTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # throttle one task
    def throttleTask_JEDI(self, jediTaskID, waitTime, errorDialog):
        comment = " /* JediDBProxy.throttleTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start waitTime={waitTime}min")
        try:
            # sql to throttle tasks
            sqlTH = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTH += "SET throttledTime=:releaseTime,modificationTime=CURRENT_DATE,"
            sqlTH += "oldStatus=status,status=:newStatus,errorDialog=:errorDialog "
            sqlTH += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            sqlTH += "AND lockedBy IS NULL "
            # begin transaction
            self.conn.begin()
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":newStatus"] = "throttled"
            varMap[":oldStatus"] = "running"
            varMap[":releaseTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(minutes=waitTime)
            varMap[":errorDialog"] = errorDialog
            self.cur.execute(sqlTH + comment, varMap)
            nRow = self.cur.rowcount
            tmpLog.debug(f"done with {nRow}")
            if nRow > 0:
                self.record_task_status_change(jediTaskID)
                self.push_task_status_message(None, jediTaskID, varMap[":newStatus"])
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # release throttled tasks
    def releaseThrottledTasks_JEDI(self, vo, prodSourceLabel):
        comment = " /* JediDBProxy.releaseThrottledTasks_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get tasks
            varMap = {}
            varMap[":status"] = "throttled"
            sqlTL = "SELECT tabT.jediTaskID,tabT.oldStatus "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA,{0}.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += "AND tabD.jediTaskID=tabT.jediTaskID AND tabD.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlTL += f"{mapKey},"
                varMap[mapKey] = tmpType
            sqlTL = sqlTL[:-1]
            sqlTL += ") AND tabD.masterID IS NULL "
            sqlTL += "AND tabT.status=:status AND tabT.lockedBy IS NULL "
            sqlTL += "AND (tabT.throttledTime<CURRENT_DATE OR "
            sqlTL += "(tabD.nFilesToBeUsed=tabD.nFilesFinished+tabD.nFilesFailed AND tabD.nFiles>0)) "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlTL += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlTL += "AND tabT.prodSourceLabel=:prodSourceLabel "

            # sql to update tasks
            sqlTU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET status=oldStatus,oldStatus=NULL,errorDialog=NULL,modificationtime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus AND lockedBy IS NULL "

            # start transaction
            self.conn.begin()
            tmpLog.debug(sqlTL + comment + str(varMap))
            self.cur.execute(sqlTL + comment, varMap)
            resTL = self.cur.fetchall()

            # loop over all tasks
            nRow = 0
            for jediTaskID, oldStatus in resTL:
                if oldStatus in [None, ""]:
                    tmpLog.debug(f"cannot release jediTaskID={jediTaskID} since oldStatus is invalid")
                    continue
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":oldStatus"] = "throttled"
                self.cur.execute(sqlTU + comment, varMap)
                iRow = self.cur.rowcount
                tmpLog.info(f"#ATM #KV action=released jediTaskID={jediTaskID} with {iRow}")
                nRow += iRow
                if iRow > 0:
                    self.record_task_status_change(jediTaskID)
                    self.push_task_status_message(None, jediTaskID, None)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"updated {nRow} rows")
            return nRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # release throttled task
    def releaseThrottledTask_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.releaseThrottledTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to update tasks
            sqlTU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET status=oldStatus,oldStatus=NULL,errorDialog=NULL,modificationtime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus AND lockedBy IS NULL "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":oldStatus"] = "throttled"
            self.cur.execute(sqlTU + comment, varMap)
            nRow = self.cur.rowcount
            tmpLog.debug(f"done with {nRow}")
            if nRow > 0:
                self.record_task_status_change(jediTaskID)
                self.push_task_status_message(None, jediTaskID, None)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # get throttled users
    def getThrottledUsersTasks_JEDI(self, vo, prodSourceLabel):
        comment = " /* JediDBProxy.getThrottledUsersTasks_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get tasks
            varMap = {}
            varMap[":status"] = "throttled"
            sqlTL = "SELECT jediTaskID,userName,errorDialog "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += "AND tabT.status=:status AND tabT.lockedBy IS NULL "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlTL += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlTL += "AND prodSourceLabel=:prodSourceLabel "
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlTL + comment, varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            userTaskMap = {}
            for jediTaskID, userName, errorDialog in resTL:
                userTaskMap.setdefault(userName, {})
                if errorDialog is None or "type=prestaging" in errorDialog:
                    trasnferType = "prestaging"
                else:
                    trasnferType = "transfer"
                userTaskMap[userName].setdefault(trasnferType, set())
                userTaskMap[userName][trasnferType].add(jediTaskID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"get {len(userTaskMap)} users")
            return userTaskMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return {}

    # duplicate files for reuse
    def duplicateFilesForReuse_JEDI(self, datasetSpec):
        comment = " /* JediDBProxy.duplicateFilesForReuse_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={datasetSpec.jediTaskID} datasetID={datasetSpec.datasetID}>"
        tmpLog = MsgWrapper(logger, methodName)
        try:
            tmpLog.debug(f"start random={datasetSpec.isRandom()}")
            # sql to get unique files
            sqlCT = "SELECT COUNT(*) FROM ("
            sqlCT += "SELECT distinct lfn,startEvent,endEvent "
            sqlCT += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlCT += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlCT += ") "
            # sql to read file spec
            defaultVales = {}
            defaultVales["status"] = "ready"
            defaultVales["PandaID"] = None
            defaultVales["attemptNr"] = 0
            defaultVales["failedAttempt"] = 0
            defaultVales["ramCount"] = 0
            sqlFR = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlFR += f"SELECT {JediFileSpec.columnNames(useSeq=True, defaultVales=defaultVales)} FROM ( "
            sqlFR += f"SELECT {JediFileSpec.columnNames(defaultVales=defaultVales, skipDefaultAttr=True)} "
            sqlFR += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID IN ( "
            sqlFR += "SELECT /*+ UNNEST */ MIN(fileID) minFileID "
            sqlFR += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlFR += "GROUP BY lfn,startEvent,endEvent) "
            if not datasetSpec.isRandom():
                sqlFR += "ORDER BY fileID) "
            else:
                sqlFR += "ORDER BY DBMS_RANDOM.value) "
            # sql to update dataset record
            sqlDU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlDU += "SET nFiles=nFiles+:iFiles,nFilesTobeUsed=nFilesTobeUsed+:iFiles "
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # read unique files
            varMap = {}
            varMap[":jediTaskID"] = datasetSpec.jediTaskID
            varMap[":datasetID"] = datasetSpec.datasetID
            self.cur.execute(sqlCT + comment, varMap)
            resCT = self.cur.fetchone()
            (iFile,) = resCT
            # insert files
            varMap = {}
            varMap[":jediTaskID"] = datasetSpec.jediTaskID
            varMap[":datasetID"] = datasetSpec.datasetID
            self.cur.execute(sqlFR + comment, varMap)
            # update dataset
            if iFile > 0:
                varMap = {}
                varMap[":jediTaskID"] = datasetSpec.jediTaskID
                varMap[":datasetID"] = datasetSpec.datasetID
                varMap[":iFiles"] = iFile
                self.cur.execute(sqlDU + comment, varMap)
            tmpLog.debug(f"inserted {iFile} files")
            return iFile
        except Exception:
            # error
            self.dumpErrorMessage(tmpLog)
            return 0

    # increase seq numbers
    def increaseSeqNumber_JEDI(self, datasetSpec, n_records):
        comment = " /* JediDBProxy.increaseSeqNumber_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={datasetSpec.jediTaskID} datasetID={datasetSpec.datasetID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get max LFN
            sqlCT = (
                "SELECT lfn,maxAttempt,maxFailure FROM {0}.JEDI_Dataset_Contents "
                "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                "AND fileID=("
                "SELECT MAX(fileID) FROM {0}.JEDI_Dataset_Contents "
                "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID"
                ") "
            ).format(jedi_config.db.schemaJEDI)
            varMap = {}
            varMap[":jediTaskID"] = datasetSpec.jediTaskID
            varMap[":datasetID"] = datasetSpec.datasetID
            self.cur.execute(sqlCT + comment, varMap)
            resCT = self.cur.fetchone()
            baseLFN, maxAttempt, maxFailure = resCT
            baseLFN = int(baseLFN) + 1
            # current date
            timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            # make files
            varMaps = []
            for i in range(n_records):
                fileSpec = JediFileSpec()
                fileSpec.jediTaskID = datasetSpec.jediTaskID
                fileSpec.datasetID = datasetSpec.datasetID
                fileSpec.GUID = str(uuid.uuid4())
                fileSpec.type = datasetSpec.type
                fileSpec.status = "ready"
                fileSpec.proc_status = "ready"
                fileSpec.lfn = baseLFN + i
                fileSpec.scope = None
                fileSpec.fsize = 0
                fileSpec.checksum = None
                fileSpec.creationDate = timeNow
                fileSpec.attemptNr = 0
                fileSpec.failedAttempt = 0
                fileSpec.maxAttempt = maxAttempt
                fileSpec.maxFailure = maxFailure
                fileSpec.ramCount = 0
                # make vars
                varMap = fileSpec.valuesMap(useSeq=True)
                varMaps.append(varMap)
            # sql for insert
            sqlIn = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlIn += JediFileSpec.bindValuesExpression()
            self.cur.executemany(sqlIn + comment, varMaps)
            # sql to update dataset record
            sqlDU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlDU += "SET nFiles=nFiles+:iFiles,nFilesTobeUsed=nFilesTobeUsed+:iFiles "
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            varMap = {}
            varMap[":jediTaskID"] = datasetSpec.jediTaskID
            varMap[":datasetID"] = datasetSpec.datasetID
            varMap[":iFiles"] = n_records
            self.cur.execute(sqlDU + comment, varMap)
            tmpLog.debug(f"inserted {n_records} files")
            return n_records
        except Exception:
            # error
            self.dumpErrorMessage(tmpLog)
            return 0

    # lock process
    def lockProcess_JEDI(self, vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid, forceOption, timeLimit):
        comment = " /* JediDBProxy.lockProcess_JEDI */"
        methodName = self.getMethodName(comment)
        # defaults
        if cloud is None:
            cloud = "default"
        if workqueue_id is None:
            workqueue_id = 0
        if resource_name is None:
            resource_name = "default"
        if component is None:
            component = "default"
        methodName += f" <vo={vo} label={prodSourceLabel} cloud={cloud} queue={workqueue_id} resource_type={resource_name} component={component} pid={pid}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retVal = False
            # sql to check
            sqlCT = "SELECT lockedBy "
            sqlCT += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Process_Lock "
            sqlCT += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud AND workqueue_id=:workqueue_id "
            sqlCT += "AND resource_type=:resource_name AND component=:component "
            sqlCT += "AND lockedTime>:timeLimit "
            sqlCT += "FOR UPDATE"
            # sql to delete
            sqlCD = f"DELETE FROM {jedi_config.db.schemaJEDI}.JEDI_Process_Lock "
            sqlCD += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud AND workqueue_id=:workqueue_id "
            sqlCD += "AND resource_type=:resource_name AND component=:component "
            # sql to insert
            sqlFR = f"INSERT INTO {jedi_config.db.schemaJEDI}.JEDI_Process_Lock "
            sqlFR += "(vo, prodSourceLabel, cloud, workqueue_id, resource_type, component, lockedBy, lockedTime) "
            sqlFR += "VALUES(:vo, :prodSourceLabel, :cloud, :workqueue_id, :resource_name, :component, :lockedBy, CURRENT_DATE) "
            # start transaction
            self.conn.begin()
            # check
            if not forceOption:
                varMap = {}
                varMap[":vo"] = vo
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":cloud"] = cloud
                varMap[":workqueue_id"] = workqueue_id
                varMap[":resource_name"] = resource_name
                varMap[":component"] = component
                varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=timeLimit)
                self.cur.execute(sqlCT + comment, varMap)
                resCT = self.cur.fetchone()
            else:
                resCT = None
            if resCT is not None:
                tmpLog.debug(f"skipped, locked by {resCT[0]}")
            else:
                # delete
                varMap = {}
                varMap[":vo"] = vo
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":cloud"] = cloud
                varMap[":workqueue_id"] = workqueue_id
                varMap[":resource_name"] = resource_name
                varMap[":component"] = component
                self.cur.execute(sqlCD + comment, varMap)
                # insert
                varMap = {}
                varMap[":vo"] = vo
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":cloud"] = cloud
                varMap[":workqueue_id"] = workqueue_id
                varMap[":resource_name"] = resource_name
                varMap[":component"] = component
                varMap[":lockedBy"] = pid
                self.cur.execute(sqlFR + comment, varMap)
                tmpLog.debug("successfully locked")
                retVal = True
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal

    # unlock process
    def unlockProcess_JEDI(self, vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid):
        comment = " /* JediDBProxy.unlockProcess_JEDI */"
        methodName = self.getMethodName(comment)
        # defaults
        if cloud is None:
            cloud = "default"
        if workqueue_id is None:
            workqueue_id = 0
        if resource_name is None:
            resource_name = "default"
        if component is None:
            component = "default"
        methodName += f" <vo={vo} label={prodSourceLabel} cloud={cloud} queue={workqueue_id} resource_type={resource_name} component={component} pid={pid}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retVal = False
            # sql to delete
            sqlCD = f"DELETE FROM {jedi_config.db.schemaJEDI}.JEDI_Process_Lock "
            sqlCD += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud "
            sqlCD += "AND workqueue_id=:workqueue_id AND lockedBy=:lockedBy "
            sqlCD += "AND resource_type=:resource_name AND component=:component "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":cloud"] = cloud
            varMap[":workqueue_id"] = workqueue_id
            varMap[":resource_name"] = resource_name
            varMap[":component"] = component
            varMap[":lockedBy"] = pid
            self.cur.execute(sqlCD + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            retVal = True
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal

    # unlock process with PID
    def unlockProcessWithPID_JEDI(self, vo, prodSourceLabel, workqueue_id, resource_name, pid, useBase):
        comment = " /* JediDBProxy.unlockProcessWithPID_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel} queue={workqueue_id} resource_type={resource_name} pid={pid} useBase={useBase}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retVal = False
            # sql to delete
            sqlCD = f"DELETE FROM {jedi_config.db.schemaJEDI}.JEDI_Process_Lock "
            sqlCD += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
            sqlCD += "AND workqueue_id=:workqueue_id "
            sqlCD += "AND resource_name=:resource_name "
            if useBase:
                sqlCD += "AND lockedBy LIKE :lockedBy "
            else:
                sqlCD += "AND lockedBy=:lockedBy "
            # start transaction
            self.conn.begin()
            # delete
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":workqueue_id"] = workqueue_id
            varMap[":resource_name"] = resource_name
            if useBase:
                varMap[":lockedBy"] = pid + "%"
            else:
                varMap[":lockedBy"] = pid
            self.cur.execute(sqlCD + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            retVal = True
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal

    # check process lock
    def checkProcessLock_JEDI(self, vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid, checkBase):
        comment = " /* JediDBProxy.checkProcessLock_JEDI */"
        methodName = self.getMethodName(comment)
        # defaults
        if cloud is None:
            cloud = "default"
        if workqueue_id is None:
            workqueue_id = 0
        if resource_name is None:
            resource_name = "default"
        if component is None:
            component = "default"
        methodName += f" <vo={vo} label={prodSourceLabel} cloud={cloud} queue={workqueue_id} resource_type={resource_name} component={component} pid={pid}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retVal = False
            # sql to check
            sqlCT = "SELECT lockedBy "
            sqlCT += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Process_Lock "
            sqlCT += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud AND workqueue_id=:workqueue_id "
            sqlCT += "AND resource_type=:resource_name AND component=:component "
            sqlCT += "AND lockedTime>:timeLimit "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":cloud"] = cloud
            varMap[":workqueue_id"] = workqueue_id
            varMap[":resource_name"] = resource_name
            varMap[":component"] = component
            varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=5)
            self.cur.execute(sqlCT + comment, varMap)
            resCT = self.cur.fetchone()
            if resCT is not None:
                (lockedBy,) = resCT
                if checkBase:
                    # check only base part
                    if not lockedBy.startswith(pid):
                        retVal = True
                else:
                    # check whole string
                    if lockedBy != pid:
                        retVal = True
                if retVal is True:
                    tmpLog.debug(f"skipped locked by {lockedBy}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {retVal}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal

    # get JEDI tasks to be assessed
    def getAchievedTasks_JEDI(self, vo, prodSourceLabel, timeLimit, nTasks):
        comment = " /* JediDBProxy.getAchievedTasks_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # return value for failure
        failedRet = None
        try:
            # sql
            varMap = {}
            varMap[":status1"] = "running"
            varMap[":status2"] = "pending"
            sqlRT = "SELECT tabT.jediTaskID,tabT.status,tabT.goal,tabT.splitRule,parent_tid "
            sqlRT += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlRT += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlRT += "AND tabT.status IN (:status1,:status2) "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlRT += "AND tabT.vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlRT += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sqlRT += "AND goal IS NOT NULL "
            sqlRT += "AND (assessmentTime IS NULL OR assessmentTime<:timeLimit) "
            sqlRT += f"AND rownum<{nTasks} "
            sqlLK = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET assessmentTime=CURRENT_DATE "
            sqlLK += "WHERE jediTaskID=:jediTaskID AND (assessmentTime IS NULL OR assessmentTime<:timeLimit) AND status=:status "
            sqlDS = "SELECT datasetID,type,nEvents,status "
            sqlDS += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlDS += "WHERE jediTaskID=:jediTaskID AND ((type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlDS += f"{mapKey},"
            sqlDS = sqlDS[:-1]
            sqlDS += ") AND masterID IS NULL) OR (type=:type1)) "
            sqlFC = "SELECT COUNT(*) "
            sqlFC += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFC += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status AND failedAttempt=:failedAttempt "
            # sql to check parent
            sqlCP = f"SELECT status FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlCP += "WHERE jediTaskID=:parent_tid "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            timeToCheck = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=timeLimit)
            varMap[":timeLimit"] = timeToCheck
            tmpLog.debug(sqlRT + comment + str(varMap))
            self.cur.execute(sqlRT + comment, varMap)
            taskStatList = self.cur.fetchall()
            retTasks = []
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # get tasks and datasets
            for jediTaskID, taskStatus, taskGoal, splitRule, parent_tid in taskStatList:
                # begin transaction
                self.conn.begin()
                # lock task
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":timeLimit"] = timeToCheck
                varMap[":status"] = taskStatus
                self.cur.execute(sqlLK + comment, varMap)
                nRow = self.cur.rowcount
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                if nRow == 1:
                    # make a task spec to check if auto finish is disabled
                    taskSpec = JediTaskSpec()
                    taskSpec.splitRule = splitRule
                    if taskSpec.disableAutoFinish():
                        tmpLog.debug(f"skip jediTaskID={jediTaskID} as auto-finish is disabled")
                        continue
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    for tmpType in JediDatasetSpec.getInputTypes():
                        mapKey = ":type_" + tmpType
                        varMap[mapKey] = tmpType
                    varMap[":type1"] = "output"
                    # begin transaction
                    self.conn.begin()
                    # check parent
                    if parent_tid not in [None, jediTaskID]:
                        varMapTmp = {}
                        varMapTmp[":parent_tid"] = parent_tid
                        self.cur.execute(sqlCP + comment, varMapTmp)
                        resCP = self.cur.fetchone()
                        if resCP[0] not in ["finished", "failed", "done", "aborted", "broken"]:
                            tmpLog.debug(f"skip jediTaskID={jediTaskID} as parent {parent_tid} is still {resCP[0]}")
                            # commit
                            if not self._commit():
                                raise RuntimeError("Commit error")
                            continue
                    # check datasets
                    self.cur.execute(sqlDS + comment, varMap)
                    resDS = self.cur.fetchall()
                    totalInputEvents = 0
                    totalOutputEvents = 0
                    firstOutput = True
                    # loop over all datasets
                    taskToFinish = True
                    for datasetID, datasetType, nEvents, dsStatus in resDS:
                        # to update contents
                        if dsStatus in JediDatasetSpec.statusToUpdateContents():
                            tmpLog.debug(f"skip jediTaskID={jediTaskID} datasetID={datasetID} is in {dsStatus}")
                            taskToFinish = False
                            break
                        # counts events
                        if datasetType in JediDatasetSpec.getInputTypes():
                            # input
                            try:
                                totalInputEvents += nEvents
                            except Exception:
                                pass
                            # check if there are unused files
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = datasetID
                            varMap[":status"] = "ready"
                            varMap[":failedAttempt"] = 0
                            self.cur.execute(sqlFC + comment, varMap)
                            (nUnUsed,) = self.cur.fetchone()
                            if nUnUsed != 0:
                                tmpLog.debug(f"skip jediTaskID={jediTaskID} datasetID={datasetID} has {nUnUsed} unused files")
                                taskToFinish = False
                                break
                        else:
                            # only one output
                            if firstOutput:
                                # output
                                try:
                                    totalOutputEvents += nEvents
                                except Exception:
                                    pass
                            firstOutput = False
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    # check number of events
                    if taskToFinish:
                        if totalInputEvents == 0:
                            # input has 0 events
                            tmpLog.debug(f"skip jediTaskID={jediTaskID} input has 0 events")
                            taskToFinish = False
                        elif float(totalOutputEvents) / float(totalInputEvents) * 1000.0 < taskGoal:
                            # goal is not yet reached
                            tmpLog.debug(
                                f"skip jediTaskID={jediTaskID} goal is not yet reached {taskGoal / 10}.{taskGoal % 10}%>{totalOutputEvents}/{totalInputEvents}"
                            )
                            taskToFinish = False
                        else:
                            tmpLog.debug(
                                f"to finsh jediTaskID={jediTaskID} goal is reached {taskGoal / 10}.{taskGoal % 10}%<={totalOutputEvents}/{totalInputEvents}"
                            )
                    # append
                    if taskToFinish:
                        retTasks.append(jediTaskID)
            tmpLog.debug(f"got {len(retTasks)} tasks")
            return retTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

    # get inactive sites
    def getInactiveSites_JEDI(self, flag, timeLimit):
        comment = " /* JediDBProxy.getInactiveSites_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <flag={flag} timeLimit={timeLimit}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retVal = set()
            # sql
            sqlCD = f"SELECT site FROM {jedi_config.db.schemaMETA}.SiteData "
            sqlCD += "WHERE flag=:flag AND hours=:hours AND laststart<:laststart "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":flag"] = flag
            varMap[":hours"] = 3
            varMap[":laststart"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=timeLimit)
            self.cur.execute(sqlCD + comment, varMap)
            resCD = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            for (tmpSiteID,) in resCD:
                retVal.add(tmpSiteID)
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal

    # get total walltime
    def getTotalWallTime_JEDI(self, vo, prodSourceLabel, workQueue, resource_name):
        comment = " /* JediDBProxy.getTotalWallTime_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel} queue={workQueue.queue_name}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get size
            var_map = {":vo": vo, ":prodSourceLabel": prodSourceLabel, ":resource_name": resource_name}
            sql = "SELECT total_walltime, n_has_value, n_no_value "
            sql += f"FROM {jedi_config.db.schemaPANDA}.total_walltime_cache "
            sql += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND resource_type=:resource_name "
            sql += "AND agg_type=:agg_type AND agg_key=:agg_key"

            if workQueue.is_global_share:
                var_map[":agg_type"] = "gshare"
                var_map[":agg_key"] = workQueue.queue_name
            else:
                var_map[":agg_type"] = "workqueue"
                var_map[":agg_key"] = str(workQueue.queue_id)

            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, var_map)
            totWalltime, nHasVal, nNoVal = 0, 0, 0
            try:
                tmpTotWalltime, tmpHasVal, tmpNoVal = self.cur.fetchone()
                if tmpTotWalltime is not None:
                    totWalltime = tmpTotWalltime
                if tmpHasVal is not None:
                    nHasVal = tmpHasVal
                if tmpNoVal is not None:
                    nNoVal = tmpNoVal
            except TypeError:  # there was no result
                pass

            tmpLog.debug(f"totWalltime={totWalltime} nHasVal={nHasVal} nNoVal={nNoVal}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if nHasVal != 0:
                totWalltime = int(totWalltime * (1 + float(nNoVal) / float(nHasVal)))
            else:
                totWalltime = None
            tmpLog.debug(f"done totWalltime={totWalltime}")
            return totWalltime
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # check duplication with internal merge
    def checkDuplication_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.checkDuplication_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # sql to check useJumbo
        sqlJ = f"SELECT useJumbo FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
        # sql to get input datasetID
        sqlM = f"SELECT datasetID FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
        sqlM += "WHERE jediTaskID=:jediTaskID AND type IN ("
        for tmpType in JediDatasetSpec.getInputTypes():
            mapKey = ":type_" + tmpType
            sqlM += f"{mapKey},"
        sqlM = sqlM[:-1]
        sqlM += ") AND masterID IS NULL "
        # sql to get output datasetID and templateID
        sqlO = f"SELECT datasetID,provenanceID FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
        sqlO += "WHERE jediTaskID=:jediTaskID AND type=:type "
        # sql to check duplication without internal merge
        sqlWM = "SELECT distinct outPandaID "
        sqlWM += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
        sqlWM += "WHERE jediTaskID=:jediTaskID AND datasetID=:outDatasetID AND status IN (:statT1,:statT2) "
        sqlWM += "MINUS "
        sqlWM += "SELECT distinct PandaID "
        sqlWM += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
        sqlWM += "WHERE jediTaskID=:jediTaskID AND datasetID=:inDatasetID AND status=:statI "
        # sql to check duplication with jumbo
        sqlJM = "WITH tmpTab AS ("
        sqlJM += f"SELECT f.fileID,f.PandaID FROM {jedi_config.db.schemaPANDA}.filesTable4 f, ("
        sqlJM += f"SELECT PandaID FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
        sqlJM += "WHERE jediTaskID=:jediTaskID AND datasetID=:outDatasetID AND status IN (:statT1,:statT2)) t "
        sqlJM += "WHERE f.PandaID=t.PandaID AND f.datasetID=:inDatasetID "
        sqlJM += "UNION "
        sqlJM += f"SELECT f.fileID,f.PandaID FROM {jedi_config.db.schemaPANDAARCH}.filesTable_Arch f, ("
        sqlJM += f"SELECT PandaID FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
        sqlJM += "WHERE jediTaskID=:jediTaskID AND datasetID=:outDatasetID AND status IN (:statT1,:statT2)) t "
        sqlJM += "WHERE f.PandaID=t.PandaID AND f.datasetID=:inDatasetID AND f.modificationTime>CURRENT_DATE-365 "
        sqlJM += ") "
        sqlJM += "SELECT t1.PandaID FROM tmpTab t1, tmpTab t2 WHERE t1.fileID=t2.fileID AND t1.PandaID>t2.PandaID "
        # sql to check duplication with internal merge
        sqlCM = "SELECT distinct c1.outPandaID "
        sqlCM += "FROM {0}.JEDI_Dataset_Contents c1,{0}.JEDI_Dataset_Contents c2,{0}.JEDI_Datasets d ".format(jedi_config.db.schemaJEDI)
        sqlCM += "WHERE d.jediTaskID=:jediTaskID AND c1.jediTaskID=d.jediTaskID AND c1.datasetID=d.datasetID AND d.templateID=:templateID "
        sqlCM += "AND c1.jediTaskID=c2.jediTaskID AND c2.datasetID=:outDatasetID AND c1.pandaID=c2.pandaID and c2.status IN (:statT1,:statT2) "
        sqlCM += "MINUS "
        sqlCM += "SELECT distinct PandaID "
        sqlCM += f"FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
        sqlCM += "WHERE jediTaskID=:jediTaskID AND datasetID=:inDatasetID and status=:statI "
        try:
            # start transaction
            self.conn.begin()
            # check useJumbo
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlJ + comment, varMap)
            resJ = self.cur.fetchone()
            (useJumbo,) = resJ
            # get input datasetID
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                varMap[mapKey] = tmpType
            self.cur.execute(sqlM + comment, varMap)
            resM = self.cur.fetchone()
            if resM is not None:
                (inDatasetID,) = resM
                # get output datasetID and templateID
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":type"] = "output"
                self.cur.execute(sqlO + comment, varMap)
                resO = self.cur.fetchone()
                if resO is None:
                    # no output
                    retVal = 0
                else:
                    outDatasetID, templateID = resO
                    # check duplication
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":inDatasetID"] = inDatasetID
                    varMap[":outDatasetID"] = outDatasetID
                    varMap[":statI"] = "finished"
                    varMap[":statT1"] = "finished"
                    varMap[":statT2"] = "nooutput"
                    if templateID is not None:
                        # with internal merge
                        varMap[":templateID"] = templateID
                        self.cur.execute(sqlCM + comment, varMap)
                    else:
                        if useJumbo is None:
                            # without internal merge
                            self.cur.execute(sqlWM + comment, varMap)
                        else:
                            # with jumbo
                            del varMap[":statI"]
                            self.cur.execute(sqlJM + comment, varMap)
                    retList = self.cur.fetchall()
                    dupPandaIDs = []
                    for (dupPandaID,) in retList:
                        dupPandaIDs.append(dupPandaID)
                        tmpLog.debug(f"bad PandaID={dupPandaID}")
                    retVal = len(dupPandaIDs)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"dup={retVal}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    def getNetworkMetrics(self, dst, keyList):
        """
        Get the network metrics from a source to all possible destinations
        :param src: source site
        :param key_list: activity keys.
        :return: returns a dictionary with network values in the style
        {
            <dest>: {<key>: <value>, <key>: <value>},
            <dest>: {<key>: <value>, <key>: <value>},
            ...
        }
        """
        comment = " /* JediDBProxy.getNetworkMetrics */"
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")

        latest_validity = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=60)

        varMap = {"dst": dst, "latest_validity": latest_validity}
        i = 0
        for key in keyList:
            varMap[f":key{i}"] = key
            i += 1
        key_bindings = ",".join(f":key{i}" for i in range(len(keyList)))

        sql = f"""
        SELECT src, key, value, ts FROM {jedi_config.db.schemaJEDI}.network_matrix_kv
        WHERE dst = :dst AND key IN ({key_bindings})
        AND ts > :latest_validity
        """

        self.cur.execute(sql + comment, varMap)
        resList = self.cur.fetchall()

        networkMap = {}
        total = {}
        for res in resList:
            src, key, value, ts = res
            networkMap.setdefault(src, {})
            networkMap[src][key] = value
            total.setdefault(key, 0)
            try:
                total[key] += value
            except Exception:
                pass
        networkMap["total"] = total
        tmpLog.debug(f"network map to nucleus {dst} is: {networkMap}")

        return networkMap

    def getBackloggedNuclei(self):
        """
        Return a list of nuclei, which has built up transfer backlog. We will consider a nucleus as backlogged,
         when it has over 2000 output transfers queued and there are more than 3 sites with queues over
        """

        comment = " /* JediDBProxy.getBackloggedNuclei */"
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")

        latest_validity = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=60)

        nqueued_cap = self.getConfigValue("taskbrokerage", "NQUEUED_NUC_CAP", "jedi")
        if nqueued_cap is None:
            nqueued_cap = 2000

        varMap = {":latest_validity": latest_validity, ":nqueued_cap": nqueued_cap}

        sql = f"""
              SELECT dst
              FROM {jedi_config.db.schemaJEDI}.network_matrix_kv
              WHERE key = 'Production Output_queued'
              AND ts > :latest_validity
              GROUP BY dst
              HAVING SUM(value) > :nqueued_cap
        """

        self.cur.execute(sql + comment, varMap)
        try:
            backlogged_nuclei = [entry[0] for entry in self.cur.fetchall()]
        except IndexError:
            backlogged_nuclei = []

        tmpLog.debug(f"Nuclei with a long backlog are: {backlogged_nuclei}")

        return backlogged_nuclei

    def getPandaSiteToOutputStorageSiteMapping(self):
        """
        Get a  mapping of panda sites to their storage site. We consider the storage site of the default ddm endpoint
        :return: dictionary with panda_site_name keys and site_name values
        """
        comment = " /* JediDBProxy.getPandaSiteToOutputStorageSiteMapping */"
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")

        sql = """
        SELECT pdr.panda_site_name, de.site_name, nvl(pdr.scope, 'default')
        FROM atlas_panda.panda_ddm_relation pdr, atlas_panda.ddm_endpoint de
        WHERE pdr.default_write = 'Y'
        AND pdr.ddm_endpoint_name = de.ddm_endpoint_name
        """

        self.cur.execute(sql + comment)
        resList = self.cur.fetchall()
        mapping = {}

        for res in resList:
            pandaSiteName, siteName, scope = res
            mapping.setdefault(pandaSiteName, {})
            mapping[pandaSiteName][scope] = siteName

        # tmpLog.debug('panda site to ATLAS site mapping is: {0}'.format(mapping))

        tmpLog.debug("done")
        return mapping

    # get failure counts for a task
    def getFailureCountsForTask_JEDI(self, jediTaskID, timeWindow):
        comment = " /* JediDBProxy.getFailureCountsForTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql
            sql = "SELECT COUNT(*),computingSite,jobStatus "
            sql += f"FROM {jedi_config.db.schemaPANDA}.jobsArchived4 "
            sql += f"WHERE jediTaskID=:jediTaskID AND modificationTime>CURRENT_DATE-{timeWindow}/24 "
            sql += "AND ("
            sql += "(jobStatus=:jobFailed AND pilotErrorCode IS NOT NULL AND pilotErrorCode<>0) OR "
            sql += "(jobStatus=:jobClosed AND jobSubStatus=:toReassign AND relocationFlag<>:relThrottled) OR "
            sql += "(jobStatus=:jobFinished) "
            sql += ") "
            sql += "GROUP BY computingSite,jobStatus "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":jobClosed"] = "closed"
            varMap[":jobFailed"] = "failed"
            varMap[":jobFinished"] = "finished"
            varMap[":toReassign"] = "toreassign"
            varMap[":relThrottled"] = 3
            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # make dict
            retMap = {}
            for cnt, computingSite, jobStatus in resList:
                if computingSite not in retMap:
                    retMap[computingSite] = {}
                if jobStatus not in retMap[computingSite]:
                    retMap[computingSite][jobStatus] = 0
                retMap[computingSite][jobStatus] += cnt
            tmpLog.debug(str(retMap))
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return {}

    # count the number of jobs and cores per user or working group
    def countJobsPerTarget_JEDI(self, target, is_user):
        comment = " /* JediDBProxy.countJobsPerTarget_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <target={target}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql
            sql = "SELECT COUNT(*),SUM(coreCount),jobStatus FROM ("
            sql += f"SELECT PandaID,jobStatus,coreCount FROM {jedi_config.db.schemaPANDA}.jobsDefined4 "
            if is_user:
                sql += "WHERE prodUserName=:target "
            else:
                sql += "WHERE workingGroup=:target "
            sql += "UNION "
            sql += f"SELECT PandaID,jobStatus,coreCount FROM {jedi_config.db.schemaPANDA}.jobsActive4 "
            if is_user:
                sql += "WHERE prodUserName=:target AND workingGroup IS NULL "
            else:
                sql += "WHERE workingGroup=:target "
            sql += ") GROUP BY jobStatus "
            varMap = {}
            varMap[":target"] = target
            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # make dict
            retMap = {"nQueuedJobs": 0, "nQueuedCores": 0, "nRunJobs": 0, "nRunCores": 0}
            for nJobs, nCores, jobStatus in resList:
                if jobStatus in ["defined", "assigned", "activated", "starting", "throttled"]:
                    retMap["nQueuedJobs"] += nJobs
                    retMap["nQueuedCores"] += nCores
                elif jobStatus in ["running"]:
                    retMap["nRunJobs"] += nJobs
                    retMap["nRunCores"] += nCores
            tmpLog.debug(str(retMap))
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return {}

    # get old merge job PandaIDs
    def getOldMergeJobPandaIDs_JEDI(self, jediTaskID, pandaID):
        comment = " /* JediDBProxy.getOldMergeJobPandaIDs_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} PandaID={pandaID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql
            sql = "SELECT distinct tabC.PandaID "
            sql += "FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE tabD.jediTaskID=:jediTaskID AND tabD.jediTaskID=tabC.jediTaskID "
            sql += "AND tabD.datasetID=tabC.datasetID "
            sql += "AND tabD.type=:dsType AND tabC.outPandaID=:pandaID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":pandaID"] = pandaID
            varMap[":dsType"] = "trn_log"
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retVal = []
            for (tmpPandaID,) in resList:
                if tmpPandaID != pandaID:
                    retVal.append(tmpPandaID)
            tmpLog.debug(str(retVal))
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return []

    # get active jumbo jobs for a task
    def getActiveJumboJobs_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getActiveJumboJobs_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql
            sql = "SELECT PandaID,jobStatus,computingSite "
            sql += f"FROM {jedi_config.db.schemaPANDA}.jobsWaiting4 "
            sql += "WHERE jediTaskID=:jediTaskID AND eventService=:jumboJob "
            sql += "UNION "
            sql += "SELECT PandaID,jobStatus,computingSite "
            sql += f"FROM {jedi_config.db.schemaPANDA}.jobsDefined4 "
            sql += "WHERE jediTaskID=:jediTaskID AND eventService=:jumboJob "
            sql += "UNION "
            sql += "SELECT PandaID,jobStatus,computingSite "
            sql += f"FROM {jedi_config.db.schemaPANDA}.jobsActive4 "
            sql += "WHERE jediTaskID=:jediTaskID AND eventService=:jumboJob "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":jumboJob"] = EventServiceUtils.jumboJobFlagNumber
            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retMap = {}
            for pandaID, jobStatus, computingSite in resList:
                if jobStatus in ["transferring", "holding"]:
                    continue
                retMap[pandaID] = {"status": jobStatus, "site": computingSite}
            tmpLog.debug(str(retMap))
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return {}

    # get jobParms of the first job
    def getJobParamsOfFirstJob_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getJobParamsOfFirstJob_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retVal = None
            outFileMap = dict()
            # sql to get PandaID of the first job
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sql = "SELECT * FROM ("
            sql += "SELECT tabF.datasetID,tabF.fileID "
            sql += "FROM {0}.JEDI_Datasets tabD, {0}.JEDI_Dataset_Contents tabF ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE tabD.jediTaskID=tabF.jediTaskID AND tabD.jediTaskID=:jediTaskID "
            sql += "AND tabD.datasetID=tabF.datasetID "
            sql += "AND tabD.masterID IS NULL "
            sql += "AND tabF.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sql += f"{mapKey},"
                varMap[mapKey] = tmpType
            sql = sql[:-1]
            sql += ") "
            sql += "ORDER BY fileID "
            sql += ") WHERE rownum<2 "
            # sql to get PandaIDs
            sqlP = f"SELECT PandaID FROM {jedi_config.db.schemaPANDA}.filesTable4 WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlPA = "SELECT PandaID FROM {0}.filesTable_arch WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID ".format(
                jedi_config.db.schemaPANDAARCH
            )
            # sql to get jobParms
            sqlJ = f"SELECT jobParameters FROM {jedi_config.db.schemaPANDA}.jobParamsTable WHERE PandaID=:PandaID "
            sqlJA = f"SELECT jobParameters FROM {jedi_config.db.schemaPANDAARCH}.jobParamsTable_ARCH WHERE PandaID=:PandaID"
            # sql to get file
            sqlF = f"SELECT lfn,datasetID FROM {jedi_config.db.schemaPANDA}.filesTable4 where PandaID=:PandaID AND type=:type "
            sqlFA = f"SELECT lfn,datasetID FROM {jedi_config.db.schemaPANDAARCH}.filesTable_Arch where PandaID=:PandaID AND type=:type "
            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchone()
            if res is not None:
                datasetID, fileID = res
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                self.cur.execute(sqlP + comment, varMap)
                resP = self.cur.fetchone()
                if resP is None:
                    self.cur.execute(sqlPA + comment, varMap)
                    resP = self.cur.fetchone()
                (pandaID,) = resP
                varMap = {}
                varMap[":PandaID"] = pandaID
                self.cur.execute(sqlJ + comment, varMap)
                for (clobJobP,) in self.cur:
                    retVal = clobJobP
                    break
                if retVal is None:
                    self.cur.execute(sqlJA + comment, varMap)
                    for (clobJobP,) in self.cur:
                        retVal = clobJobP
                        break
                # get output
                varMap = dict()
                varMap[":PandaID"] = pandaID
                varMap[":type"] = "output"
                self.cur.execute(sqlF + comment, varMap)
                resF = self.cur.fetchall()
                if len(resF) == 0:
                    self.cur.execute(sqlFA + comment, varMap)
                    resF = self.cur.fetchall()
                for lfn, datasetID in resF:
                    outFileMap[datasetID] = lfn
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"get {len(retVal)} bytes")
            return retVal, outFileMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None, None

    # bulk fetch fileIDs
    def bulkFetchFileIDs_JEDI(self, jediTaskID, nIDs):
        comment = " /* JediDBProxy.bulkFetchFileIDs_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} nIDs={nIDs}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            newFileIDs = []
            varMap = {}
            varMap[":nIDs"] = nIDs
            # sql to get fileID
            sqlFID = f"SELECT {jedi_config.db.schemaJEDI}.JEDI_DATASET_CONT_FILEID_SEQ.nextval FROM "
            sqlFID += "(SELECT level FROM dual CONNECT BY level<=:nIDs) "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            self.cur.execute(sqlFID + comment, varMap)
            resFID = self.cur.fetchall()
            for (fileID,) in resFID:
                newFileIDs.append(fileID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"got {len(newFileIDs)} IDs")
            return newFileIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return []

    # set del flag to events
    def setDelFlagToEvents_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.setDelFlagToEvents_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":delFlag"] = "Y"
            # sql to set del flag
            sqlFID = f"UPDATE /*+ INDEX_RS_ASC(JEDI_EVENTS JEDI_EVENTS_PK) */ {jedi_config.db.schemaJEDI}.JEDI_Events "
            sqlFID += "SET file_not_deleted=:delFlag "
            sqlFID += "WHERE jediTaskID=:jediTaskID AND file_not_deleted IS NULL AND objStore_ID IS NOT NULL "
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlFID + comment, varMap)
            nRow = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"set Y to {nRow} event ranges")
            return nRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # set del flag to events
    def removeFilesIndexInconsistent_JEDI(self, jediTaskID, datasetIDs):
        comment = " /* JediDBProxy.removeFilesIndexInconsistent_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get files
            sqlFID = f"SELECT lfn,fileID FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFID += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # start transaction
            self.conn.begin()
            # get files
            lfnMap = {}
            for datasetID in datasetIDs:
                if datasetID not in lfnMap:
                    lfnMap[datasetID] = {}
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                self.cur.execute(sqlFID + comment, varMap)
                tmpRes = self.cur.fetchall()
                for lfn, fileID in tmpRes:
                    items = lfn.split(".")
                    if len(items) < 3:
                        continue
                    idx = items[1] + items[2]
                    if idx not in lfnMap[datasetID]:
                        lfnMap[datasetID][idx] = []
                    lfnMap[datasetID][idx].append(fileID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # find common elements
            datasetID = datasetIDs[0]
            commonIdx = set(lfnMap[datasetID].keys())
            for datasetID in datasetIDs[1:]:
                commonIdx = commonIdx & set(lfnMap[datasetID].keys())
            tmpLog.debug(f"{len(commonIdx)} common files")
            # sql to remove uncommon
            sqlRF = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlRF += "SET status=:newStatus "
            sqlRF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlRF += "AND status=:oldStatus "
            # sql to count files
            sqlCF = f"SELECT COUNT(*) FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlCF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status<>:status "
            # sql to update nFiles
            sqlUD = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlUD += "SET nFiles=:nFiles,nFilesTobeUsed=:nFilesTobeUsed "
            sqlUD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            self.conn.begin()
            # remove uncommon
            for datasetID in datasetIDs:
                nLost = 0
                for idx, fileIDs in lfnMap[datasetID].items():
                    if idx not in commonIdx:
                        for fileID in fileIDs:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = datasetID
                            varMap[":fileID"] = fileID
                            varMap[":oldStatus"] = "ready"
                            varMap[":newStatus"] = "lost"
                            self.cur.execute(sqlRF + comment, varMap)
                            nRow = self.cur.rowcount
                            if nRow > 0:
                                nLost += 1
                tmpLog.debug(f"set {nLost} files to lost for datasetID={datasetID}")
                # count files
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":status"] = "lost"
                self.cur.execute(sqlCF + comment, varMap)
                (nFiles,) = self.cur.fetchone()
                # update nFiles
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":nFiles"] = nFiles
                varMap[":nFilesTobeUsed"] = nFiles
                self.cur.execute(sqlUD + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # throttle jobs in pauses tasks
    def throttleJobsInPausedTasks_JEDI(self, vo, prodSourceLabel):
        comment = " /* JediDBProxy.throttleJobsInPausedTasks_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get tasks
            varMap = {}
            varMap[":status"] = "paused"
            varMap[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=10)
            sqlTL = "SELECT jediTaskID "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += "AND tabT.status=:status AND tabT.modificationTime<:timeLimit AND tabT.lockedBy IS NULL "
            if vo not in [None, "any"]:
                varMap[":vo"] = vo
                sqlTL += "AND vo=:vo "
            if prodSourceLabel not in [None, "any"]:
                varMap[":prodSourceLabel"] = prodSourceLabel
                sqlTL += "AND prodSourceLabel=:prodSourceLabel "
            # sql to update tasks
            sqlTU = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlTU += "SET modificationtime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND status=:status AND lockedBy IS NULL "
            # sql to throttle jobs
            sqlJT = f"UPDATE {jedi_config.db.schemaPANDA}.jobsActive4 "
            sqlJT += "SET jobStatus=:newJobStatus "
            sqlJT += "WHERE jediTaskID=:jediTaskID AND jobStatus=:oldJobStatus "
            # sql to get jobs in jobsDefined
            sqlJD = f"SELECT PandaID FROM {jedi_config.db.schemaPANDA}.jobsDefined4 "
            sqlJD += "WHERE jediTaskID=:jediTaskID "
            # start transaction
            self.conn.begin()
            tmpLog.debug(sqlTL + comment + str(varMap))
            self.cur.execute(sqlTL + comment, varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            retMap = {}
            for (jediTaskID,) in resTL:
                retMap[jediTaskID] = set()
                # lock task
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":status"] = "paused"
                self.cur.execute(sqlTU + comment, varMap)
                iRow = self.cur.rowcount
                if iRow > 0:
                    # throttle jobs
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":newJobStatus"] = "throttled"
                    varMap[":oldJobStatus"] = "activated"
                    self.cur.execute(sqlJT + comment, varMap)
                    iRow = self.cur.rowcount
                    tmpLog.debug(f"throttled {iRow} jobs for jediTaskID={jediTaskID}")
                # get jobs
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                self.cur.execute(sqlJD + comment, varMap)
                resJD = self.cur.fetchall()
                for (tmpPandaID,) in resJD:
                    retMap[jediTaskID].add(tmpPandaID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # set useJumbo flag
    def setUseJumboFlag_JEDI(self, jediTaskID, statusStr):
        comment = " /* JediDBProxy.setUseJumboFlag_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jediTaskID} status={statusStr}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # check current flag
            sqlCF = f"SELECT useJumbo FROM {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlCF += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlCF + comment, varMap)
            (curStr,) = self.cur.fetchone()
            # check files
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sqlFF = f"SELECT nFilesToBeUsed-nFilesUsed-nFilesWaiting FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlFF += "WHERE jediTaskID=:jediTaskID AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlFF += f"{mapKey},"
                varMap[mapKey] = tmpType
            sqlFF = sqlFF[:-1]
            sqlFF += ") AND masterID IS NULL "
            self.cur.execute(sqlFF + comment, varMap)
            (nFiles,) = self.cur.fetchone()
            # disallow some transition
            retVal = True
            if statusStr == "pending" and curStr == JediTaskSpec.enum_useJumbo["lack"]:
                # to prevent from changing lack to pending
                statusStr = "lack"
                tmpLog.debug(f"changed to {statusStr} since to pending is not allowed")
                retVal = False
            elif statusStr == "running" and curStr == JediTaskSpec.enum_useJumbo["pending"]:
                # to running from pending only when all files are used
                if nFiles != 0:
                    statusStr = "pending"
                    tmpLog.debug(f"changed to {statusStr} since nFiles={nFiles}")
                    retVal = False
            elif statusStr == "pending" and curStr == JediTaskSpec.enum_useJumbo["running"]:
                # to pending from running only when some files are available
                if nFiles == 0:
                    statusStr = "running"
                    tmpLog.debug(f"changed to {statusStr} since nFiles == 0")
                    retVal = False
            # set jumbo
            sqlDJ = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET useJumbo=:status "
            sqlDJ += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":status"] = JediTaskSpec.enum_useJumbo[statusStr]
            self.cur.execute(sqlDJ + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"set {curStr} -> {varMap[':status']}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # get number of tasks with running jumbo jobs
    def getNumTasksWithRunningJumbo_JEDI(self, vo, prodSourceLabel, cloudName, workqueue):
        comment = " /* JediDBProxy.getNumTasksWithRunningJumbo_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel} cloud={cloudName} queue={workqueue.queue_name}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # get tasks
            sqlDJ = f"SELECT task_count FROM {jedi_config.db.schemaJEDI}.MV_RUNNING_JUMBO_TASK_COUNT "
            sqlDJ += "WHERE vo=:vo AND prodSourceLabel=:label AND cloud=:cloud "
            sqlDJ += "AND useJumbo in (:useJumbo1,:useJumbo2) AND status IN (:st1,:st2,:st3) "
            varMap = {}
            varMap[":vo"] = vo
            varMap[":label"] = prodSourceLabel
            varMap[":cloud"] = cloudName
            if workqueue.is_global_share:
                sqlDJ += "AND gshare =:gshare "
                sqlDJ += f"AND workqueue_id NOT IN (SELECT queue_id FROM {jedi_config.db.schemaJEDI}.jedi_work_queue WHERE queue_function = 'Resource') "
                varMap[":gshare"] = workqueue.queue_name
            else:
                sqlDJ += "AND workQueue_ID =:queue_id "
                varMap[":queue_id"] = workqueue.queue_id
            varMap[":st1"] = "running"
            varMap[":st2"] = "pending"
            varMap[":st3"] = "ready"
            varMap[":useJumbo1"] = JediTaskSpec.enum_useJumbo["running"]
            varMap[":useJumbo2"] = JediTaskSpec.enum_useJumbo["pending"]
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlDJ + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            res = self.cur.fetchone()
            if res is None:
                nTasks = 0
            else:
                nTasks = res[0]
            # return
            tmpLog.debug(f"got {nTasks} tasks")
            return nTasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return 0

    # get number of unprocessed events
    def getNumUnprocessedEvents_JEDI(self, vo, prodSourceLabel, criteria, neg_criteria):
        comment = " /* JediDBProxy.getNumUnprocessedEvents_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <vo={vo} label={prodSourceLabel}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start with criteria={str(criteria)} neg={str(neg_criteria)}")
        try:
            # get num events
            varMap = {}
            varMap[":vo"] = vo
            varMap[":label"] = prodSourceLabel
            varMap[":type"] = "input"
            sqlDJ = "SELECT SUM(nEvents),MAX(creationDate) FROM ("
            sqlDJ += "SELECT CASE tabD.nFiles WHEN 0 THEN 0 ELSE tabD.nEvents*(tabD.nFiles-tabD.nFilesUsed)/tabD.nFiles END nEvents,"
            sqlDJ += "tabT.creationDate creationDate "
            sqlDJ += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlDJ += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlDJ += "AND tabT.jediTaskID=tabD.jediTaskID "
            sqlDJ += "AND tabT.vo=:vo AND tabT.prodSourceLabel=:label "
            sqlDJ += "AND tabT.status IN (:st1,:st2,:st3,:st4,:st5,:st6,:st7) AND tabD.type=:type AND tabD.masterID IS NULL "
            for key, val in criteria.items():
                sqlDJ += "AND tabT.{0}=:{0} ".format(key)
                varMap[f":{key}"] = val
            for key, val in neg_criteria.items():
                sqlDJ += "AND tabT.{0}<>:neg_{0} ".format(key)
                varMap[f":neg_{key}"] = val
            sqlDJ += ") "
            varMap[":st1"] = "running"
            varMap[":st2"] = "pending"
            varMap[":st3"] = "ready"
            varMap[":st4"] = "scouting"
            varMap[":st5"] = "registered"
            varMap[":st6"] = "defined"
            varMap[":st7"] = "assigning"
            # sql to get pending tasks
            sqlPD = "SELECT COUNT(1) "
            sqlPD += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlPD += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlPD += "AND tabT.vo=:vo AND tabT.prodSourceLabel=:label "
            sqlPD += "AND tabT.status IN (:st1,:st2) "
            for key, val in criteria.items():
                sqlPD += "AND tabT.{0}=:{0} ".format(key)
            # get num events
            self.conn.begin()
            self.cur.execute(sqlDJ + comment, varMap)
            nEvents, lastTaskTime = self.cur.fetchone()
            if nEvents is None:
                nEvents = 0
            # get num of pending tasks
            varMap = dict()
            varMap[":vo"] = vo
            varMap[":label"] = prodSourceLabel
            varMap[":st1"] = "pending"
            varMap[":st2"] = "registered"
            for key, val in criteria.items():
                varMap[f":{key}"] = val
            self.cur.execute(sqlPD + comment, varMap)
            (nPending,) = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got nEvents={nEvents} lastTaskTime={lastTaskTime} nPendingTasks={nPending}")
            return nEvents, lastTaskTime, nPending
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None, None, None

    # get number of jobs for a task
    def getNumJobsForTask_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.getNumJobsForTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jediTaskID} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # get num of done jobs
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            sql = "SELECT COUNT(*) FROM ("
            sql += "SELECT distinct c.PandaID "
            sql += "FROM {0}.JEDI_Datasets d,{0}.JEDI_Dataset_Contents c ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID "
            sql += "AND d.jediTaskID=:jediTaskID AND d.masterID IS NULL "
            sql += "AND d.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sql += f"{mapKey},"
                varMap[mapKey] = tmpType
            sql = sql[:-1]
            sql += ") "
            sql += ") "
            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            (nDone,) = self.cur.fetchone()
            # return
            tmpLog.debug(f"got {nDone} jobs")
            return nDone
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get number map for standby jobs
    def getNumMapForStandbyJobs_JEDI(self, workqueue):
        comment = " /* JediDBProxy.getNumMapForStandbyJobs_JEDI */"
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retMapStatic = dict()
            retMapDynamic = dict()
            # get num of done jobs
            varMap = dict()
            varMap[":status"] = "standby"
            sql = f"SELECT /* use_json_type */ panda_queue, scj.data.catchall FROM {jedi_config.db.schemaJEDI}.schedconfig_json scj "
            sql += "WHERE scj.data.status=:status "
            self.conn.begin()
            self.cur.arraysize = 1000
            self.cur.execute(sql + comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # sum per gshare/workqueue and resource type
            for siteid, catchall in resList:
                numMap = JobUtils.parseNumStandby(catchall)
                if numMap is not None:
                    for wq_tag, resource_num in numMap.items():
                        if workqueue.is_global_share:
                            if workqueue.queue_name != wq_tag:
                                continue
                        else:
                            if str(workqueue.queue_id) != wq_tag:
                                continue
                        for resource_type, num in resource_num.items():
                            if num == 0:
                                retMap = retMapDynamic
                                # dynamic : use # of starting jobs as # of standby jobs
                                varMap = dict()
                                varMap[":vo"] = workqueue.VO
                                varMap[":status"] = "starting"
                                varMap[":resource_type"] = resource_type
                                varMap[":computingsite"] = siteid
                                sql = f"SELECT /*+ RESULT_CACHE */ njobs FROM {jedi_config.db.schemaPANDA}.JOBS_SHARE_STATS "
                                sql += "WHERE vo=:vo AND resource_type=:resource_type AND jobstatus=:status AND computingsite=:computingsite "
                                if workqueue.is_global_share:
                                    sql += "AND gshare=:gshare "
                                    sql += "AND workqueue_id NOT IN (SELECT queue_id FROM {0}.jedi_work_queue WHERE queue_function=:func) ".format(
                                        jedi_config.db.schemaPANDA
                                    )
                                    varMap[":gshare"] = workqueue.queue_name
                                    varMap[":func"] = "Resource"
                                else:
                                    sql += "AND workqueue_id=:workqueue_id "
                                    varMap[":workqueue_id"] = workqueue.queue_id
                                self.cur.execute(sql, varMap)
                                res = self.cur.fetchone()
                                if res is None:
                                    num = 0
                                else:
                                    (num,) = res
                            else:
                                retMap = retMapStatic
                            if resource_type not in retMap:
                                retMap[resource_type] = 0
                            retMap[resource_type] += num
            # return
            tmpLog.debug(f"got static={str(retMapStatic)} dynamic={str(retMapDynamic)}")
            return retMapStatic, retMapDynamic
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return {}, {}

    # update datasets to finish a task
    def updateDatasetsToFinishTask_JEDI(self, jediTaskID, lockedBy):
        comment = " /* JediDBProxy.updateDatasetsToFinishTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jediTaskID} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to lock task
            sqlLK = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE "
            sqlLK += "WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL "
            # sql to get datasets
            sqlAV = f"SELECT datasetID FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlAV += "WHERE jediTaskID=:jediTaskID AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlAV += f"{mapKey},"
            sqlAV = sqlAV[:-1]
            sqlAV += ") "
            # sql to update attemptNr for files
            sqlFR = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFR += "SET attemptNr=maxAttempt "
            sqlFR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlFR += "AND status=:status AND keepTrack=:keepTrack "
            sqlFR += "AND maxAttempt IS NOT NULL AND attemptNr<maxAttempt "
            sqlFR += "AND (maxFailure IS NULL OR failedAttempt<maxFailure) "
            # sql to update output/lib/log datasets
            sqlUO = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlUO += "SET nFilesFailed=nFilesFailed+:nDiff "
            sqlUO += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to release task
            sqlRT = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET lockedBy=NULL,lockedTime=NULL "
            sqlRT += "WHERE jediTaskID=:jediTaskID AND lockedBy=:lockedBy "
            # lock task
            self.conn.begin()
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap[":lockedBy"] = lockedBy
            self.cur.execute(sqlLK + comment, varMap)
            iRow = self.cur.rowcount
            if iRow == 0:
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                tmpLog.debug("cannot lock")
                return False
            # get datasets
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                varMap[mapKey] = tmpType
            self.cur.execute(sqlAV + comment, varMap)
            resAV = self.cur.fetchall()
            for (datasetID,) in resAV:
                # update files
                varMap = dict()
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":status"] = "ready"
                varMap[":keepTrack"] = 1
                self.cur.execute(sqlFR + comment, varMap)
                nDiff = self.cur.rowcount
                if nDiff > 0:
                    varMap = dict()
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetID
                    varMap[":nDiff"] = nDiff
                    tmpLog.debug(sqlUO + comment + str(varMap))
                    self.cur.execute(sqlUO + comment, varMap)
            # release task
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap[":lockedBy"] = lockedBy
            self.cur.execute(sqlRT + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # check if should enable jumbo
    def toEnableJumbo_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.toEnableJumbo_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jediTaskID} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get thresholds
            sqlLK = f"SELECT value FROM {jedi_config.db.schemaPANDA}.CONFIG "
            sqlLK += "WHERE component=:component AND key=:key AND app=:app "
            # sql to get nevents
            sqlAV = f"SELECT nEvents,nFilesToBeUsed,nFilesUsed FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlAV += "WHERE jediTaskID=:jediTaskID AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlAV += f"{mapKey},"
            sqlAV = sqlAV[:-1]
            sqlAV += ") AND masterID IS NULL "
            # sql to get # of active jumbo jobs
            sqlAJ = "SELECT COUNT(*) "
            sqlAJ += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlAJ += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlAJ += "AND tabT.eventService=:eventService AND tabT.useJumbo IS NOT NULL AND tabT.useJumbo<>:useJumbo "
            sqlAJ += "AND tabT.site IS NULL AND tabT.status IN (:st1,:st2,:st3) "
            # get thresholds
            configMaxJumbo = "AES_MAX_NUM_JUMBO_TASKS"
            varMap = dict()
            varMap[":component"] = "taskrefiner"
            varMap[":app"] = "jedi"
            varMap[":key"] = configMaxJumbo
            self.cur.execute(sqlLK + comment, varMap)
            resLK = self.cur.fetchone()
            if resLK is None:
                tmpLog.debug(f"False since {configMaxJumbo} is not defined")
                return False
            try:
                (maxJumbo,) = resLK
            except Exception:
                tmpLog.debug(f"False since {configMaxJumbo} is not an int")
                return False
            varMap = dict()
            varMap[":component"] = "taskrefiner"
            varMap[":app"] = "jedi"
            varMap[":key"] = "AES_MIN_EVENTS_PER_JUMBO_TASK"
            self.cur.execute(sqlLK + comment, varMap)
            resLK = self.cur.fetchone()
            try:
                (minEvents,) = resLK
                minEvents = int(minEvents)
            except Exception:
                minEvents = 100 * 1000 * 1000
            # get nevents
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                varMap[mapKey] = tmpType
            self.cur.execute(sqlAV + comment, varMap)
            resAV = self.cur.fetchone()
            if resAV is None:
                tmpLog.debug("False since cannot get nEvents")
                return False
            nEvents, nFilesToBeUsed, nFilesUsed = resAV
            try:
                nEvents = nEvents * nFilesToBeUsed // (nFilesToBeUsed - nFilesUsed)
            except Exception:
                tmpLog.debug(f"False since cannot get effective nEvents from nEvents={nEvents} nFilesToBeUsed={nFilesToBeUsed} nFilesUsed={nFilesUsed}")
                return False
            if nEvents < minEvents:
                tmpLog.debug(f"False since effective nEvents={nEvents} < minEventsJumbo={minEvents}")
                return False
            # get num jombo tasks
            varMap = dict()
            varMap[":eventService"] = 1
            varMap[":useJumbo"] = "D"
            varMap[":st1"] = "ready"
            varMap[":st2"] = "pending"
            varMap[":st3"] = "running"
            self.cur.execute(sqlAJ + comment, varMap)
            resAJ = self.cur.fetchone()
            nJumbo = 0
            if resAJ is not None:
                (nJumbo,) = resAJ
            if nJumbo > maxJumbo:
                tmpLog.debug(f"False since nJumbo={nJumbo} > maxJumbo={maxJumbo}")
                return False
            tmpLog.debug("True since nJumbo={0} < maxJumbo={1} and nEvents={0} > minEventsJumbo={1}".format(nJumbo, maxJumbo, nEvents, minEvents))
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # enable jumbo jobs in a task
    def enableJumboInTask_JEDI(self, jediTaskID, eventService, site, useJumbo, splitRule):
        comment = " /* JediDBProxy.enableJumboInTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jediTaskID} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"eventService={eventService} site={site} useJumbo={useJumbo}")
        if eventService == 1 and site is None and useJumbo is None:
            taskSpec = JediTaskSpec()
            taskSpec.splitRule = splitRule
            # go to scouting
            if taskSpec.useScout() and not taskSpec.isPostScout():
                return
            # check if should enable jumbo
            toEnable = self.toEnableJumbo_JEDI(jediTaskID)
            if not toEnable:
                return
            # get nJumbo jobs
            sqlLK = f"SELECT value, type FROM {jedi_config.db.schemaPANDA}.CONFIG "
            sqlLK += "WHERE component=:component AND key=:key AND app=:app "
            varMap = dict()
            varMap[":component"] = "taskrefiner"
            varMap[":app"] = "jedi"
            varMap[":key"] = "AES_NUM_JUMBO_PER_TASK"
            self.cur.execute(sqlLK + comment, varMap)
            resLK = self.cur.fetchone()
            try:
                (nJumboJobs,) = resLK
                nJumboJobs = int(nJumboJobs)
            except Exception:
                nJumboJobs = 1
            # enable jumbo
            # self.enableJumboJobs(jediTaskID, nJumboJobs, False, False)

    # get tasks with jumbo jobs
    def getTaskWithJumbo_JEDI(self, vo, prodSourceLabel):
        comment = " /* JediDBProxy.getTaskWithJumbo_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < vo={vo} label={prodSourceLabel} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to get tasks
            sqlAV = "SELECT t.jediTaskID,t.status,t.splitRule,t.useJumbo,d.nEvents,t.currentPriority,"
            sqlAV += "d.nFiles,d.nFilesFinished,d.nFilesFailed,t.site,d.nEventsUsed "
            sqlAV += "FROM {0}.JEDI_Tasks t,{0}.JEDI_Datasets d ".format(jedi_config.db.schemaJEDI)
            sqlAV += "WHERE t.prodSourceLabel=:prodSourceLabel AND t.vo=:vo AND t.useJumbo IS NOT NULL "
            sqlAV += "AND t.status IN (:s1,:s2,:s3,:s4,:s5) "
            sqlAV += "AND d.jediTaskID=t.jediTaskID AND d.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlAV += f"{mapKey},"
            sqlAV = sqlAV[:-1]
            sqlAV += ") AND d.masterID IS NULL "
            # sql to get event stat info
            sqlFR = "SELECT /*+ INDEX_RS_ASC(c (JEDI_DATASET_CONTENTS.JEDITASKID JEDI_DATASET_CONTENTS.DATASETID JEDI_DATASET_CONTENTS.FILEID)) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) NO_INDEX(tab JEDI_EVENTS_PANDAID_STATUS_IDX)*/ "
            sqlFR += "tab.status,COUNT(*) "
            sqlFR += "FROM {0}.JEDI_Events tab,{0}.JEDI_Dataset_Contents c ".format(jedi_config.db.schemaJEDI)
            sqlFR += "WHERE tab.jediTaskID=:jediTaskID AND c.jediTaskID=tab.jediTaskID AND c.datasetid=tab.datasetID "
            sqlFR += "AND c.fileID=tab.fileID AND c.status<>:status "
            sqlFR += "GROUP BY tab.status "
            # sql to get jumbo jobs
            sqlUO = f"SELECT computingSite,jobStatus FROM {jedi_config.db.schemaPANDA}.jobsDefined4 "
            sqlUO += "WHERE jediTaskID=:jediTaskID AND eventService=:eventService "
            sqlUO += "UNION "
            sqlUO += f"SELECT computingSite,jobStatus FROM {jedi_config.db.schemaPANDA}.jobsActive4 "
            sqlUO += "WHERE jediTaskID=:jediTaskID AND eventService=:eventService "
            sqlUO += "UNION "
            sqlUO += f"SELECT computingSite,jobStatus FROM {jedi_config.db.schemaPANDA}.jobsArchived4 "
            sqlUO += "WHERE jediTaskID=:jediTaskID AND eventService=:eventService "
            sqlUO += "AND modificationTime>CURRENT_DATE-1 "
            self.conn.begin()
            # get tasks
            varMap = dict()
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":s1"] = "running"
            varMap[":s2"] = "pending"
            varMap[":s3"] = "scouting"
            varMap[":s4"] = "ready"
            varMap[":s5"] = "scouted"
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                varMap[mapKey] = tmpType
            self.cur.execute(sqlAV + comment, varMap)
            resAV = self.cur.fetchall()
            tmpLog.debug("got tasks")
            tasksWithJumbo = dict()
            for jediTaskID, taskStatus, splitRule, useJumbo, nEvents, currentPriority, nFiles, nFilesFinished, nFilesFailed, taskSite, nEventsUsed in resAV:
                tasksWithJumbo[jediTaskID] = dict()
                taskData = tasksWithJumbo[jediTaskID]
                taskData["taskStatus"] = taskStatus
                taskData["nEvents"] = nEvents
                taskData["useJumbo"] = useJumbo
                taskData["currentPriority"] = currentPriority
                taskData["site"] = taskSite
                taskSpec = JediTaskSpec()
                taskSpec.useJumbo = useJumbo
                taskSpec.splitRule = splitRule
                taskData["nJumboJobs"] = taskSpec.getNumJumboJobs()
                taskData["maxJumboPerSite"] = taskSpec.getMaxJumboPerSite()
                taskData["nFiles"] = nFiles
                taskData["nFilesDone"] = nFilesFinished + nFilesFailed
                # get event stat info
                varMap = dict()
                varMap[":jediTaskID"] = jediTaskID
                varMap[":status"] = "finished"
                self.cur.execute(sqlFR + comment, varMap)
                resFR = self.cur.fetchall()
                tmpLog.debug(f"got event stat info for jediTaskID={jediTaskID}")
                nEventsDone = nEventsUsed
                nEventsRunning = 0
                for eventStatus, eventCount in resFR:
                    if eventStatus in [EventServiceUtils.ST_done, EventServiceUtils.ST_finished, EventServiceUtils.ST_merged]:
                        nEventsDone += eventCount
                    elif eventStatus in [EventServiceUtils.ST_sent, EventServiceUtils.ST_running]:
                        nEventsRunning += eventCount
                taskData["nEventsDone"] = nEventsDone
                taskData["nEventsRunning"] = nEventsRunning
                # get jumbo jobs
                varMap = dict()
                varMap[":jediTaskID"] = jediTaskID
                varMap[":eventService"] = EventServiceUtils.jumboJobFlagNumber
                self.cur.execute(sqlUO + comment, varMap)
                resUO = self.cur.fetchall()
                tmpLog.debug(f"got jumbo jobs for jediTaskID={jediTaskID}")
                taskData["jumboJobs"] = dict()
                for computingSite, jobStatus in resUO:
                    taskData["jumboJobs"].setdefault(computingSite, dict())
                    taskData["jumboJobs"][computingSite].setdefault(jobStatus, 0)
                    taskData["jumboJobs"][computingSite][jobStatus] += 1
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"done with {str(tasksWithJumbo)}")
            return tasksWithJumbo
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return dict()

    # kick pending tasks with jumbo jobs
    def kickPendingTasksWithJumbo_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.kickPendingTasksWithJumbo_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jediTaskID} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to kick
            sqlAV = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks "
            sqlAV += "SET useJumbo=:useJumboL "
            sqlAV += "WHERE jediTaskID=:jediTaskID AND useJumbo IN (:useJumboP,:useJumboR) "
            sqlAV += "AND status IN (:statusR,:statusP) AND lockedBy IS NULL "
            self.conn.begin()
            # get tasks
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap[":statusP"] = "pending"
            varMap[":statusR"] = "running"
            varMap[":useJumboL"] = JediTaskSpec.enum_useJumbo["lack"]
            varMap[":useJumboP"] = JediTaskSpec.enum_useJumbo["pending"]
            varMap[":useJumboR"] = JediTaskSpec.enum_useJumbo["running"]
            self.cur.execute(sqlAV + comment, varMap)
            nDone = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"kicked with {nDone}")
            return nDone
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # reset input to re-generate co-jumbo jobs
    def resetInputToReGenCoJumbo_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.resetInputToReGenCoJumbo_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jediTaskID} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            nReset = 0
            # sql to get JDI files
            sqlF = "SELECT c.datasetID,c.fileID FROM {0}.JEDI_Datasets d, {0}.JEDI_Dataset_Contents c ".format(jedi_config.db.schemaJEDI)
            sqlF += "WHERE d.jediTaskID=:jediTaskID AND d.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlF += f"{mapKey},"
            sqlF = sqlF[:-1]
            sqlF += ") AND d.masterID IS NULL "
            sqlF += "AND c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID AND c.status=:status "
            # sql to get PandaIDs
            sqlP = f"SELECT PandaID FROM {jedi_config.db.schemaPANDA}.filesTable4 "
            sqlP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileid=:fileID "
            sqlP += "ORDER BY PandaID DESC "
            # sql to check jobs
            sqlJ = f"SELECT 1 FROM {jedi_config.db.schemaPANDA}.jobsWaiting4 WHERE PandaID=:PandaID "
            sqlJ += "UNION "
            sqlJ += f"SELECT 1 FROM {jedi_config.db.schemaPANDA}.jobsDefined4 WHERE PandaID=:PandaID "
            sqlJ += "UNION "
            sqlJ += f"SELECT 1 FROM {jedi_config.db.schemaPANDA}.jobsActive4 WHERE PandaID=:PandaID "
            # sql to get files
            sqlFL = f"SELECT datasetID,fileID FROM {jedi_config.db.schemaPANDA}.filesTable4 WHERE PandaID=:PandaID AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                sqlFL += f"{mapKey},"
            sqlFL = sqlFL[:-1]
            sqlFL += ") "
            # sql to update files
            sqlUF = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents "
            sqlUF += "SET status=:newStatus,proc_status=:proc_status,attemptNr=attemptNr+1,maxAttempt=maxAttempt+1,"
            sqlUF += "maxFailure=(CASE WHEN maxFailure IS NULL THEN NULL ELSE maxFailure+1 END) "
            sqlUF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlUF += "AND status=:oldStatus AND keepTrack=:keepTrack "
            # sql to update datasets
            sqlUD = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Datasets "
            sqlUD += "SET nFilesUsed=nFilesUsed-1 WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            self.conn.begin()
            # get JEDI files
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap[":status"] = "running"
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ":type_" + tmpType
                varMap[mapKey] = tmpType
            self.cur.execute(sqlF + comment, varMap)
            resF = self.cur.fetchall()
            # get PandaIDs
            for datasetID, fileID in resF:
                varMap = dict()
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                self.cur.execute(sqlP + comment, varMap)
                resP = self.cur.fetchall()
                # check jobs
                hasJob = False
                for (PandaID,) in resP:
                    varMap = dict()
                    varMap[":PandaID"] = PandaID
                    self.cur.execute(sqlJ + comment, varMap)
                    resJ = self.cur.fetchone()
                    if resJ is not None:
                        hasJob = True
                        break
                # get files
                if not hasJob:
                    varMap = dict()
                    varMap[":PandaID"] = PandaID
                    for tmpType in JediDatasetSpec.getInputTypes():
                        mapKey = ":type_" + tmpType
                        varMap[mapKey] = tmpType
                    self.cur.execute(sqlFL + comment, varMap)
                    resFL = self.cur.fetchall()
                    # update file
                    for f_datasetID, f_fileID in resFL:
                        varMap = dict()
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = f_datasetID
                        varMap[":fileID"] = f_fileID
                        varMap[":oldStatus"] = "running"
                        varMap[":newStatus"] = "ready"
                        varMap[":proc_status"] = "ready"
                        varMap[":keepTrack"] = 1
                        self.cur.execute(sqlUF + comment, varMap)
                        nRow = self.cur.rowcount
                        tmpLog.debug(f"reset datasetID={f_datasetID} fileID={f_fileID} with {nRow}")
                        if nRow > 0:
                            varMap = dict()
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = f_datasetID
                            self.cur.execute(sqlUD + comment, varMap)
                            nReset += 1
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"done with {nReset}")
            return nReset
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get averaged disk IO
    def getAvgDiskIO_JEDI(self):
        comment = " /* JediDBProxy.getAvgDiskIO_JEDI */"
        method_name = self.getMethodName(comment)
        tmp_log = MsgWrapper(logger, method_name)
        tmp_log.debug("start")
        try:
            # sql
            sql = f"SELECT sum(prorated_diskio_avg * njobs) / sum(njobs), computingSite FROM {jedi_config.db.schemaPANDA}.JOBS_SHARE_STATS "
            sql += "WHERE jobStatus=:jobStatus GROUP BY computingSite "
            var_map = dict()
            var_map[":jobStatus"] = "running"
            # begin transaction
            self.conn.begin()
            self.cur.execute(sql + comment, var_map)
            resFL = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            ret_map = dict()
            for avg, computing_site in resFL:
                if avg:
                    avg = float(avg)
                ret_map[computing_site] = avg
            tmp_log.debug("done")
            return ret_map
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmp_log)
            return {}

    # get nQ/nR ratio
    def get_nq_nr_ratio_JEDI(self, source_label):
        comment = " /* JediDBProxy.get_nq_nr_ratio_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < label={source_label} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retMap = dict()
            # sql
            sql = f"SELECT computingSite,jobStatus,COUNT(*) FROM {jedi_config.db.schemaPANDA}.{{0}} "
            sql += "WHERE prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) "
            sql += "GROUP BY computingSite,jobStatus "
            for tableName, jobStatus1, jobStatus2 in [("jobsDefined4", "defined", "assigned"), ("jobsActive4", "activated, " "running")]:
                varMap = dict()
                varMap[":jobStatus1"] = jobStatus1
                varMap[":jobStatus2"] = jobStatus2
                # begin transaction
                self.conn.begin()
                self.cur.execute(sql.format(tableName) + comment, varMap)
                resFL = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                for computingSite, jobStatus, nJobs in resFL:
                    retMap.setdefault(computingSite, {"nRunning": 0, "nQueue": 0})
                retMap[computingSite] = avg
            tmpLog.debug("done")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return {}

    # update input files stage-in done according to message from idds
    def updateInputFilesStagedAboutIdds_JEDI(self, jeditaskid, scope, filenames_dict):
        comment = " /* JediDBProxy.updateInputFilesStagedAboutIdds_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jeditaskid} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            to_update_files = True
            retVal = 0
            # varMap
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":type1"] = "input"
            varMap[":type2"] = "pseudo_input"
            # sql to get datasetIDs
            sqlGD = f"SELECT datasetID,masterID FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # sql to update file status
            if scope != "pseudo_dataset":
                sqlUF = (
                    "UPDATE {0}.JEDI_Dataset_Contents "
                    "SET status=:new_status "
                    "WHERE jediTaskID=:jediTaskID "
                    "AND status=:old_status "
                    "AND scope=:scope "
                    "AND lfn=:lfn "
                ).format(jedi_config.db.schemaJEDI)
            else:
                sqlUF = (
                    "UPDATE {0}.JEDI_Dataset_Contents "
                    "SET status=:new_status "
                    "WHERE jediTaskID=:jediTaskID "
                    "AND status=:old_status "
                    "AND scope IS NULL "
                    "AND lfn like :lfn "
                ).format(jedi_config.db.schemaJEDI)
            # begin transaction
            self.conn.begin()
            # get datasetIDs from DB if no fileID nor datasetID provided by the message
            tmpLog.debug(f"running sql: {sqlGD} {varMap}")
            self.cur.execute(sqlGD + comment, varMap)
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            if scope != "pseudo_dataset":
                varMap[":scope"] = scope
            varMap[":old_status"] = "staging"
            varMap[":new_status"] = "pending"
            resGD = self.cur.fetchall()
            primaryID = None
            params_key_list = []
            var_map_datasetids = {}
            if len(resGD) > 0:
                for idx, (tmp_datasetID, masterID) in enumerate(resGD):
                    if masterID is None:
                        primaryID = tmp_datasetID
                    key = f":datasetID_{idx}"
                    params_key_list.append(key)
                    var_map_datasetids[key] = tmp_datasetID
            else:
                to_update_files = False
            # set sqls to update file status
            params_key_str = ",".join(params_key_list)
            datesetid_list_str = f"AND datasetID IN ({params_key_str}) "
            sqlUF_without_ID = sqlUF + datesetid_list_str
            sqlUF_with_fileID = sqlUF + "AND fileID=:fileID "
            sqlUF_with_datasetID = sqlUF + "AND datasetID=:datasetID "
            # update files
            if to_update_files:
                # split into groups according to whether with ids
                filenames_dict_with_fileID = {}
                filenames_dict_with_datasetID = {}
                filenames_dict_without_ID = {}
                for filename, (datasetid, fileid) in filenames_dict.items():
                    if fileid is not None:
                        # with fileID from message
                        filenames_dict_with_fileID[filename] = (datasetid, fileid)
                    elif datasetid is not None:
                        # with datasetID from message
                        filenames_dict_with_datasetID[filename] = (datasetid, fileid)
                    else:
                        # without datasetID from message
                        filenames_dict_without_ID[filename] = (datasetid, fileid)
                # loop over files with fileID
                if filenames_dict_with_fileID:
                    varMaps = []
                    for filename, (datasetid, fileid) in filenames_dict_with_fileID.items():
                        tmp_varMap = varMap.copy()
                        if scope != "pseudo_dataset":
                            tmp_varMap[":lfn"] = filename
                        else:
                            tmp_varMap[":lfn"] = "%" + filename
                        tmp_varMap[":fileID"] = fileid
                        varMaps.append(tmp_varMap)
                        tmpLog.debug(f"tmp_varMap: {tmp_varMap}")
                    tmpLog.debug(f"running sql executemany: {sqlUF_with_fileID}")
                    self.cur.executemany(sqlUF_with_fileID + comment, varMaps)
                    retVal += self.cur.rowcount
                # loop over files with datasetID
                if filenames_dict_with_datasetID:
                    varMaps = []
                    for filename, (datasetid, fileid) in filenames_dict_with_datasetID.items():
                        tmp_varMap = varMap.copy()
                        if scope != "pseudo_dataset":
                            tmp_varMap[":lfn"] = filename
                        else:
                            tmp_varMap[":lfn"] = "%" + filename
                        tmp_varMap[":datasetID"] = datasetid
                        varMaps.append(tmp_varMap)
                        tmpLog.debug(f"tmp_varMap: {tmp_varMap}")
                    tmpLog.debug(f"running sql executemany: {sqlUF_with_datasetID}")
                    self.cur.executemany(sqlUF_with_datasetID + comment, varMaps)
                    retVal += self.cur.rowcount
                # loop over files without ID
                if filenames_dict_without_ID:
                    varMaps = []
                    for filename, (datasetid, fileid) in filenames_dict_without_ID.items():
                        tmp_varMap = varMap.copy()
                        if scope != "pseudo_dataset":
                            tmp_varMap[":lfn"] = filename
                        else:
                            tmp_varMap[":lfn"] = "%" + filename
                        tmp_varMap.update(var_map_datasetids)
                        varMaps.append(tmp_varMap)
                        tmpLog.debug(f"tmp_varMap: {tmp_varMap}")
                    tmpLog.debug(f"running sql executemany: {sqlUF_without_ID}")
                    self.cur.executemany(sqlUF_without_ID + comment, varMaps)
                    retVal += self.cur.rowcount
            # update associated files
            if primaryID is not None:
                self.fix_associated_files_in_staging(jeditaskid, primary_id=primaryID)
            # update task to trigger CF immediately
            if retVal:
                sqlUT = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET modificationTime=CURRENT_DATE-1 WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL "
                varMap = dict()
                varMap[":jediTaskID"] = jeditaskid
                self.cur.execute(sqlUT + comment, varMap)
                tmpLog.debug(f"unlocked task with {self.cur.rowcount}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"updated {retVal} files")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # fix associated files in staging
    def fix_associated_files_in_staging(self, jeditaskid, primary_id=None, secondary_id=None):
        comment = " /* JediDBProxy.fix_associated_files_in_staging */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jeditaskid} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # get primary dataset
        if primary_id is None:
            sqlGD = f"SELECT datasetID FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND type=:type AND masterID IS NULL "
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":type"] = "input"
            self.cur.execute(sqlGD + comment, varMap)
            resGD = self.cur.fetchone()
            if resGD is None:
                return
            (primary_id,) = resGD
        # get secondary dataset
        if secondary_id is not None:
            secondary_id_list = [secondary_id]
        else:
            sqlGS = f"SELECT datasetID FROM {jedi_config.db.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND type=:type AND masterID IS NOT NULL "
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":type"] = "pseudo_input"
            self.cur.execute(sqlGS + comment, varMap)
            resGDA = self.cur.fetchall()
            secondary_id_list = [tmpID for tmpID, in resGDA]
        if len(secondary_id_list) == 0:
            return
        # get primary files
        sqlGP = f"SELECT status FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents  WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID ORDER BY lfn "
        varMap = dict()
        varMap[":jediTaskID"] = jeditaskid
        varMap[":datasetID"] = primary_id
        self.cur.execute(sqlGP + comment, varMap)
        resFP = self.cur.fetchall()
        primaryList = [status for status, in resFP]
        # sql to get secondary files
        sqlGS = ("SELECT fileID,status FROM {0}.JEDI_Dataset_Contents " " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID " "ORDER BY fileID ").format(
            jedi_config.db.schemaJEDI
        )
        # sql to update files
        sqlUS = (
            "UPDATE {0}.JEDI_Dataset_Contents "
            "SET status=:new_status "
            "WHERE jediTaskID=:jediTaskID "
            "AND datasetID=:datasetID "
            "AND fileID=:fileID "
            "AND status=:old_status "
        ).format(jedi_config.db.schemaJEDI)
        # sql to update dataset
        sqlUD = (
            "UPDATE {0}.JEDI_Datasets "
            "SET nFilesToBeUsed="
            "(SELECT COUNT(*) FROM {0}.JEDI_Dataset_Contents "
            "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status<>:status) "
            "WHERE jediTaskID=:jediTaskID "
            "AND datasetID=:datasetID "
        ).format(jedi_config.db.schemaJEDI)
        # loop over secondary datasets
        for secondaryID in secondary_id_list:
            # get secondary files
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":datasetID"] = secondaryID
            self.cur.execute(sqlGS + comment, varMap)
            resFS = self.cur.fetchall()
            # check files
            n = 0
            for priStatus, (secFileID, secStatus) in zip(primaryList, resFS):
                if priStatus != "staging" and secStatus == "staging":
                    # update files
                    varMap = dict()
                    varMap[":jediTaskID"] = jeditaskid
                    varMap[":datasetID"] = secondaryID
                    varMap[":fileID"] = secFileID
                    varMap[":old_status"] = "staging"
                    varMap[":new_status"] = "ready"
                    self.cur.execute(sqlUS + comment, varMap)
                    n += self.cur.rowcount
            # update dataset
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":datasetID"] = secondaryID
            varMap[":status"] = "staging"
            self.cur.execute(sqlUD + comment, varMap)
            tmpLog.debug(f"updated {n} files for datasetID={secondaryID}")

    # update input datasets stage-in done according to message from idds
    def updateInputDatasetsStagedAboutIdds_JEDI(self, jeditaskid, scope, dsnames_dict=None, use_commit=True):
        comment = " /* JediDBProxy.updateInputDatasetsStagedAboutIdds_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jeditaskid} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # update all files when scope is None
            if scope is None:
                dsnames_dict = [None]
            retVal = 0
            # varMap
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":type1"] = "input"
            varMap[":type2"] = "pseudo_input"
            varMap[":old_status"] = "staging"
            varMap[":new_status"] = "pending"
            # sql with dataset name
            sqlUD = (
                "UPDATE {0}.JEDI_Dataset_Contents "
                "SET status=:new_status "
                "WHERE jediTaskID=:jediTaskID "
                "AND datasetID IN ("
                "SELECT datasetID FROM {0}.JEDI_Datasets "
                "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) AND datasetName=:datasetName) "
                "AND status=:old_status "
            ).format(jedi_config.db.schemaJEDI)
            # sql without dataset name
            sql_wo_dataset_name = (
                "UPDATE {0}.JEDI_Dataset_Contents "
                "SET status=:new_status "
                "WHERE jediTaskID=:jediTaskID "
                "AND datasetID IN ("
                "SELECT datasetID FROM {0}.JEDI_Datasets "
                "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2)) "
                "AND status=:old_status "
            ).format(jedi_config.db.schemaJEDI)
            # begin transaction
            if use_commit:
                self.conn.begin()
            for dsname in dsnames_dict:
                if scope:
                    varMap[":datasetName"] = f"{scope}:{dsname}"
                    sql = sqlUD
                else:
                    sql = sql_wo_dataset_name
                tmpLog.debug(f"running sql: {sql} {str(varMap)}")
                self.cur.execute(sql + comment, varMap)
                retVal += self.cur.rowcount
            self.fix_associated_files_in_staging(jeditaskid)
            # update task to trigger CF immediately
            if retVal:
                sqlUT = f"UPDATE {jedi_config.db.schemaJEDI}.JEDI_Tasks SET modificationTime=CURRENT_DATE-1 WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL "
                varMap = dict()
                varMap[":jediTaskID"] = jeditaskid
                self.cur.execute(sqlUT + comment, varMap)
                tmpLog.debug(f"unlocked task with {self.cur.rowcount}")
            # commit
            if use_commit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmpLog.debug(f"updated {retVal} files")
            return retVal
        except Exception:
            if use_commit:
                # roll back
                self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get number of staging files
    def getNumStagingFiles_JEDI(self, jeditaskid):
        comment = " /* JediDBProxy.getNumStagingFiles_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jeditaskid} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retVal = 0
            # varMap
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":type1"] = "input"
            varMap[":type2"] = "pseudo_input"
            varMap[":status"] = "staging"
            # sql
            sqlNS = (
                "SELECT COUNT(*) FROM {0}.JEDI_Datasets d, {0}.JEDI_Dataset_Contents c "
                "WHERE d.jediTaskID=:jediTaskID AND d.type IN (:type1,:type2) "
                "AND c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID "
                "AND c.status=:status "
            ).format(jedi_config.db.schemaJEDI)
            # begin transaction
            self.conn.begin()
            self.cur.execute(sqlNS + comment, varMap)
            (retVal,) = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"got {retVal} staging files")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # task status logging
    def record_task_status_change(self, jedi_task_id):
        comment = " /* JediDBProxy.record_task_status_change */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jedi_task_id} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        varMap = dict()
        varMap[":jediTaskID"] = jedi_task_id
        varMap[":modificationHost"] = socket.getfqdn()
        # sql
        sqlNS = (
            "INSERT INTO {0}.TASKS_STATUSLOG "
            "(jediTaskID,modificationTime,status,modificationHost,attemptNr,reason) "
            "SELECT jediTaskID,CURRENT_TIMESTAMP,status,:modificationHost,attemptNr,"
            "SUBSTR(errorDialog,0,255) "
            "FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
        ).format(jedi_config.db.schemaJEDI)
        self.cur.execute(sqlNS + comment, varMap)
        tmpLog.debug("done")

    # push task status message
    def push_task_status_message(self, task_spec, jedi_task_id, status, split_rule=None):
        to_push = False
        if task_spec is not None:
            to_push = task_spec.push_status_changes()
        elif split_rule is not None:
            to_push = push_status_changes(split_rule)
        # only run if to push status change
        if not to_push:
            return
        # skip statuses unnecessary to push
        # if status in ['pending']:
        #     return
        comment = " /* JediDBProxy.push_task_status_message */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jedi_task_id} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # send task status messages to mq
        try:
            now_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            now_ts = int(now_time.timestamp())
            msg_dict = {
                "msg_type": "task_status",
                "taskid": jedi_task_id,
                "status": status,
                "timestamp": now_ts,
            }
            msg = json.dumps(msg_dict)
            if self.jedi_mb_proxy_dict is None:
                self.jedi_mb_proxy_dict = get_mb_proxy_dict()
                if self.jedi_mb_proxy_dict is None:
                    tmpLog.debug("Failed to get mb_proxy of internal MQs. Skipped ")
                    return
            try:
                mb_proxy = self.jedi_mb_proxy_dict["out"]["jedi_jobtaskstatus"]
            except KeyError as e:
                tmpLog.warning(f"Skipped due to {e} ; jedi_mb_proxy_dict is {self.jedi_mb_proxy_dict}")
                return
            if mb_proxy.got_disconnected:
                mb_proxy.restart()
            mb_proxy.send(msg)
        except Exception:
            self.dumpErrorMessage(tmpLog)
        tmpLog.debug("done")

    # push message to message processors which triggers functions of agents
    def push_task_trigger_message(self, msg_type, jedi_task_id, data_dict=None, priority=None):
        comment = " /* JediDBProxy.push_task_trigger_message */"
        methodName = self.getMethodName(comment)
        methodName += f" < msg_type={msg_type} jediTaskID={jedi_task_id} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # send task status messages to mq
        try:
            now_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            now_ts = int(now_time.timestamp())
            msg_dict = {}
            if data_dict:
                msg_dict.update(data_dict)
            msg_dict.update(
                {
                    "msg_type": msg_type,
                    "taskid": jedi_task_id,
                    "timestamp": now_ts,
                }
            )
            msg = json.dumps(msg_dict)
            if self.jedi_mb_proxy_dict is None:
                self.jedi_mb_proxy_dict = get_mb_proxy_dict()
                if self.jedi_mb_proxy_dict is None:
                    tmpLog.debug("Failed to get mb_proxy of internal MQs. Skipped ")
                    return
            try:
                mq_name = msg_type
                mb_proxy = self.jedi_mb_proxy_dict["out"][mq_name]
            except KeyError as e:
                tmpLog.warning(f"Skipped due to {e} ; jedi_mb_proxy_dict is {self.jedi_mb_proxy_dict}")
                return
            if mb_proxy.got_disconnected:
                mb_proxy.restart()
            if priority:
                mb_proxy.send(msg, priority=priority)
            else:
                mb_proxy.send(msg)
        except Exception:
            self.dumpErrorMessage(tmpLog)
            return
        tmpLog.debug("done")
        return True

    # task attempt start logging
    def log_task_attempt_start(self, jedi_task_id):
        comment = " /* JediDBProxy.log_task_attempt_start */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jedi_task_id} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # sql
        sqlGLTA = f"SELECT MAX(attemptnr) FROM {jedi_config.db.schemaJEDI}.TASK_ATTEMPTS WHERE jediTaskID=:jediTaskID "
        sqlELTA = (
            "UPDATE {0}.TASK_ATTEMPTS "
            "SET (endtime, endstatus) = ( "
            "SELECT CURRENT_DATE,status "
            "FROM {0}.JEDI_Tasks "
            "WHERE jediTaskID=:jediTaskID "
            ") "
            "WHERE jediTaskID=:jediTaskID "
            "AND attemptnr=:last_attemptnr "
            "AND endtime IS NULL "
        ).format(jedi_config.db.schemaJEDI)
        sqlITA = (
            "INSERT INTO {0}.TASK_ATTEMPTS "
            "(jeditaskid, attemptnr, starttime, startstatus) "
            "SELECT jediTaskID, GREATEST(:grandAttemptNr, COALESCE(attemptNr, 0)), CURRENT_DATE, status "
            "FROM {0}.JEDI_Tasks "
            "WHERE jediTaskID=:jediTaskID "
        ).format(jedi_config.db.schemaJEDI)
        # get grand attempt number
        varMap = dict()
        varMap[":jediTaskID"] = jedi_task_id
        self.cur.execute(sqlGLTA + comment, varMap)
        (last_attemptnr,) = self.cur.fetchone()
        grand_attemptnr = 0
        if last_attemptnr is not None:
            grand_attemptnr = last_attemptnr + 1
            # end last attempt in case log_task_attempt_end is not called
            varMap = dict()
            varMap[":jediTaskID"] = jedi_task_id
            varMap[":last_attemptnr"] = last_attemptnr
            self.cur.execute(sqlELTA + comment, varMap)
        varMap = dict()
        varMap[":jediTaskID"] = jedi_task_id
        varMap[":grandAttemptNr"] = grand_attemptnr
        # insert task attempt
        self.cur.execute(sqlITA + comment, varMap)
        tmpLog.debug("done")

    # task attempt end logging
    def log_task_attempt_end(self, jedi_task_id):
        comment = " /* JediDBProxy.log_task_attempt_end */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jedi_task_id} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        varMap = dict()
        varMap[":jediTaskID"] = jedi_task_id
        # sql
        sqlUTA = (
            "UPDATE {0}.TASK_ATTEMPTS "
            "SET (endtime, endstatus) = ( "
            "SELECT CURRENT_DATE,status "
            "FROM {0}.JEDI_Tasks "
            "WHERE jediTaskID=:jediTaskID "
            ") "
            "WHERE jediTaskID=:jediTaskID "
            "AND endtime IS NULL "
        ).format(jedi_config.db.schemaJEDI)
        self.cur.execute(sqlUTA + comment, varMap)
        tmpLog.debug("done")

    # task progress
    def get_task_progress(self, jedi_task_id):
        comment = " /* JediDBProxy.get_task_progress */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jedi_task_id} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            varMap = dict()
            varMap[":jediTaskID"] = jedi_task_id
            # sql
            sqlT = (
                "SELECT d.jediTaskID,SUM(d.nFiles),SUM(d.nFilesFinished),SUM(d.nFilesFailed) "
                "FROM {0}.JEDI_Datasets d "
                "WHERE d.jediTaskID= AND d.type in ('input', 'pseudo_input') "
            ).format(jedi_config.db.schemaJEDI)
            self.cur.execute(sqlT + comment, varMap)
            # result
            jediTaskID, nFiles, nFilesFinished, nFilesFailed = self.cur.fetchone()
            n_files_processed = nFilesFinished + nFilesFailed
            n_files_remaining = nFiles - n_files_processed
            ret_dict = {
                "jediTaskID": jediTaskID,
                "nFiles": nFiles,
                "nFilesFinished": nFilesFinished,
                "nFilesFailed": nFilesFailed,
                "n_files_remaining": n_files_remaining,
                "finished_ratio": nFilesFinished / nFiles if nFiles > 0 else 0,
                "failed_ratio": nFilesFailed / nFiles if nFiles > 0 else 0,
                "remaining_ratio": n_files_remaining / nFiles if nFiles > 0 else 0,
                "progress": n_files_processed / nFiles if nFiles > 0 else 0,
            }

            # return
            tmpLog.debug("done")
            return ret_dict
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get usage breakdown by users and sites
    def getUsageBreakdown_JEDI(self, prod_source_label="user"):
        comment = " /* JediDBProxy.getUsageBreakdown_JEDI */"
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # get usage breakdown
            usageBreakDownPerUser = {}
            usageBreakDownPerSite = {}
            for table in ["jobsActive4", "jobsArchived4"]:
                varMap = {}
                varMap[":prodSourceLabel"] = prod_source_label
                varMap[":pmerge"] = "pmerge"
                if table == "ATLAS_PANDA.jobsActive4":
                    sqlJ = (
                        "SELECT COUNT(*),prodUserName,jobStatus,workingGroup,computingSite,coreCount "
                        "FROM {0}.{1} "
                        "WHERE prodSourceLabel=:prodSourceLabel AND processingType<>:pmerge "
                        "GROUP BY prodUserName,jobStatus,workingGroup,computingSite,coreCount "
                    ).format(jedi_config.db.schemaPANDA, table)
                else:
                    # with time range for archived table
                    varMap[":modificationTime"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=60)
                    sqlJ = (
                        "SELECT COUNT(*),prodUserName,jobStatus,workingGroup,computingSite,coreCount "
                        "FROM {0}.{1} "
                        "WHERE prodSourceLabel=:prodSourceLabel AND processingType<>:pmerge AND modificationTime>:modificationTime "
                        "GROUP BY prodUserName,jobStatus,workingGroup,computingSite,coreCount "
                    ).format(jedi_config.db.schemaPANDA, table)
                # exec
                tmpLog.debug(sqlJ + comment + str(varMap))
                self.cur.execute(sqlJ + comment, varMap)
                # result
                res = self.cur.fetchall()
                if res is None:
                    tmpLog.debug(f"total {res} ")
                else:
                    tmpLog.debug(f"total {len(res)} ")
                    # make map
                    for cnt, prodUserName, jobStatus, workingGroup, computingSite, coreCount in res:
                        if coreCount is None:
                            coreCount = 1
                        # append to PerUser map
                        usageBreakDownPerUser.setdefault(prodUserName, {})
                        usageBreakDownPerUser[prodUserName].setdefault(workingGroup, {})
                        usageBreakDownPerUser[prodUserName][workingGroup].setdefault(computingSite, {"rundone": 0, "activated": 0, "running": 0, "runcores": 0})
                        # append to PerSite map
                        usageBreakDownPerSite.setdefault(computingSite, {})
                        usageBreakDownPerSite[computingSite].setdefault(prodUserName, {})
                        usageBreakDownPerSite[computingSite][prodUserName].setdefault(workingGroup, {"rundone": 0, "activated": 0})
                        # count # of running/done and activated
                        if jobStatus in ["activated"]:
                            usageBreakDownPerUser[prodUserName][workingGroup][computingSite]["activated"] += cnt
                            usageBreakDownPerSite[computingSite][prodUserName][workingGroup]["activated"] += cnt
                        elif jobStatus in ["cancelled", "holding"]:
                            pass
                        else:
                            if jobStatus in ["running", "starting", "sent"]:
                                usageBreakDownPerUser[prodUserName][workingGroup][computingSite]["running"] += cnt
                                usageBreakDownPerUser[prodUserName][workingGroup][computingSite]["runcores"] += cnt * coreCount
                            usageBreakDownPerUser[prodUserName][workingGroup][computingSite]["rundone"] += cnt
                            usageBreakDownPerSite[computingSite][prodUserName][workingGroup]["rundone"] += cnt
            # return
            tmpLog.debug("done")
            return usageBreakDownPerUser, usageBreakDownPerSite
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get jobs stat of each user
    def getUsersJobsStats_JEDI(self, prod_source_label="user"):
        comment = " /* JediDBProxy.getUsersJobsStats_JEDI */"
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # get users jobs stats
            jobsStatsPerUser = {}
            varMap = {}
            varMap[":prodSourceLabel"] = prod_source_label
            varMap[":pmerge"] = "pmerge"
            sqlJ = (
                "SELECT COUNT(*),prodUserName,jobStatus,gshare,computingSite "
                "FROM {0}.{1} "
                "WHERE prodSourceLabel=:prodSourceLabel AND processingType<>:pmerge "
                "GROUP BY prodUserName,jobStatus,gshare,computingSite "
            ).format(jedi_config.db.schemaPANDA, "jobsActive4")
            # exec
            tmpLog.debug(sqlJ + comment + str(varMap))
            self.cur.execute(sqlJ + comment, varMap)
            # result
            res = self.cur.fetchall()
            if res is None:
                tmpLog.debug(f"total {res} ")
            else:
                tmpLog.debug(f"total {len(res)} ")
                # make map
                for cnt, prodUserName, jobStatus, gshare, computingSite in res:
                    # append to PerUser map
                    jobsStatsPerUser.setdefault(computingSite, {})
                    jobsStatsPerUser[computingSite].setdefault(gshare, {})
                    jobsStatsPerUser[computingSite][gshare].setdefault(
                        prodUserName, {"nDefined": 0, "nAssigned": 0, "nActivated": 0, "nStarting": 0, "nQueue": 0, "nRunning": 0}
                    )
                    jobsStatsPerUser[computingSite][gshare].setdefault(
                        "_total", {"nDefined": 0, "nAssigned": 0, "nActivated": 0, "nStarting": 0, "nQueue": 0, "nRunning": 0}
                    )
                    # count # of running/done and activated
                    if jobStatus in ["defined", "assigned", "activated", "starting"]:
                        status_name = f"n{jobStatus.capitalize()}"
                        jobsStatsPerUser[computingSite][gshare][prodUserName][status_name] += cnt
                        jobsStatsPerUser[computingSite][gshare][prodUserName]["nQueue"] += cnt
                        jobsStatsPerUser[computingSite][gshare]["_total"][status_name] += cnt
                        jobsStatsPerUser[computingSite][gshare]["_total"]["nQueue"] += cnt
                    elif jobStatus in ["running"]:
                        jobsStatsPerUser[computingSite][gshare][prodUserName]["nRunning"] += cnt
                        jobsStatsPerUser[computingSite][gshare]["_total"]["nRunning"] += cnt
            # return
            tmpLog.debug("done")
            return jobsStatsPerUser
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # add events
    def add_events_jedi(self, jedi_task_id, start_number, end_number, max_attempt):
        comment = " /* JediDBProxy.add_events_jedi */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jedi_task_id} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start start={start_number} end={end_number}")
        varMap = dict()
        varMap[":jediTaskID"] = jedi_task_id
        varMap[":modificationHost"] = socket.getfqdn()
        # sql
        sqlJediEvent = (
            "INSERT INTO {0}.JEDI_Events "
            "(jediTaskID,datasetID,PandaID,fileID,attemptNr,status,"
            "job_processID,def_min_eventID,def_max_eventID,processed_upto_eventID,"
            "event_offset) "
            "VALUES(:jediTaskID,:datasetID,:pandaID,:fileID,:attemptNr,:eventStatus,"
            ":startEvent,:startEvent,:lastEvent,:processedEvent,"
            ":eventOffset) "
        ).format(jedi_config.db.schemaJEDI)
        varMaps = []
        i = start_number
        while i <= end_number:
            varMap = dict()
            varMap[":jediTaskID"] = jedi_task_id
            varMap[":datasetID"] = 0
            varMap[":pandaID"] = 0
            varMap[":fileID"] = 0
            varMap[":attemptNr"] = max_attempt
            varMap[":eventStatus"] = EventServiceUtils.ST_ready
            varMap[":processedEvent"] = 0
            varMap[":startEvent"] = i
            varMap[":lastEvent"] = i
            varMap[":eventOffset"] = 0
            varMaps.append(varMap)
            i += 1
        try:
            self.conn.begin()
            self.cur.executemany(sqlJediEvent + comment, varMaps)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"added {end_number - start_number + 1} events")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # insert HPO pseudo event according to message from idds
    def insertHpoEventAboutIdds_JEDI(self, jedi_task_id, event_id_list):
        comment = " /* JediDBProxy.insertHpoEventAboutIdds_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jedi_task_id} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start event_id_list={event_id_list}")
        varMap = dict()
        varMap[":jediTaskID"] = jedi_task_id
        varMap[":modificationHost"] = socket.getfqdn()
        # sql
        sqlJediEvent = (
            "INSERT INTO {0}.JEDI_Events "
            "(jediTaskID,datasetID,PandaID,fileID,attemptNr,status,"
            "job_processID,def_min_eventID,def_max_eventID,processed_upto_eventID,"
            "event_offset) "
            "VALUES(:jediTaskID,"
            "(SELECT datasetID FROM {0}.JEDI_Datasets "
            "WHERE jediTaskID=:jediTaskID AND type=:type AND masterID IS NULL AND containerName LIKE :cont),"
            ":pandaID,:fileID,:attemptNr,:eventStatus,"
            ":startEvent,:startEvent,:lastEvent,:processedEvent,"
            ":eventOffset) "
        ).format(jedi_config.db.schemaJEDI)
        varMaps = []
        n_events = 0
        for event_id, model_id in event_id_list:
            varMap = dict()
            varMap[":jediTaskID"] = jedi_task_id
            varMap[":type"] = "pseudo_input"
            varMap[":pandaID"] = 0
            varMap[":fileID"] = 0
            varMap[":attemptNr"] = 5
            varMap[":eventStatus"] = EventServiceUtils.ST_ready
            varMap[":processedEvent"] = 0
            varMap[":startEvent"] = event_id
            varMap[":lastEvent"] = event_id
            varMap[":eventOffset"] = 0
            varMap[":cont"] = f"%/{model_id}"
            varMaps.append(varMap)
            n_events += 1
        try:
            self.conn.begin()
            self.cur.executemany(sqlJediEvent + comment, varMaps)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"added {n_events} events")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

    # get event statistics
    def get_event_statistics(self, jedi_task_id):
        comment = " /* JediDBProxy.get_event_statistics */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jedi_task_id} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            self.conn.begin()
            varMap = dict()
            varMap[":jediTaskID"] = jedi_task_id
            # sql
            sqlGNE = f"SELECT status,COUNT(*) FROM {jedi_config.db.schemaJEDI}.JEDI_Events WHERE jediTaskID=:jediTaskID GROUP BY status "
            self.cur.execute(sqlGNE + comment, varMap)
            # result
            ret_dict = dict()
            res = self.cur.fetchall()
            for s, c in res:
                ret_dict[s] = c
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"got {str(ret_dict)}")
            return ret_dict
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get site to-running rate statistics
    def getSiteToRunRateStats(self, vo, exclude_rwq, starttime_min, starttime_max):
        """
        :param vo: Virtual Organization
        :param exclude_rwq: True/False. Indicates whether we want to indicate special workqueues from the statistics
        :param time_window: float, time window in hours to compute to-running rate
        """
        comment = " /* DBProxy.getSiteToRunRateStats */"
        methodName = self.getMethodName(comment)
        methodName += f" < vo={vo} >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        # interval in hours
        real_interval_hours = (starttime_max - starttime_min).total_seconds() / 3600
        # define the var map of query parameters
        var_map = {":vo": vo, ":startTimeMin": starttime_min, ":startTimeMax": starttime_max}
        # sql to query on jobs-tables (jobsactive4 and jobsdefined4)
        sql_jt = """
               SELECT computingSite, COUNT(*) FROM %s
               WHERE vo=:vo
               AND startTime IS NOT NULL AND startTime>=:startTimeMin AND startTime<:startTimeMax
               AND jobStatus IN ('running', 'holding', 'transferring', 'finished', 'cancelled')
               """
        if exclude_rwq:
            sql_jt += f"""
               AND workqueue_id NOT IN
               (SELECT queue_id FROM {jedi_config.db.schemaPANDA}.jedi_work_queue WHERE queue_function = 'Resource')
               """
        sql_jt += """
               GROUP BY computingSite
               """
        # job tables
        tables = [f"{jedi_config.db.schemaPANDA}.jobsActive4", f"{jedi_config.db.schemaPANDA}.jobsDefined4"]
        # get
        return_map = {}
        try:
            for table in tables:
                self.cur.arraysize = 10000
                sql_exe = (sql_jt + comment) % table
                self.cur.execute(sql_exe, var_map)
                res = self.cur.fetchall()
                # create map
                for panda_site, n_count in res:
                    # add site
                    return_map.setdefault(panda_site, 0)
                    # increase to-running rate
                    to_running_rate = n_count / real_interval_hours if real_interval_hours > 0 else 0
                    return_map[panda_site] += to_running_rate
            # end loop
            tmpLog.debug("done")
            return True, return_map
        except Exception:
            self.dumpErrorMessage(tmpLog)
            return False, {}

    # update cache
    def updateCache_JEDI(self, main_key, sub_key, data):
        comment = " /* JediDBProxy.updateCache_JEDI */"
        methodName = self.getMethodName(comment)
        # defaults
        if sub_key is None:
            sub_key = "default"
        # last update time
        last_update = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        last_update_str = last_update.strftime("%Y-%m-%d_%H:%M:%S")
        methodName += f" <main_key={main_key} sub_key={sub_key} last_update={last_update_str}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retVal = False
            # sql to check
            sqlC = f"SELECT last_update FROM {jedi_config.db.schemaJEDI}.Cache WHERE main_key=:main_key AND sub_key=:sub_key "
            # sql to insert
            sqlI = f"INSERT INTO {jedi_config.db.schemaJEDI}.Cache ({JediCacheSpec.columnNames()}) {JediCacheSpec.bindValuesExpression()} "
            # sql to update
            sqlU = f"UPDATE {jedi_config.db.schemaJEDI}.Cache SET {JediCacheSpec.bindUpdateChangesExpression()} WHERE main_key=:main_key AND sub_key=:sub_key "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":main_key"] = main_key
            varMap[":sub_key"] = sub_key
            self.cur.execute(sqlC + comment, varMap)
            resC = self.cur.fetchone()
            varMap[":data"] = data
            varMap[":last_update"] = last_update
            if resC is None:
                # insert if missing
                tmpLog.debug("insert")
                self.cur.execute(sqlI + comment, varMap)
            else:
                # update
                tmpLog.debug("update")
                self.cur.execute(sqlU + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            retVal = True
            tmpLog.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal

    # get cache
    def getCache_JEDI(self, main_key, sub_key):
        comment = " /* JediDBProxy.getCache_JEDI */"
        methodName = self.getMethodName(comment)
        # defaults
        if sub_key is None:
            sub_key = "default"
        methodName += f" <main_key={main_key} sub_key={sub_key}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retVal = False
            # sql to get
            sqlC = f"SELECT {JediCacheSpec.columnNames()} FROM {jedi_config.db.schemaJEDI}.Cache WHERE main_key=:main_key AND sub_key=:sub_key "
            # check
            varMap = {}
            varMap[":main_key"] = main_key
            varMap[":sub_key"] = sub_key
            self.cur.execute(sqlC + comment, varMap)
            resC = self.cur.fetchone()
            if resC is None:
                tmpLog.debug("got nothing, skipped")
                return None
            cache_spec = JediCacheSpec()
            cache_spec.pack(resC)
            tmpLog.debug("got cache, done")
            # return
            return cache_spec
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)

    # extend lifetime of sandbox file
    def extendSandboxLifetime_JEDI(self, jedi_taskid, file_name):
        comment = " /* JediDBProxy.extendSandboxLifetime_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jedi_taskid} >"
        tmpLog = MsgWrapper(logger, methodName)
        try:
            self.conn.begin()
            retVal = False
            # sql to update
            sqlC = f"UPDATE {jedi_config.db.schemaMETA}.userCacheUsage SET creationTime=CURRENT_DATE WHERE fileName=:fileName "
            varMap = {}
            varMap[":fileName"] = file_name
            self.cur.execute(sqlC + comment, varMap)
            nRows = self.cur.rowcount
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done {file_name} with {nRows}")
            # return
            return nRows
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # turn a task into pending status for some reason
    def makeTaskPending_JEDI(self, jedi_taskid, reason):
        comment = " /* JediDBProxy.makeTaskPending_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jedi_taskid} >"
        tmpLog = MsgWrapper(logger, methodName)
        try:
            self.conn.begin()
            retVal = False
            # sql to put the task in pending
            sqlPDG = (
                "UPDATE {0}.JEDI_Tasks "
                "SET lockedBy=NULL, lockedTime=NULL, "
                "status=:status, errorDialog=:err, "
                "modificationtime=CURRENT_DATE, oldStatus=status "
                "WHERE jediTaskID=:jediTaskID "
                "AND status IN ('ready','running','scouting') "
                "AND lockedBy IS NULL "
            ).format(jedi_config.db.schemaJEDI)
            varMap = {}
            varMap[":jediTaskID"] = jedi_taskid
            varMap[":err"] = reason
            varMap[":status"] = "pending"
            self.cur.execute(sqlPDG + comment, varMap)
            nRows = self.cur.rowcount
            # add missing record_task_status_change and push_task_status_message updates
            self.record_task_status_change(jedi_taskid)
            self.push_task_status_message(None, jedi_taskid, varMap[":status"])
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {nRows} rows")
            # return
            return nRows
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # query tasks and turn them into pending status for some reason, sql_query should query jeditaskid
    def queryTasksToBePending_JEDI(self, sql_query, params_map, reason):
        comment = " /* JediDBProxy.queryTasksToBePending_JEDI */"
        methodName = self.getMethodName(comment)
        # methodName += " < sql={0} >".format(sql_query)
        tmpLog = MsgWrapper(logger, methodName)
        try:
            self.conn.begin()
            # sql to query
            self.cur.execute(sql_query + comment, params_map)
            taskIDs = self.cur.fetchall()
            # sql to put the task in pending
            sqlPDG = (
                "UPDATE {0}.JEDI_Tasks "
                "SET lockedBy=NULL, lockedTime=NULL, "
                "status=:status, errorDialog=:err, "
                "modificationtime=CURRENT_DATE, oldStatus=status "
                "WHERE jediTaskID=:jediTaskID "
                "AND status IN ('ready','running','scouting') "
                "AND lockedBy IS NULL "
            ).format(jedi_config.db.schemaJEDI)
            # loop over tasks
            n_updated = 0
            for (jedi_taskid,) in taskIDs:
                varMap = {}
                varMap[":jediTaskID"] = jedi_taskid
                varMap[":err"] = reason
                varMap[":status"] = "pending"
                self.cur.execute(sqlPDG + comment, varMap)
                nRow = self.cur.rowcount
                if nRow == 1:
                    self.record_task_status_change(jedi_taskid)
                    self.push_task_status_message(None, jedi_taskid, varMap[":status"])
                    n_updated += 1
                    tmpLog.debug(f"made pending jediTaskID={jedi_taskid}")
                elif nRow > 1:
                    tmpLog.error(f"updated {nRow} rows with same jediTaskID={jedi_taskid}")
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {n_updated} rows")
            # return
            return n_updated
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # get datasets of input and lib, to update data locality records
    def get_tasks_inputdatasets_JEDI(self, vo):
        comment = " /* JediDBProxy.get_tasks_inputdatasets_JEDI */"
        methodName = self.getMethodName(comment)
        # last update time
        methodName += f" <vo={vo}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        now_ts = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        try:
            retVal = None
            # sql to get all jediTaskID and datasetID of input
            sql = (
                "SELECT tabT.jediTaskID,datasetID, tabD.datasetName "
                "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA "
                "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
                "AND tabT.vo=:vo AND tabT.status IN ('running', 'ready', 'scouting', 'pending') "
                "AND tabT.prodSourceLabel='managed' "
                "AND tabD.type IN ('input') AND tabD.masterID IS NULL "
            ).format(jedi_config.db.schemaJEDI)
            # start transaction
            self.conn.begin()
            # get
            varMap = {}
            varMap[":vo"] = vo
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchall()
            nRows = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            retVal = res
            tmpLog.debug(f"done with {nRows} rows")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal

    # update dataset locality
    def updateDatasetLocality_JEDI(self, jedi_taskid, datasetid, rse):
        comment = " /* JediDBProxy.updateDatasetLocality_JEDI */"
        methodName = self.getMethodName(comment)
        # last update time
        timestamp = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        timestamp_str = timestamp.strftime("%Y-%m-%d_%H:%M:%S")
        methodName += f" <taskID={jedi_taskid} datasetID={datasetid} rse={rse} timestamp={timestamp_str}>"
        tmpLog = MsgWrapper(logger, methodName)
        # tmpLog.debug('start')
        try:
            retVal = False
            # sql to check
            sqlC = ("SELECT timestamp " "FROM {0}.JEDI_Dataset_Locality " "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND rse=:rse ").format(
                jedi_config.db.schemaJEDI
            )
            # sql to insert
            sqlI = (
                "INSERT INTO {0}.JEDI_Dataset_Locality " "(jediTaskID, datasetID, rse, timestamp) " "VALUES (:jediTaskID, :datasetID, :rse, :timestamp)"
            ).format(jedi_config.db.schemaJEDI)
            # sql to update
            sqlU = (
                "UPDATE {0}.JEDI_Dataset_Locality " "SET timestamp=:timestamp " "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND rse=:rse "
            ).format(jedi_config.db.schemaJEDI)
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":jediTaskID"] = jedi_taskid
            varMap[":datasetID"] = datasetid
            varMap[":rse"] = rse
            self.cur.execute(sqlC + comment, varMap)
            resC = self.cur.fetchone()
            varMap[":timestamp"] = timestamp
            if resC is None:
                # insert if missing
                tmpLog.debug("insert")
                self.cur.execute(sqlI + comment, varMap)
            else:
                # update
                tmpLog.debug("update")
                self.cur.execute(sqlU + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            retVal = True
            # tmpLog.debug('done')
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal

    # delete outdated dataset locality records
    def deleteOutdatedDatasetLocality_JEDI(self, before_timestamp):
        comment = " /* JediDBProxy.deleteOutdatedDatasetLocality_JEDI */"
        methodName = self.getMethodName(comment)
        # last update time
        before_timestamp_str = before_timestamp.strftime("%Y-%m-%d_%H:%M:%S")
        methodName += f" <before_timestamp={before_timestamp_str}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            retVal = 0
            # sql to delete
            sqlD = f"DELETE {jedi_config.db.schemaJEDI}.Jedi_Dataset_Locality WHERE timestamp<=:timestamp "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":timestamp"] = before_timestamp
            # delete
            self.cur.execute(sqlD + comment, varMap)
            retVal = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"done, deleted {retVal} records")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal

    # query tasks and preassign them to dedicate workqueue, sql_query should query jeditaskid
    def queryTasksToPreassign_JEDI(self, sql_query, params_map, site, blacklist, limit):
        comment = " /* JediDBProxy.queryTasksToPreassign_JEDI */"
        methodName = self.getMethodName(comment)
        # methodName += " < sql={0} >".format(sql_query)
        tmpLog = MsgWrapper(logger, methodName)
        magic_workqueue_id = 400
        try:
            self.conn.begin()
            # sql to query
            self.cur.execute(sql_query + comment, params_map)
            taskIDs = self.cur.fetchall()
            tmpLog.debug(f"{sql_query} {params_map} ; got {len(taskIDs)} taskIDs")
            # sql to preassign the task to a site
            sqlPDG = (
                "UPDATE {0}.JEDI_Tasks "
                "SET lockedBy=NULL, lockedTime=NULL, "
                "site=:site, "
                "workQueue_ID=:workQueue_ID, "
                "modificationtime=CURRENT_DATE "
                "WHERE jediTaskID=:jediTaskID "
                "AND status IN ('ready','running','scouting') "
                "AND site IS NULL "
                "AND lockedBy IS NULL "
            ).format(jedi_config.db.schemaJEDI)
            # loop over tasks
            n_updated = 0
            updated_tasks_attr = []
            for jedi_taskid, orig_workqueue_id in taskIDs:
                if n_updated >= limit:
                    # respect the limit
                    break
                if jedi_taskid in blacklist:
                    # skip blacklisted tasks
                    continue
                varMap = {}
                varMap[":jediTaskID"] = jedi_taskid
                varMap[":site"] = site
                varMap[":workQueue_ID"] = magic_workqueue_id
                self.cur.execute(sqlPDG + comment, varMap)
                nRow = self.cur.rowcount
                if nRow == 1:
                    # self.record_task_status_change(jedi_taskid)
                    n_updated += 1
                    orig_attr = {
                        "workQueue_ID": orig_workqueue_id,
                    }
                    updated_tasks_attr.append((jedi_taskid, orig_attr))
                    tmpLog.debug(f"preassigned jediTaskID={jedi_taskid} to site={site} , orig_attr={orig_attr}")
                elif nRow > 1:
                    tmpLog.error(f"updated {nRow} rows with same jediTaskID={jedi_taskid}")
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {n_updated} rows to site={site}")
            # return
            return updated_tasks_attr
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # close and reassign N jobs of a preassigned task
    def reassignJobsInPreassignedTask_JEDI(self, jedi_taskid, site, n_jobs_to_close):
        comment = " /* JediDBProxy.reassignJobsInPreassignedTask_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" < jediTaskID={jedi_taskid} to {site} to close {n_jobs_to_close} jobs >"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            self.conn.begin()
            # check if task is still running and brokered to the site
            sqlT = (
                "SELECT jediTaskID " "FROM {0}.JEDI_Tasks t " "WHERE t.jediTaskID=:jediTaskID " "AND t.site =:site " "AND t.status IN ('ready','running') "
            ).format(jedi_config.db.schemaJEDI)
            varMap = {}
            varMap[":jediTaskID"] = jedi_taskid
            varMap[":site"] = site
            self.cur.execute(sqlT + comment, varMap)
            resT = self.cur.fetchall()
            if not resT:
                # skip as preassigned task not running and brokered
                tmpLog.debug("no longer brokered to site or not ready/running ; skipped")
                return None
            # close jobs
            sqlJC = (
                "SELECT pandaID " "FROM {0}.jobsActive4 " "WHERE jediTaskID=:jediTaskID " "AND jobStatus='activated' " "AND computingSite!=:computingSite "
            ).format(jedi_config.db.schemaPANDA)
            varMap = {}
            varMap[":jediTaskID"] = jedi_taskid
            varMap[":computingSite"] = site
            self.cur.execute(sqlJC + comment, varMap)
            pandaIDs = self.cur.fetchall()
            n_jobs_closed = 0
            for (pandaID,) in pandaIDs:
                res_close = self.killJob(pandaID, "reassign", "51", True)
                if res_close:
                    n_jobs_closed += 1
                if n_jobs_closed >= n_jobs_to_close:
                    break
            tmpLog.debug(f"closed {n_jobs_closed} jobs")
            return n_jobs_closed
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)

    # undo preassigned tasks
    def undoPreassignedTasks_JEDI(self, jedi_taskids, task_orig_attr_map, params_map, force):
        comment = " /* JediDBProxy.undoPreassignedTasks_JEDI */"
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger, methodName)
        magic_workqueue_id = 400
        # sql to undo a preassigned task if it moves off the status to generate jobs
        sqlUPT = (
            "UPDATE {0}.JEDI_Tasks t "
            "SET "
            "t.site=NULL, "
            "t.workQueue_ID=( "
            "CASE "
            "WHEN t.workQueue_ID=:magic_workqueue_id "
            "THEN :orig_workqueue_id "
            "ELSE t.workQueue_ID "
            "END "
            "), "
            "t.modificationtime=CURRENT_DATE "
            "WHERE t.jediTaskID=:jediTaskID "
            "AND t.site IS NOT NULL "
            "AND NOT ( "
            "t.status IN ('ready','running') "
            "AND EXISTS ( "
            "SELECT d.datasetID FROM {0}.JEDI_Datasets d "
            "WHERE t.jediTaskID=d.jediTaskID AND d.type='input' "
            "AND d.nFilesToBeUsed-d.nFilesUsed>=:min_files_ready AND d.nFiles-d.nFilesUsed>=:min_files_remaining "
            ") "
            ") "
        ).format(jedi_config.db.schemaJEDI)
        # sql to force to undo a preassigned task no matter what
        sqlUPTF = (
            "UPDATE {0}.JEDI_Tasks t "
            "SET "
            "t.site=NULL, "
            "t.workQueue_ID=( "
            "CASE "
            "WHEN t.workQueue_ID=:magic_workqueue_id "
            "THEN :orig_workqueue_id "
            "ELSE t.workQueue_ID "
            "END "
            "), "
            "t.modificationtime=CURRENT_DATE "
            "WHERE t.jediTaskID=:jediTaskID "
            "AND t.site IS NOT NULL "
        ).format(jedi_config.db.schemaJEDI)
        try:
            self.conn.begin()
            # loop over tasks
            n_updated = 0
            updated_tasks = []
            force_str = ""
            for jedi_taskid in jedi_taskids:
                try:
                    orig_attr = task_orig_attr_map[str(jedi_taskid)]
                    orig_workqueue_id = orig_attr["workQueue_ID"]
                except KeyError:
                    tmpLog.warning(f"missed original attributes of jediTaskID={jedi_taskid} ; use default values ")
                    orig_workqueue_id = magic_workqueue_id
                varMap = {}
                varMap[":jediTaskID"] = jedi_taskid
                varMap[":orig_workqueue_id"] = orig_workqueue_id
                varMap[":magic_workqueue_id"] = magic_workqueue_id
                if force:
                    force_str = "force"
                    self.cur.execute(sqlUPTF + comment, varMap)
                else:
                    varMap[":min_files_ready"] = params_map[":min_files_ready"]
                    varMap[":min_files_remaining"] = params_map[":min_files_remaining"]
                    self.cur.execute(sqlUPT + comment, varMap)
                nRow = self.cur.rowcount
                if nRow == 1:
                    # self.record_task_status_change(jedi_taskid)
                    n_updated += 1
                    updated_tasks.append(jedi_taskid)
                    tmpLog.debug(f"{force_str} undid preassigned jediTaskID={jedi_taskid}")
                elif nRow > 1:
                    tmpLog.error(f"{force_str} updated {nRow} rows with same jediTaskID={jedi_taskid}")
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"{force_str} done with {n_updated} rows")
            # return
            return updated_tasks
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    # set missing files according to iDDS messages
    def setMissingFilesAboutIdds_JEDI(self, jeditaskid, filenames_dict):
        comment = " /* JediDBProxy.setMissingFilesAboutIdds_JEDI */"
        methodName = self.getMethodName(comment)
        methodName += f" <jediTaskID={jeditaskid} nfiles={len(filenames_dict)}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # sql to set missing files
            sqlF = (
                "UPDATE {0}.JEDI_Dataset_Contents " "SET status=:nStatus " "WHERE jediTaskID=:jediTaskID " "AND lfn LIKE :lfn AND status!=:nStatus "
            ).format(jedi_config.db.schemaJEDI)
            # begin transaction
            self.conn.begin()
            nFileRow = 0
            # update contents
            for filename, (datasetid, fileid) in filenames_dict.items():
                tmp_sqlF = sqlF
                varMap = {}
                varMap[":jediTaskID"] = jeditaskid
                varMap[":lfn"] = "%" + filename
                varMap[":nStatus"] = "missing"
                if datasetid is not None:
                    # with datasetID from message
                    tmp_sqlF += "AND datasetID=:datasetID "
                    varMap[":datasetID"] = datasetid
                if fileid is not None:
                    # with fileID from message
                    tmp_sqlF += "AND fileID=:fileID "
                    varMap[":fileID"] = fileid
                self.cur.execute(tmp_sqlF + comment, varMap)
                nRow = self.cur.rowcount
                nFileRow += nRow
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done set {nFileRow} missing files")
            return nFileRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None

    def load_sw_map(self):
        comment = " /* JediDBProxy.load_sw_map */"
        method_name = self.getMethodName(comment)
        tmp_log = MsgWrapper(logger, method_name)
        tmp_log.debug("start")

        sw_map = {}

        try:
            # sql to get size
            sql = f"SELECT PANDA_QUEUE, DATA FROM {jedi_config.db.schemaPANDA}.SW_TAGS"
            self.cur.execute(sql + comment)
            results = self.cur.fetchall()
            for panda_queue, data in results:
                sw_map[panda_queue] = json.loads(data)

            tmp_log.debug("done")
            return sw_map

        except Exception:
            self._rollback()
            self.dumpErrorMessage(tmp_log)
            return None

    # get origin datasets
    def get_origin_datasets(self, jedi_task_id, dataset_name, lfns):
        comment = " /* JediDBProxy.get_origin_datasets */"
        method_name = self.getMethodName(comment)
        method_name += f" < jediTaskID={jedi_task_id} {dataset_name} n_files={len(lfns)} >"
        tmp_log = MsgWrapper(logger, method_name)
        tmp_log.debug("start")
        try:
            dataset_names = []
            known_lfns = set()
            # sql to get dataset
            sql_d = (
                "SELECT tabD.jediTaskID, tabD.datasetID, tabD.datasetName "
                "FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC "
                "WHERE tabC.lfn=:lfn AND tabC.type=:type AND tabD.datasetID=tabC.datasetID ".format(jedi_config.db.schemaJEDI)
            )
            sql_c = (
                f"SELECT lfn FROM {jedi_config.db.schemaJEDI}.JEDI_Dataset_Contents WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            )
            to_break = False
            for lfn in lfns:
                if lfn in known_lfns:
                    continue
                # start transaction
                self.conn.begin()
                # get dataset
                var_map = {":lfn": lfn, ":type": "output"}
                self.cur.execute(sql_d + comment, var_map)
                res = self.cur.fetchone()
                if res:
                    task_id, dataset_id, dataset_name = res
                    dataset_names.append(dataset_name)
                    # get files
                    var_map = {":jediTaskID": task_id, ":datasetID": dataset_id, ":status": "finished"}
                    self.cur.execute(sql_c + comment, var_map)
                    res = self.cur.fetchall()
                    for (tmp_lfn,) in res:
                        known_lfns.add(tmp_lfn)
                else:
                    tmp_log.debug(f"no dataset for {lfn}")
                    # return nothing if any dataset is not found
                    dataset_names = None
                    to_break = True
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                if to_break:
                    break
            # return
            tmp_log.debug(f"found {str(dataset_names)}")
            return dataset_names
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmp_log)
            return None

    # get carbon footprint for a task, the level has to be 'regional' or 'global'. If misspelled, it defaults to 'global'
    def get_task_carbon_footprint(self, jedi_task_id, level):
        comment = " /* JediDBProxy.get_task_carbon_footprint */"
        method_name = self.getMethodName(comment)
        method_name += f" < jediTaskID={jedi_task_id} n_files={level} >"
        tmp_log = MsgWrapper(logger, method_name)
        tmp_log.debug("start")

        if level == "regional":
            gco2_column = "GCO2_REGIONAL"
        else:
            gco2_column = "GCO2_GLOBAL"

        try:
            sql = (
                "SELECT jobstatus, SUM(sum_gco2) FROM ( "
                "  SELECT jobstatus, SUM({gco2_column}) sum_gco2 FROM {active_schema}.jobsarchived4 "
                "  WHERE jeditaskid =:jeditaskid "
                "  GROUP BY jobstatus "
                "  UNION "
                "  SELECT jobstatus, SUM({gco2_column}) sum_gco2 FROM {archive_schema}.jobsarchived "
                "  WHERE jeditaskid =:jeditaskid "
                "  GROUP BY jobstatus)"
                "GROUP BY jobstatus".format(gco2_column=gco2_column, active_schema=jedi_config.db.schemaJEDI, archive_schema=jedi_config.db.schemaPANDAARCH)
            )
            var_map = {":jeditaskid": jedi_task_id}

            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, var_map)
            results = self.cur.fetchall()

            footprint = {"total": 0}
            data = False
            for job_status, g_co2 in results:
                if not g_co2:
                    g_co2 = 0
                else:
                    data = True
                footprint[job_status] = g_co2
                footprint["total"] += g_co2

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug(f"done: {footprint}")

            if not data:
                return None

            return footprint
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmp_log)
            return None

    # get pending data carousel tasks and their input datasets
    def get_pending_dc_tasks_JEDI(self, task_type="prod", time_limit_minutes=60):
        comment = " /* JediDBProxy.get_pending_dc_tasks_JEDI */"
        method_name = self.getMethodName(comment)
        tmp_log = MsgWrapper(logger, method_name)
        tmp_log.debug("start")
        try:
            # sql to get pending tasks
            sql_tasks = (
                "SELECT tabT.jediTaskID, tabT.splitRule "
                "FROM {0}.JEDI_Tasks tabT, {0}.JEDI_AUX_Status_MinTaskID tabA "
                "WHERE tabT.status=:status AND tabA.status=tabT.status "
                "AND tabT.taskType=:taskType AND tabT.modificationTime<:timeLimit".format(jedi_config.db.schemaJEDI)
            )
            # sql to get input dataset
            sql_ds = (
                "SELECT tabD.datasetID, tabD.datasetName "
                "FROM {0}.JEDI_Datasets tabD "
                "WHERE tabD.jediTaskID=:jediTaskID AND tabD.type IN (:type1, :type2) ".format(jedi_config.db.schemaJEDI)
            )
            # initialize
            ret_tasks_dict = {}
            # start transaction
            self.conn.begin()
            # get pending tasks
            var_map = {":status": "pending", ":taskType": task_type}
            var_map[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=time_limit_minutes)
            self.cur.execute(sql_tasks + comment, var_map)
            res = self.cur.fetchall()
            if res:
                for task_id, split_rule in res:
                    tmp_taskspec = JediTaskSpec()
                    tmp_taskspec.splitRule = split_rule
                    if tmp_taskspec.inputPreStaging():
                        # is data carousel task
                        var_map = {
                            ":jediTaskID": task_id,
                            ":type1": "input",
                            ":type2": "pseudo_input",
                        }
                        self.cur.execute(sql_ds + comment, var_map)
                        ds_res = self.cur.fetchall()
                        if ds_res:
                            ret_tasks_dict[task_id] = []
                            for ds_id, ds_name in ds_res:
                                ret_tasks_dict[task_id].append(ds_name)
            else:
                tmp_log.debug("no pending task")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"found pending dc tasks: {ret_tasks_dict}")
            return ret_tasks_dict
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmp_log)
            return None
