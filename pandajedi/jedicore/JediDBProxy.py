import re
import os
import sys
import copy
import math
import types
import numpy
import random
import datetime
import traceback
import cx_Oracle

from pandajedi.jediconfig import jedi_config

from pandaserver import taskbuffer
import taskbuffer.OraDBProxy
from WorkQueueMapper import WorkQueueMapper

from JediTaskSpec import JediTaskSpec
from JediFileSpec import JediFileSpec
from JediDatasetSpec import JediDatasetSpec
from InputChunk import InputChunk
from MsgWrapper import MsgWrapper

import ParseJobXML
import JediCoreUtils


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])
taskbuffer.OraDBProxy._logger = logger


class DBProxy(taskbuffer.OraDBProxy.DBProxy):

    # constructor
    def __init__(self,useOtherError=False):
        taskbuffer.OraDBProxy.DBProxy.__init__(self,useOtherError)
        # attributes for JEDI
        # 
        # list of work queues
        self.workQueueMap = WorkQueueMapper()
        # update time for work queue map
        self.updateTimeForWorkQueue = None



    # connect to DB (just for INTR)
    def connect(self,dbhost=jedi_config.db.dbhost,dbpasswd=jedi_config.db.dbpasswd,
                dbuser=jedi_config.db.dbuser,dbname=jedi_config.db.dbname,
                dbtimeout=None,reconnect=False):
        return taskbuffer.OraDBProxy.DBProxy.connect(self,dbhost=dbhost,dbpasswd=dbpasswd,
                                                     dbuser=dbuser,dbname=dbname,
                                                     dbtimeout=dbtimeout,reconnect=reconnect)



    # extract method name from comment
    def getMethodName(self,comment):
        tmpMatch = re.search('([^ /*]+)',comment)
        if tmpMatch != None:
            methodName = tmpMatch.group(1).split('.')[-1]
        else:
            methodName = comment
        return methodName    



    # check if exception is from NOWAIT
    def isNoWaitException(self,errValue):
        oraErrCode = str(errValue).split()[0]
        oraErrCode = oraErrCode[:-1]
        if oraErrCode == 'ORA-00054':
            return True
        return False



    # dump error message
    def dumpErrorMessage(self,tmpLog,methodName=None,msgType=None):
        # error
        errtype,errvalue = sys.exc_info()[:2]
        if methodName != None:
            errStr = methodName
        else:
            errStr = ''
        errStr += ": %s %s" % (errtype.__name__,errvalue)
        errStr.strip()
        errStr += traceback.format_exc()
        if msgType == 'debug':
            tmpLog.debug(errStr)
        else:
            tmpLog.error(errStr)



    # get work queue map
    def getWorkQueueMap(self):
        self.refreshWrokQueueMap()
        return self.workQueueMap

    

    # refresh work queue map
    def refreshWrokQueueMap(self):
        # avoid frequent lookup
        if self.updateTimeForWorkQueue != None and \
               (datetime.datetime.utcnow()-self.updateTimeForWorkQueue) < datetime.timedelta(minutes=10):
            return
        comment = ' /* JediDBProxy.refreshWrokQueueMap */'
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # SQL
        sql = self.workQueueMap.getSqlQuery()
        try:
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000
            self.cur.execute(sql+comment)
            res = self.cur.fetchall()
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # make map
            self.workQueueMap.makeMap(res)
            tmpLog.debug('done')
            self.updateTimeForWorkQueue = datetime.datetime.utcnow()
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

                                            

    # get the list of datasets to feed contents to DB
    def getDatasetsToFeedContents_JEDI(self,vo,prodSourceLabel):
        comment = ' /* JediDBProxy.getDatasetsToFeedContents_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1}>'.format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # SQL
            varMap = {}
            varMap[':ts_running']       = 'running'
            varMap[':ts_defined']       = 'defined'
            varMap[':dsStatus_pending'] = 'pending'
            varMap[':dsState_mutable']  = 'mutable'
            varMap[':checkTimeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=60)
            varMap[':lockTimeLimit']  = datetime.datetime.utcnow() - datetime.timedelta(minutes=10)
            sql  = "SELECT {0} ".format(JediDatasetSpec.columnNames('tabD'))
            sql += 'FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA '.format(jedi_config.db.schemaJEDI)
            sql += 'WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID '
            sql += 'AND (tabT.lockedTime IS NULL OR tabT.lockedTime<:lockTimeLimit) '
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sql += "AND tabT.vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sql += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sql += 'AND tabT.jediTaskID=tabD.jediTaskID '
            sql += 'AND type IN ('
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sql += '{0},'.format(mapKey)
                varMap[mapKey] = tmpType
            sql  = sql[:-1]    
            sql += ') '
            sql += ' AND ((tabT.status=:ts_defined AND tabD.status IN ('
            for tmpStat in JediDatasetSpec.statusToUpdateContents():
                mapKey = ':dsStatus_'+tmpStat
                sql += '{0},'.format(mapKey)
                varMap[mapKey] = tmpStat
            sql  = sql[:-1]    
            sql += ')) OR (tabT.status=:ts_running AND tabD.state=:dsState_mutable AND tabD.stateCheckTime<:checkTimeLimit)) '
            sql += 'AND tabT.lockedBy IS NULL AND tabD.lockedBy IS NULL '
            sql += 'AND NOT EXISTS '
            sql += '(SELECT 1 FROM {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
            sql += 'WHERE {0}.JEDI_Datasets.jediTaskID=tabT.jediTaskID '.format(jedi_config.db.schemaJEDI)
            sql += 'AND type IN ('
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sql += '{0},'.format(mapKey)
            sql  = sql[:-1]
            sql += ') AND status=:dsStatus_pending) '
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            tmpLog.debug(sql+comment+str(varMap))
            self.cur.execute(sql+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            resList = self.cur.fetchall()
            returnMap = {}
            taskDatasetMap = {}
            nDS = 0
            for res in resList:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(res)
                if not returnMap.has_key(datasetSpec.jediTaskID):
                    returnMap[datasetSpec.jediTaskID] = []
                returnMap[datasetSpec.jediTaskID].append(datasetSpec)
                nDS += 1
                if not datasetSpec.jediTaskID in taskDatasetMap:
                    taskDatasetMap[datasetSpec.jediTaskID] = []
                taskDatasetMap[datasetSpec.jediTaskID].append(datasetSpec.datasetID)
            jediTaskIDs = returnMap.keys()
            jediTaskIDs.sort()
            # get seq_number
            sqlSEQ  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
            sqlSEQ += 'FROM {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
            sqlSEQ += 'WHERE jediTaskID=:jediTaskID AND datasetName=:datasetName '
            for jediTaskID in jediTaskIDs:
                varMap = {}
                varMap[':jediTaskID']  = jediTaskID
                varMap[':datasetName'] = 'seq_number'
                self.conn.begin()
                self.cur.execute(sqlSEQ+comment,varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                resSeqList = self.cur.fetchall()
                for resSeq in resSeqList:
                    datasetSpec = JediDatasetSpec()
                    datasetSpec.pack(resSeq)
                    # append if missing
                    if not datasetSpec.datasetID in taskDatasetMap[datasetSpec.jediTaskID]:
                        taskDatasetMap[datasetSpec.jediTaskID].append(datasetSpec.datasetID)
                        returnMap[datasetSpec.jediTaskID].append(datasetSpec)
            returnList  = []
            for jediTaskID in jediTaskIDs:
                returnList.append((jediTaskID,returnMap[jediTaskID]))
            tmpLog.debug('got {0} datasets for {1} tasks'.format(nDS,len(jediTaskIDs)))
            return returnList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None


                                                
    # check if item is matched with one of list items
    def isMatched(self,itemName,pattList):
        for tmpName in pattList:
            # normal pattern
            if re.search(tmpName,itemName) != None or tmpName == itemName:
                return True
        # return
        return False



    # feed files to the JEDI contents table
    def insertFilesForDataset_JEDI(self,datasetSpec,fileMap,datasetState,stateUpdateTime,
                                   nEventsPerFile,nEventsPerJob,maxAttempt,firstEventNumber,
                                   nMaxFiles,nMaxEvents,useScout,givenFileList,useFilesWithNewAttemptNr,
                                   nFilesPerJob,nEventsPerRange,nChunksForScout,includePatt,excludePatt,
                                   xmlConfig,noWaitParent,parent_tid,pid,maxFailure,useRealNumEvents,
                                   respectLB,tgtNumEventsPerJob,skipFilesUsedBy,ramCount):
        comment = ' /* JediDBProxy.insertFilesForDataset_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0} datasetID={1}>'.format(datasetSpec.jediTaskID,
                                                           datasetSpec.datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start nEventsPerFile={0} nEventsPerJob={1} maxAttempt={2} maxFailure={3}'.format(nEventsPerFile,
                                                                                                       nEventsPerJob,
                                                                                                       maxAttempt,
                                                                                                       maxFailure))
        tmpLog.debug('firstEventNumber={0} nMaxFiles={1} nMaxEvents={2}'.format(firstEventNumber,
                                                                                nMaxFiles,nMaxEvents))
        tmpLog.debug('useFilesWithNewAttemptNr={0} nFilesPerJob={1} nEventsPerRange={2}'.format(useFilesWithNewAttemptNr,
                                                                                                nFilesPerJob,
                                                                                                nEventsPerRange))
        tmpLog.debug('useScout={0} nChunksForScout={1} userRealEventNumber={2}'.format(useScout,nChunksForScout,
                                                                                       useRealNumEvents))
        tmpLog.debug('includePatt={0} excludePatt={1}'.format(str(includePatt),str(excludePatt)))
        tmpLog.debug('xmlConfig={0} noWaitParent={1} parent_tid={2}'.format(type(xmlConfig),noWaitParent,parent_tid))
        tmpLog.debug('len(fileMap)={0} pid={1}'.format(len(fileMap),pid))
        tmpLog.debug('datasetState={0} dataset.state={1}'.format(datasetState,datasetSpec.state))
        tmpLog.debug('respectLB={0} tgtNumEventsPerJob={1} skipFilesUsedBy={2} ramCount={3}'.format(respectLB,tgtNumEventsPerJob,
                                                                                       skipFilesUsedBy, ramCount))

        # return value for failure
        diagMap = {'errMsg':'',
                   'nChunksForScout':nChunksForScout,
                   'nActivatedPending':0,
                   'isRunningTask':False}
        failedRet = False,0,None,diagMap
        regStart = datetime.datetime.utcnow()
        # max number of file records per dataset
        maxFileRecords = 200000
        # mutable
        if noWaitParent and datasetState == 'mutable':
            isMutableDataset = True
        else:
            isMutableDataset = False
        # event level splitting
        if nEventsPerJob != None and nFilesPerJob == None:
            isEventSplit = True
        else:
            isEventSplit = False
        try:
            # current date
            timeNow = datetime.datetime.utcnow()
            # get list of files produced by parent
            if datasetSpec.checkConsistency():
                # sql to get the list
                sqlPPC  = "SELECT lfn FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(jedi_config.db.schemaJEDI)
                sqlPPC += "WHERE tabD.jediTaskID=tabC.jediTaskID AND tabD.datasetID=tabC.datasetID "
                sqlPPC += "AND tabD.jediTaskID=:jediTaskID AND tabD.type IN (:type1,:type2) "
                sqlPPC += "AND tabD.datasetName IN (:dsName,:didName) AND tabC.status=:fileStatus "
                varMap = {}
                varMap[':type1'] = 'output'
                varMap[':type2'] = 'log'
                varMap[':jediTaskID']  = parent_tid
                varMap[':fileStatus']  = 'finished'
                varMap[':didName'] = datasetSpec.datasetName
                varMap[':dsName'] = datasetSpec.datasetName.split(':')[-1]
                # begin transaction
                self.conn.begin()
                self.cur.execute(sqlPPC+comment,varMap)
                tmpPPC = self.cur.fetchall()
                producedFileList = set()
                for tmpLFN, in tmpPPC:
                    producedFileList.add(tmpLFN)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # check if files are 'finished' in JEDI table
                newFileMap = {}
                for guid,fileVal in fileMap.iteritems():
                    if fileVal['lfn'] in producedFileList:
                        newFileMap[guid] = fileVal
                    else:
                        tmpLog.debug('{0} skipped since was not properly produced by the parent according to JEDI table'.format(fileVal['lfn']))
                fileMap = newFileMap
            # get files used by another task
            usedFilesToSkip = set()
            if skipFilesUsedBy != None:
                # sql to get the list
                sqlSFU  = "SELECT lfn,startEvent,endEvent FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(jedi_config.db.schemaJEDI)
                sqlSFU += "WHERE tabD.jediTaskID=tabC.jediTaskID AND tabD.datasetID=tabC.datasetID "
                sqlSFU += "AND tabD.jediTaskID=:jediTaskID AND tabD.type IN (:type1,:type2) "
                sqlSFU += "AND tabD.datasetName IN (:dsName,:didName) AND tabC.status=:fileStatus "
                for tmpTaskID in str(skipFilesUsedBy).split(','):
                    varMap = {}
                    varMap[':type1'] = 'input'
                    varMap[':type2'] = 'pseudo_input'
                    varMap[':jediTaskID']  = tmpTaskID
                    varMap[':fileStatus']  = 'finished'
                    varMap[':didName'] = datasetSpec.datasetName
                    varMap[':dsName'] = datasetSpec.datasetName.split(':')[-1]
                    # begin transaction
                    self.conn.begin()
                    self.cur.execute(sqlSFU+comment,varMap)
                    tmpSFU = self.cur.fetchall()
                    for tmpLFN,tmpStartEvent,tmpEndEvent in tmpSFU:
                        tmpID = '{0}.{1}.{2}'.format(tmpLFN,tmpStartEvent,tmpEndEvent)
                        usedFilesToSkip.add(tmpID)
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
            # include files
            if includePatt != []:
                newFileMap = {}
                for guid,fileVal in fileMap.iteritems():
                    if self.isMatched(fileVal['lfn'],includePatt):
                        newFileMap[guid] = fileVal
                fileMap = newFileMap
            # exclude files
            if excludePatt != []:
                newFileMap = {}
                for guid,fileVal in fileMap.iteritems():
                    if not self.isMatched(fileVal['lfn'],excludePatt):
                        newFileMap[guid] = fileVal
                fileMap = newFileMap
            # file list is given
            givenFileMap = {}
            if givenFileList != []:
                for tmpFileItem in givenFileList:
                    if type(tmpFileItem) == types.DictType:
                        tmpLFN = tmpFileItem['lfn']
                        fileItem = tmpFileItem
                    else:
                        tmpLFN = tmpFileItem
                        fileItem = {'lfn':tmpFileItem}
                    givenFileMap[tmpLFN] = fileItem
                newFileMap = {}
                for guid,fileVal in fileMap.iteritems():
                    if fileVal['lfn'] in givenFileMap:
                        newFileMap[guid] = fileVal
                fileMap = newFileMap
            # XML config
            if xmlConfig != None:
                try:
                    xmlConfig = ParseJobXML.dom_parser(xmlStr=xmlConfig)
                except:
                    errtype,errvalue = sys.exc_info()[:2]
                    tmpErrStr = 'failed to load XML config with {0}:{1}'.format(errtype.__name__,errvalue)
                    raise RuntimeError,tmpErrStr
                newFileMap = {}
                for guid,fileVal in fileMap.iteritems():
                    if fileVal['lfn'] in xmlConfig.files_in_DS(datasetSpec.datasetName):
                        newFileMap[guid] = fileVal
                fileMap = newFileMap
            # make map with LFN as key
            filelValMap = {}
            for guid,fileVal in fileMap.iteritems():
                filelValMap[fileVal['lfn']] = (guid,fileVal)
            # make LFN list
            listBoundaryID = []
            if xmlConfig == None:
                # sort by LFN
                lfnList = filelValMap.keys()
                lfnList.sort()
            else:
                # sort as described in XML
                tmpBoundaryID = 0
                lfnList = []
                for tmpJobXML in xmlConfig.jobs:
                    for tmpLFN in tmpJobXML.files_in_DS(datasetSpec.datasetName):
                        # check if the file is available
                        if not filelValMap.has_key(tmpLFN):
                            diagMap['errMsg'] = "{0} is not found in {1}".format(tmpLFN,datasetSpec.datasetName)
                            tmpLog.error(diagMap['errMsg'])
                            return failedRet
                        lfnList.append(tmpLFN)
                        listBoundaryID.append(tmpBoundaryID)
                    # increment boundaryID
                    tmpBoundaryID += 1
            # truncate if nessesary
            if datasetSpec.isSeqNumber():
                offsetVal = 0
            else:
                offsetVal = datasetSpec.getOffset()
            if offsetVal > 0:
                lfnList = lfnList[offsetVal:]
            tmpLog.debug('offset={0}'.format(offsetVal))
            # use perRange as perJob
            if nEventsPerJob == None and nEventsPerRange != None:
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
            for tmpIdx,tmpLFN in enumerate(lfnList):
                # collect unique LFN list    
                if not tmpLFN in uniqueLfnList:
                    uniqueLfnList[tmpLFN] = None
                # check if enough files
                if nMaxFiles != None and len(uniqueLfnList) > nMaxFiles:
                    break
                guid,fileVal = filelValMap[tmpLFN]
                fileSpec = JediFileSpec()
                fileSpec.jediTaskID   = datasetSpec.jediTaskID
                fileSpec.datasetID    = datasetSpec.datasetID
                fileSpec.GUID         = guid
                fileSpec.type         = datasetSpec.type
                fileSpec.status       = 'ready'            
                fileSpec.lfn          = fileVal['lfn']
                fileSpec.scope        = fileVal['scope']
                fileSpec.fsize        = fileVal['filesize']
                fileSpec.checksum     = fileVal['checksum']
                fileSpec.creationDate = timeNow
                fileSpec.attemptNr    = 0
                fileSpec.failedAttempt = 0
                fileSpec.maxAttempt = maxAttempt
                fileSpec.maxFailure = maxFailure
                fileSpec.ramCount = ramCount
                if nEventsPerFile != None:
                    fileSpec.nEvents = nEventsPerFile
                elif fileVal.has_key('events') and not fileVal['events'] in ['None',None]:
                    try:
                        fileSpec.nEvents = long(fileVal['events'])
                    except:
                        fileSpec.nEvents = None
                if fileVal.has_key('lumiblocknr'):
                    try:
                        fileSpec.lumiBlockNr = long(fileVal['lumiblocknr'])
                    except:
                        pass
                # keep track
                if datasetSpec.toKeepTrack():
                    fileSpec.keepTrack = 1
                tmpFileSpecList = []
                if xmlConfig != None:
                    # splitting with XML
                    fileSpec.boundaryID = listBoundaryID[tmpIdx]
                    tmpFileSpecList.append(fileSpec)
                elif givenFileList != []:
                    # given file list
                    if fileSpec.lfn in givenFileMap:
                        fileItem = givenFileMap[fileSpec.lfn]
                        if not fileItem['lfn'] in foundFileList:
                            foundFileList.append(fileItem['lfn'])
                        copiedFileSpec = copy.copy(fileSpec)
                        if fileItem.has_key('firstEvent'):
                            copiedFileSpec.firstEvent = fileItem['firstEvent']
                        if fileItem.has_key('startEvent'):
                            copiedFileSpec.startEvent = fileItem['startEvent']
                        if fileItem.has_key('endEvent'):
                            copiedFileSpec.endEvent = fileItem['endEvent']
                        if fileItem.has_key('boundaryID'):
                            copiedFileSpec.boundaryID = fileItem['boundaryID']
                        if fileItem.has_key('keepTrack'):
                            copiedFileSpec.keepTrack = fileItem['keepTrack']
                        tmpFileSpecList.append(copiedFileSpec)
                elif ((nEventsPerJob == None or nEventsPerJob <= 0) and \
                          (tgtNumEventsPerJob == None or tgtNumEventsPerJob <= 0)) or \
                       fileSpec.nEvents == None or fileSpec.nEvents <= 0 or \
                       ((nEventsPerFile == None or nEventsPerFile <= 0) and not useRealNumEvents): 
                    if firstEventNumber != None and nEventsPerFile != None:
                        fileSpec.firstEvent = totalEventNumber
                        totalEventNumber += fileSpec.nEvents
                    # file-level splitting
                    tmpFileSpecList.append(fileSpec)
                else:
                    # event-level splitting
                    tmpStartEvent = 0
                    # change nEventsPerJob if target number is specified
                    if tgtNumEventsPerJob != None and tgtNumEventsPerJob > 0:
                        # calcurate to how many chunks the file is split
                        tmpItem = divmod(fileSpec.nEvents,tgtNumEventsPerJob)
                        nSubChunk = tmpItem[0]
                        if tmpItem[1] > 0:
                            nSubChunk += 1
                        if nSubChunk <= 0:
                            nSubChunk = 1
                        # get nEventsPerJob
                        tmpItem= divmod(fileSpec.nEvents,nSubChunk)
                        nEventsPerJob = tmpItem[0]
                        if tmpItem[1] > 0:
                            nEventsPerJob += 1
                        if nEventsPerJob <= 0:
                            nEventsPerJob = 1
                        nRemEvents = nEventsPerJob
                    # LB boundaries
                    if respectLB:
                        if lumiBlockNr == None or lumiBlockNr != fileSpec.lumiBlockNr:
                            lumiBlockNr = fileSpec.lumiBlockNr
                            nRemEvents = nEventsPerJob
                    # make file specs
                    while nRemEvents > 0:
                        splitFileSpec = copy.copy(fileSpec)
                        if tmpStartEvent + nRemEvents >= splitFileSpec.nEvents:
                            splitFileSpec.startEvent = tmpStartEvent
                            splitFileSpec.endEvent = splitFileSpec.nEvents - 1
                            nRemEvents -= (splitFileSpec.nEvents - tmpStartEvent)
                            if nRemEvents == 0:
                                nRemEvents = nEventsPerJob
                            if firstEventNumber != None and (nEventsPerFile != None or useRealNumEvents):
                                splitFileSpec.firstEvent = totalEventNumber
                                totalEventNumber += (splitFileSpec.endEvent-splitFileSpec.startEvent+1)
                            tmpFileSpecList.append(splitFileSpec)
                            break
                        else:
                            splitFileSpec.startEvent = tmpStartEvent
                            splitFileSpec.endEvent   = tmpStartEvent + nRemEvents -1
                            tmpStartEvent += nRemEvents
                            nRemEvents = nEventsPerJob
                            if firstEventNumber != None and (nEventsPerFile != None or useRealNumEvents):
                                splitFileSpec.firstEvent = totalEventNumber
                                totalEventNumber += (splitFileSpec.endEvent-splitFileSpec.startEvent+1)
                            tmpFileSpecList.append(splitFileSpec)
                        if len(tmpFileSpecList) >= maxFileRecords:
                            break
                # append
                for fileSpec in tmpFileSpecList:
                    # check if to skip
                    tmpID = '{0}.{1}.{2}'.format(fileSpec.lfn,fileSpec.startEvent,fileSpec.endEvent)
                    if tmpID in usedFilesToSkip:
                        continue
                    # append
                    uniqueFileKey = '{0}.{1}.{2}.{3}'.format(fileSpec.lfn,fileSpec.startEvent,
                                                             fileSpec.endEvent,fileSpec.boundaryID)
                    uniqueFileKeyList.append(uniqueFileKey)                
                    fileSpecMap[uniqueFileKey] = fileSpec
                # check if number of events is enough
                if fileSpec.nEvents != None:
                    totalNumEventsF += fileSpec.nEvents
                if nMaxEvents != None and totalNumEventsF >= nMaxEvents:
                    break
                # too long list
                if len(uniqueFileKeyList) > maxFileRecords:
                    diagMap['errMsg'] = "too many file records >{0}".format(maxFileRecords)
                    tmpLog.error(diagMap['errMsg'])
                    return failedRet
            missingFileList = []    
            tmpLog.debug('{0} files missing'.format(len(missingFileList)))
            # sql to check if task is locked
            sqlTL = "SELECT status,lockedBy FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE NOWAIT ".format(jedi_config.db.schemaJEDI)
            # sql to check dataset status
            sqlDs  = "SELECT status,nFilesToBeUsed-nFilesUsed FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlDs += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID FOR UPDATE "
            # sql to get existing files
            sqlCh  = "SELECT fileID,lfn,status,startEvent,endEvent,boundaryID,nEvents FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlCh += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID FOR UPDATE "
            # sql to count existing files
            sqlCo  = "SELECT count(*) FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlCo += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql for insert
            sqlIn  = "INSERT INTO {0}.JEDI_Dataset_Contents ({1}) ".format(jedi_config.db.schemaJEDI,JediFileSpec.columnNames())
            sqlIn += JediFileSpec.bindValuesExpression(useSeq=False)
            # sql to get fileID
            sqlFID = "SELECT {0}.JEDI_DATASET_CONT_FILEID_SEQ.nextval FROM dual ".format(jedi_config.db.schemaJEDI)
            # sql to update file status
            sqlFU  = "UPDATE {0}.JEDI_Dataset_Contents SET status=:status ".format(jedi_config.db.schemaJEDI)
            sqlFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # sql to get master status
            sqlMS  = "SELECT status FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlMS += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update dataset
            sqlDU  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlDU += "SET status=:status,state=:state,stateCheckTime=:stateUpdateTime,"
            sqlDU += "nFiles=:nFiles,nFilesTobeUsed=:nFilesTobeUsed,nEvents=:nEvents "
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to propagate number of input events to DEFT
            sqlCE  = "UPDATE {0}.T_TASK ".format(jedi_config.db.schemaDEFT)
            sqlCE += "SET total_input_events=("
            sqlCE += "SELECT SUM(nEvents) FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlCE += "WHERE jediTaskID=:jediTaskID AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sqlCE += '{0},'.format(mapKey)
            sqlCE  = sqlCE[:-1]
            sqlCE += ") AND masterID IS NULL) "
            sqlCE += "WHERE taskID=:jediTaskID "
            nInsert  = 0
            nReady   = 0
            nPending = 0
            nUsed    = 0
            nLost    = 0
            pendingFID = []
            oldDsStatus = None
            newDsStatus = None
            nActivatedPending = 0
            nEventsToUseEventSplit = 0
            nFilesToUseEventSplit = 0
            nFilesUnprocessed = 0
            nEventsInsert = 0
            nEventsLost   = 0
            nEventsExist  = 0
            retVal = None,missingFileList,None,diagMap
            # begin transaction
            self.conn.begin()
            # check task
            try:
                varMap = {}
                varMap[':jediTaskID'] = datasetSpec.jediTaskID
                self.cur.execute(sqlTL+comment,varMap)
                resTask = self.cur.fetchone()
            except:
                errType,errValue = sys.exc_info()[:2]
                if self.isNoWaitException(errValue):
                    # resource busy and acquire with NOWAIT specified
                    tmpLog.debug('skip locked jediTaskID={0}'.format(datasetSpec.jediTaskID))
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    return retVal
                else:
                    # failed with something else
                    raise errType,errValue
            if resTask == None:
                tmpLog.debug('task not found in Task table')
            else:
                taskStatus,taskLockedBy = resTask
                if taskLockedBy != pid:
                    # task is locked
                    tmpLog.debug('task is locked by {0}'.format(taskLockedBy))
                elif not (taskStatus in JediTaskSpec.statusToUpdateContents() or \
                              (taskStatus == 'running' and \
                                   (datasetState == 'mutable' or datasetSpec.state == 'mutable' or datasetSpec.isSeqNumber()))):
                    # task status is irrelevant
                    tmpLog.debug('task.status={0} is not for contents update'.format(taskStatus))
                else:
                    tmpLog.debug('task.status={0}'.format(taskStatus))
                    # running task
                    if taskStatus == 'running':
                        diagMap['isRunningTask'] = True
                    # size of pending input chunk to be activated
                    sizePendingEventChunk = None
                    strSizePendingEventChunk = ''
                    if taskStatus == 'defined' and useScout:
                        # number of files for scout
                        sizePendingFileChunk = nChunksForScout
                        strSizePendingFileChunk = '{0}'.format(sizePendingFileChunk)
                        # number of files per job is specified
                        if not nFilesPerJob in [None,0]:
                            sizePendingFileChunk *= nFilesPerJob
                            strSizePendingFileChunk = '{0}*'.format(nFilesPerJob) + strSizePendingFileChunk
                        strSizePendingFileChunk += ' files required for scout'
                        # number of events for scout
                        if isEventSplit:
                            sizePendingEventChunk = nChunksForScout * nEventsPerJob
                            strSizePendingEventChunk = '{0}*{1} events required for scout'.format(nEventsPerJob,nChunksForScout)
                    else:
                        # the number of chunks in one bunch
                        nChunkInBunch = 20
                        # number of files to be activated
                        sizePendingFileChunk = nChunkInBunch
                        strSizePendingFileChunk = '{0}'.format(sizePendingFileChunk)
                        # number of files per job is specified
                        if not nFilesPerJob in [None,0]:
                            sizePendingFileChunk *= nFilesPerJob
                            strSizePendingFileChunk = '{0}*'.format(nFilesPerJob) + strSizePendingFileChunk
                        strSizePendingFileChunk += ' files required'
                        # number of events to be activated
                        if isEventSplit:
                            sizePendingEventChunk = nChunkInBunch * nEventsPerJob
                            strSizePendingEventChunk = '{0}*{1} events required'.format(nEventsPerJob,nChunkInBunch)
                    # check dataset status
                    varMap = {}
                    varMap[':jediTaskID'] = datasetSpec.jediTaskID
                    varMap[':datasetID'] = datasetSpec.datasetID
                    self.cur.execute(sqlDs+comment,varMap)
                    resDs = self.cur.fetchone()
                    if resDs == None:
                        tmpLog.debug('dataset not found in Datasets table')
                    elif not (resDs[0] in JediDatasetSpec.statusToUpdateContents() or \
                                  (taskStatus == 'running' and \
                                       (datasetState == 'mutable' or datasetSpec.state == 'mutable') or \
                                       (taskStatus in ['running','defined'] and datasetSpec.isSeqNumber()))):
                        tmpLog.debug('ds.status={0} is not for contents update'.format(resDs[0]))
                        oldDsStatus = resDs[0]
                        nFilesUnprocessed = resDs[1]
                        # count existing files
                        if resDs[0] == 'ready':
                            varMap = {}
                            varMap[':jediTaskID'] = datasetSpec.jediTaskID
                            varMap[':datasetID'] = datasetSpec.datasetID
                            self.cur.execute(sqlCo+comment,varMap)
                            resCo = self.cur.fetchone()
                            numUniqueLfn = resCo[0]
                            retVal = True,missingFileList,numUniqueLfn,diagMap
                    else:
                        oldDsStatus = resDs[0]
                        nFilesUnprocessed = resDs[1]
                        # get existing file list
                        varMap = {}
                        varMap[':jediTaskID'] = datasetSpec.jediTaskID
                        varMap[':datasetID'] = datasetSpec.datasetID
                        self.cur.execute(sqlCh+comment,varMap)
                        tmpRes = self.cur.fetchall()
                        existingFiles = {}
                        for fileID,lfn,status,startEvent,endEvent,boundaryID,nEventsInDS in tmpRes:
                            uniqueFileKey = '{0}.{1}.{2}.{3}'.format(lfn,startEvent,endEvent,boundaryID)
                            existingFiles[uniqueFileKey] = {'fileID':fileID,'status':status}
                            if startEvent != None and endEvent != None:
                                existingFiles[uniqueFileKey]['nevents'] = endEvent-startEvent+1
                            elif nEventsInDS != None:
                                existingFiles[uniqueFileKey]['nevents'] = nEventsInDS
                            else:
                                existingFiles[uniqueFileKey]['nevents'] = None
                            lostFlag = False
                            if status == 'ready':
                                nReady += 1
                            elif status == 'pending':
                                nPending += 1
                                pendingFID.append(fileID)
                                # count number of events for scouts with event-level splitting
                                if isEventSplit:
                                    try:
                                        if nEventsToUseEventSplit < sizePendingEventChunk:
                                            nEventsToUseEventSplit += (endEvent-startEvent+1)
                                            nFilesToUseEventSplit += 1
                                    except:
                                        pass
                            elif not status in ['lost','missing']:
                                nUsed += 1
                            elif status in ['lost','missing']:
                                nLost += 1
                                lostFlag = True
                            if existingFiles[uniqueFileKey]['nevents'] != None:
                                if lostFlag:
                                    nEventsLost += existingFiles[uniqueFileKey]['nevents']
                                else:
                                    nEventsExist += existingFiles[uniqueFileKey]['nevents']
                        tmpLog.debug('inDB nReady={0} nPending={1} nUsed={2} nLost={3}'.format(nReady,nPending,nUsed,nLost))
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
                            if not fileSpec.lfn in uniqueLfnList:
                                # the limit is reached at the previous file
                                if escapeNextFile:
                                    break
                                uniqueLfnList[fileSpec.lfn] = None
                                # maximum number of files to be processed
                                if nMaxFiles != None and len(uniqueLfnList) > nMaxFiles:
                                    break
                                # counts number of events for non event-level splitting
                                if fileSpec.nEvents != None:
                                    totalNumEventsF += fileSpec.nEvents
                                    # maximum number of events to be processed
                                    if nMaxEvents != None and totalNumEventsF >= nMaxEvents:
                                        escapeNextFile = True
                                # count number of unique LFNs
                                numUniqueLfn += 1
                            # count number of events for event-level splitting
                            if fileSpec.startEvent != None and fileSpec.endEvent != None:
                                totalNumEventsE += (fileSpec.endEvent-fileSpec.startEvent+1)
                                if nMaxEvents != None and totalNumEventsE > nMaxEvents:
                                    break
                            # avoid duplication
                            if uniqueFileKey in existingFiles:
                                continue
                            # go pending if no wait
                            if isMutableDataset:
                                fileSpec.status = 'pending'
                                nPending += 1
                            nInsert += 1
                            if fileSpec.startEvent != None and fileSpec.endEvent != None:
                                nEventsInsert += (fileSpec.endEvent-fileSpec.startEvent+1)
                            elif fileSpec.nEvents != None:
                                nEventsInsert += fileSpec.nEvents
                            # count number of events for scouts with event-level splitting
                            if isEventSplit:
                                try:
                                    if nEventsToUseEventSplit < sizePendingEventChunk:
                                        nEventsToUseEventSplit += (fileSpec.endEvent-fileSpec.startEvent+1)
                                        nFilesToUseEventSplit += 1
                                except:
                                    pass
                            fileSpecsForInsert.append(fileSpec)
                        # get fileID
                        tmpLog.debug('get fileIDs')
                        newFileIDs = []
                        for i in range(nInsert):
                            self.cur.execute(sqlFID)
                            fileID, = self.cur.fetchone()
                            newFileIDs.append(fileID)
                        if isMutableDataset:
                            pendingFID += newFileIDs
                        # sort fileID
                        tmpLog.debug('sort fileIDs')
                        newFileIDs.sort()
                        # set fileID
                        tmpLog.debug('set fileIDs')
                        varMaps = []
                        for fileID,fileSpec in zip(newFileIDs,fileSpecsForInsert):
                            fileSpec.fileID = fileID
                            # make vars
                            varMap = fileSpec.valuesMap()
                            varMaps.append(varMap)
                        # bulk insert
                        tmpLog.debug('bulk insert')
                        self.cur.executemany(sqlIn+comment,varMaps)
                        # activate pending
                        tmpLog.debug('activate pending')
                        toActivateFID = []
                        if isMutableDataset:
                            if not datasetSpec.isMaster():
                                # activate all files except master dataset
                                toActivateFID = pendingFID
                            else:
                                if isEventSplit:
                                    # enough events are pending
                                    if nEventsToUseEventSplit >= sizePendingEventChunk and nFilesToUseEventSplit > 0:
                                        toActivateFID = pendingFID[:(int(nPending/nFilesToUseEventSplit)*nFilesToUseEventSplit)]
                                    else:
                                        diagMap['errMsg'] = '{0} events ({1} files) available, {2}'.format(nEventsToUseEventSplit,
                                                                                                           nPending,
                                                                                                           strSizePendingEventChunk)
                                else:
                                    # enough files are pending
                                    if nPending >= sizePendingFileChunk and sizePendingFileChunk > 0:
                                        toActivateFID = pendingFID[:(int(nPending/sizePendingFileChunk)*sizePendingFileChunk)]
                                    else:
                                        diagMap['errMsg'] = '{0} files available, {1}'.format(nPending,strSizePendingFileChunk)
                        else:
                            nReady += nInsert
                            toActivateFID = pendingFID
                        for tmpFileID in toActivateFID:
                            varMap = {}
                            varMap[':status'] = 'ready'
                            varMap[':jediTaskID'] = datasetSpec.jediTaskID
                            varMap[':datasetID'] = datasetSpec.datasetID
                            varMap[':fileID'] = tmpFileID
                            self.cur.execute(sqlFU+comment,varMap)
                            nActivatedPending += 1
                            nReady += 1
                        # lost or recovered files
                        tmpLog.debug('lost or recovered files')
                        uniqueFileKeySet = set(uniqueFileKeyList)    
                        for uniqueFileKey,fileVarMap in existingFiles.iteritems():
                            varMap = {}
                            varMap[':jediTaskID'] = datasetSpec.jediTaskID
                            varMap[':datasetID'] = datasetSpec.datasetID
                            varMap[':fileID'] = fileVarMap['fileID']
                            if not uniqueFileKey in uniqueFileKeySet:
                                if fileVarMap['status'] == 'lost':
                                    continue
                                if fileVarMap['status'] != 'ready':
                                    continue
                                varMap['status'] = 'lost'
                            elif fileVarMap['status'] in ['lost','missing'] and \
                                     fileSpecMap[uniqueFileKey].status != fileVarMap['status']:
                                varMap['status'] = fileSpecMap[uniqueFileKey].status
                                if fileVarMap['nevents'] != None:
                                    nEventsExist += fileVarMap['nevents']
                            else:
                                continue
                            if varMap['status'] == 'ready':
                                nLost -= 1
                                nReady += 1
                                if fileVarMap['nevents'] != None:
                                    nEventsExist += fileVarMap['nevents']
                            if varMap['status'] in ['lost','missing']:
                                nLost += 1
                                nReady -= 1
                                if fileVarMap['nevents'] != None:
                                    nEventsExist -= fileVarMap['nevents']
                            self.cur.execute(sqlFU+comment,varMap)
                        tmpLog.debug('changed nReady={0} nLost={1}'.format(nReady,nLost))
                        # get master status
                        masterStatus = None
                        if not datasetSpec.isMaster():
                            varMap = {}
                            varMap[':jediTaskID'] = datasetSpec.jediTaskID
                            varMap[':datasetID'] = datasetSpec.masterID
                            self.cur.execute(sqlMS+comment,varMap)
                            resMS = self.cur.fetchone()
                            masterStatus, = resMS
                        tmpLog.debug('masterStatus={0}'.format(masterStatus))
                        # updata dataset
                        varMap = {}
                        varMap[':jediTaskID'] = datasetSpec.jediTaskID
                        varMap[':datasetID'] = datasetSpec.datasetID
                        varMap[':nFiles'] = nInsert + len(existingFiles) - nLost
                        varMap[':nEvents'] = nEventsInsert + nEventsExist
                        if xmlConfig != None:
                            # disable scout for --loadXML
                            varMap[':nFilesTobeUsed'] = nReady + nUsed
                        elif taskStatus == 'defined' and useScout and not isEventSplit and nChunksForScout != None and nReady > sizePendingFileChunk:
                            # set a fewer number for scout for file level splitting
                            varMap[':nFilesTobeUsed'] = sizePendingFileChunk
                        elif taskStatus == 'defined' and useScout and isEventSplit and nReady > nFilesToUseEventSplit:
                            # set a fewer number for scout for event level splitting
                            varMap[':nFilesTobeUsed'] = nFilesToUseEventSplit
                        else:
                            varMap[':nFilesTobeUsed'] = nReady + nUsed
                        if useScout:
                            if not isEventSplit:
                                # file level splitting
                                if nFilesPerJob in [None,0]:
                                    # number of files per job is not specified
                                    diagMap['nChunksForScout'] = nChunksForScout-varMap[':nFilesTobeUsed']
                                else:
                                    tmpQ,tmpR = divmod(varMap[':nFilesTobeUsed'],nFilesPerJob)
                                    diagMap['nChunksForScout'] = nChunksForScout-tmpQ
                                    if tmpR > 0:
                                        diagMap['nChunksForScout'] -= 1
                            else:
                                # event level splitting
                                if varMap[':nFilesTobeUsed'] > 0:
                                    tmpQ,tmpR = divmod(nEventsToUseEventSplit,nEventsPerJob)
                                    diagMap['nChunksForScout'] = nChunksForScout-tmpQ
                                    if tmpR > 0:
                                        diagMap['nChunksForScout'] -= 1
                        if missingFileList != [] or (isMutableDataset and nActivatedPending == 0 and nFilesUnprocessed in [0,None]):
                            if datasetSpec.isMaster() or masterStatus == None:
                                # don't change status when some files are missing or no pending inputs are activated
                                tmpLog.debug('using datasetSpec.status={0}'.format(datasetSpec.status))
                                varMap[':status'] = datasetSpec.status
                            else:
                                # use master status
                                tmpLog.debug('using masterStatus={0}'.format(masterStatus))
                                varMap[':status'] = masterStatus
                        else:
                            varMap[':status'] = 'ready'
                        # no more inputs are required even if parent is still running
                        numReqFileRecords = nMaxFiles
                        try:
                            if nEventsPerFile > nEventsPerJob:
                                numReqFileRecords = numReqFileRecords * nEventsPerFile / nEventsPerJob
                        except:
                            pass
                        tmpLog.debug("the number of requested file records : {0}".format(numReqFileRecords))
                        if isMutableDataset and numReqFileRecords != None and varMap[':nFilesTobeUsed'] >= numReqFileRecords:
                            varMap[':state'] = 'open'
                        else:
                            varMap[':state'] = datasetState
                        varMap[':stateUpdateTime'] = stateUpdateTime
                        newDsStatus = varMap[':status'] 
                        tmpLog.debug(sqlDU+comment+str(varMap))
                        self.cur.execute(sqlDU+comment,varMap)
                        # propagate number of input events to DEFT
                        if datasetSpec.isMaster():
                            varMap = {}
                            varMap[':jediTaskID'] = datasetSpec.jediTaskID
                            for tmpType in JediDatasetSpec.getInputTypes():
                                mapKey = ':type_'+tmpType
                                varMap[mapKey] = tmpType
                            tmpLog.debug(sqlCE+comment+str(varMap))
                            self.cur.execute(sqlCE+comment,varMap)
                        # return number of activated pending inputs
                        diagMap['nActivatedPending'] = nActivatedPending
                        if not nFilesUnprocessed in [0,None]:
                            diagMap['nActivatedPending'] += nFilesUnprocessed
                        # set return value
                        retVal = True,missingFileList,numUniqueLfn,diagMap
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('inserted {0} rows with {1} activated, {2} pending, {3} ready, {4} unprocessed, status={5}->{6}'.format(nInsert,
                                                                                                                                 nActivatedPending,
                                                                                                                                 nPending-nActivatedPending,
                                                                                                                                 nReady,
                                                                                                                                 nFilesUnprocessed,
                                                                                                                                 oldDsStatus,
                                                                                                                                 newDsStatus))
            regTime = datetime.datetime.utcnow() - regStart
            tmpLog.debug('took %s.%03d sec' % (regTime.seconds,regTime.microseconds/1000))
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            regTime = datetime.datetime.utcnow() - regStart
            tmpLog.debug('took %s.%03d sec' % (regTime.seconds,regTime.microseconds/1000))
            return failedRet



    # get files from the JEDI contents table with jediTaskID and/or datasetID
    def getFilesInDatasetWithID_JEDI(self,jediTaskID,datasetID,nFiles,status):
        comment = ' /* JediDBProxy.getFilesInDataset_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0} datasetID={1}>'.format(jediTaskID,datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start nFiles={0} status={1}'.format(nFiles,status))
        # return value for failure
        failedRet = False,0
        if jediTaskID==None and datasetID==None:
            tmpLog.error("either jediTaskID or datasetID is not defined")
            return failedRet
        try:
            # sql 
            varMap = {}
            sql  = "SELECT * FROM (SELECT {0} ".format(JediFileSpec.columnNames())
            sql += "FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
            useAND = False
            if jediTaskID != None:    
                sql += "jediTaskID=:jediTaskID "
                varMap[':jediTaskID'] = jediTaskID
                useAND = True
            if datasetID != None:
                if useAND:
                    sql += "AND "
                sql += "datasetID=:datasetID "
                varMap[':datasetID'] = datasetID
                useAND = True
            if status != None:
                if useAND:
                    sql += "AND "
                sql += "status=:status "
                varMap[':status'] = status
                useAND = True
            sql += " ORDER BY fileID) "
            if nFiles != None:
                sql += "WHERE rownum <= %s" % nFiles
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            # get existing file list
            self.cur.execute(sql+comment,varMap)
            tmpResList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # make file specs 
            fileSpecList = []
            for tmpRes in tmpResList:
                fileSpec = JediFileSpec()
                fileSpec.pack(tmpRes)
                fileSpecList.append(fileSpec)
            tmpLog.debug('got {0} files'.format(len(fileSpecList)))
            return True,fileSpecList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # insert dataset to the JEDI datasets table
    def insertDataset_JEDI(self,datasetSpec):
        comment = ' /* JediDBProxy.insertDataset_JEDI */'
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # set attributes
            timeNow = datetime.datetime.utcnow()
            datasetSpec.creationTime = timeNow
            datasetSpec.modificationTime = timeNow
            # sql
            sql  = "INSERT INTO {0}.JEDI_Datasets ({1}) ".format(jedi_config.db.schemaJEDI,JediDatasetSpec.columnNames())
            sql += JediDatasetSpec.bindValuesExpression()
            sql += " RETURNING datasetID INTO :newDatasetID"
            varMap = datasetSpec.valuesMap(useSeq=True)
            varMap[':newDatasetID'] = self.cur.var(cx_Oracle.NUMBER)            
            # begin transaction
            self.conn.begin()
            # insert dataset
            self.cur.execute(sql+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return True,long(varMap[':newDatasetID'].getvalue())
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,None



    # update JEDI dataset
    def updateDataset_JEDI(self,datasetSpec,criteria,lockTask):
        comment = ' /* JediDBProxy.updateDataset_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <datasetID={0}>'.format(datasetSpec.datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # return value for failure
        failedRet = False,0
        # no criteria
        if criteria == {}:
            tmpLog.error('no selection criteria')
            return failedRet
        # check criteria
        for tmpKey in criteria.keys():
            if not hasattr(datasetSpec,tmpKey):
                tmpLog.error('unknown attribute {0} is used in criteria'.format(tmpKey))
                return failedRet
        try:
            # set attributes
            timeNow = datetime.datetime.utcnow()
            datasetSpec.modificationTime = timeNow
            # values for UPDATE
            varMap = datasetSpec.valuesMap(useSeq=False,onlyChanged=True)
            # sql for update
            sql  = "UPDATE {0}.JEDI_Datasets SET {1} WHERE ".format(jedi_config.db.schemaJEDI,
                                                                    datasetSpec.bindUpdateChangesExpression())
            useAND = False
            for tmpKey,tmpVal in criteria.iteritems():
                crKey = ':cr_%s' % tmpKey
                if useAND:
                    sql += ' AND'
                else:
                    useAND = True
                sql += ' %s=%s' % (tmpKey,crKey)
                varMap[crKey] = tmpVal
                
            # sql for loc
            varMapLock = {}
            varMapLock[':jediTaskID'] = datasetSpec.jediTaskID
            sqlLock = "SELECT 1 FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE".format(jedi_config.db.schemaJEDI)
            # begin transaction
            self.conn.begin()
            # lock task
            if lockTask:
                self.cur.execute(sqlLock+comment,varMapLock)
            # update dataset
            tmpLog.debug(sql+comment+str(varMap))            
            self.cur.execute(sql+comment,varMap)
            # the number of updated rows
            nRows = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('updated {0} rows'.format(nRows))
            return True,nRows
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # update JEDI dataset attributes
    def updateDatasetAttributes_JEDI(self,jediTaskID,datasetID,attributes):
        comment = ' /* JediDBProxy.updateDatasetAttributes_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0} datasetID={1}>'.format(jediTaskID,datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # return value for failure
        failedRet = False
        try:
            # sql for update
            sql  = "UPDATE {0}.JEDI_Datasets SET ".format(jedi_config.db.schemaJEDI)
            # values for UPDATE
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':datasetID'] = datasetID
            for tmpKey,tmpVal in attributes.iteritems():
                crKey = ':{0}'.format(tmpKey)
                sql += '{0}={1},'.format(tmpKey,crKey)
                varMap[crKey] = tmpVal
            sql = sql[:-1]
            sql += ' '
            sql += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID '
            # begin transaction
            self.conn.begin()
            # update dataset
            tmpLog.debug(sql+comment+str(varMap))            
            self.cur.execute(sql+comment,varMap)
            # the number of updated rows
            nRows = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('updated {0} rows'.format(nRows))
            return True,nRows
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # get JEDI dataset attributes
    def getDatasetAttributes_JEDI(self,jediTaskID,datasetID,attributes):
        comment = ' /* JediDBProxy.getDatasetAttributes_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0} datasetID={1}>'.format(jediTaskID,datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # return value for failure
        failedRet = {}
        try:
            # sql for get attributes
            sql  = "SELECT "
            for tmpKey in attributes:
                sql += '{0},'.format(tmpKey)
            sql = sql[:-1] + ' '
            sql += "FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sql += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID '
            # values for UPDATE
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':datasetID'] = datasetID
            # begin transaction
            self.conn.begin()
            # select
            self.cur.execute(sql+comment,varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # make return
            retMap = {}
            if res != None:
                for tmpIdx,tmpKey in enumerate(attributes):
                    retMap[tmpKey] = res[tmpIdx]
            tmpLog.debug('got {0}'.format(str(retMap)))
            return retMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # get JEDI dataset attributes with map
    def getDatasetAttributesWithMap_JEDI(self,jediTaskID,criteria,attributes):
        comment = ' /* JediDBProxy.getDatasetAttributesWithMap_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0} criteria={1}>'.format(jediTaskID,str(criteria))
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # return value for failure
        failedRet = {}
        try:
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            # sql for get attributes
            sql  = "SELECT "
            for tmpKey in attributes:
                sql += '{0},'.format(tmpKey)
            sql = sql[:-1] + ' '
            sql += "FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sql += 'WHERE jediTaskID=:jediTaskID '
            for crKey,crVal in criteria.iteritems():
                sql += 'AND {0}=:{0} '.format(crKey)
                varMap[':{0}'.format(crKey)] = crVal
            # begin transaction
            self.conn.begin()
            # select
            self.cur.execute(sql+comment,varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # make return
            retMap = {}
            if res != None:
                for tmpIdx,tmpKey in enumerate(attributes):
                    retMap[tmpKey] = res[tmpIdx]
            tmpLog.debug('got {0}'.format(str(retMap)))
            return retMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet

                
        
    # get JEDI dataset with datasetID
    def getDatasetWithID_JEDI(self,jediTaskID,datasetID):
        comment = ' /* JediDBProxy.getDatasetWithID_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0} datasetID={1}>'.format(jediTaskID,datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # return value for failure
        failedRet = False,None
        try:
            # sql
            sql  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
            sql += "FROM {0}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID ".format(jedi_config.db.schemaJEDI)
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':datasetID'] = datasetID
            # begin transaction
            self.conn.begin()
            # select
            self.cur.execute(sql+comment,varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            if res != None:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(res)
            else:
                datasetSpec = None
            tmpLog.debug('done')
            return True,datasetSpec
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # get JEDI datasets with jediTaskID
    def getDatasetsWithJediTaskID_JEDI(self,jediTaskID,datasetTypes=None):
        comment = ' /* JediDBProxy.getDatasetsWithJediTaskID_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0} datasetTypes={1}>'.format(jediTaskID,datasetTypes)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # return value for failure
        failedRet = False,None
        try:
            # sql
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            sql  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
            sql += "FROM {0}.JEDI_Datasets WHERE jediTaskID=:jediTaskID ".format(jedi_config.db.schemaJEDI)
            if datasetTypes != None:
                sql += "AND type IN ("
                for tmpType in datasetTypes:
                    mapKey = ':type_'+tmpType
                    varMap[mapKey] = tmpType
                    sql += "{0},".format(mapKey)
                sql = sql[:-1]
                sql += ") "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # select
            self.cur.execute(sql+comment,varMap)
            tmpResList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # make file specs
            datasetSpecList = []
            for tmpRes in tmpResList:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(tmpRes)
                datasetSpecList.append(datasetSpec)
            tmpLog.debug('done with {0} datasets'.format(len(datasetSpecList)))
            return True,datasetSpecList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet


        
    # insert task to the JEDI task table
    def insertTask_JEDI(self,taskSpec):
        comment = ' /* JediDBProxy.insertTask_JEDI */'
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # set attributes
            timeNow = datetime.datetime.utcnow()
            taskSpec.creationDate = timeNow
            taskSpec.modificationTime = timeNow
            # sql
            sql  = "INSERT INTO {0}.JEDI_Tasks ({1}) ".format(jedi_config.db.schemaJEDI,JediTaskSpec.columnNames())
            sql += JediTaskSpec.bindValuesExpression()
            varMap = taskSpec.valuesMap()
            # begin transaction
            self.conn.begin()
            # insert dataset
            self.cur.execute(sql+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # update JEDI task status by ContentsFeeder
    def updateTaskStatusByContFeeder_JEDI(self,jediTaskID,taskSpec=None,getTaskStatus=False,pid=None,setFrozenTime=True,
                                          useWorldCloud=False):
        comment = ' /* JediDBProxy.updateTaskStatusByContFeeder_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to check status
            sqlS  = "SELECT status,lockedBy,cloud,prodSourceLabel,frozenTime FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlS += "WHERE jediTaskID=:jediTaskID FOR UPDATE "
            # sql to get number of unassigned datasets
            sqlD  = "SELECT COUNT(*) FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlD += "WHERE jediTaskID=:jediTaskID AND destination IS NULL AND type IN (:type1,:type2) "
            # sql to update task
            sqlU  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlU += "SET status=:status,modificationTime=:updateTime,stateChangeTime=CURRENT_DATE,"
            sqlU += "lockedBy=NULL,lockedTime=NULL,frozenTime=:frozenTime"
            if taskSpec != None:
                sqlU += ",oldStatus=:oldStatus,errorDialog=:errorDialog"
            sqlU += " WHERE jediTaskID=:jediTaskID "
            # sql to unlock task
            sqlL  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlL += "SET lockedBy=NULL,lockedTime=NULL "
            sqlL += "WHERE jediTaskID=:jediTaskID AND status=:status "
            if pid != None: 
                sqlL += "AND lockedBy=:pid "
            # begin transaction
            self.conn.begin()
            # check status
            taskStatus = None
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            tmpLog.debug(sqlS+comment+str(varMap))
            self.cur.execute(sqlS+comment,varMap)            
            res = self.cur.fetchone()
            if res == None:
                tmpLog.debug('task is not found in Tasks table')
            else:
                taskStatus,lockedBy,cloudName,prodSourceLabel,frozenTime = res
                if lockedBy != pid:
                    # task is locked
                    tmpLog.debug('task is locked by {0}'.format(lockedBy))
                elif not taskStatus in JediTaskSpec.statusToUpdateContents():
                    # task status is irrelevant
                    tmpLog.debug('task.status={0} is not for contents update'.format(taskStatus))
                    # unlock
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':status']     = taskStatus
                    if pid != None:
                        varMap[':pid'] = pid
                    self.cur.execute(sqlL+comment,varMap)
                    tmpLog.debug('unlocked')
                else:
                    # get number of unassigned datasets
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':type1'] = 'output'
                    varMap[':type2'] = 'log'
                    self.cur.execute(sqlD+comment,varMap)
                    nUnassignedDSs, = self.cur.fetchone()
                    # update task
                    timeNow = datetime.datetime.utcnow()
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':updateTime'] = timeNow
                    if taskSpec != None:
                        # new task status is specified 
                        varMap[':status']      = taskSpec.status
                        varMap[':oldStatus']   = taskSpec.oldStatus
                        varMap[':errorDialog'] = taskSpec.errorDialog
                        # set/unset frozen time
                        if taskSpec.status == 'pending' and setFrozenTime:
                            if frozenTime == None:
                                varMap[':frozenTime'] = timeNow
                            else:
                                varMap[':frozenTime'] = frozenTime
                        else:
                            varMap[':frozenTime'] = None
                    elif (cloudName == None or (useWorldCloud and nUnassignedDSs > 0)) \
                            and prodSourceLabel in ['managed','test']:
                        # set assigning for TaskBrokerage
                        varMap[':status'] = 'assigning'
                        varMap[':frozenTime'] = None
                        # set old update time to trigger TaskBrokerage immediately
                        varMap[':updateTime'] = datetime.datetime.utcnow() - datetime.timedelta(hours=6)
                    else:
                        # skip task brokerage since cloud is preassigned
                        varMap[':status'] = 'ready'
                        varMap[':frozenTime'] = None
                        # set old update time to trigger JG immediately
                        varMap[':updateTime'] = datetime.datetime.utcnow() - datetime.timedelta(hours=6)
                    tmpLog.debug(sqlU+comment+str(varMap))
                    self.cur.execute(sqlU+comment,varMap)
                    # update DEFT task status
                    taskStatus = varMap[':status']
                    if taskStatus in ['broken','assigning']:
                        sqlD  = "UPDATE {0}.T_TASK ".format(jedi_config.db.schemaDEFT)
                        sqlD += "SET status=:status,timeStamp=CURRENT_DATE "
                        sqlD += "WHERE taskID=:jediTaskID "
                        varMap = {}
                        varMap[':status'] = taskStatus
                        varMap[':jediTaskID'] = jediTaskID
                        tmpLog.debug(sqlD+comment+str(varMap))
                        self.cur.execute(sqlD+comment,varMap)
                        self.setSuperStatus_JEDI(jediTaskID,taskStatus)
                    tmpLog.debug('set to {0}'.format(taskStatus))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            if not getTaskStatus:
                return True
            else:
                return True,taskStatus
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            if not getTaskStatus:
                return False
            else:
                return False,None



    # update JEDI task
    def updateTask_JEDI(self,taskSpec,criteria,oldStatus=None,updateDEFT=True,insertUnknown=None,setFrozenTime=True):
        comment = ' /* JediDBProxy.updateTask_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(taskSpec.jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # return value for failure
        failedRet = False,0
        # no criteria
        if criteria == {}:
            tmpLog.error('no selection criteria')
            return failedRet
        # check criteria
        for tmpKey in criteria.keys():
            if not hasattr(taskSpec,tmpKey):
                tmpLog.error('unknown attribute {0} is used in criteria'.format(tmpKey))
                return failedRet
        try:
            # set attributes
            timeNow = datetime.datetime.utcnow()
            taskSpec.modificationTime = timeNow
            # sql to get old status
            sqlS  = "SELECT status,frozenTime FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI) 
            sql = 'WHERE '
            varMap = {}
            for tmpKey,tmpVal in criteria.iteritems():
                crKey = ':cr_{0}'.format(tmpKey)
                sql += '{0}={1} AND '.format(tmpKey,crKey)
                varMap[crKey] = tmpVal
            if oldStatus != None:
                sql += 'status IN ('
                for tmpStat in oldStatus:
                    crKey = ':old_{0}'.format(tmpStat)
                    sql += '{0},'.format(crKey)
                    varMap[crKey] = tmpStat
                sql = sql[:-1]
                sql += ') AND '
            sql = sql[:-4]
            # begin transaction
            self.conn.begin()
            # get old status
            frozenTime = None
            self.cur.execute(sqlS+sql+comment,varMap)
            res = self.cur.fetchone()
            if res != None:
                statusInDB,frozenTime = res
                if statusInDB != taskSpec.status:
                    taskSpec.stateChangeTime = timeNow
            # set/unset frozen time
            if taskSpec.status == 'pending' and setFrozenTime:
                if frozenTime == None:
                    taskSpec.frozenTime = timeNow
            else:
                if frozenTime != None:
                    taskSpec.frozenTime = None
            # update task
            sqlU  = "UPDATE {0}.JEDI_Tasks SET {1} ".format(jedi_config.db.schemaJEDI,
                                                            taskSpec.bindUpdateChangesExpression())
            for tmpKey,tmpVal in taskSpec.valuesMap(useSeq=False,onlyChanged=True).iteritems():
                varMap[tmpKey] = tmpVal
            tmpLog.debug(sqlU+sql+comment+str(varMap))
            self.cur.execute(sqlU+sql+comment,varMap)
            # insert unknown datasets
            if insertUnknown != None:
                # sql to check
                sqlUC  = "SELECT datasetID FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
                sqlUC += "WHERE jediTaskID=:jediTaskID AND type=:type AND datasetName=:datasetName "
                # sql to insert dataset
                sqlUI  = "INSERT INTO {0}.JEDI_Datasets ({1}) ".format(jedi_config.db.schemaJEDI,JediDatasetSpec.columnNames())
                sqlUI += JediDatasetSpec.bindValuesExpression()
                # loop over all datasets
                for tmpUnknownDataset in insertUnknown:
                    # check if already in DB
                    varMap = {}
                    varMap[':type'] = JediDatasetSpec.getUnknownInputType()
                    varMap[':jediTaskID'] = taskSpec.jediTaskID
                    varMap[':datasetName'] = tmpUnknownDataset
                    self.cur.execute(sqlUC+comment,varMap)
                    resUC = self.cur.fetchone()
                    if resUC == None:
                        # insert dataset
                        datasetSpec = JediDatasetSpec()
                        datasetSpec.jediTaskID = taskSpec.jediTaskID
                        datasetSpec.datasetName = tmpUnknownDataset
                        datasetSpec.creationTime = datetime.datetime.utcnow()
                        datasetSpec.modificationTime = datasetSpec.creationTime
                        datasetSpec.type = JediDatasetSpec.getUnknownInputType()
                        varMap = datasetSpec.valuesMap(useSeq=True)
                        self.cur.execute(sqlUI+comment,varMap)
            # the number of updated rows
            nRows = self.cur.rowcount
            # update DEFT
            if updateDEFT:
                # count number of finished jobs
                sqlC  = "SELECT count(distinct pandaID) "
                sqlC += "FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(jedi_config.db.schemaJEDI)
                sqlC += "WHERE tabD.jediTaskID=tabC.jediTaskID AND tabD.jediTaskID=:jediTaskID "
                sqlC += "AND tabC.status=:status "
                sqlC += "AND masterID IS NULL AND pandaID IS NOT NULL "
                varMap = {}
                varMap[':jediTaskID'] = taskSpec.jediTaskID
                varMap[':status'] = 'finished'
                self.cur.execute(sqlC+comment,varMap)
                res = self.cur.fetchone()
                if res == None:
                    tmpLog.debug('failed to count # of finished jobs when updating DEFT table')
                else:
                    nDone, = res 
                    sqlD  = "UPDATE {0}.T_TASK ".format(jedi_config.db.schemaDEFT)
                    sqlD += "SET status=:status,total_done_jobs=:nDone,timeStamp=CURRENT_DATE "
                    sqlD += "WHERE taskID=:jediTaskID "
                    varMap = {}
                    varMap[':status'] = taskSpec.status
                    varMap[':jediTaskID'] = taskSpec.jediTaskID
                    varMap[':nDone'] = nDone
                    tmpLog.debug(sqlD+comment+str(varMap))
                    self.cur.execute(sqlD+comment,varMap)
                    self.setSuperStatus_JEDI(taskSpec.jediTaskID,taskSpec.status)
            elif taskSpec.status in ['running','broken','assigning','scouting','aborted','aborting']:
                # update DEFT task status
                if taskSpec.status == 'scouting':
                    deftStatus = 'submitting'
                else:
                    deftStatus = taskSpec.status
                sqlD  = "UPDATE {0}.T_TASK ".format(jedi_config.db.schemaDEFT)
                sqlD += "SET status=:status,timeStamp=CURRENT_DATE"
                if taskSpec.status == 'scouting':
                    sqlD += ",start_time=CURRENT_DATE"
                sqlD += " WHERE taskID=:jediTaskID "
                varMap = {}
                varMap[':status'] = deftStatus
                varMap[':jediTaskID'] = taskSpec.jediTaskID
                tmpLog.debug(sqlD+comment+str(varMap))
                self.cur.execute(sqlD+comment,varMap)
                self.setSuperStatus_JEDI(taskSpec.jediTaskID,deftStatus)
                if taskSpec.status == 'running':
                    varMap = {}
                    varMap[':jediTaskID'] = taskSpec.jediTaskID
                    sqlDS  = "UPDATE {0}.T_TASK ".format(jedi_config.db.schemaDEFT)
                    sqlDS += "SET start_time=timeStamp "
                    sqlDS += "WHERE taskID=:jediTaskID AND start_time IS NULL "
                    tmpLog.debug(sqlDS+comment+str(varMap))
                    self.cur.execute(sqlDS+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('updated {0} rows'.format(nRows))
            return True,nRows
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # update JEDI task lock
    def updateTaskLock_JEDI(self,jediTaskID):
        comment = ' /* JediDBProxy.updateTaskLock_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # return value for failure
        failedRet = False
        try:
            # sql to update lock
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            sqlS  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI) 
            sqlS += 'SET lockedTime=CURRENT_DATE '
            sqlS += 'WHERE jediTaskID=:jediTaskID '
            # begin transaction
            self.conn.begin()
            # get old status
            self.cur.execute(sqlS+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # get JEDI task with ID
    def getTaskWithID_JEDI(self,jediTaskID,fullFlag,lockTask=False,pid=None,lockInterval=None):
        comment = ' /* JediDBProxy.getTaskWithID_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start lockTask={0}'.format(lockTask))
        # return value for failure
        failedRet = False,None
        try:
            # sql
            sql  = "SELECT {0} ".format(JediTaskSpec.columnNames())
            sql += "FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID ".format(jedi_config.db.schemaJEDI)
            if lockInterval != None:
                sql += "AND (lockedTime IS NULL OR lockedTime<:timeLimit) "
            if lockTask:
                sql += "AND lockedBy IS NULL FOR UPDATE NOWAIT"
            sqlLock  = "UPDATE {0}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlLock += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            if lockInterval != None:
                varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=lockInterval)
            # begin transaction
            self.conn.begin()
            # select
            res = None
            try:
                self.cur.execute(sql+comment,varMap)
                res = self.cur.fetchone()
                if res != None:
                    # template to generate job parameters
                    jobParamsTemplate = None
                    if fullFlag:
                        # sql to read template
                        sqlJobP  = "SELECT jobParamsTemplate FROM {0}.JEDI_JobParams_Template ".format(jedi_config.db.schemaJEDI)
                        sqlJobP += "WHERE jediTaskID=:jediTaskID "
                        self.cur.execute(sqlJobP+comment,varMap)
                        for clobJobP, in self.cur:
                            if clobJobP != None:
                                jobParamsTemplate = clobJobP.read()
                                break
                    if lockTask:
                        varMap = {}
                        varMap[':lockedBy'] = pid
                        varMap[':jediTaskID'] = jediTaskID
                        self.cur.execute(sqlLock+comment,varMap)
            except:
                errType,errValue = sys.exc_info()[:2]
                if self.isNoWaitException(errValue):
                    # resource busy and acquire with NOWAIT specified
                    tmpLog.debug('skip locked')
                else:
                    # failed with something else
                    raise errType,errValue
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            if res != None:
                taskSpec = JediTaskSpec()
                taskSpec.pack(res)
                if jobParamsTemplate != None:
                    taskSpec.jobParamsTemplate = jobParamsTemplate
            else:
                taskSpec = None
            if taskSpec == None:
                tmpLog.debug('done with skip')
            else:
                tmpLog.debug('done with got')
            return True,taskSpec
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # get JEDI task and datasets with ID and lock it
    def getTaskDatasetsWithID_JEDI(self,jediTaskID,pid,lockTask=True):
        comment = ' /* JediDBProxy.getTaskDatasetsWithID_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start pid={0}'.format(pid))
        # return value for failure
        failedRet = False,None
        try:
            # sql
            sql  = "SELECT {0} ".format(JediTaskSpec.columnNames())
            sql += "FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL ".format(jedi_config.db.schemaJEDI)
            if lockTask:
                sql += "FOR UPDATE NOWAIT"
            sqlLK  = "UPDATE {0}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlLK += "WHERE jediTaskID=:jediTaskID "
            sqlDS  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
            sqlDS += "FROM {0}.JEDI_Datasets WHERE jediTaskID=:jediTaskID ".format(jedi_config.db.schemaJEDI)
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # select
            res = None
            try:
                # read task
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                self.cur.execute(sql+comment,varMap)
                res = self.cur.fetchone()
                if res == None:
                    taskSpec = None
                else:
                    taskSpec = JediTaskSpec()
                    taskSpec.pack(res)
                    # lock task
                    if lockTask:
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        self.cur.execute(sqlLK+comment,varMap)
                    # read datasets
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    self.cur.execute(sqlDS+comment,varMap)
                    resList = self.cur.fetchall()
                    for res in resList:
                        datasetSpec = JediDatasetSpec()
                        datasetSpec.pack(res)
                        taskSpec.datasetSpecList.append(datasetSpec)
            except:
                errType,errValue = sys.exc_info()[:2]
                if self.isNoWaitException(errValue):
                    # resource busy and acquire with NOWAIT specified                                                                                      
                    tmpLog.debug('skip locked')
                else:
                    # failed with something else                                                                                                           
                    raise errType,errValue
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return True,taskSpec
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # get JEDI tasks with selection criteria
    def getTaskIDsWithCriteria_JEDI(self,criteria,nTasks=50):
        comment = ' /* JediDBProxy.getTaskIDsWithCriteria_JEDI */'
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # return value for failure
        failedRet = None
        # no criteria
        if criteria == {}:
            tmpLog.error('no selection criteria')
            return failedRet
        # check criteria
        for tmpKey in criteria.keys():
            if not tmpKey in JediTaskSpec.attributes:
                tmpLog.error('unknown attribute {0} is used in criteria'.format(tmpKey))
                return failedRet
        varMap = {}
        try:
            # sql
            sql  = "SELECT jediTaskID "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            isFirst = True
            for tmpKey,tmpVal in criteria.iteritems():
                if not isFirst:
                    sql += "AND "
                else:
                    isFirst = False
                if tmpVal in ['NULL','NOT NULL']:
                    sql += '{0} IS {1} '.format(tmpKey,tmpVal)
                elif tmpVal == None:
                    sql += '{0} IS NULL '.format(tmpKey)
                else:
                    crKey = ':cr_{0}'.format(tmpKey)
                    sql += '{0}={1} '.format(tmpKey,crKey)
                    varMap[crKey] = tmpVal
            sql += 'AND rownum<={0}'.format(nTasks)
            # begin transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10000
            tmpLog.debug(sql+comment+str(varMap))
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            # collect jediTaskIDs
            retTaskIDs = []
            for jediTaskID, in resList:
                retTaskIDs.append(jediTaskID)
            retTaskIDs.sort()    
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('got {0} tasks'.format(len(retTaskIDs)))
            return retTaskIDs
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # get JEDI tasks to be finished
    def getTasksToBeFinished_JEDI(self,vo,prodSourceLabel,pid,nTasks=50):
        comment = ' /* JediDBProxy.getTasksToBeFinished_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1} pid={2}>'.format(vo,prodSourceLabel,pid)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # return value for failure
        failedRet = None
        try:
            # sql
            varMap = {}
            varMap[':status1'] = 'prepared'
            varMap[':status2'] = 'scouted'
            varMap[':status3'] = 'tobroken'
            varMap[':status4'] = 'toabort'
            varMap[':status5'] = 'passed'
            sqlRT  = "SELECT tabT.jediTaskID,tabT.status "
            sqlRT += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlRT += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlRT += "AND tabT.status IN (:status1,:status2,:status3,:status4,:status5) "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlRT += "AND tabT.vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlRT += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sqlRT += "AND (lockedBy IS NULL OR lockedTime<:timeLimit) "
            sqlRT += "AND rownum<{0} ".format(nTasks)
            sqlNW  = "SELECT jediTaskID FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlNW += "WHERE jediTaskID=:jediTaskID FOR UPDATE NOWAIT"
            sqlLK  = "UPDATE {0}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlLK += "WHERE jediTaskID=:jediTaskID AND (lockedBy IS NULL OR lockedTime<:timeLimit) AND status=:status "
            sqlTS  = "SELECT {0} ".format(JediTaskSpec.columnNames())
            sqlTS += "FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTS += "WHERE jediTaskID=:jediTaskID "
            sqlDS  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
            sqlDS += "FROM {0}.JEDI_Datasets WHERE jediTaskID=:jediTaskID ".format(jedi_config.db.schemaJEDI)
            sqlSC  = "UPDATE {0}.JEDI_Tasks SET status=:newStatus,modificationTime=:updateTime,stateChangeTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlSC += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=10)
            varMap[':timeLimit'] = timeLimit
            tmpLog.debug(sqlRT+comment+str(varMap))
            self.cur.execute(sqlRT+comment,varMap)
            resList = self.cur.fetchall()
            retTasks = []
            allTasks = []
            taskStatList = []
            for jediTaskID,taskStatus in resList:
                taskStatList.append((jediTaskID,taskStatus))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # get tasks and datasets    
            for jediTaskID,taskStatus in taskStatList:
                # begin transaction
                self.conn.begin()
                # check task
                try:
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    self.cur.execute(sqlNW+comment,varMap)
                except:
                    tmpLog.debug('skip locked jediTaskID={0}'.format(jediTaskID))
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    continue
                # special action for scouted
                if taskStatus == 'scouted':
                    # make avalanche
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':newStatus'] = 'running'
                    varMap[':oldStatus'] = taskStatus
                    # set old update time to trigger JG immediately
                    varMap[':updateTime'] = datetime.datetime.utcnow() - datetime.timedelta(hours=6)
                    self.cur.execute(sqlSC+comment,varMap)
                    nRows = self.cur.rowcount
                    tmpLog.debug("changed status to {0} for jediTaskID={1} with {2}".format(varMap[':newStatus'],
                                                                                            jediTaskID,nRows))
                else:
                    # lock task
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':lockedBy'] = pid
                    varMap[':status'] = taskStatus
                    varMap[':timeLimit'] = timeLimit
                    self.cur.execute(sqlLK+comment,varMap)
                    nRows = self.cur.rowcount
                    if nRows == 1:
                        # read task
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        self.cur.execute(sqlTS+comment,varMap)
                        resTS = self.cur.fetchone()
                        if resTS != None:
                            taskSpec = JediTaskSpec()
                            taskSpec.pack(resTS)
                            retTasks.append(taskSpec)
                            # read datasets
                            varMap = {}
                            varMap[':jediTaskID'] = taskSpec.jediTaskID
                            self.cur.execute(sqlDS+comment,varMap)
                            resList = self.cur.fetchall()
                            for resDS in resList:
                                datasetSpec = JediDatasetSpec()
                                datasetSpec.pack(resDS)
                                taskSpec.datasetSpecList.append(datasetSpec)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            tmpLog.debug('got {0} tasks'.format(len(retTasks)))
            return retTasks
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # get job statistics with work queue
    def getJobStatisticsWithWorkQueue_JEDI(self,vo,prodSourceLabel,minPriority=None,cloud=None):
        comment = ' /* DBProxy.getJobStatisticsWithWorkQueue_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1} cloud={2}>'.format(vo,prodSourceLabel,cloud)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start minPriority={0}'.format(minPriority))
        sql0 = "SELECT computingSite,cloud,jobStatus,workQueue_ID,COUNT(*) FROM %s "
        sql0 += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
        if cloud != None:
            sql0 += "AND cloud=:cloud "
        tmpPrioMap = {}
        if minPriority != None:
            sql0 += "AND currentPriority>=:minPriority "
            tmpPrioMap[':minPriority'] = minPriority
        sql0 += "GROUP BY computingSite,cloud,prodSourceLabel,jobStatus,workQueue_ID "
        sqlMV = sql0
        sqlMV = re.sub('COUNT\(\*\)','SUM(num_of_jobs)',sqlMV)
        sqlMV = re.sub('SELECT ','SELECT /*+ RESULT_CACHE */ ',sqlMV)        
        tables = ['{0}.jobsActive4'.format(jedi_config.db.schemaPANDA),
                  '{0}.jobsDefined4'.format(jedi_config.db.schemaPANDA)]
        if minPriority != None:
            # read the number of running jobs with prio<=MIN
            tables.append('{0}.jobsActive4'.format(jedi_config.db.schemaPANDA))
            sqlMVforRun = re.sub('currentPriority>=','currentPriority<=',sqlMV)
        varMap = {}
        varMap[':vo'] = vo
        varMap[':prodSourceLabel'] = prodSourceLabel
        if cloud != None:
            varMap[':cloud'] = cloud
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
                if table == '{0}.jobsActive4'.format(jedi_config.db.schemaPANDA):
                    mvTableName = '{0}.MV_JOBSACTIVE4_STATS'.format(jedi_config.db.schemaPANDA)
                    # first count non-running and then running if minPriority is specified
                    if minPriority != None:
                        if iActive == 0:
                            useRunning = False
                        else:
                            useRunning = True
                        iActive += 1
                    if useRunning in [None,False]:    
                        sqlExeTmp = (sqlMV+comment) % mvTableName
                    else:
                        sqlExeTmp = (sqlMVforRun+comment) % mvTableName
                else:
                    sqlExeTmp = (sql0+comment) % table
                self.cur.execute(sqlExeTmp,varMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # create map
                for computingSite,cloud,jobStatus,workQueue_ID,nCount in res:
                    # count the number of non-running with prio>=MIN
                    if useRunning == True and jobStatus != 'running':
                        continue
                    # count the number of running with prio<=MIN
                    if  useRunning == False and jobStatus == 'running':
                        continue
                    # add site
                    if not returnMap.has_key(computingSite):
                        returnMap[computingSite] = {}
                    # add cloud
                    if not returnMap[computingSite].has_key(cloud):
                        returnMap[computingSite][cloud] = {}
                    # add workQueue
                    if not returnMap[computingSite][cloud].has_key(workQueue_ID):
                        returnMap[computingSite][cloud][workQueue_ID] = {}
                    # add jobstatus
                    if not returnMap[computingSite][cloud][workQueue_ID].has_key(jobStatus):
                        returnMap[computingSite][cloud][workQueue_ID][jobStatus] = 0
                    # add    
                    returnMap[computingSite][cloud][workQueue_ID][jobStatus] += nCount
            # return
            tmpLog.debug('done')
            return True,returnMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,{}



    # generate output files for task, and instantiate template datasets if necessary
    def getOutputFiles_JEDI(self,jediTaskID,provenanceID,simul,instantiateTmpl,instantiatedSites,isUnMerging,
                            isPrePro,xmlConfigJob,siteDsMap,middleName,registerDatasets,parallelOutMap):
        comment = ' /* JediDBProxy.getOutputFiles_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start with simul={0} instantiateTmpl={1} instantiatedSites={2}'.format(simul,
                                                                                            instantiateTmpl,
                                                                                            instantiatedSites))
        tmpLog.debug('isUnMerging={0} isPrePro={1} xmlConfigJob={2}'.format(isUnMerging,isPrePro,type(xmlConfigJob)))
        tmpLog.debug('middleName={0} registerDatasets={1}'.format(middleName,registerDatasets))
        try:
            if instantiatedSites == None:
                instantiatedSites = ''
            if siteDsMap == None:
                siteDsMap = {}
            if parallelOutMap == None:
                parallelOutMap = {}
            outMap = {}
            datasetToRegister = []
            # sql to get dataset
            sqlD  = "SELECT "
            sqlD += "datasetID,datasetName,vo,masterID,status,type FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlD += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            if provenanceID != None:
                sqlD += "AND (provenanceID IS NULL OR provenanceID=:provenanceID) "
            # sql to read template
            sqlR  = "SELECT outTempID,datasetID,fileNameTemplate,serialNr,outType,streamName "
            sqlR += "FROM {0}.JEDI_Output_Template ".format(jedi_config.db.schemaJEDI)
            sqlR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID FOR UPDATE"
            # sql to insert files
            sqlI  = "INSERT INTO {0}.JEDI_Dataset_Contents ({1}) ".format(jedi_config.db.schemaJEDI,JediFileSpec.columnNames())
            sqlI += JediFileSpec.bindValuesExpression()
            sqlI += " RETURNING fileID INTO :newFileID"
            # sql to increment SN
            sqlU  = "UPDATE {0}.JEDI_Output_Template SET serialNr=serialNr+1 ".format(jedi_config.db.schemaJEDI)
            sqlU += "WHERE jediTaskID=:jediTaskID AND outTempID=:outTempID "
            # sql to instantiate template dataset
            sqlT1  = "SELECT {0} FROM {1}.JEDI_Datasets ".format(JediDatasetSpec.columnNames(),
                                                                 jedi_config.db.schemaJEDI)
            sqlT1 += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlT2  = "INSERT INTO {0}.JEDI_Datasets ({1}) ".format(jedi_config.db.schemaJEDI,
                                                                   JediDatasetSpec.columnNames())
            sqlT2 += JediDatasetSpec.bindValuesExpression()
            sqlT2 += "RETURNING datasetID INTO :newDatasetID "
            # sql to change concrete dataset name
            sqlCN  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlCN += "SET site=:site,datasetName=:datasetName,destination=:destination "
            sqlCN += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to set masterID to concrete datasets
            sqlMC  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlMC += "SET masterID=:masterID "
            sqlMC += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # current current date
            timeNow = datetime.datetime.utcnow()
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 100
            # get datasets
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':type1'] = 'output'
            varMap[':type2'] = 'log'
            # unmerged datasets
            if isUnMerging:
                varMap[':type1'] = 'trn_' + varMap[':type1']
                varMap[':type2'] = 'trn_' + varMap[':type2']
            elif isPrePro:
                varMap[':type1'] = 'pp_' + varMap[':type1']
                varMap[':type2'] = 'pp_' + varMap[':type2']
            # template datasets
            if instantiateTmpl:
                varMap[':type1'] = 'tmpl_' + varMap[':type1']
                varMap[':type2'] = 'tmpl_' + varMap[':type2']
            # keep dataset types
            tmpl_VarMap = {}
            tmpl_VarMap[':type1'] = varMap[':type1']
            tmpl_VarMap[':type2'] = varMap[':type2']
            if provenanceID != None:
                varMap[':provenanceID'] = provenanceID
            self.cur.execute(sqlD+comment,varMap)
            resList = self.cur.fetchall()
            tmpl_RelationMap = {}
            mstr_RelationMap = {}
            for datasetID,datasetName,vo,masterID,datsetStatus,datasetType in resList:
                fileDatasetIDs = []
                for instantiatedSite in instantiatedSites.split(','):
                    fileDatasetID = datasetID
                    if registerDatasets and datasetType in ['output','log'] and not fileDatasetID in datasetToRegister:
                        datasetToRegister.append(fileDatasetID)
                    # instantiate template datasets
                    if instantiateTmpl:
                        doInstantiate = False
                        if isUnMerging:
                            # instantiate new datasets in each submission for premerged 
                            if siteDsMap.has_key(datasetID) and siteDsMap[datasetID].has_key(instantiatedSite):
                                fileDatasetID = siteDsMap[datasetID][instantiatedSite]
                                tmpLog.debug('found concrete premerged datasetID={0}'.format(fileDatasetID))
                            else:
                                doInstantiate = True
                        else:
                            # check if concrete dataset is already there
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':type1']    = re.sub('^tmpl_','',tmpl_VarMap[':type1'])
                            varMap[':type2']    = re.sub('^tmpl_','',tmpl_VarMap[':type2'])
                            varMap[':templateID'] = datasetID
                            varMap[':closedState'] = 'closed'
                            if provenanceID != None:
                                varMap[':provenanceID'] = provenanceID
                            if instantiatedSite != None:
                                sqlDT = sqlD + "AND site=:site "
                                varMap[':site'] = instantiatedSite
                            else:
                                sqlDT = sqlD
                            sqlDT += "AND (state IS NULL OR state<>:closedState) "
                            sqlDT += "AND templateID=:templateID "    
                            self.cur.execute(sqlDT+comment,varMap)
                            resDT = self.cur.fetchone()
                            if resDT != None:
                                fileDatasetID = resDT[0]
                                # collect ID of dataset to be registered 
                                if resDT[-1] == 'defined':
                                    datasetToRegister.append(fileDatasetID)
                                tmpLog.debug('found concrete datasetID={0}'.format(fileDatasetID))
                            else:
                                doInstantiate = True
                        if doInstantiate:
                            # read dataset template
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID']  = datasetID
                            self.cur.execute(sqlT1+comment,varMap)
                            resT1 = self.cur.fetchone()
                            cDatasetSpec = JediDatasetSpec()
                            cDatasetSpec.pack(resT1)
                            # instantiate template dataset
                            cDatasetSpec.type             = re.sub('^tmpl_','',cDatasetSpec.type)
                            cDatasetSpec.templateID       = datasetID
                            cDatasetSpec.creationTime     = timeNow
                            cDatasetSpec.modificationTime = timeNow
                            varMap = cDatasetSpec.valuesMap(useSeq=True)
                            varMap[':newDatasetID'] = self.cur.var(cx_Oracle.NUMBER)
                            self.cur.execute(sqlT2+comment,varMap)
                            fileDatasetID = long(varMap[':newDatasetID'].getvalue())
                            if instantiatedSite != None:
                                # set concreate name
                                cDatasetSpec.site = instantiatedSite
                                cDatasetSpec.datasetName = re.sub('/*$','.{0}'.format(fileDatasetID),datasetName)
                                # set destination
                                if cDatasetSpec.destination in [None,'']:
                                    cDatasetSpec.destination = cDatasetSpec.site
                                varMap = {}
                                varMap[':datasetName'] = cDatasetSpec.datasetName
                                varMap[':jediTaskID'] = jediTaskID
                                varMap[':datasetID'] = fileDatasetID
                                varMap[':site'] = cDatasetSpec.site
                                varMap[':destination'] = cDatasetSpec.destination
                                self.cur.execute(sqlCN+comment,varMap)
                            tmpLog.debug('instantiated {0} datasetID={1}'.format(cDatasetSpec.datasetName,fileDatasetID))
                            if masterID != None:
                                mstr_RelationMap[fileDatasetID] = (masterID,instantiatedSite)
                            # collect ID of dataset to be registered 
                            if not fileDatasetID in datasetToRegister: 
                                datasetToRegister.append(fileDatasetID)
                            # collect IDs for pre-merging
                            if isUnMerging:
                                if not siteDsMap.has_key(datasetID):
                                    siteDsMap[datasetID] = {}
                                if not siteDsMap[datasetID].has_key(instantiatedSite):
                                    siteDsMap[datasetID][instantiatedSite] = fileDatasetID
                        # keep relation between template and concrete    
                        if not datasetID in tmpl_RelationMap:
                            tmpl_RelationMap[datasetID] = {}
                        tmpl_RelationMap[datasetID][instantiatedSite] = fileDatasetID
                    fileDatasetIDs.append(fileDatasetID)
                # get output templates
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':datasetID']  = datasetID
                self.cur.execute(sqlR+comment,varMap)
                resTmpList = self.cur.fetchall()
                maxSerialNr = None
                for resR in resTmpList:
                    # make FileSpec
                    outTempID,datasetID,fileNameTemplate,serialNr,outType,streamName = resR
                    if xmlConfigJob == None or outType.endswith('log'):
                        fileNameTemplateList = [(fileNameTemplate,streamName)]
                    else:
                        fileNameTemplateList = []
                        # get output filenames from XML config
                        for tmpFileName in xmlConfigJob.outputs().split(','):
                            # ignore empty
                            if tmpFileName == '':
                                continue
                            newStreamName = tmpFileName
                            newFileNameTemplate = fileNameTemplate + '.' + xmlConfigJob.prepend_string() + '.' + newStreamName
                            fileNameTemplateList.append((newFileNameTemplate,newStreamName))
                    # loop over all filename templates
                    for fileNameTemplate,streamName in fileNameTemplateList:
                        firstFileID = None
                        for fileDatasetID in fileDatasetIDs:
                            fileSpec = JediFileSpec()
                            fileSpec.jediTaskID   = jediTaskID
                            fileSpec.datasetID = fileDatasetID
                            nameTemplate = fileNameTemplate.replace('${SN}','{SN:06d}')
                            nameTemplate = nameTemplate.replace('${SN/P}','{SN:06d}')
                            nameTemplate = nameTemplate.replace('${SN','{SN')
                            nameTemplate = nameTemplate.replace('${MIDDLENAME}',middleName)
                            fileSpec.lfn          = nameTemplate.format(SN=serialNr)
                            fileSpec.status       = 'defined'
                            fileSpec.creationDate = timeNow
                            fileSpec.type         = outType
                            fileSpec.keepTrack    = 1
                            if maxSerialNr == None or maxSerialNr < serialNr:
                                maxSerialNr = serialNr
                            # scope
                            if vo in jedi_config.ddm.voWithScope.split(','):
                                fileSpec.scope = self.extractScope(datasetName)
                            if not simul:    
                                # insert
                                varMap = fileSpec.valuesMap(useSeq=True)
                                varMap[':newFileID'] = self.cur.var(cx_Oracle.NUMBER)
                                self.cur.execute(sqlI+comment,varMap)
                                fileSpec.fileID = long(varMap[':newFileID'].getvalue())
                                # increment SN
                                varMap = {}
                                varMap[':jediTaskID'] = jediTaskID
                                varMap[':outTempID']  = outTempID
                                self.cur.execute(sqlU+comment,varMap)
                                nRow = self.cur.rowcount
                                if nRow != 1:
                                    raise RuntimeError, 'Failed to increment SN for outTempID={0}'.format(outTempID)
                            else:
                                # set dummy for simulation
                                fileSpec.fileID = fileSpec.datasetID
                            # append
                            if firstFileID == None:
                                outMap[streamName] = fileSpec
                                firstFileID = fileSpec.fileID
                                parallelOutMap[firstFileID] = []
                            parallelOutMap[firstFileID].append(fileSpec)
            # set masterID to concrete datasets 
            for fileDatasetID,(masterID,instantiatedSite) in mstr_RelationMap.iteritems():
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':datasetID']  = fileDatasetID
                if masterID in tmpl_RelationMap and instantiatedSite in tmpl_RelationMap[masterID]:
                    varMap[':masterID'] = tmpl_RelationMap[masterID][instantiatedSite]
                else:
                    varMap[':masterID'] = masterID
                self.cur.execute(sqlMC+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return outMap,maxSerialNr,datasetToRegister,siteDsMap,parallelOutMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None,None,None,siteDsMap,parallelOutMap


    # insert output file templates
    def insertOutputTemplate_JEDI(self,templates):
        comment = ' /* JediDBProxy.insertOutputTemplate_JEDI */'
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # begin transaction
            self.conn.begin()
            # loop over all templates
            for template in templates:
                # make sql
                varMap = {}
                sqlH = "INSERT INTO {0}.JEDI_Output_Template (outTempID,".format(jedi_config.db.schemaJEDI)
                sqlL = "VALUES({0}.JEDI_OUTPUT_TEMPLATE_ID_SEQ.nextval,".format(jedi_config.db.schemaJEDI) 
                for tmpAttr,tmpVal in template.iteritems():
                    tmpKey = ':'+tmpAttr
                    sqlH += '{0},'.format(tmpAttr)
                    sqlL += '{0},'.format(tmpKey)
                    varMap[tmpKey] = tmpVal
                sqlH = sqlH[:-1] + ') '     
                sqlL = sqlL[:-1] + ') '
                sql = sqlH + sqlL
                self.cur.execute(sql+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # get tasks to be processed
    def getTasksToBeProcessed_JEDI(self,pid,vo,workQueue,prodSourceLabel,cloudName,
                                   nTasks=50,nFiles=100,isPeeking=False,simTasks=None,
                                   minPriority=None,maxNumJobs=None,typicalNumFilesMap=None,
                                   fullSimulation=False,simDatasets=None,
                                   mergeUnThrottled=None,readMinFiles=False):
        comment = ' /* JediDBProxy.getTasksToBeProcessed_JEDI */'
        methodName = self.getMethodName(comment)
        if simTasks != None:
            methodName += ' <jediTasks={0}>'.format(str(simTasks))
        elif workQueue == None:
            methodName += ' <vo={0} queue={1} cloud={2} pid={3}>'.format(vo,None,cloudName,pid)
        else:
            methodName += ' <vo={0} queue={1} cloud={2} pid={3}>'.format(vo,workQueue.queue_name,cloudName,pid)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start label={0} nTasks={1} nFiles={2} minPriority={3}'.format(prodSourceLabel,nTasks,
                                                                                    nFiles,minPriority))
        tmpLog.debug('maxNumJobs={0} typicalNumFilesMap={1}'.format(maxNumJobs,str(typicalNumFilesMap)))
        tmpLog.debug('simTasks={0} mergeUnThrottled={1}'.format(str(simTasks),str(mergeUnThrottled)))
        memStart = JediCoreUtils.getMemoryUsage()
        tmpLog.debug('memUsage start {0} MB pid={1}'.format(memStart,os.getpid()))
        # return value for failure
        failedRet = None
        # set max number of jobs if undefined
        if maxNumJobs == None:
            tmpLog.debug('set maxNumJobs={0} since undefined '.format(maxNumJobs))
        # time limit to avoid duplication
        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=10)
        try:
            # sql to get tasks/datasets
            if simTasks == None:
                varMap = {}
                varMap[':vo']              = vo
                if not prodSourceLabel in [None,'','any']:
                    varMap[':prodSourceLabel'] = prodSourceLabel
                if not cloudName in [None,'','any']:
                    varMap[':cloud']       = cloudName
                varMap[':dsStatus']        = 'ready'            
                varMap[':dsOKStatus1']     = 'ready'
                varMap[':dsOKStatus2']     = 'done'
                varMap[':dsOKStatus3']     = 'defined'
                varMap[':dsOKStatus4']     = 'registered'
                varMap[':dsOKStatus5']     = 'failed'
                varMap[':timeLimit']       = timeLimit
                sql  = "SELECT tabT.jediTaskID,datasetID,currentPriority,nFilesToBeUsed-nFilesUsed,tabD.type,tabT.status,tabT.userName,nFiles,nEvents "
                sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
                sql += "AND tabT.vo=:vo AND workQueue_ID IN ("
                for tmpQueue_ID in workQueue.getIDs():
                    tmpKey = ':queueID_{0}'.format(tmpQueue_ID)
                    varMap[tmpKey] = tmpQueue_ID
                    sql += '{0},'.format(tmpKey)
                sql  = sql[:-1]
                sql += ') '
                if not prodSourceLabel in [None,'','any']:
                    sql += "AND prodSourceLabel=:prodSourceLabel "
                if not cloudName in [None,'','any']:
                    sql += "AND tabT.cloud=:cloud "
                sql += "AND tabT.status IN ("
                for tmpStat in JediTaskSpec.statusForJobGenerator():
                    tmpKey = ':tstat_{0}'.format(tmpStat)
                    varMap[tmpKey] = tmpStat
                    sql += '{0},'.format(tmpKey)
                sql  = sql[:-1]
                sql += ') '
                sql += "AND tabT.lockedBy IS NULL "
                sql += "AND tabT.modificationTime<:timeLimit "
                sql += "AND nFilesToBeUsed > nFilesUsed AND type IN ("
                if mergeUnThrottled == True:
                    for tmpType in JediDatasetSpec.getMergeProcessTypes():
                        mapKey = ':type_'+tmpType
                        sql += '{0},'.format(mapKey)
                        varMap[mapKey] = tmpType
                else:
                    for tmpType in JediDatasetSpec.getProcessTypes(): 
                        mapKey = ':type_'+tmpType
                        sql += '{0},'.format(mapKey)
                        varMap[mapKey] = tmpType
                sql  = sql[:-1]
                sql += ') AND tabD.status=:dsStatus '
                sql += 'AND masterID IS NULL '
                if minPriority != None:
                    varMap[':minPriority'] = minPriority
                    sql += 'AND currentPriority>=:minPriority '
                sql += 'AND NOT EXISTS '
                sql += '(SELECT 1 FROM {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
                sql += 'WHERE {0}.JEDI_Datasets.jediTaskID=tabT.jediTaskID '.format(jedi_config.db.schemaJEDI)
                sql += 'AND type IN ('
                if mergeUnThrottled == True:
                    for tmpType in JediDatasetSpec.getMergeProcessTypes():
                        mapKey = ':type_'+tmpType
                        sql += '{0},'.format(mapKey)
                else:
                    for tmpType in JediDatasetSpec.getProcessTypes():
                        mapKey = ':type_'+tmpType
                        sql += '{0},'.format(mapKey)
                sql  = sql[:-1]
                sql += ') AND NOT status IN (:dsOKStatus1,:dsOKStatus2,:dsOKStatus3,:dsOKStatus4,:dsOKStatus5)) '
                sql += "ORDER BY currentPriority DESC,jediTaskID "
            else:
                varMap = {}
                if not fullSimulation:
                    sql  = "SELECT tabT.jediTaskID,datasetID,currentPriority,nFilesToBeUsed-nFilesUsed,tabD.type,tabT.status,tabT.userName,nFiles,nEvents "
                else:
                    sql  = "SELECT tabT.jediTaskID,datasetID,currentPriority,nFilesToBeUsed,tabD.type,tabT.status,tabT.userName,nFiles,nEvents "
                sql += "FROM {0}.JEDI_Tasks tabT,{1}.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI,
                                                                                 jedi_config.db.schemaJEDI)
                sql += "WHERE tabT.jediTaskID=tabD.jediTaskID AND tabT.jediTaskID IN ("
                for tmpTaskIdx,tmpTaskID in enumerate(simTasks):
                    tmpKey = ':jediTaskID{0}'.format(tmpTaskIdx)
                    varMap[tmpKey] = tmpTaskID
                    sql += '{0},'.format(tmpKey)
                sql = sql[:-1]
                sql += ') AND type IN ('
                for tmpType in JediDatasetSpec.getProcessTypes():
                    mapKey = ':type_'+tmpType
                    sql += '{0},'.format(mapKey)
                    varMap[mapKey] = tmpType
                sql  = sql[:-1]
                sql += ') AND masterID IS NULL '
                if simDatasets != None:
                    sql += "AND tabD.datasetID IN ("
                    for tmpDsIdx,tmpDatasetID in enumerate(simDatasets):
                        tmpKey = ':datasetID{0}'.format(tmpDsIdx)
                        varMap[tmpKey] = tmpDatasetID
                        sql += '{0},'.format(tmpKey)
                    sql = sql[:-1]
                    sql += ') '
                if not fullSimulation:
                    sql += "AND nFilesToBeUsed > nFilesUsed "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 1000000
            # select
            tmpLog.debug(sql+comment+str(varMap))
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
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
            for jediTaskID,datasetID,currentPriority,tmpNumFiles,datasetType,taskStatus,userName,tmpNumInputFiles,tmpNumInputEvents in resList:
                tmpLog.debug('jediTaskID={0} datasetID={1} tmpNumFiles={2} type={3} prio={4}'.format(jediTaskID,datasetID,
                                                                                                     tmpNumFiles,datasetType,
                                                                                                     currentPriority))
                # just return the max priority
                if isPeeking:
                    return currentPriority
                # make task-status mapping
                taskStatusMap[jediTaskID] = taskStatus
                # make task-dataset mapping
                if not taskDatasetMap.has_key(jediTaskID):
                    taskDatasetMap[jediTaskID] = []
                 
                taskDatasetMap[jediTaskID].append((datasetID,tmpNumFiles,datasetType,tmpNumInputFiles,tmpNumInputEvents))
                # use single username if WQ has a share
                if workQueue != None and workQueue.queue_share != None:
                    userName = ''
                # increase priority so that scouts do not wait behind the bulk
                if taskStatus in ['ready','scouting']:
                    currentPriority += 1
                # make task-prio mapping
                taskPrioMap[jediTaskID] = currentPriority
                if not userName in taskUserPrioMap:
                    taskUserPrioMap[userName] = {}
                if not currentPriority in taskUserPrioMap[userName]:
                    taskUserPrioMap[userName][currentPriority] = []
                if not jediTaskID in taskUserPrioMap[userName][currentPriority]:
                    taskUserPrioMap[userName][currentPriority].append(jediTaskID)
            # make user-task mapping
            userTaskMap = {}
            for userName in taskUserPrioMap.keys():
                # use high priority tasks first
                priorityList = taskUserPrioMap[userName].keys()
                priorityList.sort()
                priorityList.reverse()
                for currentPriority in priorityList:
                    if not userName in userTaskMap:
                        userTaskMap[userName] = []
                    userTaskMap[userName] += taskUserPrioMap[userName][currentPriority]
            tmpLog.debug('got {0} tasks'.format(len(taskDatasetMap)))
            # make list
            userNameList = userTaskMap.keys()
            random.shuffle(userNameList)
            tmpLog.debug('{0} users ready'.format(len(userNameList)))
            while userNameList != []:
                for userName in userNameList:
                    if userTaskMap[userName] == []:
                        userNameList.remove(userName)
                    else:
                        jediTaskIDList.append(userTaskMap[userName].pop(0))
            # sql to read task
            sqlRT  = "SELECT {0} ".format(JediTaskSpec.columnNames())
            sqlRT += "FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlRT += "WHERE jediTaskID=:jediTaskID AND status=:statusInDB AND lockedBy IS NULL "
            if simTasks == None:
                sqlRT += "FOR UPDATE NOWAIT "
            # sql to read locked task
            sqlRL  = "SELECT {0} ".format(JediTaskSpec.columnNames())
            sqlRL += "FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlRL += "WHERE jediTaskID=:jediTaskID AND status=:statusInDB AND lockedBy=:newLockedBy "
            if simTasks == None:
                sqlRL += "FOR UPDATE NOWAIT "
            # sql to lock task
            sqlLock  = "UPDATE {0}.JEDI_Tasks  ".format(jedi_config.db.schemaJEDI)
            sqlLock += "SET lockedBy=:newLockedBy,lockedTime=CURRENT_DATE,modificationTime=CURRENT_DATE "
            sqlLock += "WHERE jediTaskID=:jediTaskID AND status=:status AND lockedBy IS NULL AND modificationTime<:timeLimit "
            # sql to read template
            sqlJobP = "SELECT jobParamsTemplate FROM {0}.JEDI_JobParams_Template WHERE jediTaskID=:jediTaskID ".format(jedi_config.db.schemaJEDI)
            # sql to read datasets
            sqlRD  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
            sqlRD += "FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlRD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            if simTasks == None:
                sqlRD += "FOR UPDATE NOWAIT "
            # sql to read files
            sqlFR  = "SELECT * FROM (SELECT {0} ".format(JediFileSpec.columnNames())
            sqlFR += "FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID "
            if not fullSimulation:
                sqlFR += "AND status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt) "
                sqlFR += "AND (maxFailure IS NULL OR failedAttempt<maxFailure) "
                sqlFR += "AND ramCount=:ramCount "
            sqlFR += "ORDER BY {0}) "
            sqlFR += "WHERE rownum <= {1}"
            
            #For the cases where the ram count is not set
            sqlFR_RCNull  = "SELECT * FROM (SELECT {0} ".format(JediFileSpec.columnNames())
            sqlFR_RCNull += "FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
            sqlFR_RCNull += "jediTaskID=:jediTaskID AND datasetID=:datasetID "
            if not fullSimulation:
                sqlFR_RCNull += "AND status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt) "
                sqlFR_RCNull += "AND (maxFailure IS NULL OR failedAttempt<maxFailure) "
                sqlFR_RCNull += "AND (ramCount IS NULL OR ramCount=0) "
            sqlFR_RCNull += "ORDER BY {0}) "
            sqlFR_RCNull += "WHERE rownum <= {1}"
            
            # sql to read files without ramcount
            sqlFRNR  = "SELECT * FROM (SELECT {0} ".format(JediFileSpec.columnNames())
            sqlFRNR += "FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
            sqlFRNR += "jediTaskID=:jediTaskID AND datasetID=:datasetID "
            if not fullSimulation:
                sqlFRNR += "AND status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt) "
                sqlFRNR += "AND (maxFailure IS NULL OR failedAttempt<maxFailure) "
            sqlFRNR += "ORDER BY {0}) "
            sqlFRNR += "WHERE rownum <= {1}"
            #sql to read memory requirements of files in dataset
            sqlRM = """SELECT ramCount FROM {0}.JEDI_Dataset_Contents 
                       WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID """.format(jedi_config.db.schemaJEDI)
            if not fullSimulation:
                sqlRM += """AND status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt) 
                            AND (maxFailure IS NULL OR failedAttempt<maxFailure) """
            sqlRM += "GROUP BY ramCount "
            # sql to update file status
            sqlFU  = "UPDATE {0}.JEDI_Dataset_Contents SET status=:nStatus ".format(jedi_config.db.schemaJEDI)
            sqlFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND status=:oStatus "
            # sql to update file usage info in dataset
            sqlDU  = "UPDATE {0}.JEDI_Datasets SET nFilesUsed=:nFilesUsed ".format(jedi_config.db.schemaJEDI)
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlDU += "RETURNING nFilesUsed,nFilesTobeUsed INTO :newnFilesUsed,:newnFilesTobeUsed "
            # sql to read DN
            sqlDN  = "SELECT dn FROM {0}.users WHERE name=:name ".format(jedi_config.db.schemaMETA)
            # sql to count the number of files for avalanche
            sqlAV  = "SELECT SUM(nFiles-nFilesToBeUsed) FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlAV += "WHERE jediTaskID=:jediTaskID AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sqlAV += '{0},'.format(mapKey)
            sqlAV  = sqlAV[:-1]
            sqlAV += ') AND masterID IS NULL '
            # loop over all tasks
            iTasks = 0
            lockedTasks = []
            lockedByAnother = []
            memoryExceed = False
            for tmpIdxTask,jediTaskID in enumerate(jediTaskIDList):
                # process only merging if enough jobs are already generated
                if maxNumJobs != None and maxNumJobs <= 0:
                    containMergeing = False
                    for datasetID,tmpNumFiles,datasetType,tmpNumInputFiles,tmpNumInputEvents in taskDatasetMap[jediTaskID]:
                        if datasetType in JediDatasetSpec.getMergeProcessTypes():
                            containMergeing = True
                            break
                    if not containMergeing:
                        tmpLog.debug('skipping jediTaskID={0} {1}/{2}/{3} prio={4}'.format(jediTaskID,tmpIdxTask,
                                                                                           len(jediTaskIDList),iTasks,
                                                                                           taskPrioMap[jediTaskID]))
                        continue
                tmpLog.debug('getting jediTaskID={0} {1}/{2}/{3} prio={4}'.format(jediTaskID,tmpIdxTask,
                                                                                  len(jediTaskIDList),iTasks,
                                                                                  taskPrioMap[jediTaskID]))
                # locked by another
                if jediTaskID in lockedByAnother:
                    tmpLog.debug('skip locked by another jediTaskID={0}'.format(jediTaskID))
                    continue
                # begin transaction
                self.conn.begin()
                # read task
                toSkip = False
                try:
                    # select
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':statusInDB'] = taskStatusMap[jediTaskID]
                    if not jediTaskID in lockedTasks:
                        tmpLog.debug(sqlRT+comment+str(varMap))
                        self.cur.execute(sqlRT+comment,varMap)
                    else:
                        varMap[':newLockedBy'] = pid
                        tmpLog.debug(sqlRL+comment+str(varMap))
                        self.cur.execute(sqlRL+comment,varMap)
                    resRT = self.cur.fetchone()
                    # locked by another
                    if resRT == None:
                        toSkip = True
                        tmpLog.debug('skip locked jediTaskID={0}'.format(jediTaskID))
                        lockedByAnother.append(jediTaskID)
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                        continue
                    else:
                        origTaskSpec = JediTaskSpec()
                        origTaskSpec.pack(resRT)
                    # lock task
                    if simTasks == None and not jediTaskID in lockedTasks:
                        varMap = {}
                        varMap[':jediTaskID']  = jediTaskID
                        varMap[':newLockedBy'] = pid
                        varMap[':status'] = taskStatusMap[jediTaskID]
                        varMap[':timeLimit'] = timeLimit
                        tmpLog.debug(sqlLock+comment+str(varMap))
                        self.cur.execute(sqlLock+comment,varMap)
                        nRow = self.cur.rowcount
                        if nRow != 1:
                            tmpLog.debug('failed to lock jediTaskID={0}'.format(jediTaskID))
                            lockedByAnother.append(jediTaskID)
                            toSkip = True
                            if not self._commit():
                                raise RuntimeError, 'Commit error'
                            continue
                        # list of locked tasks
                        if not jediTaskID in lockedTasks:
                            lockedTasks.append(jediTaskID)
                except:
                    errType,errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        # resource busy and acquire with NOWAIT specified
                        toSkip = True
                        tmpLog.debug('skip locked with NOWAIT jediTaskID={0}'.format(jediTaskID))
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                        continue
                    else:
                        # failed with something else
                        raise errType,errValue
                # count the number of files for avalanche
                if not toSkip:
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    for tmpType in JediDatasetSpec.getInputTypes():
                        mapKey = ':type_'+tmpType
                        varMap[mapKey] = tmpType
                    tmpLog.debug(sqlAV+comment+str(varMap))
                    self.cur.execute(sqlAV+comment,varMap)
                    resAV = self.cur.fetchone()
                    tmpLog.debug(str(resAV))
                    if resAV == None:
                        # no file info
                        toSkip = True
                        tmpLog.error('skipped since failed to get number of files for avalanche')
                    else:
                        numAvalanche, = resAV
                # change userName for analysis
                if not toSkip:
                    # for analysis use DN as userName
                    if origTaskSpec.prodSourceLabel in ['user']:
                        varMap = {}
                        varMap[':name'] = origTaskSpec.userName
                        tmpLog.debug(sqlDN+comment+str(varMap))
                        self.cur.execute(sqlDN+comment,varMap)
                        resDN = self.cur.fetchone()
                        tmpLog.debug(resDN)
                        if resDN == None:
                            # no user info
                            toSkip = True
                            tmpLog.error('skipped since failed to get DN for {0}'.format(origTaskSpec.userName))
                        else:
                            origTaskSpec.userName, = resDN
                            if origTaskSpec.userName in ['',None]:
                                # DN is empty
                                toSkip = True
                                tmpLog.error('skipped since DN is empty for {0}'.format(origTaskSpec.userName))
                            else:
                                # reset change to not update userName
                                origTaskSpec.resetChangedAttr('userName')
                # read datasets
                if not toSkip:
                    iDsPerTask = 0
                    nDsPerTask = 10
                    for datasetID,tmpNumFiles,datasetType,tmpNumInputFiles,tmpNumInputEvents in taskDatasetMap[jediTaskID]:
                        
                        primaryDatasetID = datasetID
                        datasetIDs = [datasetID]
                        taskSpec = copy.copy(origTaskSpec)
                        
                        #See if there are different memory requirements that need to be mapped to different chuncks
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':datasetID'] = datasetID
                        if not fullSimulation:
                            varMap[':status'] = 'ready'
                        self.cur.arraysize = 1000000
                        # figure out if there are different memory requirements in the dataset
                        self.cur.execute(sqlRM+comment, varMap)
                        memReqs = map (lambda req: req[0], self.cur.fetchall()) #Unpack resultset
                        
                        #Group 0 and NULL memReqs
                        if 0 in memReqs and None in memReqs:
                            memReqs.remove(None)
                        
                        tmpLog.debug("memory requirements for files in jediTaskID=%s datasetID=%s are: %s"%(jediTaskID, datasetID, memReqs))
                        if not memReqs:
                            toSkip = True
                        else:
                            # make InputChunks by ram count
                            inputChunks = []
                            for memReq in memReqs:
                                inputChunks.append(InputChunk(taskSpec, ramCount=memReq))
                            # merging
                            if datasetType in JediDatasetSpec.getMergeProcessTypes():
                                for inputChunk in inputChunks:
                                    inputChunk.isMerging = True
                            else:
                                # only process merging if enough jobs are already generated
                                if maxNumJobs != None and maxNumJobs <= 0:
                                    tmpLog.debug('skip jediTaskID={0} datasetID={1} due to non-merge + enough jobs'.format(jediTaskID,
                                                                                                                           primaryDatasetID)) 
                                    continue
                        # read secondary dataset IDs
                        if not toSkip:
                            # sql to get seconday dataset list
                            sqlDS  = "SELECT datasetID FROM {0}.JEDI_Datasets WHERE jediTaskID=:jediTaskID ".format(jedi_config.db.schemaJEDI)
                            if not fullSimulation:
                                sqlDS += "AND nFilesToBeUsed >= nFilesUsed AND type IN ("
                            else:
                                sqlDS += "AND type IN ("
                            varMap = {}
                            if not datasetType in JediDatasetSpec.getMergeProcessTypes():
                                # for normal process
                                for tmpType in JediDatasetSpec.getInputTypes():
                                    mapKey = ':type_'+tmpType
                                    varMap[mapKey] = tmpType
                                    sqlDS += '{0},'.format(mapKey)
                            else:
                                # for merge process
                                for tmpType in JediDatasetSpec.getMergeProcessTypes():
                                    mapKey = ':type_'+tmpType
                                    varMap[mapKey] = tmpType
                                    sqlDS += '{0},'.format(mapKey)
                            sqlDS  = sqlDS[:-1]
                            if simTasks == None:
                                sqlDS += ') AND status=:dsStatus '
                                varMap[':dsStatus']   = 'ready'
                            else:
                                sqlDS += ') '
                            sqlDS += 'AND masterID=:masterID '
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':masterID']   = datasetID 
                            # select
                            self.cur.execute(sqlDS+comment,varMap)
                            resSecDsList = self.cur.fetchall()
                            for tmpDatasetID, in resSecDsList:
                                datasetIDs.append(tmpDatasetID)
                        # read dataset
                        if not toSkip:
                            for datasetID in datasetIDs:
                                varMap = {}
                                varMap[':jediTaskID'] = jediTaskID
                                varMap[':datasetID']  = datasetID
                                try:
                                    for inputChunk in inputChunks:
                                        # select
                                        self.cur.execute(sqlRD+comment,varMap)
                                        resRD = self.cur.fetchone()
                                        datasetSpec = JediDatasetSpec()
                                        datasetSpec.pack(resRD)
                                        # change stream name for merging
                                        if datasetSpec.type in JediDatasetSpec.getMergeProcessTypes():
                                            # change OUTPUT to IN
                                            datasetSpec.streamName = re.sub('^OUTPUT','TRN_OUTPUT',datasetSpec.streamName)
                                            # change LOG to INLOG
                                            datasetSpec.streamName = re.sub('^LOG','TRN_LOG',datasetSpec.streamName)
                                        # add to InputChunk
                                        if datasetSpec.isMaster():
                                            inputChunk.addMasterDS(datasetSpec)
                                        else:
                                            inputChunk.addSecondaryDS(datasetSpec)
                                except:
                                    errType,errValue = sys.exc_info()[:2]
                                    if self.isNoWaitException(errValue):
                                        # resource busy and acquire with NOWAIT specified
                                        toSkip = True
                                        tmpLog.debug('skip locked jediTaskID={0} datasetID={1}'.format(jediTaskID,datasetID))
                                    else:
                                        # failed with something else
                                        raise errType,errValue
                            # set useScout
                            if (numAvalanche == 0 and not inputChunks[0].isMutableMaster()) or not taskSpec.useScout():
                                for inputChunk in inputChunks:
                                    inputChunk.setUseScout(False)
                            else:
                                for inputChunk in inputChunks:
                                    inputChunk.setUseScout(True)
                        # read job params and files
                        if not toSkip:
                            # read template to generate job parameters
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            self.cur.execute(sqlJobP+comment,varMap)
                            for clobJobP, in self.cur:
                                if clobJobP != None:
                                    taskSpec.jobParamsTemplate = clobJobP.read()
                                break
                            # typical number of files
                            typicalNumFilesPerJob = 5
                            if taskSpec.getNumFilesPerJob() != None:
                                # the number of files is specified
                                typicalNumFilesPerJob = taskSpec.getNumFilesPerJob()
                            elif taskSpec.getNumEventsPerJob() != None:
                                typicalNumFilesPerJob = 1
                                try:
                                    if taskSpec.getNumEventsPerJob() > (tmpNumInputEvents/tmpNumInputFiles):
                                        typicalNumFilesPerJob = taskSpec.getNumEventsPerJob() * tmpNumInputFiles / tmpNumInputEvents
                                except:
                                    pass
                                if typicalNumFilesPerJob < 1:
                                    typicalNumFilesPerJob = 1
                            elif typicalNumFilesMap != None and typicalNumFilesMap.has_key(taskSpec.processingType) \
                                    and typicalNumFilesMap[taskSpec.processingType] > 0:
                                # typical usage
                                typicalNumFilesPerJob = typicalNumFilesMap[taskSpec.processingType]
                            tmpLog.debug('jediTaskID={0} typicalNumFilesPerJob={1}'.format(jediTaskID,typicalNumFilesPerJob))
                            # max number of files based on typical usage
                            if maxNumJobs != None and not inputChunks[0].isMerging and not inputChunks[0].useScout():
                                maxNumFiles = min(nFiles,typicalNumFilesPerJob*maxNumJobs+10)
                            else:
                                maxNumFiles = nFiles
                            # set lower limit to avoid too fine slashing
                            lowerLimitOnMaxNumFiles = 100    
                            if maxNumFiles < lowerLimitOnMaxNumFiles:
                                maxNumFiles = lowerLimitOnMaxNumFiles
                            # read files
                            readBlock = False
                            if maxNumFiles > tmpNumFiles:
                                maxMasterFilesTobeRead = tmpNumFiles
                            else:
                                # reading with a fix size of block
                                readBlock = True
                                maxMasterFilesTobeRead = maxNumFiles
                            
                            iFiles={}
                            for inputChunk in inputChunks:
                                for datasetID in datasetIDs:
                                    iFiles.setdefault(datasetID, 0)
                                    # get DatasetSpec
                                    tmpDatasetSpec = inputChunk.getDatasetWithID(datasetID)
                                    # the number of files to be read
                                    if tmpDatasetSpec.isMaster():
                                        maxFilesTobeRead = maxMasterFilesTobeRead
                                    else:
                                        # for secondaries
                                        if taskSpec.useLoadXML() or tmpDatasetSpec.isNoSplit():
                                            maxFilesTobeRead = 10000
                                        elif tmpDatasetSpec.getNumFilesPerJob() != None:
                                            maxFilesTobeRead = maxMasterFilesTobeRead * tmpDatasetSpec.getNumFilesPerJob()
                                        else:
                                            maxFilesTobeRead = int(maxMasterFilesTobeRead * tmpDatasetSpec.getRatioToMaster())
                                    # minimum read
                                    if readMinFiles:
                                        maxFilesForMinRead = 10
                                        if maxFilesTobeRead > maxFilesForMinRead:
                                            maxFilesTobeRead = maxFilesForMinRead
                                    tmpLog.debug('jediTaskID={0} trying to read {1} files from datasetID={2} with ramCount={3}'.format(jediTaskID,
                                                                                                                                       maxFilesTobeRead-iFiles[datasetID],
                                                                                                                                       datasetID,inputChunk.ramCount))
                                    if tmpDatasetSpec.isSeqNumber():
                                        orderBy = 'fileID'
                                    elif not tmpDatasetSpec.isMaster() and taskSpec.reuseSecOnDemand():
                                        orderBy = 'fileID'
                                    elif not taskSpec.useLoadXML():
                                        orderBy = 'lfn'
                                    else:
                                        orderBy = 'boundaryID'
                                    # read files to make FileSpec
                                    iFiles_tmp = 0
                                    for iDup in range(100): # avoid infinite loop just in case
                                        varMap = {}
                                        varMap[':datasetID']  = datasetID
                                        varMap[':jediTaskID'] = jediTaskID
                                        if not tmpDatasetSpec.toKeepTrack():
                                            if not fullSimulation:
                                                varMap[':status'] = 'ready'
                                            self.cur.execute(sqlFRNR.format(orderBy,maxFilesTobeRead-iFiles[datasetID])+comment,varMap)
                                        else:
                                            if not fullSimulation:
                                                varMap[':status'] = 'ready'
                                                if inputChunk.ramCount not in (None, 0):
                                                    varMap[':ramCount'] = inputChunk.ramCount
                                            if inputChunk.ramCount not in (None, 0):
                                                self.cur.execute(sqlFR.format(orderBy,maxFilesTobeRead-iFiles[datasetID])+comment,varMap)
                                            else: #We goup inputChunk.ramCount None and 0 together
                                                self.cur.execute(sqlFR_RCNull.format(orderBy,maxFilesTobeRead-iFiles[datasetID])+comment,varMap)
                                        resFileList = self.cur.fetchall()
                                        for resFile in resFileList:
                                            # make FileSpec
                                            tmpFileSpec = JediFileSpec()
                                            tmpFileSpec.pack(resFile)
                                            # update file status
                                            if simTasks == None and tmpDatasetSpec.toKeepTrack():
                                                varMap = {}
                                                varMap[':jediTaskID'] = tmpFileSpec.jediTaskID
                                                varMap[':datasetID']  = tmpFileSpec.datasetID
                                                varMap[':fileID']     = tmpFileSpec.fileID
                                                varMap[':nStatus']    = 'picked'
                                                varMap[':oStatus']    = 'ready'
                                                self.cur.execute(sqlFU+comment,varMap)
                                                nFileRow = self.cur.rowcount
                                                if nFileRow != 1:
                                                    tmpLog.debug('skip fileID={0} already used by another'.format(tmpFileSpec.fileID))
                                                    continue
                                            # add to InputChunk
                                            tmpDatasetSpec.addFile(tmpFileSpec)
                                            iFiles[datasetID] += 1
                                            iFiles_tmp += 1
                                        # no reuse
                                        if not taskSpec.reuseSecOnDemand() or tmpDatasetSpec.isMaster() or taskSpec.useLoadXML() or \
                                                tmpDatasetSpec.isSeqNumber() or tmpDatasetSpec.isNoSplit() or tmpDatasetSpec.toMerge() or \
                                                inputChunk.ramCount not in (None, 0):
                                            break
                                        # enough files were read
                                        if iFiles[datasetID] == maxFilesTobeRead:
                                            break
                                        # duplicate files for reuse
                                        tmpLog.debug('try to duplicate files for datasetID={0} since only {1}/{2} files were read'.format(tmpDatasetSpec.datasetID,
                                                                                                                                          iFiles,maxFilesTobeRead))
                                        nNewRec = self.duplicateFilesForReuse_JEDI(tmpDatasetSpec)
                                        tmpLog.debug('{0} files were duplicated'.format(nNewRec))
                                        if nNewRec == 0:
                                            break
                                    
                                    if tmpDatasetSpec.isMaster() and iFiles_tmp==0:
                                        inputChunk.isEmpty = True
                                    
                                    if iFiles[datasetID] == 0:
                                        # no input files
                                        if not readMinFiles or not tmpDatasetSpec.isPseudo():
                                            tmpLog.debug('jediTaskID={0} datasetID={1} has no files to be processed'.format(jediTaskID,datasetID))
                                            #toSkip = True
                                            break
                                    elif simTasks == None and tmpDatasetSpec.toKeepTrack() and iFiles_tmp!=0:
                                        # update nFilesUsed in DatasetSpec
                                        nFilesUsed = tmpDatasetSpec.nFilesUsed + iFiles[datasetID]
                                        tmpDatasetSpec.nFilesUsed = nFilesUsed
                                        varMap = {}
                                        varMap[':jediTaskID'] = jediTaskID
                                        varMap[':datasetID']  = datasetID
                                        varMap[':nFilesUsed'] = nFilesUsed
                                        varMap[':newnFilesUsed'] = self.cur.var(cx_Oracle.NUMBER)
                                        varMap[':newnFilesTobeUsed'] = self.cur.var(cx_Oracle.NUMBER)
                                        self.cur.execute(sqlDU+comment,varMap)
                                        newnFilesUsed = long(varMap[':newnFilesUsed'].getvalue())
                                        newnFilesTobeUsed = long(varMap[':newnFilesTobeUsed'].getvalue())
                                    tmpLog.debug('jediTaskID={2} datasetID={0} has {1} files to be processed'.format(datasetID,iFiles[datasetID],
                                                                                                                     jediTaskID))
                                    # set flag if it is a block read
                                    if tmpDatasetSpec.isMaster():
                                        if readBlock and iFiles[datasetID] == maxFilesTobeRead:
                                            inputChunk.readBlock = True
                                        else:
                                            inputChunk.readBlock = False
                                    # randomize
                                    if tmpDatasetSpec.isRandom():
                                        random.shuffle(tmpDatasetSpec.Files)
                        # add to return
                        if not toSkip:
                            if not jediTaskID in returnMap:
                                returnMap[jediTaskID] = []
                                iTasks += 1
                            for inputChunk in inputChunks:
                                if not inputChunk.isEmpty:
                                    returnMap[jediTaskID].append((taskSpec,cloudName,inputChunk))
                                    iDsPerTask += 1
                                # reduce the number of jobs
                                if maxNumJobs != None and not inputChunk.isMerging:
                                    maxNumJobs -= int(math.ceil(float(len(inputChunk.masterDataset.Files))/float(typicalNumFilesPerJob)))
                        else:
                            tmpLog.debug('escape due to toSkip for jediTaskID={0} datasetID={1}'.format(jediTaskID,primaryDatasetID)) 
                            break
                        if iDsPerTask > nDsPerTask:
                            break
                        if maxNumJobs != None and maxNumJobs <= 0:
                            pass
                        # memory check
                        try:
                            memLimit = 1*1024
                            memNow = JediCoreUtils.getMemoryUsage()
                            tmpLog.debug('memUsage now {0} MB pid={1}'.format(memNow,os.getpid()))
                            if memNow-memStart > memLimit:
                                tmpLog.warning('memory limit exceeds {0}-{1} > {2} MB : JediTasKID={3}'.format(memNow,memStart,memLimit,
                                                                                                               jediTaskID))
                                memoryExceed = True
                                break
                        except:
                            pass
                if not toSkip:        
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                else:
                    tmpLog.debug('rollback for jediTaskID={0}'.format(jediTaskID)) 
                    # roll back
                    self._rollback()
                # enough tasks 
                if iTasks >= nTasks:
                    break
                # already read enough files to generate jobs 
                if maxNumJobs != None and maxNumJobs <= 0:
                    pass
                # memory limit exceeds
                if memoryExceed:
                    break
            tmpLog.debug('done for {0} tasks'.format(iTasks))
            # change map to list
            returnList  = []
            for tmpJediTaskID,tmpTaskDsList in returnMap.iteritems():
                for ds in tmpTaskDsList:
                    tmpLog.debug("returning inputchunk {0}".format(ds[2]))
                returnList.append((tmpJediTaskID,tmpTaskDsList))
            tmpLog.debug('memUsage end {0} MB pid={1}'.format(JediCoreUtils.getMemoryUsage(),os.getpid()))
            return returnList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet


    # insert JobParamsTemplate
    def insertJobParamsTemplate_JEDI(self,jediTaskID,templ):
        comment = ' /* JediDBProxy.insertJobParamsTemplate_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # SQL
            sql  = "INSERT INTO {0}.JEDI_JobParams_Template (jediTaskID,jobParamsTemplate) VALUES (:jediTaskID,:templ) ".format(jedi_config.db.schemaJEDI)
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':templ']  = templ
            # begin transaction
            self.conn.begin()
            # insert
            self.cur.execute(sql+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # insert TaskParams
    def insertTaskParams_JEDI(self,vo,prodSourceLabel,userName,taskName,taskParams,parent_tid=None):
        comment = ' /* JediDBProxy.insertTaskParams_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += '<userName={0} taskName={1}>'.format(userName,taskName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to insert task parameters
            sqlT  = "INSERT INTO {0}.T_TASK ".format(jedi_config.db.schemaDEFT)
            sqlT += "(taskid,status,submit_time,vo,prodSourceLabel,userName,taskName,jedi_task_parameters,parent_tid) VALUES "
            sqlT += "({0}.PRODSYS2_TASK_ID_SEQ.nextval,".format(jedi_config.db.schemaDEFT)
            sqlT += ":status,CURRENT_DATE,:vo,:prodSourceLabel,:userName,:taskName,:param,"
            if parent_tid == None:
                sqlT += "{0}.PRODSYS2_TASK_ID_SEQ.currval) ".format(jedi_config.db.schemaDEFT)
            else:
                sqlT += ":parent_tid) "
            sqlT += "RETURNING taskid INTO :jediTaskID"
            # begin transaction
            self.conn.begin()
            # insert task parameters
            varMap = {}
            varMap[':vo']     = vo
            varMap[':param']  = taskParams
            varMap[':status'] = 'waiting'
            varMap[':userName'] = userName
            varMap[':taskName'] = taskName
            if parent_tid != None:
                varMap[':parent_tid']  = parent_tid
            varMap[':prodSourceLabel'] = prodSourceLabel
            varMap[':jediTaskID'] = self.cur.var(cx_Oracle.NUMBER)
            self.cur.execute(sqlT+comment,varMap)
            jediTaskID = long(varMap[':jediTaskID'].getvalue())
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done new jediTaskID={0}'.format(jediTaskID))
            return True,jediTaskID
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,None



    # insert new TaskParams and update parent TaskParams. mainly used by TaskGenerator
    def insertUpdateTaskParams_JEDI(self,jediTaskID,vo,prodSourceLabel,updateTaskParams,insertTaskParamsList):
        comment = ' /* JediDBProxy.insertUpdateTaskParams_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += '<jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to insert new task parameters
            sqlIT  = "INSERT INTO {0}.T_TASK ".format(jedi_config.db.schemaDEFT)
            sqlIT += "(taskid,status,submit_time,vo,prodSourceLabel,jedi_task_parameters,parent_tid) VALUES "
            sqlIT += "({0}.PRODSYS2_TASK_ID_SEQ.nextval,".format(jedi_config.db.schemaDEFT)
            sqlIT += ":status,CURRENT_DATE,:vo,:prodSourceLabel,:param,:parent_tid) "
            sqlIT += "RETURNING taskid INTO :jediTaskID"
            # sql to update parent task parameters
            sqlUT  = "UPDATE {0}.JEDI_TaskParams SET taskParams=:taskParams ".format(jedi_config.db.schemaJEDI)
            sqlUT += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # insert task parameters
            newJediTaskIDs = []
            for taskParams in insertTaskParamsList:
                varMap = {}
                varMap[':vo']     = vo
                varMap[':param']  = taskParams
                varMap[':status'] = 'waiting'
                varMap[':parent_tid'] = jediTaskID
                varMap[':prodSourceLabel'] = prodSourceLabel
                varMap[':jediTaskID'] = self.cur.var(cx_Oracle.NUMBER)
                self.cur.execute(sqlIT+comment,varMap)
                newJediTaskID = long(varMap[':jediTaskID'].getvalue())
                newJediTaskIDs.append(newJediTaskID)
            # update task parameters
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':taskParams'] = updateTaskParams
            self.cur.execute(sqlUT+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done new jediTaskIDs={0}'.format(str(newJediTaskIDs)))
            return True,newJediTaskIDs
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,None



    # reset unused files
    def resetUnusedFiles_JEDI(self,jediTaskID,inputChunk):
        comment = ' /* JediDBProxy.resetUnusedFiles_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to rollback files
            sql  = "UPDATE {0}.JEDI_Dataset_Contents SET status=:nStatus ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:oStatus "
            if inputChunk.ramCount in (None, 0):
                sql += "AND (ramCount IS NULL OR ramCount=:ramCount) "
            else:
                sql += "AND ramCount=:ramCount "
            # sql to reset nFilesUsed
            sqlD  = "UPDATE {0}.JEDI_Datasets SET nFilesUsed=nFilesUsed-:nFileRow ".format(jedi_config.db.schemaJEDI)
            sqlD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID " 
            # begin transaction
            self.conn.begin()
            for datasetSpec in inputChunk.getDatasets(includePseudo=True):
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':datasetID']  = datasetSpec.datasetID
                varMap[':nStatus']    = 'ready'
                varMap[':oStatus']    = 'picked'
                varMap[':ramCount']   = inputChunk.ramCount
                # update contents
                self.cur.execute(sql+comment,varMap)
                nFileRow = self.cur.rowcount
                tmpLog.debug('reset {0} rows for datasetID={1}'.format(nFileRow,datasetSpec.datasetID))
                if nFileRow > 0:
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':datasetID']  = datasetSpec.datasetID
                    varMap[':nFileRow'] = nFileRow
                    # update dataset
                    self.cur.execute(sqlD+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # set missing files
    def setMissingFiles_JEDI(self,jediTaskID,datasetID,fileIDs):
        comment = ' /* JediDBProxy.setMissingFiles_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0} datasetID={1}>'.format(jediTaskID,datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to set missing files
            sqlF  = "UPDATE {0}.JEDI_Dataset_Contents SET status=:nStatus ".format(jedi_config.db.schemaJEDI)
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID and status<>:nStatus"
            # sql to set nFilesFailed
            sqlD  = "UPDATE {0}.JEDI_Datasets SET nFilesFailed=nFilesFailed+:nFileRow ".format(jedi_config.db.schemaJEDI)
            sqlD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID " 
            # begin transaction
            self.conn.begin()
            nFileRow = 0
            # update contents
            for fileID in fileIDs:
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':datasetID']  = datasetID
                varMap[':fileID']     = fileID
                varMap[':nStatus']    = 'missing'
                self.cur.execute(sqlF+comment,varMap)
                nRow = self.cur.rowcount
                nFileRow += nRow
            # update dataset
            if nFileRow > 0:
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':datasetID']  = datasetID
                varMap[':nFileRow']   = nFileRow
                self.cur.execute(sqlD+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done set {0} missing files'.format(nFileRow))
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # rescue picked files
    def rescuePickedFiles_JEDI(self,vo,prodSourceLabel,waitTime):
        comment = ' /* JediDBProxy.rescuePickedFiles_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1}>'.format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get orphaned tasks
            sqlTR  = "SELECT jediTaskID,lockedBy "
            sqlTR += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTR += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTR += "AND tabT.status IN (:status1,:status2,:status3,:status4) AND lockedBy IS NOT NULL AND lockedTime<:timeLimit "
            if vo != None:
                sqlTR += "AND vo=:vo "
            if prodSourceLabel != None:
                sqlTR += "AND prodSourceLabel=:prodSourceLabel " 
            # sql to get picked datasets
            sqlDP  = "SELECT datasetID FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlDP += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2,:type3,:type4,:type5) " 
            # sql to rollback files
            sqlF  = "UPDATE {0}.JEDI_Dataset_Contents SET status=:nStatus ".format(jedi_config.db.schemaJEDI)
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:oStatus AND keepTrack=:keepTrack "
            # sql to reset nFilesUsed
            sqlDU  = "UPDATE {0}.JEDI_Datasets SET nFilesUsed=nFilesUsed-:nFileRow ".format(jedi_config.db.schemaJEDI)
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID " 
            # sql to unlock task
            sqlTU  = "UPDATE {0}.JEDI_Tasks SET lockedBy=NULL,lockedTime=NULL ".format(jedi_config.db.schemaJEDI)
            sqlTU += "WHERE jediTaskID=:jediTaskID "
            # sql to re-lock task
            sqlRL  = "UPDATE {0}.JEDI_Tasks SET lockedTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlRL += "WHERE jediTaskID=:jediTaskID AND lockedBy=:lockedBy AND lockedTime<:timeLimit "
            # sql to re-lock task with nowait
            sqlNW  = "SELECT jediTaskID FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlNW += "WHERE jediTaskID=:jediTaskID AND lockedBy=:lockedBy AND lockedTime<:timeLimit "
            sqlNW += "FOR UPDATE NOWAIT "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=waitTime)
            # get orphaned tasks
            varMap = {}
            varMap[':status1'] = 'ready'
            varMap[':status2'] = 'scouting'
            varMap[':status3'] = 'running'
            varMap[':status4'] = 'merging'
            varMap[':timeLimit'] = timeLimit
            if vo != None:
                varMap[':vo'] = vo
            if prodSourceLabel != None:
                varMap[':prodSourceLabel'] = prodSourceLabel
            self.cur.execute(sqlTR+comment,varMap)
            resTaskList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # loop over all tasks
            nTasks = 0
            for jediTaskID,lockedBy in resTaskList:
                tmpLog.debug('[jediTaskID={0}] rescue'.format(jediTaskID))
                self.conn.begin()
                # re-lock the task with NOWAIT
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':lockedBy']   = lockedBy
                varMap[':timeLimit']  = timeLimit
                toSkip = False
                try:
                    self.cur.execute(sqlNW+comment,varMap)
                except:
                    errType,errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        tmpLog.debug('[jediTaskID={0}] skip to rescue since locked by another'.format(jediTaskID))
                        toSkip = True
                    else:
                        # failed with something else
                        raise errType,errValue
                if not toSkip:    
                    # re-lock the task
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':lockedBy']   = lockedBy
                    varMap[':timeLimit']  = timeLimit
                    self.cur.execute(sqlRL+comment,varMap)
                    nRow = self.cur.rowcount
                    if nRow == 0:
                        tmpLog.debug('[jediTaskID={0}] skip to rescue since failed to re-lock'.format(jediTaskID))
                    else:
                        # get input datasets
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':type1'] = 'input'
                        varMap[':type2'] = 'trn_log'
                        varMap[':type3'] = 'trn_output'
                        varMap[':type4'] = 'pseudo_input'
                        varMap[':type5'] = 'random_seed'
                        self.cur.execute(sqlDP+comment,varMap)
                        resDatasetList = self.cur.fetchall()
                        # loop over all input datasets
                        for datasetID, in resDatasetList:
                            # update contents
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID']  = datasetID
                            varMap[':nStatus'] = 'ready'
                            varMap[':oStatus'] = 'picked'
                            varMap[':keepTrack'] = 1
                            self.cur.execute(sqlF+comment,varMap)
                            nFileRow = self.cur.rowcount
                            tmpLog.debug('[jediTaskID={0}] reset {1} rows for datasetID={2}'.format(jediTaskID,nFileRow,datasetID))
                            if nFileRow > 0:
                                # reset nFilesUsed
                                varMap = {}
                                varMap[':jediTaskID'] = jediTaskID
                                varMap[':datasetID']  = datasetID
                                varMap[':nFileRow'] = nFileRow
                                self.cur.execute(sqlDU+comment,varMap)
                        # unlock task
                        tmpLog.debug('[jediTaskID={0}] unlock'.format(jediTaskID))        
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        self.cur.execute(sqlTU+comment,varMap)
                        nRows = self.cur.rowcount
                        tmpLog.debug('[jediTaskID={0}] done with nRows={1}'.format(jediTaskID,nRows))
                        if nRows == 1:
                            nTasks += 1
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return nTasks
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # unlock tasks
    def unlockTasks_JEDI(self,vo,prodSourceLabel,waitTime):
        comment = ' /* JediDBProxy.unlockTasks_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1}>'.format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to look for locked tasks
            sqlTR  = "SELECT jediTaskID,lockedBy,lockedTime FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTR += "WHERE lockedBy IS NOT NULL AND lockedTime<:timeLimit "
            if vo != None:
                sqlTR += "AND vo=:vo "
            if prodSourceLabel != None:
                sqlTR += "AND prodSourceLabel=:prodSourceLabel " 
            # sql to unlock
            sqlTU  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTU += "SET lockedBy=NULL,lockedTime=NULL "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND lockedBy IS NOT NULL AND lockedTime<:timeLimit "
            if vo != None:
                sqlTU += "AND vo=:vo "
            if prodSourceLabel != None:
                sqlTU += "AND prodSourceLabel=:prodSourceLabel " 
            timeNow = datetime.datetime.utcnow()
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 1000
            # get locked task list
            varMap = {}
            varMap[':timeLimit'] = timeNow - datetime.timedelta(minutes=waitTime)
            if vo != None:
                varMap[':vo'] = vo
            if prodSourceLabel != None:
                varMap[':prodSourceLabel'] = prodSourceLabel
            self.cur.execute(sqlTR+comment,varMap)
            taskList = self.cur.fetchall()
            # unlock tasks
            nTasks = 0
            for jediTaskID,lockedBy,lockedTime in taskList:
                varMap[':jediTaskID'] = jediTaskID
                self.cur.execute(sqlTU+comment,varMap)
                iTasks = self.cur.rowcount
                if iTasks == 1:
                    tmpLog.debug('unlocked jediTaskID={0} lockedBy={1} lockedTime={2}'.format(jediTaskID,lockedBy,
                                                                                              lockedTime))
                nTasks += iTasks
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done with {0} tasks'.format(nTasks))
            return nTasks
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # get the size of input files which will be copied to the site
    def getMovingInputSize_JEDI(self,siteName):
        comment = ' /* JediDBProxy.getMovingInputSize_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' site={0}'.format(siteName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get size
            sql  = "SELECT SUM(inputFileBytes)/1024/1024/1024 FROM {0}.jobsDefined4 ".format(jedi_config.db.schemaPANDA)
            sql += "WHERE computingSite=:computingSite "
            # begin transaction
            self.conn.begin()
            varMap = {}
            varMap[':computingSite']  = siteName
            # exec
            self.cur.execute(sql+comment,varMap)
            resSum = self.cur.fetchone()
            retVal = 0
            if resSum != None:
                retVal, = resSum
            if retVal == None:
                retVal = 0
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None
        


    # get typical number of input files for each workQueue+processingType
    def getTypicalNumInput_JEDI(self,vo,prodSourceLabel,workQueue):
        comment = ' /* JediDBProxy.getTypicalNumInput_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' vo={0} label={1} queue={2}'.format(vo,prodSourceLabel,workQueue.queue_name)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get size
            varMap = {}
            varMap[':vo'] = vo
            varMap[':prodSourceLabel'] = prodSourceLabel
            sql  = "SELECT MEDIAN(nInputDataFiles),processingType FROM {0}.jobsActive4 ".format(jedi_config.db.schemaPANDA)
            sql += "WHERE prodSourceLabel=:prodSourceLabel and vo=:vo and workQueue_ID IN ("
            for tmpQueue_ID in workQueue.getIDs():
                tmpKey = ':queueID_{0}'.format(tmpQueue_ID)
                varMap[tmpKey] = tmpQueue_ID
                sql += '{0},'.format(tmpKey)
            sql  = sql[:-1]
            sql += ') '
            sql += "GROUP BY processingType "
            # begin transaction
            self.conn.begin()
            # exec
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            # loop over all processingTypes
            retMap = {}
            for numFile,processingType in resList:
                if numFile == None:
                    numFile = 0
                retMap[processingType] = int(math.ceil(numFile))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # use predefined values
            tmpLog.debug(hasattr(jedi_config.jobgen,'typicalNumFile'))
            try:
                if hasattr(jedi_config.jobgen,'typicalNumFile'):
                    for tmpItem in jedi_config.jobgen.typicalNumFile.split(','):
                        confVo,confProdSourceLabel,confWorkQueue,confProcessingType,confNumFiles = tmpItem.split(':')
                        if vo != confVo and not confVo in [None,'','any']:
                            continue
                        if prodSourceLabel != confProdSourceLabel and not confProdSourceLabel in [None,'','any']:
                            continue
                        if workQueue != confWorkQueue and not confWorkQueue in [None,'','any']:
                            continue
                        retMap[confProcessingType] = int(confNumFiles)
            except:
                pass
            tmpLog.debug('done -> {0}'.format(retMap))
            return retMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None
        


    # get highest prio jobs with workQueueID
    def getHighestPrioJobStat_JEDI(self,prodSourceLabel,cloudName,workQueue):
        comment = ' /* JediDBProxy.getHighestPrioJobStat_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <cloud={0} queue={1}>".format(cloudName,workQueue.queue_name)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        varMapO = {}
        varMapO[':cloud']           = cloudName
        varMapO[':prodSourceLabel'] = prodSourceLabel
        sql0  = "SELECT max(currentPriority) FROM {0} "
        sqlS  = "WHERE prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) "
        sqlS += "AND processingType<>:pmerge "
        sqlS += "AND cloud=:cloud AND workQueue_ID IN ("
        for tmpQueue_ID in workQueue.getIDs():
            tmpKey = ':queueID_{0}'.format(tmpQueue_ID)
            varMapO[tmpKey] = tmpQueue_ID
            sqlS += '{0},'.format(tmpKey)
        sqlS  = sqlS[:-1]
        sqlS += ") "
        sql0 += sqlS
        sqlC  = "SELECT COUNT(*) FROM {0} "
        sqlC += sqlS
        sqlC += "AND currentPriority=:currentPriority"
        tables = ['{0}.jobsActive4'.format(jedi_config.db.schemaPANDA),
                  '{0}.jobsDefined4'.format(jedi_config.db.schemaPANDA)]
        # make return map
        prioKey = 'highestPrio'
        nNotRunKey = 'nNotRun'
        retMap = {prioKey:0,nNotRunKey:0}
        try:
            for table in tables:
                # start transaction
                self.conn.begin()
                varMap = copy.copy(varMapO) 
                # select
                if table == '{0}.jobsActive4'.format(jedi_config.db.schemaPANDA):
                    varMap[':jobStatus1'] = 'activated'
                    varMap[':jobStatus2'] = 'starting'
                else:
                    varMap[':jobStatus1'] = 'defined'
                    varMap[':jobStatus2'] = 'assigned'
                varMap[':pmerge'] = 'pmerge'
                self.cur.arraysize = 100
                tmpLog.debug((sql0+comment).format(table)+str(varMap))
                self.cur.execute((sql0+comment).format(table), varMap)
                res = self.cur.fetchone()
                # if there is a job
                if res != None and res[0] != None:
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
                        varMap[':currentPriority'] = maxPriority
                        self.cur.arraysize = 10
                        tmpLog.debug((sqlC+comment).format(table))
                        self.cur.execute((sqlC+comment).format(table),varMap)
                        resC = self.cur.fetchone()
                        retMap[nNotRunKey] += resC[0]
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug(str(retMap))
            return True,retMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,None



    # get the list of tasks to refine
    def getTasksToRefine_JEDI(self,vo=None,prodSourceLabel=None):
        comment = ' /* JediDBProxy.getTasksToRefine_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <vo={0} label={1}>".format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retTaskIDs = []
        try:
            # sql to get jediTaskIDs to refine from the command table
            sqlC  = "SELECT taskid,parent_tid FROM {0}.T_TASK ".format(jedi_config.db.schemaDEFT)
            sqlC += "WHERE status=:status "
            varMap = {}
            varMap[':status'] = 'waiting'
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlC += "AND vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlC += "AND prodSourceLabel=:prodSourceLabel "
            sqlC += "ORDER BY submit_time "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            tmpLog.debug(sqlC+comment+str(varMap))
            self.cur.execute(sqlC+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('got {0} tasks'.format(len(resList)))            
            for jediTaskID,parent_tid in resList:
                tmpLog.debug('start jediTaskID={0}'.format(jediTaskID))
               # start transaction
                self.conn.begin()
                # lock
                varMap = {}
                varMap[':taskid'] = jediTaskID
                varMap[':status'] = 'waiting'
                sqlLock  = "SELECT taskid FROM {0}.T_TASK WHERE taskid=:taskid AND status=:status ".format(jedi_config.db.schemaDEFT)
                sqlLock += "FOR UPDATE "
                toSkip = False                
                try:
                    tmpLog.debug(sqlLock+comment+str(varMap))
                    self.cur.execute(sqlLock+comment,varMap)
                except:
                    errType,errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        # resource busy and acquire with NOWAIT specified
                        toSkip = True
                        tmpLog.debug('skip locked jediTaskID={0}'.format(jediTaskID))
                    else:
                        # failed with something else
                        raise errType,errValue
                if not toSkip:
                    resLock = self.cur.fetchone()
                    if resLock == None:
                        # already processed
                        toSkip = True
                        tmpLog.debug('skip jediTaskID={0} already processed'.format(jediTaskID))
                isOK = True
                if not toSkip:     
                    if isOK:
                        # insert task to JEDI
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        import uuid
                        varMap[':taskName'] = str(uuid.uuid4())
                        varMap[':status'] = 'registered'
                        varMap[':userName'] = 'tobeset'
                        varMap[':parent_tid'] = parent_tid
                        sqlIT =  "INSERT INTO {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
                        sqlIT += "(jediTaskID,taskName,status,userName,creationDate,modificationTime,parent_tid,stateChangeTime"
                        if vo != None:
                            sqlIT += ',vo'
                        if prodSourceLabel != None:
                            sqlIT += ',prodSourceLabel'
                        sqlIT += ") "
                        sqlIT += "VALUES(:jediTaskID,:taskName,:status,:userName,CURRENT_DATE,CURRENT_DATE,:parent_tid,CURRENT_DATE"
                        if vo != None:
                            sqlIT += ',:vo'
                            varMap[':vo'] = vo
                        if prodSourceLabel != None:
                            sqlIT += ',:prodSourceLabel'
                            varMap[':prodSourceLabel'] = prodSourceLabel
                        sqlIT += ") "
                        try:
                            tmpLog.debug(sqlIT+comment+str(varMap))
                            self.cur.execute(sqlIT+comment,varMap)
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error("failed to insert jediTaskID={0} with {1} {2}".format(jediTaskID,errtype,errvalue))
                            isOK = False
                    if isOK:
                        # check task parameters
                        varMap = {}
                        varMap[':taskid'] = jediTaskID
                        sqlTC = "SELECT taskid FROM {0}.T_TASK WHERE taskid=:taskid ".format(jedi_config.db.schemaDEFT)
                        tmpLog.debug(sqlTC+comment+str(varMap))
                        self.cur.execute(sqlTC+comment,varMap)
                        resTC = self.cur.fetchone()
                        if resTC == None or resTC[0] == None:
                            tmpLog.error("task parameters not found in T_TASK")
                            isOK = False
                    if isOK:        
                        # copy task parameters
                        varMap = {}
                        varMap[':taskid'] = jediTaskID
                        sqlPaste  = "INSERT INTO {0}.JEDI_TaskParams (jediTaskID,taskParams) ".format(jedi_config.db.schemaJEDI)
                        sqlPaste += "VALUES(:taskid,:taskParams) "
                        sqlSize  = "SELECT LENGTH(jedi_task_parameters) FROM {0}.T_TASK ".format(jedi_config.db.schemaDEFT)
                        sqlSize += "WHERE taskid=:taskid "
                        sqlCopy  = "SELECT jedi_task_parameters FROM {0}.T_TASK ".format(jedi_config.db.schemaDEFT)
                        sqlCopy += "WHERE taskid=:taskid "
                        try:
                            # get size
                            self.cur.execute(sqlSize+comment,varMap)
                            totalSize, = self.cur.fetchone()
                            # decomposed to SELECT and INSERT since sometimes oracle truncated params
                            tmpLog.debug(sqlCopy+comment+str(varMap))
                            self.cur.execute(sqlCopy+comment,varMap)
                            retStr = ''
                            for tmpItem, in self.cur:
                                retStr = tmpItem.read(amount=1000000)
                                break
                            # check size
                            if len(retStr) != totalSize:
                                raise RuntimeError, 'taskParams was truncated {0}/{1} bytes'.format(len(retStr),totalSize)
                            varMap = {}
                            varMap[':taskid'] = jediTaskID
                            varMap[':taskParams'] = retStr
                            self.cur.execute(sqlPaste+comment,varMap)
                            tmpLog.debug("inserted taskParams for jediTaskID={0} {1}/{2}".format(jediTaskID,len(retStr),totalSize))
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error("failed to insert param for jediTaskID={0} with {1} {2}".format(jediTaskID,errtype,errvalue))
                            isOK = False
                    # update
                    if isOK:       
                        deftStatus = 'registered'
                        varMap = {}
                        varMap[':taskid'] = jediTaskID
                        varMap[':status'] = deftStatus
                        varMap[':ndone']  = 0
                        varMap[':nreq']   = 0
                        varMap[':tevts']   = 0
                        sqlUC  = "UPDATE {0}.T_TASK ".format(jedi_config.db.schemaDEFT)
                        sqlUC += "SET status=:status,timestamp=CURRENT_DATE,total_done_jobs=:ndone,total_req_jobs=:nreq,total_events=:tevts "
                        sqlUC += "WHERE taskid=:taskid "
                        tmpLog.debug(sqlUC+comment+str(varMap))
                        self.cur.execute(sqlUC+comment,varMap)
                        self.setSuperStatus_JEDI(jediTaskID,deftStatus)
                    # append
                    if isOK:
                        retTaskIDs.append((jediTaskID,None,'registered',parent_tid))
                # commit
                if isOK:
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                else:
                    # roll back
                    self._rollback()
            # find orphaned tasks to rescue
            self.conn.begin()
            varMap = {}
            varMap[':status1'] = 'registered'
            varMap[':status2'] = JediTaskSpec.commandStatusMap()['incexec']['done']
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=10)
            sqlOrpS  = "SELECT tabT.jediTaskID,tabT.splitRule,tabT.status,tabT.parent_tid "
            sqlOrpS += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlOrpS += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlOrpS += "AND tabT.status IN (:status1,:status2) AND tabT.modificationtime<:timeLimit "
            if vo != None:
                sqlOrpS += 'AND vo=:vo '
                varMap[':vo'] = vo
            if prodSourceLabel != None:
                sqlOrpS += 'AND prodSourceLabel=:prodSourceLabel '
                varMap[':prodSourceLabel'] = prodSourceLabel
            sqlOrpS += "FOR UPDATE "
            tmpLog.debug(sqlOrpS+comment+str(varMap))
            self.cur.execute(sqlOrpS+comment,varMap)
            resList = self.cur.fetchall()
            # update modtime to avoid immediate reattempts
            sqlOrpU  = "UPDATE {0}.JEDI_Tasks SET modificationtime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlOrpU += "WHERE jediTaskID=:jediTaskID "
            for jediTaskID,splitRule,taskStatus,parent_tid in resList:
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                tmpLog.debug(sqlOrpU+comment+str(varMap))
                self.cur.execute(sqlOrpU+comment,varMap)
                nRow = self.cur.rowcount
                if nRow == 1 and not jediTaskID in retTaskIDs:
                    retTaskIDs.append((jediTaskID,splitRule,taskStatus,parent_tid))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("return {0} tasks".format(len(retTaskIDs)))
            return retTaskIDs
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # get task parameters with jediTaskID
    def getTaskParamsWithID_JEDI(self,jediTaskID):
        comment = ' /* JediDBProxy.getTaskParamsWithID_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql
            sql  = "SELECT taskParams FROM {0}.JEDI_TaskParams WHERE jediTaskID=:jediTaskID ".format(jedi_config.db.schemaJEDI)
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 100
            self.cur.execute(sql+comment,varMap)
            retStr = ''
            totalSize = 0
            for tmpItem, in self.cur:
                retStr = tmpItem.read(amount=1000000)
                totalSize += tmpItem.size()
                break
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('read {0}/{1} bytes'.format(len(retStr),totalSize))
            return retStr
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # register task/dataset/templ/param in a single transaction
    def registerTaskInOneShot_JEDI(self,jediTaskID,taskSpec,inMasterDatasetSpecList,
                                   inSecDatasetSpecList,outDatasetSpecList,
                                   outputTemplateMap,jobParamsTemplate,taskParams,
                                   unmergeMasterDatasetSpec,unmergeDatasetSpecMap,
                                   uniqueTaskName,oldTaskStatus):
        comment = ' /* JediDBProxy.registerTaskInOneShot_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            timeNow = datetime.datetime.utcnow()
            # set attributes
            if not taskSpec.status in ['topreprocess']:
                taskSpec.status = 'defined'
            tmpLog.debug('taskStatus={0}'.format(taskSpec.status))
            taskSpec.modificationTime = timeNow
            taskSpec.resetChangedAttr('jediTaskID')
            # begin transaction
            self.conn.begin()
            # check duplication
            duplicatedFlag = False
            if uniqueTaskName == True:
                sqlDup  = "SELECT jediTaskID FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
                sqlDup += "WHERE userName=:userName AND taskName=:taskName AND jediTaskID<>:jediTaskID FOR UPDATE "
                varMap = {}
                varMap[':userName']   = taskSpec.userName
                varMap[':taskName']   = taskSpec.taskName
                varMap[':jediTaskID'] = jediTaskID    
                self.cur.execute(sqlDup+comment,varMap)
                resDupList = self.cur.fetchall()
                tmpErrStr = ''
                for tmpJediTaskID, in resDupList:
                    duplicatedFlag = True
                    tmpErrStr += '{0},'.format(tmpJediTaskID)
                if duplicatedFlag:
                    taskSpec.status = 'toabort'
                    tmpErrStr = tmpErrStr[:-1]
                    tmpErrStr = '{0} since there is duplicated task -> jediTaskID={1}'.format(taskSpec.status,tmpErrStr)
                    taskSpec.setErrDiag(tmpErrStr)
                    # reset task name
                    taskSpec.taskName = None
                    tmpLog.debug(tmpErrStr)
            # update task
            varMap = taskSpec.valuesMap(useSeq=False,onlyChanged=True)
            varMap[':jediTaskID'] = jediTaskID
            varMap[':preStatus']  = oldTaskStatus
            sql  = "UPDATE {0}.JEDI_Tasks SET {1} WHERE ".format(jedi_config.db.schemaJEDI,
                                                                 taskSpec.bindUpdateChangesExpression())
            sql += "jediTaskID=:jediTaskID AND status=:preStatus "
            self.cur.execute(sql+comment,varMap)
            nRow = self.cur.rowcount
            tmpLog.debug('update {0} row in task table'.format(nRow))
            if nRow != 1:
                tmpLog.error('the task not found in task table or already registered')
            elif duplicatedFlag:
                pass
            else:
                # delete unknown datasets
                tmpLog.debug('deleting unknown datasets')
                sql  = "DELETE FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE jediTaskID=:jediTaskID AND type=:type "
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':type'] = JediDatasetSpec.getUnknownInputType()
                self.cur.execute(sql+comment,varMap)
                tmpLog.debug('inserting datasets')
                # sql to insert datasets
                sql  = "INSERT INTO {0}.JEDI_Datasets ({1}) ".format(jedi_config.db.schemaJEDI,
                                                                     JediDatasetSpec.columnNames())
                sql += JediDatasetSpec.bindValuesExpression()
                sql += " RETURNING datasetID INTO :newDatasetID"
                # sql to insert files
                sqlI  = "INSERT INTO {0}.JEDI_Dataset_Contents ({1}) ".format(jedi_config.db.schemaJEDI,
                                                                              JediFileSpec.columnNames())
                sqlI += JediFileSpec.bindValuesExpression()
                # insert master dataset
                masterID = -1
                datasetIdMap = {}
                for datasetSpec in inMasterDatasetSpecList:
                    if datasetSpec != None:
                        datasetSpec.creationTime = timeNow
                        datasetSpec.modificationTime = timeNow
                        varMap = datasetSpec.valuesMap(useSeq=True)
                        varMap[':newDatasetID'] = self.cur.var(cx_Oracle.NUMBER)            
                        # insert dataset
                        self.cur.execute(sql+comment,varMap)
                        datasetID = long(varMap[':newDatasetID'].getvalue())
                        masterID = datasetID
                        datasetIdMap[datasetSpec.uniqueMapKey()] = datasetID
                        datasetSpec.datasetID = datasetID
                        # insert files
                        for fileSpec in datasetSpec.Files:
                            fileSpec.datasetID = datasetID
                            fileSpec.creationDate = timeNow
                            varMap = fileSpec.valuesMap(useSeq=True)
                            self.cur.execute(sqlI+comment,varMap)
                    # insert secondary datasets
                    for datasetSpec in inSecDatasetSpecList:
                        datasetSpec.creationTime = timeNow
                        datasetSpec.modificationTime = timeNow
                        datasetSpec.masterID = masterID
                        varMap = datasetSpec.valuesMap(useSeq=True)
                        varMap[':newDatasetID'] = self.cur.var(cx_Oracle.NUMBER)            
                        # insert dataset
                        self.cur.execute(sql+comment,varMap)
                        datasetID = long(varMap[':newDatasetID'].getvalue())
                        datasetIdMap[datasetSpec.uniqueMapKey()] = datasetID
                        datasetSpec.datasetID = datasetID
                        # insert files
                        for fileSpec in datasetSpec.Files:
                            fileSpec.datasetID = datasetID
                            fileSpec.creationDate = timeNow
                            varMap = fileSpec.valuesMap(useSeq=True)
                            self.cur.execute(sqlI+comment,varMap)
                # insert unmerged master dataset
                unmergeMasterID = -1
                for datasetSpec in unmergeMasterDatasetSpec.values():
                    datasetSpec.creationTime = timeNow
                    datasetSpec.modificationTime = timeNow
                    varMap = datasetSpec.valuesMap(useSeq=True)
                    varMap[':newDatasetID'] = self.cur.var(cx_Oracle.NUMBER)            
                    # insert dataset
                    self.cur.execute(sql+comment,varMap)
                    datasetID = long(varMap[':newDatasetID'].getvalue())
                    datasetIdMap[datasetSpec.outputMapKey()] = datasetID
                    datasetSpec.datasetID = datasetID
                    unmergeMasterID = datasetID
                # insert unmerged output datasets
                for datasetSpec in unmergeDatasetSpecMap.values():
                    datasetSpec.creationTime = timeNow
                    datasetSpec.modificationTime = timeNow
                    datasetSpec.masterID = unmergeMasterID
                    varMap = datasetSpec.valuesMap(useSeq=True)
                    varMap[':newDatasetID'] = self.cur.var(cx_Oracle.NUMBER)            
                    # insert dataset
                    self.cur.execute(sql+comment,varMap)
                    datasetID = long(varMap[':newDatasetID'].getvalue())
                    datasetIdMap[datasetSpec.outputMapKey()] = datasetID
                    datasetSpec.datasetID = datasetID
                # insert output datasets
                for datasetSpec in outDatasetSpecList:
                    datasetSpec.creationTime = timeNow
                    datasetSpec.modificationTime = timeNow
                    # keep original outputMapKey since provenanceID may change
                    outputMapKey = datasetSpec.outputMapKey()
                    # associate to unmerged dataset
                    if unmergeMasterDatasetSpec.has_key(datasetSpec.outputMapKey()):
                        datasetSpec.provenanceID = unmergeMasterDatasetSpec[datasetSpec.outputMapKey()].datasetID
                    elif unmergeDatasetSpecMap.has_key(datasetSpec.outputMapKey()):
                        datasetSpec.provenanceID = unmergeDatasetSpecMap[datasetSpec.outputMapKey()].datasetID
                    varMap = datasetSpec.valuesMap(useSeq=True)
                    varMap[':newDatasetID'] = self.cur.var(cx_Oracle.NUMBER)            
                    # insert dataset
                    self.cur.execute(sql+comment,varMap)
                    datasetID = long(varMap[':newDatasetID'].getvalue())
                    datasetIdMap[outputMapKey] = datasetID
                    datasetSpec.datasetID = datasetID
                # insert outputTemplates
                tmpLog.debug('inserting outTmpl')
                for outputMapKey,outputTemplateList in outputTemplateMap.iteritems():
                    if not datasetIdMap.has_key(outputMapKey):
                        raise RuntimeError,'datasetID is not defined for {0}'.format(outputMapKey)
                    for outputTemplate in outputTemplateList:
                        sqlH = "INSERT INTO {0}.JEDI_Output_Template (outTempID,datasetID,".format(jedi_config.db.schemaJEDI)
                        sqlL = "VALUES({0}.JEDI_OUTPUT_TEMPLATE_ID_SEQ.nextval,:datasetID,".format(jedi_config.db.schemaJEDI) 
                        varMap = {}
                        varMap[':datasetID'] = datasetIdMap[outputMapKey]
                        for tmpAttr,tmpVal in outputTemplate.iteritems():
                            tmpKey = ':'+tmpAttr
                            sqlH += '{0},'.format(tmpAttr)
                            sqlL += '{0},'.format(tmpKey)
                            varMap[tmpKey] = tmpVal
                        sqlH = sqlH[:-1] + ') '
                        sqlL = sqlL[:-1] + ') '
                        sql = sqlH + sqlL
                        self.cur.execute(sql+comment,varMap)
                # check if jobParams is already there
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                sql  = "SELECT jediTaskID FROM {0}.JEDI_JobParams_Template ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE jediTaskID=:jediTaskID "
                self.cur.execute(sql+comment,varMap)
                resPar = self.cur.fetchone()
                if resPar == None:
                    # insert job parameters
                    tmpLog.debug('inserting jobParamsTmpl')
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':templ']  = jobParamsTemplate
                    sql  = "INSERT INTO {0}.JEDI_JobParams_Template ".format(jedi_config.db.schemaJEDI)
                    sql += "(jediTaskID,jobParamsTemplate) VALUES (:jediTaskID,:templ) "
                else:
                    tmpLog.debug('replacing jobParamsTmpl')
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':templ']  = jobParamsTemplate
                    sql  = "UPDATE {0}.JEDI_JobParams_Template ".format(jedi_config.db.schemaJEDI)
                    sql += "SET jobParamsTemplate=:templ WHERE jediTaskID=:jediTaskID"
                self.cur.execute(sql+comment,varMap)
                # update task parameters
                if taskParams != None: 
                    tmpLog.debug('updating taskParams')
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':taskParams'] = taskParams
                    sql  = "UPDATE {0}.JEDI_TaskParams SET taskParams=:taskParams ".format(jedi_config.db.schemaJEDI)
                    sql += "WHERE jediTaskID=:jediTaskID "
                    self.cur.execute(sql+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return True,taskSpec.status
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,'tobroken'



    # get scout job data
    def getScoutJobData_JEDI(self,jediTaskID,useTransaction=False,scoutSuccessRate=None,
                             mergeScout=False):
        comment = ' /* JediDBProxy.getScoutJobData_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start mergeScout={0}'.format(mergeScout))
        returnMap = {}
        # sql to get preset values
        if not mergeScout:
            sqlGPV  = "SELECT outDiskCount,outDiskUnit,walltime,ramCount,ramUnit,baseRamCount,workDiskCount,cpuTime "
        else:
            sqlGPV  = "SELECT outDiskCount,outDiskUnit,mergeWalltime,mergeRamCount,ramUnit,baseRamCount,workDiskCount,cpuTime "
        sqlGPV += "FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
        sqlGPV += "WHERE jediTaskID=:jediTaskID "
        # sql to get scout job data
        sqlSCF  = "SELECT tabF.PandaID,tabF.fsize,tabF.startEvent,tabF.endEvent,tabF.nEvents "
        sqlSCF += "FROM {0}.JEDI_Datasets tabD, {0}.JEDI_Dataset_Contents tabF WHERE ".format(jedi_config.db.schemaJEDI)
        sqlSCF += "tabD.jediTaskID=tabF.jediTaskID AND tabD.jediTaskID=:jediTaskID AND tabF.status=:status "
        sqlSCF += "AND tabD.datasetID=tabF.datasetID "
        if not mergeScout:
            sqlSCF += "AND tabF.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sqlSCF += '{0},'.format(mapKey)
        else:
            sqlSCF += "AND tabD.type IN ("
            for tmpType in JediDatasetSpec.getMergeProcessTypes():
                mapKey = ':type_'+tmpType
                sqlSCF += '{0},'.format(mapKey)
        sqlSCF  = sqlSCF[:-1]
        sqlSCF += ") AND tabD.masterID IS NULL " 
        sqlSCD  = "SELECT jobStatus,outputFileBytes,jobMetrics,cpuConsumptionTime,actualCoreCount,coreCount,startTime,endTime,computingSite,maxPSS "
        sqlSCD += "FROM {0}.jobsArchived4 ".format(jedi_config.db.schemaPANDA)
        sqlSCD += "WHERE PandaID=:pandaID AND jobStatus=:jobStatus "
        sqlSCD += "UNION "
        sqlSCD += "SELECT jobStatus,outputFileBytes,jobMetrics,cpuConsumptionTime,actualCoreCount,coreCount,startTime,endTime,computingSite,maxPSS "
        sqlSCD += "FROM {0}.jobsArchived ".format(jedi_config.db.schemaPANDAARCH)
        sqlSCD += "WHERE PandaID=:pandaID AND jobStatus=:jobStatus AND modificationTime>(CURRENT_DATE-14) "
        # get size of lib
        sqlLIB  = "SELECT MAX(fsize) "
        sqlLIB += "FROM {0}.JEDI_Datasets tabD, {0}.JEDI_Dataset_Contents tabF WHERE ".format(jedi_config.db.schemaJEDI)
        sqlLIB += "tabD.jediTaskID=tabF.jediTaskID AND tabD.jediTaskID=:jediTaskID AND tabF.status=:status AND "
        sqlLIB += "tabD.type=:type AND tabF.type=:type "
        # get core power
        sqlCore  = "SELECT corepower FROM {0}.schedconfig ".format(jedi_config.db.schemaMETA)
        sqlCore += "WHERE siteID=:site "
        if useTransaction:
            # begin transaction
            self.conn.begin()
        self.cur.arraysize = 100000
        # get preset values
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        self.cur.execute(sqlGPV+comment,varMap)
        resGPV = self.cur.fetchone()
        if resGPV != None:
            preOutDiskCount,preOutDiskUnit,preWalltime,preRamCount,preRamUnit,preBaseRamCount,preWorkDiskCount,preCpuTime = resGPV
            # get preOutDiskCount in kB
            if not preOutDiskCount in [0,None]:
                if preOutDiskUnit != None:
                    if preOutDiskUnit.startswith('GB'):
                        preOutDiskCount = preOutDiskCount * 1024 * 1024
                    elif preOutDiskUnit.startswith('MB'):
                        preOutDiskCount = preOutDiskCount * 1024
                    elif preOutDiskUnit.startswith('kB'):
                        pass
                    else:
                        preOutDiskCount = preOutDiskCount / 1024
        else:
            preOutDiskCount = 0
            preOutDiskUnit = None
            preWalltime = 0
            preRamCount = 0
            preRamUnit = None
            preBaseRamCount = 0
            preWorkDiskCount = 0
            preCpuTime = 0
        if preOutDiskUnit != None and preOutDiskUnit.endswith('PerEvent'):
            preOutputScaleWithEvents = True
        else:
            preOutputScaleWithEvents = False
        # get the size of lib 
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        varMap[':type'] = 'lib'
        varMap[':status'] = 'finished'
        self.cur.execute(sqlLIB+comment,varMap)
        resLIB = self.cur.fetchone()
        libSize = None
        if resLIB != None:
            try:
                libSize, = resLIB
                libSize /= (1024 * 1024)
            except:
                pass
        # get files    
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        varMap[':status'] = 'finished'
        if not mergeScout:
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                varMap[mapKey] = tmpType
        else:
            for tmpType in JediDatasetSpec.getMergeProcessTypes():
                mapKey = ':type_'+tmpType
                varMap[mapKey] = tmpType
        self.cur.execute(sqlSCF+comment,varMap)
        resList = self.cur.fetchall()
        # scout scceeded or not
        if resList == [] or (scoutSuccessRate != None and len(scoutSucceeded) < scoutSuccessRate):
            scoutSucceeded = False
        else:
            scoutSucceeded = True
        # upper limit
        limitWallTime = 999999999
        # loop over all files    
        outSizeList  = []
        walltimeList = []
        memSizeList  = []
        workSizeList = []
        cpuTimeList  = []
        finishedJobs = []
        inFSizeList  = []
        inFSizeMap   = {}
        inEventsMap  = {}
        corePowerMap = {}
        for pandaID,fsize,startEvent,endEvent,nEvents in resList:
            if not inFSizeMap.has_key(pandaID):
                inFSizeMap[pandaID] = 0
            # get effective file size
            effectiveFsize = JediCoreUtils.getEffectiveFileSize(fsize,startEvent,endEvent,nEvents)
            inFSizeMap[pandaID] += effectiveFsize
            # events
            if not pandaID in inEventsMap:
                inEventsMap[pandaID] = 0
            inEventsMap[pandaID] += JediCoreUtils.getEffectiveNumEvents(startEvent,endEvent,nEvents)
        # loop over all jobs
        for pandaID,totalFSize in inFSizeMap.iteritems():
            # get job data
            varMap = {}
            varMap[':pandaID']  = pandaID
            varMap[':jobStatus']= 'finished'
            self.cur.execute(sqlSCD+comment,varMap)
            resData = self.cur.fetchone()
            if resData != None:
                jobStatus,outputFileBytes,jobMetrics,cpuConsumptionTime,actualCoreCount,\
                    defCoreCount,startTime,endTime,computingSite,maxPSS = resData
                # get core power
                if not computingSite in corePowerMap:
                    varMap = {}
                    varMap[':site'] = computingSite
                    self.cur.execute(sqlCore+comment,varMap)
                    resCore = self.cur.fetchone()
                    if resCore != None:
                        corePower, = resCore
                    corePowerMap[computingSite] = corePower
                corePower = corePowerMap[computingSite]
                if corePower in [0,None]:
                    corePower = 10
                finishedJobs.append(pandaID)
                inFSizeList.append(totalFSize)
                # core count
                coreCount = 1
                try:
                    if actualCoreCount != None:
                        coreCount = actualCoreCount
                    else:
                        # extract coreCount
                        tmpMatch = re.search('coreCount=(\d+)',jobMetrics)
                        if tmpMatch != None:
                            coreCount = long(tmpMatch.group(1))
                        else:
                            # use jobdef
                            if not defCoreCount in [None,0]:
                                coreCount = defCoreCount
                except:
                    pass
                # output size
                try:
                    try:
                        # add size of intermediate files
                        if jobMetrics != None:
                            tmpMatch = re.search('workDirSize=(\d+)',jobMetrics)
                            outputFileBytes += long(tmpMatch.group(1))
                    except:
                        pass
                    if preOutputScaleWithEvents:
                        # scale with events
                        if pandaID in inEventsMap and inEventsMap[pandaID] > 0:
                            tmpVal = long(math.ceil(float(outputFileBytes) / inEventsMap[pandaID]))
                        if (not pandaID in inEventsMap) or inEventsMap[pandaID] >= 10 or inEventsMap[pandaID] == 0:
                            outSizeList.append(tmpVal)
                    else:
                        # scale with input size
                        tmpVal = long(math.ceil(float(outputFileBytes) / totalFSize))
                        outSizeList.append(tmpVal)
                except:
                    pass
                # execution time
                try:
                    tmpVal = cpuConsumptionTime
                    walltimeList.append(tmpVal)
                except:
                    pass
                # CPU time
                try:
                    tmpVal = float(cpuConsumptionTime)
                    tmpVal *= corePower
                    if pandaID in inEventsMap and inEventsMap[pandaID] > 0:
                        tmpVal /= float(inEventsMap[pandaID])
                    if (not pandaID in inEventsMap) or inEventsMap[pandaID] >= 10 or inEventsMap[pandaID] == 0:
                        cpuTimeList.append(tmpVal)
                except:
                    pass
                # RAM size
                try:
                    if preRamUnit == 'MBPerCore':
                        if maxPSS > 0:
                            tmpPSS = maxPSS
                            if not preBaseRamCount in [0,None]:
                                tmpPSS -= preBaseRamCount*1024
                            tmpPSS = float(tmpPSS)/float(coreCount)
                            tmpPSS = long(math.ceil(tmpPSS*1.1))
                            memSizeList.append(tmpPSS)
                    else:
                        tmpMatch = re.search('vmPeakMax=(\d+)',jobMetrics)
                        memSizeList.append(long(tmpMatch.group(1)))
                except:
                    pass
                # use lib size as workdir size
                tmpWorkSize = 0
                if tmpWorkSize == None or (libSize != None and libSize > tmpWorkSize):
                    tmpWorkSize = libSize
                if tmpWorkSize != None:
                    workSizeList.append(tmpWorkSize)
        # calculate median values
        if outSizeList != []:
            median = max(outSizeList) 
            median /= 1024
            # upper limit 10MB output per 1MB input
            upperLimit = 10 * 1024
            if median > upperLimit:
                median = upperLimit
            returnMap['outDiskCount'] = long(median)
            if preOutputScaleWithEvents:
                returnMap['outDiskUnit']  = 'kBPerEvent'
            else:
                returnMap['outDiskUnit']  = 'kB'
            # use preset value if larger
            if preOutDiskCount != None and \
                    (preOutDiskCount > returnMap['outDiskCount'] or preOutDiskCount < 0):
                returnMap['outDiskCount'] = preOutDiskCount
        if walltimeList != []:
            maxWallTime = max(walltimeList)
            # cut off of 60min
            if maxWallTime < 60 * 60:
                maxWallTime = 0
            median = float(maxWallTime) / float(max(inFSizeList)) * 1.5
            median = math.ceil(median)
            returnMap['walltime']     = long(median)
            returnMap['walltimeUnit'] = 'kSI2kseconds'
            # use preset value if larger
            if preWalltime != None and (preWalltime > returnMap['walltime'] or preWalltime < 0):
                returnMap['walltime'] = preWalltime
            # upper limit
            if returnMap['walltime'] > limitWallTime:
                returnMap['walltime'] = limitWallTime
        if cpuTimeList != []:
            maxCpuTime = long(math.ceil(max(cpuTimeList)*1.5))
            returnMap['cpuTime'] = maxCpuTime
        if memSizeList != []:
            median = max(memSizeList)
            median /= 1024
            returnMap['ramCount'] = long(median)
            if preRamUnit == 'MBPerCore':
                returnMap['ramUnit'] = preRamUnit
            else:
                returnMap['ramUnit'] = 'MB'
                # use preset value if larger
                if preRamCount != None: # FIXME when setting ramCount is enabled again # and preRamCount > returnMap['ramCount']:
                    returnMap['ramCount'] = preRamCount
        if workSizeList != []:   
            median = max(workSizeList)
            returnMap['workDiskCount'] = long(median)
            returnMap['workDiskUnit']  = 'MB'
            # use preset value if larger
            if preWorkDiskCount != None and preWorkDiskCount > returnMap['workDiskCount']:
                returnMap['workDiskCount'] = preWorkDiskCount
        if useTransaction:    
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
        # return    
        tmpLog.debug('succeeded={0} data->{1}'.format(scoutSucceeded,str(returnMap)))
        return scoutSucceeded,returnMap



    # set scout job data
    def setScoutJobData_JEDI(self,taskSpec,useCommit):
        comment = ' /* JediDBProxy.setScoutJobData_JEDI */'
        methodName = self.getMethodName(comment)
        jediTaskID = taskSpec.jediTaskID
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        if useCommit:
            # begin transaction
            self.conn.begin()
        # set average job data
        scoutSucceeded,scoutData = self.getScoutJobData_JEDI(jediTaskID,
                                                             scoutSuccessRate=taskSpec.getScoutSuccessRate())
        # sql to update task data
        if scoutData != {}:
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            sqlTSD  = "UPDATE {0}.JEDI_Tasks SET ".format(jedi_config.db.schemaJEDI)
            for scoutKey,scoutVal in scoutData.iteritems():
                tmpScoutKey = ':{0}'.format(scoutKey)
                varMap[tmpScoutKey] = scoutVal
                sqlTSD += '{0}={1},'.format(scoutKey,tmpScoutKey)
            sqlTSD = sqlTSD[:-1] 
            sqlTSD += " WHERE jediTaskID=:jediTaskID "
            tmpLog.debug(sqlTSD+comment+str(varMap))
            self.cur.execute(sqlTSD+comment,varMap)
        # set average merge job data
        mergeScoutSucceeded = None
        if taskSpec.mergeOutput():
            mergeScoutSucceeded,mergeScoutData = self.getScoutJobData_JEDI(jediTaskID,mergeScout=True)
            if mergeScoutData != {}:
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                sqlTSD  = "UPDATE {0}.JEDI_Tasks SET ".format(jedi_config.db.schemaJEDI)
                for mergeScoutKey,mergeScoutVal in mergeScoutData.iteritems():
                    # only walltime and ramCount
                    if not mergeScoutKey.startswith('walltime') and not mergeScoutKey.startswith('ram'):
                        continue
                    tmpScoutKey = ':{0}'.format(mergeScoutKey)
                    varMap[tmpScoutKey] = mergeScoutVal
                    sqlTSD += 'merge{0}={1},'.format(mergeScoutKey,tmpScoutKey)
                sqlTSD = sqlTSD[:-1]
                sqlTSD += " WHERE jediTaskID=:jediTaskID "
                tmpLog.debug(sqlTSD+comment+str(varMap))
                self.cur.execute(sqlTSD+comment,varMap)
        if useCommit:
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
        return scoutSucceeded,mergeScoutSucceeded



    # set tasks to be assigned
    def setScoutJobDataToTasks_JEDI(self,vo,prodSourceLabel):
        comment = ' /* JediDBProxy.setScoutJobDataToTasks_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1}>'.format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retJediTaskIDs = []
        try:
            # sql to get tasks to set scout job data
            varMap = {}
            varMap[':status'] = 'running'
            varMap[':minJobs'] = 5
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
            sqlSCF  = "SELECT tabT.jediTaskID "
            sqlSCF += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA,{1}.T_TASK tabD ".format(jedi_config.db.schemaJEDI,jedi_config.db.schemaDEFT)
            sqlSCF += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlSCF += "AND tabT.jediTaskID=tabD.taskID AND tabT.modificationTime>:timeLimit "
            sqlSCF += "AND tabT.status=:status AND tabT.walltimeUnit IS NULL "
            sqlSCF += "AND tabD.total_done_jobs>=:minJobs "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlSCF += "AND tabT.vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlSCF += "AND tabT.prodSourceLabel=:prodSourceLabel "
            # begin transaction
            self.conn.begin()
            # get tasks
            tmpLog.debug(sqlSCF+comment+str(varMap))
            self.cur.execute(sqlSCF+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            nTasks = 0
            for jediTaskID, in resList:
                # get task
                tmpStat,taskSpec = self.getTaskWithID_JEDI(jediTaskID,False)
                if tmpStat:
                    tmpLog.debug('set jediTaskID={0}'.format(jediTaskID))
                    self.setScoutJobData_JEDI(taskSpec,True)
                    nTasks += 1
            # return    
            tmpLog.debug('done with {0} tasks'.format(nTasks))
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # prepare tasks to be finished
    def prepareTasksToBeFinished_JEDI(self,vo,prodSourceLabel,nTasks=50,simTasks=None,pid='lock',noBroken=False):
        comment = ' /* JediDBProxy.prepareTasksToBeFinished_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1}>'.format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # return value for failure
        failedRet = None
        try:
            # sql to get tasks/datasets
            if simTasks == None:
                varMap = {}
                varMap[':taskstatus1']  = 'running'
                varMap[':taskstatus2']  = 'scouting'
                varMap[':taskstatus3']  = 'merging'
                varMap[':taskstatus4']  = 'preprocessing'
                varMap[':taskstatus5']  = 'ready'
                varMap[':dsEndStatus1'] = 'finished'
                varMap[':dsEndStatus2'] = 'done'
                varMap[':dsEndStatus3'] = 'failed'
                if vo != None:
                    varMap[':vo'] = vo
                if prodSourceLabel != None:
                    varMap[':prodSourceLabel'] = prodSourceLabel
                sql  = "SELECT tabT.jediTaskID,tabT.status "
                sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
                sql += "AND tabT.status IN (:taskstatus1,:taskstatus2,:taskstatus3,:taskstatus4,:taskstatus5) "
                if vo != None:
                    sql += "AND tabT.vo=:vo "
                if prodSourceLabel != None:
                    sql += "AND prodSourceLabel=:prodSourceLabel "
                sql += "AND tabT.lockedBy IS NULL AND NOT EXISTS "
                sql += '(SELECT 1 FROM {0}.JEDI_Datasets tabD '.format(jedi_config.db.schemaJEDI)
                sql += 'WHERE tabD.jediTaskID=tabT.jediTaskID AND masterID IS NULL '
                sql += 'AND type IN ('
                for tmpType in JediDatasetSpec.getProcessTypes():
                    mapKey = ':type_'+tmpType
                    sql += '{0},'.format(mapKey)
                    varMap[mapKey] = tmpType
                sql  = sql[:-1]
                sql += ') AND NOT status IN (:dsEndStatus1,:dsEndStatus2,:dsEndStatus3) AND ('
                sql += 'nFilesToBeUsed>nFilesFinished+nFilesFailed '
                sql += 'OR (nFilesUsed=0 AND nFilesToBeUsed IS NOT NULL AND nFilesToBeUsed>0) '
                sql += 'OR nFilesUsed>nFilesFinished+nFilesFailed) '
                sql += ') AND rownum<={0}'.format(nTasks)
            else:
                varMap = {}
                sql  = "SELECT tabT.jediTaskID,tabT.status "
                sql += "FROM {0}.JEDI_Tasks tabT ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE jediTaskID IN ("
                for tmpTaskIdx,tmpTaskID in enumerate(simTasks):
                    tmpKey = ':jediTaskID{0}'.format(tmpTaskIdx)
                    varMap[tmpKey] = tmpTaskID
                    sql += '{0},'.format(tmpKey)
                sql = sql[:-1]
                sql += ') '
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            tmpLog.debug(sql+comment+str(varMap))
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            # make list
            jediTaskIDstatusMap = {}
            for jediTaskID,taskStatus in resList:
                jediTaskIDstatusMap[jediTaskID] = taskStatus 
            # get tasks for early avalanche
            if simTasks == None and prodSourceLabel in [None,'managed']:
                minSuccessScouts = 5
                varMap = {}
                varMap['taskstatus'] = 'scouting'
                varMap['prodSourceLabel'] = 'managed'
                if vo != None:
                    varMap[':vo'] = vo
                sqlEA  = "SELECT * FROM "
                sqlEA += "(SELECT tabT.jediTaskID,SUM(nFilesFinished) totFinished,SUM(nFilesToBeUsed) totToBeUsed "
                sqlEA += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA,{0}.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI)
                sqlEA += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
                sqlEA += "AND tabT.jediTaskID=tabD.jediTaskID "
                sqlEA += "AND tabT.status=:taskstatus AND prodSourceLabel=:prodSourceLabel "
                if vo != None:
                    sqlEA += "AND tabT.vo=:vo "
                sqlEA += "AND tabT.lockedBy IS NULL "
                sqlEA += "AND tabD.masterID IS NULL "
                sqlEA += 'AND tabD.type IN ('
                for tmpType in JediDatasetSpec.getInputTypes():
                    mapKey = ':type_'+tmpType
                    sqlEA += '{0},'.format(mapKey)
                    varMap[mapKey] = tmpType
                sqlEA  = sqlEA[:-1]
                sqlEA += ') '
                sqlEA += 'GROUP BY tabT.jediTaskID) '
                sqlEA += 'WHERE totToBeUsed>0 AND totFinished*{0}>=totToBeUsed*{1} '.format(10,minSuccessScouts)
                sqlEA += 'AND rownum<={0}'.format(nTasks)
                # get tasks
                tmpLog.debug(sqlEA+comment+str(varMap))
                self.cur.execute(sqlEA+comment,varMap)
                resList = self.cur.fetchall()
                # append to list
                for jediTaskID,totFinished,totToBeUsed in resList:
                    if not jediTaskID in jediTaskIDstatusMap:
                        jediTaskIDstatusMap[jediTaskID] = varMap['taskstatus']
                        tmpLog.debug('got jediTaskID={0} {1}/{2} for early avalanche'.format(jediTaskID,
                                                                                             totFinished,
                                                                                             totToBeUsed))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            jediTaskIDList = jediTaskIDstatusMap.keys()
            jediTaskIDList.sort()
            tmpLog.debug('got {0} tasks'.format(len(jediTaskIDList)))
            # sql to read task
            sqlRT  = "SELECT {0} ".format(JediTaskSpec.columnNames())
            sqlRT += "FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlRT += "WHERE jediTaskID=:jediTaskID AND status=:statusInDB AND lockedBy IS NULL FOR UPDATE NOWAIT "
            # sql to lock task
            sqlLK  = "UPDATE {0}.JEDI_Tasks SET lockedBy=:newLockedBy ".format(jedi_config.db.schemaJEDI)
            sqlLK += "WHERE jediTaskID=:jediTaskID AND status=:status AND lockedBy IS NULL "
            # sql to read dataset status
            sqlRD  = "SELECT datasetID,status,nFiles,nFilesFinished,masterID,state "
            sqlRD += "FROM {0}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND status=:status AND type IN (".format(jedi_config.db.schemaJEDI)
            for tmpType in JediDatasetSpec.getProcessTypes():
                mapKey = ':type_'+tmpType
                sqlRD += '{0},'.format(mapKey)
            sqlRD  = sqlRD[:-1]
            sqlRD += ') '
            # sql to check if there is mutable dataset
            sqlMTC  = "SELECT COUNT(*) FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlMTC += "WHERE jediTaskID=:jediTaskID AND state=:state AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sqlMTC += '{0},'.format(mapKey)
            sqlMTC  = sqlMTC[:-1]
            sqlMTC += ') '
            # sql to update input dataset status
            sqlDIU  = "UPDATE {0}.JEDI_Datasets SET status=:status,modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlDIU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update output/log dataset status
            sqlDOU  = "UPDATE {0}.JEDI_Datasets SET status=:status,modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlDOU += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # sql to update status of mutable dataset
            sqlMUT  = "UPDATE {0}.JEDI_Datasets SET status=:status,modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlMUT += "WHERE jediTaskID=:jediTaskID AND state=:state "
            # sql to get nFilesToBeUsed of dataset
            sqlFUD  = "SELECT tabD.datasetID,COUNT(*) FROM {0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(jedi_config.db.schemaJEDI)
            sqlFUD += "WHERE tabD.jediTaskID=tabC.jediTaskID AND tabD.datasetID=tabC.datasetID AND tabD.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sqlFUD += '{0},'.format(mapKey)
            sqlFUD  = sqlFUD[:-1]
            sqlFUD += ") AND tabD.jediTaskID=:jediTaskID AND tabD.masterID IS NULL "
            sqlFUD += "AND NOT tabC.status IN (:status1,:status2,:status3) "
            sqlFUD += "GROUP BY tabD.datasetID "
            # sql to update nFiles of dataset
            sqlFUU  = "UPDATE {0}.JEDI_Datasets SET nFilesToBeUsed=:nFilesToBeUsed,modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlFUU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update task status
            sqlTU  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTU += "SET status=:status,modificationTime=CURRENT_DATE,lockedBy=NULL,lockedTime=NULL,"
            sqlTU += "errorDialog=:errorDialog,splitRule=:splitRule,stateChangeTime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID "
            # sql to update split rule
            sqlUSL  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlUSL += "SET splitRule=:splitRule WHERE jediTaskID=:jediTaskID "
            # loop over all tasks
            iTasks = 0
            for jediTaskID in jediTaskIDList:
                taskStatus = jediTaskIDstatusMap[jediTaskID]
                tmpLog.debug('start jediTaskID={0} status={1}'.format(jediTaskID,taskStatus))
                # begin transaction
                self.conn.begin()
                # read task
                toSkip = False
                errorDialog = None
                try:
                    # read task
                    varMap = {}
                    varMap[':jediTaskID']  = jediTaskID
                    varMap[':statusInDB']  = taskStatus
                    self.cur.execute(sqlRT+comment,varMap)
                    resRT = self.cur.fetchone()
                    # locked by another
                    if resRT == None:
                        tmpLog.debug('skip jediTaskID={0} since status has changed'.format(jediTaskID))
                        toSkip = True
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                        continue
                    else:
                        taskSpec = JediTaskSpec()
                        taskSpec.pack(resRT)
                        taskSpec.lockedBy = None
                        taskSpec.lockedTime = None
                    # lock
                    varMap = {}
                    varMap[':jediTaskID']  = jediTaskID
                    varMap[':newLockedBy'] = pid
                    varMap[':status']      = taskStatus
                    self.cur.execute(sqlLK+comment,varMap)
                    nRow = self.cur.rowcount
                    if nRow != 1:
                        tmpLog.debug('failed to lock jediTaskID={0}'.format(jediTaskID))
                        toSkip = True
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                        continue
                except:
                    errType,errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        # resource busy and acquire with NOWAIT specified
                        toSkip = True
                        tmpLog.debug('skip locked jediTaskID={0}'.format(jediTaskID))
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                        continue
                    else:
                        # failed with something else
                        raise errType,errValue
                # update dataset
                if not toSkip:
                    if taskSpec.status == 'scouting':
                        # set average job data
                        scoutSucceeded,mergeScoutSucceeded = self.setScoutJobData_JEDI(taskSpec,False)
                        # get nFiles to be used
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':status1'] = 'pending'
                        varMap[':status2'] = 'lost'
                        varMap[':status3'] = 'missing'
                        for tmpType in JediDatasetSpec.getInputTypes():
                            mapKey = ':type_'+tmpType
                            varMap[mapKey] = tmpType
                        self.cur.execute(sqlFUD+comment,varMap)
                        resFUD = self.cur.fetchall()
                        # update nFiles to be used
                        for datasetID,nReadyFiles in resFUD:
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID'] = datasetID
                            varMap[':nFilesToBeUsed'] = nReadyFiles
                            self.cur.execute(sqlFUU+comment,varMap)
                        # new task status
                        if scoutSucceeded or noBroken:
                            newTaskStatus = 'scouted'
                            taskSpec.setPostScout()
                        else:
                            newTaskStatus = 'tobroken'
                            if taskSpec.getScoutSuccessRate() == None:
                                errorDialog = 'no scout jobs succeeded'
                            else:
                                errorDialog = 'not enough scout jobs succeeded'
                    elif taskSpec.status in ['running','merging','preprocessing','ready']:
                        # get input datasets
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':status'] = 'ready'
                        for tmpType in JediDatasetSpec.getProcessTypes():
                            mapKey = ':type_'+tmpType
                            varMap[mapKey] = tmpType
                        self.cur.execute(sqlRD+comment,varMap)
                        resRD = self.cur.fetchall()
                        varMapList = []
                        mutableFlag = False
                        preprocessedFlag = False
                        for datasetID,dsStatus,nFiles,nFilesFinished,masterID,dsState in resRD:
                            # parent could be still running
                            if dsState == 'mutable':
                                mutableFlag = True
                                break
                            # set status for input datasets
                            varMap = {}
                            varMap[':datasetID']  = datasetID
                            varMap[':jediTaskID'] = jediTaskID
                            if masterID != None:
                                # seconday dataset, this will be reset in post-processor
                                varMap[':status'] = 'done'
                            else:
                                # master dataset
                                if nFiles == nFilesFinished:
                                    # all succeeded
                                    varMap[':status'] = 'done'
                                    preprocessedFlag = True
                                elif nFilesFinished == 0:
                                    # all failed
                                    varMap[':status'] = 'failed'
                                else:
                                    # partially succeeded
                                    varMap[':status'] = 'finished'
                            varMapList.append(varMap)
                        # check just in case if there is mutable dataset 
                        if not mutableFlag:
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':state']  = 'mutable'
                            for tmpType in JediDatasetSpec.getInputTypes():
                                mapKey = ':type_'+tmpType
                                varMap[mapKey] = tmpType
                            self.cur.execute(sqlMTC+comment,varMap)
                            resMTC = self.cur.fetchone()
                            numMutable, = resMTC
                            tmpLog.debug('jediTaskID={0} has {1} mutable datasetes'.format(jediTaskID,numMutable))
                            if numMutable > 0:
                                mutableFlag = True
                        if mutableFlag:
                            # go to defined to trigger CF
                            newTaskStatus = 'defined'
                            # change status of mutable datasets to trigger CF
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':state']  = 'mutable'
                            varMap[':status'] = 'toupdate'
                            self.cur.execute(sqlMUT+comment,varMap)
                            nRow = self.cur.rowcount
                            tmpLog.debug('jediTaskID={0} updated {1} mutable datasetes'.format(jediTaskID,nRow))
                        else:
                            # update input datasets
                            for varMap in varMapList:
                                self.cur.execute(sqlDIU+comment,varMap)
                            # update output datasets
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':type1']  = 'log'
                            varMap[':type2']  = 'output'
                            varMap[':status'] = 'prepared'
                            self.cur.execute(sqlDOU+comment,varMap)
                            # new task status
                            if taskSpec.status == 'preprocessing' and preprocessedFlag:
                                # failed preprocess goes to prepared to terminate the task
                                newTaskStatus = 'registered'
                                # update split rule
                                taskSpec.setPreProcessed()
                                varMap = {}
                                varMap[':jediTaskID'] = jediTaskID
                                varMap[':splitRule']  = taskSpec.splitRule 
                                self.cur.execute(sqlUSL+comment,varMap)
                            else:
                                newTaskStatus = 'prepared'    
                    else:
                        toSkip = True
                        tmpLog.debug('skip jediTaskID={0} due to status={1}'.format(jediTaskID,taskSpec.status))
                    # update tasks
                    if not toSkip:    
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':status'] = newTaskStatus
                        varMap[':errorDialog'] = errorDialog
                        varMap[':splitRule'] = taskSpec.splitRule
                        self.cur.execute(sqlTU+comment,varMap)
                        tmpLog.debug('done new status={0} for jediTaskID={1}'.format(newTaskStatus,jediTaskID))
                # commit    
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # get tasks to be assigned
    def getTasksToAssign_JEDI(self,vo,prodSourceLabel,workQueue):
        comment = ' /* JediDBProxy.getTasksToAssign_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1} queue={2}>'.format(vo,prodSourceLabel,workQueue.queue_name)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retJediTaskIDs = []
        try:
            # sql to get tasks to assign
            varMap = {}
            varMap[':status'] = 'assigning'
            varMap[':worldCloud'] = JediTaskSpec.worldCloudName
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
            sqlSCF  = "SELECT jediTaskID "
            sqlSCF += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlSCF += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlSCF += "AND tabT.status=:status AND tabT.modificationTime<:timeLimit "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlSCF += "AND vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlSCF += "AND prodSourceLabel=:prodSourceLabel "
            sqlSCF += "AND (cloud IS NULL OR "
            sqlSCF += "(cloud=:worldCloud AND EXISTS "
            sqlSCF += "(SELECT 1 FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlSCF += "WHERE {0}.JEDI_Datasets.jediTaskID=tabT.jediTaskID ".format(jedi_config.db.schemaJEDI)
            sqlSCF += "AND type IN (:dsType1,:dsType2) AND destination IS NULL) "
            sqlSCF += ")) "
            varMap[':dsType1'] = 'output'
            varMap[':dsType2'] = 'log'
            sqlSCF += "AND workQueue_ID IN (" 
            for tmpQueue_ID in workQueue.getIDs():
                tmpKey = ':queueID_{0}'.format(tmpQueue_ID)
                varMap[tmpKey] = tmpQueue_ID
                sqlSCF += '{0},'.format(tmpKey)
            sqlSCF  = sqlSCF[:-1]    
            sqlSCF += ") "
            sqlSCF += "ORDER BY currentPriority DESC,jediTaskID FOR UPDATE"
            sqlSPC  = "UPDATE {0}.JEDI_Tasks SET modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlSPC += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # get tasks
            tmpLog.debug(sqlSCF+comment+str(varMap))
            self.cur.execute(sqlSCF+comment,varMap)
            resList = self.cur.fetchall()
            for jediTaskID, in resList:
                # update modificationTime
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                self.cur.execute(sqlSPC+comment,varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    retJediTaskIDs.append(jediTaskID)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return    
            tmpLog.debug('got {0} tasks'.format(len(retJediTaskIDs)))
            return retJediTaskIDs
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # get tasks to check task assignment
    def getTasksToCheckAssignment_JEDI(self,vo,prodSourceLabel,workQueue):
        comment = ' /* JediDBProxy.getTasksToCheckAssignment_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1} queue={2}>'.format(vo,prodSourceLabel,workQueue.queue_name)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retJediTaskIDs = []
        try:
            # sql to get tasks to assign
            varMap = {}
            varMap[':status'] = 'assigning'
            varMap[':worldCloud'] = JediTaskSpec.worldCloudName
            sqlSCF  = "SELECT jediTaskID "
            sqlSCF += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlSCF += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlSCF += "AND tabT.status=:status "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlSCF += "AND vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlSCF += "AND prodSourceLabel=:prodSourceLabel "
            sqlSCF += "AND (cloud IS NULL OR "
            sqlSCF += "(cloud=:worldCloud AND EXISTS "
            sqlSCF += "(SELECT 1 FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlSCF += "WHERE {0}.JEDI_Datasets.jediTaskID=tabT.jediTaskID ".format(jedi_config.db.schemaJEDI)
            sqlSCF += "AND type IN (:dsType1,:dsType2) AND destination IS NULL) "
            sqlSCF += ")) "
            varMap[':dsType1'] = 'output'
            varMap[':dsType2'] = 'log'
            sqlSCF += "AND workQueue_ID IN (" 
            for tmpQueue_ID in workQueue.getIDs():
                tmpKey = ':queueID_{0}'.format(tmpQueue_ID)
                varMap[tmpKey] = tmpQueue_ID
                sqlSCF += '{0},'.format(tmpKey)
            sqlSCF  = sqlSCF[:-1]    
            sqlSCF += ") "
            # begin transaction
            self.conn.begin()
            # get tasks
            tmpLog.debug(sqlSCF+comment+str(varMap))
            self.cur.execute(sqlSCF+comment,varMap)
            resList = self.cur.fetchall()
            for jediTaskID, in resList:
                retJediTaskIDs.append(jediTaskID)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return    
            tmpLog.debug('got {0} tasks'.format(len(retJediTaskIDs)))
            return retJediTaskIDs
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # set cloud to tasks
    def setCloudToTasks_JEDI(self,taskCloudMap):
        comment = ' /* JediDBProxy.setCloudToTasks_JEDI */'
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            if taskCloudMap != {}:
                # sql to update DEFT
                sqlD  = "UPDATE {0}.T_TASK ".format(jedi_config.db.schemaDEFT)
                sqlD += "SET status=:status,timeStamp=CURRENT_DATE"
                sqlD += " WHERE taskID=:jediTaskID "
                for jediTaskID,tmpVal in taskCloudMap.iteritems():
                    # begin transaction
                    self.conn.begin()
                    if isinstance(tmpVal,types.StringType):
                        # sql to set cloud
                        sql  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
                        sql += "SET cloud=:cloud,status=:status,oldStatus=NULL,stateChangeTime=CURRENT_DATE "
                        sql += "WHERE jediTaskID=:jediTaskID AND cloud IS NULL "
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':status']     = 'ready'
                        varMap[':cloud']      = tmpVal
                        # set cloud
                        self.cur.execute(sql+comment,varMap)
                        nRow = self.cur.rowcount
                        tmpLog.debug('set cloud={0} for jediTaskID={1} with {2}'.format(tmpVal,jediTaskID,nRow))
                    else:
                        # sql to set destinations for WORLD cloud
                        sql  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
                        sql += "SET storageToken=:token,destination=:destination "
                        sql += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                        for tmpItem in tmpVal['datasets']:
                            varMap = {}
                            varMap[':jediTaskID']  = jediTaskID
                            varMap[':datasetID']   = tmpItem['datasetID']
                            varMap[':token']       = tmpItem['token']
                            varMap[':destination'] = tmpItem['destination']
                            self.cur.execute(sql+comment,varMap)
                            nRow = self.cur.rowcount
                            tmpLog.debug('set token={0} for jediTaskID={1} datasetID={2} with {3}'.format(tmpItem['token'],
                                                                                                          jediTaskID,
                                                                                                          tmpItem['datasetID'],
                                                                                                          nRow))
                        # sql to set ready
                        sql  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
                        sql += "SET nucleus=:nucleus,status=:newStatus,oldStatus=NULL,stateChangeTime=CURRENT_DATE,modificationTime=CURRENT_DATE-1/24 "
                        sql += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':nucleus']    = tmpVal['nucleus']
                        varMap[':newStatus']  = 'ready'
                        varMap[':oldStatus']  = 'assigning'
                        self.cur.execute(sql+comment,varMap)
                    # update DEFT
                    if nRow > 0:
                        deftStatus = 'ready'
                        self.setSuperStatus_JEDI(jediTaskID,deftStatus)
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':status']     = deftStatus
                        self.cur.execute(sqlD+comment,varMap)
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
            # return    
            tmpLog.debug('done')
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # calculate RW for tasks
    def calculateTaskRW_JEDI(self,jediTaskID):
        comment = ' /* JediDBProxy.calculateTaskRW_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get RW
            sql  = "SELECT ROUND(SUM((nFiles-nFilesFinished-nFilesFailed-nFilesOnHold)*walltime)/24/3600) "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
            sql += "AND tabT.jediTaskID=:jediTaskID "
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            # begin transaction
            self.conn.begin()
            # get
            self.cur.execute(sql+comment,varMap)
            resRT = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # locked by another
            if resRT == None:
                retVal = None
            else:
                retVal = resRT[0]
            tmpLog.debug('RW={0}'.format(retVal))
            # return    
            tmpLog.debug('done')
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # calculate RW with a priority
    def calculateRWwithPrio_JEDI(self,vo,prodSourceLabel,workQueue,priority):
        comment = ' /* JediDBProxy.calculateRWwithPrio_JEDI */'
        methodName = self.getMethodName(comment)
        if workQueue == None:
            methodName += ' <vo={0} label={1} queue={2} prio={3}>'.format(vo,prodSourceLabel,None,priority)
        else:
            methodName += ' <vo={0} label={1} queue={2} prio={3}>'.format(vo,prodSourceLabel,workQueue.queue_name,priority)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get RW
            varMap = {}
            varMap[':vo'] = vo
            varMap[':prodSourceLabel'] = prodSourceLabel
            if priority != None:
                varMap[':priority'] = priority
            sql  = "SELECT tabT.jediTaskID,tabT.cloud,tabD.datasetID,nFiles-nFilesFinished-nFilesFailed,walltime "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sql += "AND tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
            sql += "AND (nFiles-nFilesFinished-nFilesFailed)>0 "
            sql += "AND tabT.vo=:vo AND prodSourceLabel=:prodSourceLabel "
            if priority != None:
                sql += "AND currentPriority>=:priority "
            if workQueue != None:
                sql += "AND workQueue_ID IN (" 
                for tmpQueue_ID in workQueue.getIDs():
                    tmpKey = ':queueID_{0}'.format(tmpQueue_ID)
                    varMap[tmpKey] = tmpQueue_ID
                    sql += '{0},'.format(tmpKey)
                sql  = sql[:-1]    
                sql += ") "
            sql += "AND tabT.status IN (:status1,:status2,:status3,:status4) "
            sql += "AND tabD.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sql += '{0},'.format(mapKey)
                varMap[mapKey] = tmpType
            sql  = sql[:-1]
            sql += ") "
            varMap[':status1'] = 'ready'
            varMap[':status2'] = 'scouting'
            varMap[':status3'] = 'running'
            varMap[':status4'] = 'pending'
            sql += "AND tabT.cloud IS NOT NULL "
            # begin transaction
            self.conn.begin()
            # set cloud
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # loop over all tasks
            retMap = {}
            sqlF  = "SELECT fsize,startEvent,endEvent,nEvents "
            sqlF += "FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND rownum<=1"
            for jediTaskID,cloud,datasetID,nRem,walltime in resList:
                # get effective size
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':datasetID'] = datasetID
                # begin transaction
                self.conn.begin()
                # get file
                self.cur.execute(sqlF+comment,varMap)
                resFile = self.cur.fetchone()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                if resFile != None:
                    # calculate RW using effective size
                    fsize,startEvent,endEvent,nEvents = resFile
                    effectiveFsize = JediCoreUtils.getEffectiveFileSize(fsize,startEvent,endEvent,nEvents)
                    tmpRW = nRem * effectiveFsize * walltime
                    if not cloud in retMap:
                        retMap[cloud] = 0
                    retMap[cloud] += tmpRW    
            for cloudName,rwValue in retMap.iteritems():
                retMap[cloudName] = int(rwValue/24/3600)
            tmpLog.debug('RW={0}'.format(str(retMap)))
            # return    
            tmpLog.debug('done')
            return retMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # calculate WORLD RW with a priority
    def calculateWorldRWwithPrio_JEDI(self,vo,prodSourceLabel,workQueue,priority):
        comment = ' /* JediDBProxy.calculateWorldRWwithPrio_JEDI */'
        methodName = self.getMethodName(comment)
        if workQueue == None:
            methodName += ' <vo={0} label={1} queue={2} prio={3}>'.format(vo,prodSourceLabel,None,priority)
        else:
            methodName += ' <vo={0} label={1} queue={2} prio={3}>'.format(vo,prodSourceLabel,workQueue.queue_name,priority)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get RW
            varMap = {}
            varMap[':vo'] = vo
            varMap[':prodSourceLabel'] = prodSourceLabel
            varMap[':worldCloud'] = JediTaskSpec.worldCloudName
            if priority != None:
                varMap[':priority'] = priority
            sql  = "SELECT tabT.nucleus,SUM((nEvents-nEventsUsed)*DECODE(cpuTime,NULL,300,cpuTime)) "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sql += "AND tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
            sql += "AND (nFiles-nFilesFinished-nFilesFailed)>0 "
            sql += "AND tabT.vo=:vo AND prodSourceLabel=:prodSourceLabel "
            sql += "AND tabT.cloud=:worldCloud "
            if priority != None:
                sql += "AND currentPriority>=:priority "
            if workQueue != None:
                sql += "AND workQueue_ID IN (" 
                for tmpQueue_ID in workQueue.getIDs():
                    tmpKey = ':queueID_{0}'.format(tmpQueue_ID)
                    varMap[tmpKey] = tmpQueue_ID
                    sql += '{0},'.format(tmpKey)
                sql  = sql[:-1]    
                sql += ") "
            sql += "AND tabT.status IN (:status1,:status2,:status3,:status4) "
            sql += "AND tabD.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sql += '{0},'.format(mapKey)
                varMap[mapKey] = tmpType
            sql  = sql[:-1]
            sql += ") "
            varMap[':status1'] = 'ready'
            varMap[':status2'] = 'scouting'
            varMap[':status3'] = 'running'
            varMap[':status4'] = 'pending'
            sql += "GROUP BY tabT.nucleus "
            # begin transaction
            self.conn.begin()
            # set cloud
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # loop over all nuclei
            retMap = {}
            for nucleus,worldRW in resList:
                retMap[nucleus] = worldRW
            tmpLog.debug('RW={0}'.format(str(retMap)))
            # return    
            tmpLog.debug('done')
            return retMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # calculate WORLD RW for tasks
    def calculateTaskWorldRW_JEDI(self,jediTaskID):
        comment = ' /* JediDBProxy.calculateTaskWorldRW_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get RW
            sql  = "SELECT (nEvents-nEventsUsed)*DECODE(cpuTime,NULL,300,cpuTime) "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
            sql += "AND tabT.jediTaskID=:jediTaskID "
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            # begin transaction
            self.conn.begin()
            # get
            self.cur.execute(sql+comment,varMap)
            resRT = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # locked by another
            if resRT == None:
                retVal = None
            else:
                retVal = resRT[0]
            tmpLog.debug('RW={0}'.format(retVal))
            # return    
            tmpLog.debug('done')
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # get the list of tasks to exec command
    def getTasksToExecCommand_JEDI(self,vo,prodSourceLabel):
        comment = ' /* JediDBProxy.getTasksToExecCommand_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <vo={0} label={1}>".format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retTaskIDs = {}
        commandStatusMap = JediTaskSpec.commandStatusMap()
        try:
            # sql to get jediTaskIDs to exec a command from the command table
            varMap = {}
            varMap[':comm_owner'] = 'DEFT'
            sqlC  = "SELECT comm_task,comm_cmd,comm_comment FROM {0}.PRODSYS_COMM ".format(jedi_config.db.schemaDEFT)
            sqlC += "WHERE comm_owner=:comm_owner AND comm_cmd IN ("
            for commandStr,taskStatusMap in commandStatusMap.iteritems():
                tmpKey = ':comm_cmd_{0}'.format(commandStr)
                varMap[tmpKey] = commandStr
                sqlC += '{0},'.format(tmpKey)
            sqlC  = sqlC[:-1]
            sqlC += ") "
            if not vo in [None,'any']:
                varMap[':comm_vo'] = vo
                sqlC += "AND comm_vo=:comm_vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':comm_prodSourceLabel'] = prodSourceLabel
                sqlC += "AND comm_prodSourceLabel=:comm_prodSourceLabel "
            sqlC += "ORDER BY comm_ts "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            tmpLog.debug(sqlC+comment+str(varMap))
            self.cur.execute(sqlC+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('got {0} tasks'.format(len(resList)))            
            for jediTaskID,commandStr,comComment in resList:
                tmpLog.debug('start jediTaskID={0} command={1}'.format(jediTaskID,commandStr))
               # start transaction
                self.conn.begin()
                # lock
                varMap = {}
                varMap[':comm_task'] = jediTaskID
                sqlLock  = "SELECT comm_cmd FROM {0}.PRODSYS_COMM WHERE comm_task=:comm_task ".format(jedi_config.db.schemaDEFT)
                sqlLock += "FOR UPDATE "
                toSkip = False                
                try:
                    tmpLog.debug(sqlLock+comment+str(varMap))
                    self.cur.execute(sqlLock+comment,varMap)
                except:
                    errType,errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        # resource busy and acquire with NOWAIT specified
                        toSkip = True
                        tmpLog.debug('skip locked+nowauit jediTaskID={0}'.format(jediTaskID))
                    else:
                        # failed with something else
                        raise errType,errValue
                isOK = True
                if not toSkip:     
                    if isOK:
                        # check task status
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        sqlTC =  "SELECT status,oldStatus FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
                        sqlTC += "WHERE jediTaskID=:jediTaskID FOR UPDATE "
                        self.cur.execute(sqlTC+comment,varMap)
                        resTC = self.cur.fetchone()
                        if resTC == None or resTC[0] == None:
                            tmpLog.error("jediTaskID={0} is not found in JEDI_Tasks".format(jediTaskID))
                            isOK = False
                        else:
                            taskStatus,taskOldStatus = resTC
                            tmpLog.debug("jediTaskID={0} in status:{1} oldStatud:{2}".format(jediTaskID,taskStatus,taskOldStatus))
                            if commandStr == 'retry':
                                if not taskStatus in JediTaskSpec.statusToRetry():
                                    # task is in a status which rejects retry
                                    tmpLog.error("jediTaskID={0} rejected command={1}. status={2} is not for retry".format(jediTaskID,
                                                                                                                           commandStr,taskStatus))
                                    isOK = False
                            elif commandStr == 'incexec':
                                if not taskStatus in JediTaskSpec.statusToIncexec():
                                    # task is in a status which rejects retry
                                    tmpLog.error("jediTaskID={0} rejected command={1}. status={2} is not for incexec".format(jediTaskID,
                                                                                                                             commandStr,taskStatus))
                                    isOK = False
                            elif commandStr == 'pause':
                                if taskStatus in JediTaskSpec.statusNotToPause():
                                    # task is in a status which rejects pause
                                    tmpLog.error("jediTaskID={0} rejected command={1}. status={2} is not for pause".format(jediTaskID,
                                                                                                                           commandStr,taskStatus))
                                    isOK = False
                            elif commandStr == 'resume':
                                if not taskStatus in ['paused','throttled']:
                                    # task is in a status which rejects resume
                                    tmpLog.error("jediTaskID={0} rejected command={1}. status={2} is not for resume".format(jediTaskID,
                                                                                                                            commandStr,taskStatus))
                                    isOK = False
                            elif taskStatus in JediTaskSpec.statusToRejectExtChange():
                                # task is in a status which rejects external changes
                                tmpLog.error("jediTaskID={0} rejected command={1} (due to status={2})".format(jediTaskID,commandStr,taskStatus))
                                isOK = False
                            if isOK:
                                # set new task status
                                if commandStatusMap.has_key(commandStr):
                                    newTaskStatus = commandStatusMap[commandStr]['doing']
                                else:
                                    tmpLog.error("jediTaskID={0} new status is undefined for command={1}".format(jediTaskID,commandStr))
                                    isOK = False
                    if isOK:
                        # update task status
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':taskStatus'] = taskStatus
                        if newTaskStatus != 'dummy':
                            varMap[':status'] = newTaskStatus
                        varMap[':errDiag'] = comComment
                        sqlTU  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
                        if newTaskStatus != 'dummy':
                            sqlTU += "SET status=:status,"
                        else:
                            sqlTU += "SET status=oldStatus,"
                        if taskStatus in ['paused']:
                            sqlTU += "oldStatus=NULL,"
                        elif not taskStatus in ['pending']:
                            sqlTU += "oldStatus=status,"
                        sqlTU += "modificationTime=CURRENT_DATE,errorDialog=:errDiag,stateChangeTime=CURRENT_DATE "
                        sqlTU += "WHERE jediTaskID=:jediTaskID AND status=:taskStatus "
                        tmpLog.debug(sqlTU+comment+str(varMap))
                        self.cur.execute(sqlTU+comment,varMap)
                        nRow = self.cur.rowcount
                        if nRow != 1:
                            tmpLog.debug('skip updated jediTaskID={0}'.format(jediTaskID))
                            toSkip = True
                    # update command table
                    if not toSkip:
                        varMap = {}
                        varMap[':comm_task'] = jediTaskID
                        if isOK:
                            varMap[':comm_cmd']  = commandStr+'ing'
                        else:
                            varMap[':comm_cmd']  = commandStr+' failed'
                        sqlUC = "UPDATE {0}.PRODSYS_COMM SET comm_cmd=:comm_cmd WHERE comm_task=:comm_task ".format(jedi_config.db.schemaDEFT)
                        self.cur.execute(sqlUC+comment,varMap)
                        # append
                        if isOK:
                            if not commandStr in ['pause','resume']:
                                retTaskIDs[jediTaskID] = {'command':commandStr,'comment':comComment,
                                                          'oldStatus':taskStatus}
                                # use old status if pending
                                if taskStatus == 'pending':
                                    retTaskIDs[jediTaskID]['oldStatus'] = taskOldStatus
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # find orphaned tasks to rescue
            for commandStr,taskStatusMap in commandStatusMap.iteritems():
                varMap = {}
                varMap[':status'] = taskStatusMap['doing']
                # skip dummy status
                if varMap[':status'] == 'dummy':
                    continue
                self.conn.begin()
                # FIXME
                #varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
                varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
                sqlOrpS  = "SELECT jediTaskID,errorDialog,oldStatus "
                sqlOrpS += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
                sqlOrpS += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
                sqlOrpS += "AND tabT.status=:status AND tabT.modificationtime<:timeLimit "
                if not vo in [None,'any']:
                    sqlOrpS += 'AND vo=:vo '
                    varMap[':vo'] = vo
                if not prodSourceLabel in [None,'any']:
                    sqlOrpS += 'AND prodSourceLabel=:prodSourceLabel '
                    varMap[':prodSourceLabel'] = prodSourceLabel
                sqlOrpS += "FOR UPDATE "
                tmpLog.debug(sqlOrpS+comment+str(varMap))
                self.cur.execute(sqlOrpS+comment,varMap)
                resList = self.cur.fetchall()
                # update modtime to avoid immediate reattempts
                sqlOrpU  = "UPDATE {0}.JEDI_Tasks SET modificationtime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
                sqlOrpU += "WHERE jediTaskID=:jediTaskID "
                for jediTaskID,comComment,oldStatus in resList:
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    tmpLog.debug(sqlOrpU+comment+str(varMap))
                    self.cur.execute(sqlOrpU+comment,varMap)
                    nRow = self.cur.rowcount
                    if nRow == 1 and not retTaskIDs.has_key(jediTaskID):
                        retTaskIDs[jediTaskID] = {'command':commandStr,'comment':comComment,
                                                  'oldStatus':oldStatus}
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # read clob
            sqlCC  = "SELECT comm_parameters FROM {0}.PRODSYS_COMM WHERE comm_task=:comm_task ".format(jedi_config.db.schemaDEFT)
            for jediTaskID in retTaskIDs.keys():
                if retTaskIDs[jediTaskID]['command'] in ['incexec']:
                    # start transaction
                    self.conn.begin()
                    varMap = {}
                    varMap[':comm_task'] = jediTaskID
                    self.cur.execute(sqlCC+comment, varMap)
                    tmpComComment = None
                    for clobCC, in self.cur:
                        if clobCC != None:
                            tmpComComment = clobCC.read()
                        break
                    if not tmpComComment in ['',None]:
                        retTaskIDs[jediTaskID]['comment'] = tmpComComment
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
            # convert to list
            retTaskList = []
            for jediTaskID,varMap in retTaskIDs.iteritems():
                retTaskList.append((jediTaskID,varMap))
            # return
            tmpLog.debug("return {0} tasks".format(len(retTaskList)))
            return retTaskList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # get the list of PandaIDs for a task
    def getPandaIDsWithTask_JEDI(self,jediTaskID,onlyActive):
        comment = ' /* JediDBProxy.getPandaIDsWithTask_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0} onlyActive={1}>".format(jediTaskID,onlyActive)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retPandaIDs = []
        try:
            # sql to get PandaIDs
            tables = ['{0}.jobsDefined4'.format(jedi_config.db.schemaPANDA),
                      '{0}.jobsWaiting4'.format(jedi_config.db.schemaPANDA),
                      '{0}.jobsActive4'.format(jedi_config.db.schemaPANDA)]
            if not onlyActive:
                tables += ['{0}.jobsArchived4'.format(jedi_config.db.schemaPANDA),
                           '{0}.jobsArchived'.format(jedi_config.db.schemaPANDAARCH)]
            sqlP = ''
            for tableName in tables:
                if sqlP != "":
                    sqlP += "UNION ALL "
                sqlP += "SELECT PandaID FROM {0} WHERE jediTaskID=:jediTaskID ".format(tableName)    
                if tableName.startswith(jedi_config.db.schemaPANDAARCH):
                    sqlP += "AND modificationTime>(CURRENT_DATE-30) "
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000000
            self.cur.execute(sqlP+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            for pandaID, in resList:
                if not pandaID in retPandaIDs:
                    retPandaIDs.append(pandaID)
            # return
            tmpLog.debug("return {0} PandaIDs".format(len(retPandaIDs)))
            return retPandaIDs
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # get the list of queued PandaIDs for a task
    def getQueuedPandaIDsWithTask_JEDI(self,jediTaskID):
        comment = ' /* JediDBProxy.getQueuedPandaIDsWithTask_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retPandaIDs = []
        try:
            # sql to get PandaIDs
            tables = ['{0}.jobsDefined4'.format(jedi_config.db.schemaPANDA),
                      '{0}.jobsWaiting4'.format(jedi_config.db.schemaPANDA),
                      '{0}.jobsActive4'.format(jedi_config.db.schemaPANDA)]
            sqlP = ''
            for tableName in tables:
                if sqlP != "":
                    sqlP += "UNION ALL "
                sqlP += "SELECT PandaID FROM {0} WHERE jediTaskID=:jediTaskID ".format(tableName)    
                sqlP += "AND jobStatus NOT IN (:st1,:st2,:st3) "
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':st1'] = 'running'
            varMap[':st2'] = 'holding'
            varMap[':st3'] = 'transferring'
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000000
            self.cur.execute(sqlP+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            for pandaID, in resList:
                if not pandaID in retPandaIDs:
                    retPandaIDs.append(pandaID)
            # return
            tmpLog.debug("return {0} PandaIDs".format(len(retPandaIDs)))
            return retPandaIDs
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # get jediTaskID/datasetID/FileID with dataset and file names
    def getIDsWithFileDataset_JEDI(self,datasetName,fileName,fileType):
        comment = ' /* JediDBProxy.getIDsWithFileDataset_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <dataset={0} file={1} type={2}>".format(datasetName,fileName,fileType)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retPandaIDs = []
        try:
            # sql to get jediTaskID and datasetID
            sqlT  = "SELECT jediTaskID,datasetID FROM {0}.JEDI_Datasets WHERE ".format(jedi_config.db.schemaJEDI)
            sqlT += "datasetName=:datasetName and type=:type "
            # sql to get fileID
            sqlF  = "SELECT FileID FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
            sqlF += "jediTaskID=:jediTaskID AND datasetID=:datasetID and lfn=:lfn "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[':datasetName'] = datasetName
            varMap[':type'] = fileType
            self.cur.arraysize = 1000000
            self.cur.execute(sqlT+comment,varMap)
            resList = self.cur.fetchall()
            retMap = None
            for jediTaskID,datasetID in resList:
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':datasetID'] = datasetID
                varMap[':lfn'] = fileName
                self.cur.execute(sqlF+comment,varMap)
                resFileList = self.cur.fetchall()
                if resFileList != []:
                    retMap = {}
                    retMap['jediTaskID'] = jediTaskID
                    retMap['datasetID']  = datasetID
                    retMap['fileID']     = resFileList[0][0]
                    break
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("return {0}".format(str(retMap)))
            return True,retMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,None




    # get JOBSARCHVIEW corresponding to a timestamp
    def getArchView(self,timeStamp):
        tableList = [
            (7,  'JOBSARCHVIEW_7DAYS'),
            (15, 'JOBSARCHVIEW_15DAYS'),
            (30, 'JOBSARCHVIEW_30DAYS'),
            (60, 'JOBSARCHVIEW_60DAYS'),
            (90, 'JOBSARCHVIEW_90DAYS'),
            (180,'JOBSARCHVIEW_180DAYS'),
            (365,'JOBSARCHVIEW_365DAYS'),
            ]
        timeDelta = datetime.datetime.utcnow() - timeStamp 
        for timeLimit,archViewName in tableList:
            # +2 for safety margin
            if timeDelta < datetime.timedelta(days=timeLimit+2):
                return archViewName
        # range over
        return None


    

    # get PandaID for a file
    def getPandaIDWithFileID_JEDI(self,jediTaskID,datasetID,fileID):
        comment = ' /* JediDBProxy.getPandaIDWithFileID_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0} datasetID={1} fileID={2}>".format(jediTaskID,datasetID,fileID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retPandaIDs = []
        try:
            # sql to get PandaID
            sqlP  = "SELECT PandaID FROM {0}.filesTable4 WHERE ".format(jedi_config.db.schemaPANDA)
            sqlP += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # get creation time of the task
            sqlCT = "SELECT creationDate FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID ".format(jedi_config.db.schemaJEDI)
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':datasetID'] = datasetID
            varMap[':fileID'] = fileID
            self.cur.arraysize = 100
            self.cur.execute(sqlP+comment,varMap)
            resP = self.cur.fetchone()
            pandaID = None
            if resP != None:
                # found in live table
                pandaID = resP[0]
            else:
                # get creation time of the task
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                self.cur.execute(sqlCT+comment,varMap)
                resCT = self.cur.fetchone()
                if resCT != None:
                    creationDate, = resCT
                    archView = self.getArchView(creationDate)
                    if archView == None:
                        tmpLog.debug("no JOBSARCHVIEW since creationDate is too old") 
                    else:
                        # sql to get PandaID using JOBSARCHVIEW
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':datasetID'] = datasetID
                        varMap[':fileID'] = fileID
                        sqlAP  = "SELECT fTab.PandaID "
                        sqlAP += "FROM {0}.filesTable_ARCH fTab,{0}.{1} aTab WHERE ".format(jedi_config.db.schemaPANDAARCH,
                                                                                           archView)
                        sqlAP += "fTab.PandaID=aTab.PandaID AND aTab.jediTaskID=:jediTaskID "
                        sqlAP += "AND fTab.jediTaskID=:jediTaskID AND fTab.datasetID=:datasetID "
                        sqlAP += "AND fTab.fileID=:fileID "
                        tmpLog.debug(sqlAP+comment+str(varMap))
                        self.cur.execute(sqlAP+comment,varMap)
                        resAP = self.cur.fetchone()
                        if resAP != None:
                            pandaID = resAP[0]
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("PandaID -> {0}".format(pandaID))
            return True,pandaID
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,None



    # get JEDI files for a job
    def getFilesWithPandaID_JEDI(self,pandaID):
        comment = ' /* JediDBProxy.getFilesWithPandaID_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <pandaID={0}>".format(pandaID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retPandaIDs = []
        try:
            # sql to get fileID
            sqlT  = "SELECT jediTaskID,datasetID,fileID FROM {0}.filesTable4 WHERE ".format(jedi_config.db.schemaPANDA)
            sqlT += "pandaID=:pandaID "
            sqlT += "UNION ALL "
            sqlT += "SELECT jediTaskID,datasetID,fileID FROM {0}.filesTable_ARCH WHERE ".format(jedi_config.db.schemaPANDAARCH)
            sqlT += "pandaID=:pandaID "
            sqlT += "AND modificationTime>CURRENT_DATE-180"
            # sql to read files
            sqlFR  = "SELECT {0} ".format(JediFileSpec.columnNames())
            sqlFR += "FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID and fileID=:fileID "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[':pandaID'] = pandaID
            self.cur.arraysize = 1000000
            self.cur.execute(sqlT+comment,varMap)
            resTC = self.cur.fetchall()
            fileIDList = []
            fileSpecList = []
            # loop over all fileIDs
            for jediTaskID,datasetID,fileID in resTC:
                # skip duplication
                if fileID in fileIDList:
                    continue
                # read files
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':datasetID'] = datasetID
                varMap[':fileID'] = fileID
                self.cur.execute(sqlFR+comment,varMap)
                tmpRes = self.cur.fetchone()
                fileSpec = JediFileSpec()
                fileSpec.pack(tmpRes)
                fileSpecList.append(fileSpec)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("got {0} files".format(len(fileSpecList)))
            return True,fileSpecList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,None



    # update task parameters
    def updateTaskParams_JEDI(self,jediTaskID,taskParams):
        comment = ' /* JediDBProxy.updateTaskParams_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retPandaIDs = []
        try:
            # sql to update task params
            sqlT  = "UPDATE {0}.JEDI_TaskParams SET taskParams=:taskParams ".format(jedi_config.db.schemaJEDI)
            sqlT += "WHERE jediTaskID=:jediTaskID "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':taskParams'] = taskParams
            self.cur.execute(sqlT+comment,varMap)
            nRow = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("updated {0} rows".format(nRow))
            if nRow == 1:
                return True
            else:
                return False
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # reactivate pending tasks
    def reactivatePendingTasks_JEDI(self,vo,prodSourceLabel,timeLimit,timeoutLimit=None,minPriority=None):
        comment = ' /* JediDBProxy.reactivatePendingTasks_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <vo={0} label={1} limit={2}min timeout={3}days minPrio={4}>".format(vo,prodSourceLabel,timeLimit,
                                                                                            timeoutLimit,minPriority)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            timeoutDate = None
            if timeoutLimit != None:
                timeoutDate = datetime.datetime.utcnow() - datetime.timedelta(days=timeoutLimit)
            # sql to get pending tasks
            varMap = {}
            varMap[':status'] = 'pending'
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=timeLimit)
            sqlTL  = "SELECT jediTaskID,frozenTime,errorDialog,parent_tid,splitRule,startTime "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += "AND tabT.status=:status AND tabT.modificationTime<:timeLimit AND tabT.oldStatus IS NOT NULL "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlTL += "AND vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlTL += "AND prodSourceLabel=:prodSourceLabel "
            if minPriority != None:
                varMap[':minPriority'] = minPriority
                sqlTL += "AND currentPriority>:minPriority "
            # sql to update tasks    
            sqlTU  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTU += "SET status=oldStatus,oldStatus=NULL,errorDialog=NULL,modificationtime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND oldStatus IS NOT NULL "
            # sql to timeout tasks    
            sqlTO  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTO += "SET status=:newStatus,errorDialog=:errorDialog,modificationtime=CURRENT_DATE,stateChangeTime=CURRENT_DATE "
            sqlTO += "WHERE jediTaskID=:jediTaskID "
            # sql to keep pending
            sqlTK  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTK += "SET modificationtime=CURRENT_DATE,frozenTime=CURRENT_DATE "
            sqlTK += "WHERE jediTaskID=:jediTaskID "
            # sql to update DEFT task status
            sqlTT  = "UPDATE {0}.T_TASK ".format(jedi_config.db.schemaDEFT)
            sqlTT += "SET status=:status,timeStamp=CURRENT_DATE "
            sqlTT += "WHERE taskID=:jediTaskID "
            # sql to check the number of finished files
            sqlND  = "SELECT SUM(nFilesFinished) FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlND += "WHERE jediTaskID=:jediTaskID AND type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sqlND += '{0},'.format(mapKey)
            sqlND  = sqlND[:-1]
            sqlND += ") AND masterID IS NULL "
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlTL+comment,varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            nRow = 0
            for jediTaskID,frozenTime,errorDialog,parent_tid,splitRule,startTime in resTL:
                timeoutFlag = False
                keepFlag = False
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                # check parent
                parentRunning = False
                if not parent_tid in [None,jediTaskID]:
                    tmpStat = self.checkParentTask_JEDI(parent_tid,useCommit=False)
                    # if parent is running
                    if tmpStat == 'running':
                        parentRunning = True
                        if not JediTaskSpec.noWaitParentSL(splitRule):
                            # keep pending 
                            sql = sqlTK
                            keepFlag = True
                if not keepFlag:
                    # if timeout
                    if not parentRunning and timeoutDate != None and frozenTime != None and frozenTime < timeoutDate:
                        timeoutFlag = True
                        # check the number of finished files
                        for tmpType in JediDatasetSpec.getInputTypes():
                            mapKey = ':type_'+tmpType
                            varMap[mapKey] = tmpType
                        self.cur.execute(sqlND+comment,varMap)
                        tmpND = self.cur.fetchone()
                        if tmpND != None and tmpND[0] > 0:
                            abortingFlag = False
                        else:
                            abortingFlag = True
                        # go to aborting if no finished
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        if abortingFlag:
                            varMap[':newStatus'] = 'aborting'
                        else:
                            varMap[':newStatus'] = 'finishing'
                        if errorDialog == None:
                            errorDialog = ''
                        else:
                            errorDialog += '. '
                        errorDialog += 'timeout while in pending since {0}'.format(frozenTime.strftime('%Y/%m/%d %H:%M:%S'))
                        varMap[':errorDialog'] = errorDialog[:JediTaskSpec._limitLength['errorDialog']]
                        sql = sqlTO
                    else:
                        sql = sqlTU
                self.cur.execute(sql+comment,varMap)
                if timeoutFlag:
                    tmpLog.debug('jediTaskID={0} timeout'.format(jediTaskID))
                elif keepFlag:
                    tmpLog.debug('jediTaskID={0} keep pending'.format(jediTaskID))
                else:
                    tmpLog.debug('jediTaskID={0} reactivated'.format(jediTaskID))
                nRow += self.cur.rowcount
                # update DEFT for timeout
                if timeoutFlag and abortingFlag:
                    deftStatus = 'aborted'
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':status'] = deftStatus
                    self.cur.execute(sqlTT+comment,varMap)
                    self.setSuperStatus_JEDI(jediTaskID,deftStatus)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("updated {0} rows".format(nRow))
            return nRow
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # restart contents update
    def restartTasksForContentsUpdate_JEDI(self,vo,prodSourceLabel,timeLimit):
        comment = ' /* JediDBProxy.restartTasksForContentsUpdate_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <vo={0} label={1} limit={2}min>".format(vo,prodSourceLabel,timeLimit)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get stalled tasks in defined
            varMap = {}
            varMap[':taskStatus1'] = 'defined'
            varMap[':taskStatus2'] = 'ready'
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=timeLimit)
            varMap[':dsType'] = 'input'
            varMap[':dsState'] = 'mutable'
            varMap[':dsStatus1'] = 'ready'
            varMap[':dsStatus2'] = 'toupdate'
            sqlTL  = "SELECT distinct tabT.jediTaskID,tabT.status "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
            sqlTL += "AND ((tabT.status=:taskStatus1 AND tabD.status=:dsStatus1) OR (tabT.status=:taskStatus2 AND tabD.status=:dsStatus2)) "
            sqlTL += "AND tabD.type=:dsType AND tabD.state=:dsState AND tabT.modificationTime<:timeLimit "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlTL += "AND tabT.vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlTL += "AND tabT.prodSourceLabel=:prodSourceLabel "
            # sql to update datasets    
            sqlTU  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlTU += "SET status=:newStatus "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND type=:type AND state=:state AND status=:oldStatus "
            # sql to update task
            sqlTD  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTD += "SET status=:newStatus,modificationtime=CURRENT_DATE "
            sqlTD += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # start transaction
            self.conn.begin()
            # get jediTaskIDs
            self.cur.execute(sqlTL+comment,varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            nTasks = 0
            for jediTaskID,taskStatus in resTL:
                if taskStatus == 'defined':
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':type'] ='input'
                    varMap[':state'] = 'mutable'
                    varMap[':oldStatus'] = 'ready'
                    varMap[':newStatus'] = 'toupdate'
                    self.cur.execute(sqlTU+comment,varMap)
                    nRow = self.cur.rowcount
                    tmpLog.debug('jediTaskID={0} toupdate {1} datasets'.format(jediTaskID,nRow))
                    if nRow > 0:
                        nTasks += 1
                        # update task
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':oldStatus'] = 'defined'
                        varMap[':newStatus'] = 'defined'
                        self.cur.execute(sqlTD+comment,varMap)
                else:
                    # update task
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':oldStatus'] = 'ready'
                    varMap[':newStatus'] = 'defined'
                    self.cur.execute(sqlTD+comment,varMap)
                    nRow = self.cur.rowcount
                    if nRow > 0:
                        tmpLog.debug('jediTaskID={0} back to defined'.format(jediTaskID,nRow))
                        nTasks += 1
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("done")
            return nTasks
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # kick exhausted tasks
    def kickExhaustedTasks_JEDI(self,vo,prodSourceLabel,timeLimit):
        comment = ' /* JediDBProxy.kickExhaustedTasks_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <vo={0} label={1} limit={2}h>".format(vo,prodSourceLabel,timeLimit)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get stalled tasks
            varMap = {}
            varMap[':taskStatus'] = 'exhausted'
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(hours=timeLimit)
            sqlTL  = "SELECT tabT.jediTaskID "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += "AND tabT.status=:taskStatus AND tabT.modificationTime<:timeLimit "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlTL += "AND tabT.vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlTL += "AND tabT.prodSourceLabel=:prodSourceLabel "
            # sql to timeout tasks    
            sqlTO  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTO += "SET status=:newStatus,modificationtime=CURRENT_DATE,stateChangeTime=CURRENT_DATE "
            sqlTO += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            # start transaction
            self.conn.begin()
            # get jediTaskIDs
            self.cur.execute(sqlTL+comment,varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            nTasks = 0
            for jediTaskID, in resTL:
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':oldStatus'] = 'exhausted'
                varMap[':newStatus'] = 'finishing'
                self.cur.execute(sqlTO+comment,varMap)
                nRow = self.cur.rowcount
                tmpLog.debug('jediTaskID={0} to {1} with {2}'.format(jediTaskID,
                                                                     varMap[':newStatus'],
                                                                     nRow))
                if nRow > 0:
                    nTasks += 1
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("done")
            return nTasks
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # get file spec of lib.tgz
    def getBuildFileSpec_JEDI(self,jediTaskID,siteName,associatedSites):
        comment = ' /* JediDBProxy.getBuildFileSpec_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0} siteName={1}>".format(jediTaskID,siteName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        tmpLog.debug('associatedSites={0}'.format(str(associatedSites)))
        try:
            # sql to get dataset
            sqlRD  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
            sqlRD += "FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlRD += "WHERE jediTaskID=:jediTaskID AND type=:type AND site=:site "
            sqlRD += "AND (state IS NULL OR state<>:state) "
            sqlRD += "ORDER BY creationTime DESC "
            # sql to read files
            sqlFR  = "SELECT {0} ".format(JediFileSpec.columnNames())
            sqlFR += "FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND type=:type "
            sqlFR += "AND status IN (:status1,:status2) "
            sqlFR += "ORDER BY creationDate DESC "
            # start transaction
            self.conn.begin()
            foundFlag = False
            for tmpSiteName in [siteName]+associatedSites:
                # get dataset
                varMap = {}
                varMap[':type'] = 'lib'
                varMap[':site'] = tmpSiteName
                varMap[':state'] = 'closed'
                varMap[':jediTaskID'] = jediTaskID
                self.cur.execute(sqlRD+comment,varMap)
                resList = self.cur.fetchall()
                # loop over all datasets
                fileSpec = None
                datasetSpec = None
                for resItem in resList:
                    datasetSpec = JediDatasetSpec()
                    datasetSpec.pack(resItem)
                    # get file
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':datasetID']  = datasetSpec.datasetID
                    varMap[':type']       = 'lib'
                    varMap[':status1']    = 'finished'
                    varMap[':status2']    = 'running'
                    self.cur.execute(sqlFR+comment,varMap)
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
                raise RuntimeError, 'Commit error'
            # return
            if fileSpec != None:
                tmpLog.debug("got lib.tgz={0}".format(fileSpec.lfn))
            else:
                tmpLog.debug("no lib.tgz")
            return True,fileSpec,datasetSpec
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,None 



    # get file spec of old lib.tgz
    def getOldBuildFileSpec_JEDI(self,jediTaskID,datasetID,fileID):
        comment = ' /* JediDBProxy.getOldBuildFileSpec_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0} datasetID={1} fileID={2}>".format(jediTaskID,datasetID,fileID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get dataset
            sqlRD  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
            sqlRD += "FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlRD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to read files
            sqlFR  = "SELECT {0} ".format(JediFileSpec.columnNames())
            sqlFR += "FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # start transaction
            self.conn.begin()
            # get dataset
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':datasetID']  = datasetID
            self.cur.execute(sqlRD+comment,varMap)
            tmpRes = self.cur.fetchone()
            datasetSpec = JediDatasetSpec()
            datasetSpec.pack(tmpRes)
            # get file
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':datasetID']  = datasetID
            varMap[':fileID']     = fileID
            self.cur.execute(sqlFR+comment,varMap)
            tmpRes = self.cur.fetchone()
            fileSpec = JediFileSpec()
            fileSpec.pack(tmpRes)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            if fileSpec != None:
                tmpLog.debug("got lib.tgz={0}".format(fileSpec.lfn))
            else:
                tmpLog.debug("no lib.tgz")
            return True,fileSpec,datasetSpec
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,None 



    # insert lib dataset and files
    def insertBuildFileSpec_JEDI(self,jobSpec,reusedDatasetID,simul):
        comment = ' /* JediDBProxy.insertBuildFileSpec_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0}>".format(jobSpec.jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to insert dataset
            sqlDS  = "INSERT INTO {0}.JEDI_Datasets ({1}) ".format(jedi_config.db.schemaJEDI,JediDatasetSpec.columnNames())
            sqlDS += JediDatasetSpec.bindValuesExpression()
            sqlDS += " RETURNING datasetID INTO :newDatasetID"
            # sql to insert file
            sqlFI  = "INSERT INTO {0}.JEDI_Dataset_Contents ({1}) ".format(jedi_config.db.schemaJEDI,JediFileSpec.columnNames())
            sqlFI += JediFileSpec.bindValuesExpression()
            sqlFI += " RETURNING fileID INTO :newFileID"
            # sql to update LFN
            sqlFU  = "UPDATE {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlFU += "SET lfn=:newLFN "
            sqlFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # make datasetSpec
            pandaFileSpec = jobSpec.Files[0]
            timeNow = datetime.datetime.utcnow()
            datasetSpec = JediDatasetSpec()
            datasetSpec.jediTaskID = jobSpec.jediTaskID
            datasetSpec.creationTime = timeNow
            datasetSpec.modificationTime = timeNow
            datasetSpec.datasetName = pandaFileSpec.dataset
            datasetSpec.status = 'defined'
            datasetSpec.type = 'lib'
            datasetSpec.vo = jobSpec.VO
            datasetSpec.cloud = jobSpec.cloud
            datasetSpec.site  = jobSpec.computingSite
            # make fileSpec
            fileSpecList = []
            for pandaFileSpec in jobSpec.Files:
                fileSpec = JediFileSpec()
                fileSpec.convertFromJobFileSpec(pandaFileSpec)
                fileSpec.status       = 'defined'
                fileSpec.creationDate = timeNow
                fileSpec.keepTrack    = 1
                # change type to lib
                if fileSpec.type == 'output':
                    fileSpec.type = 'lib'
                # scope
                if datasetSpec.vo in jedi_config.ddm.voWithScope.split(','):
                    fileSpec.scope = self.extractScope(datasetSpec.datasetName)
                # append
                fileSpecList.append((fileSpec,pandaFileSpec))
            # start transaction
            self.conn.begin()
            varMap = datasetSpec.valuesMap(useSeq=True)
            varMap[':newDatasetID'] = self.cur.var(cx_Oracle.NUMBER)
            # insert dataset
            if reusedDatasetID != None:
                datasetID = reusedDatasetID
            elif not simul:
                self.cur.execute(sqlDS+comment,varMap)
                datasetID = long(varMap[':newDatasetID'].getvalue())
            else:
                datasetID = 0
            # insert files
            fileIdMap = {}    
            for fileSpec,pandaFileSpec in fileSpecList:
                fileSpec.datasetID = datasetID
                varMap = fileSpec.valuesMap(useSeq=True)
                varMap[':newFileID'] = self.cur.var(cx_Oracle.NUMBER)
                if not simul:
                    self.cur.execute(sqlFI+comment,varMap)
                    fileID = long(varMap[':newFileID'].getvalue())
                else:
                    fileID = 0
                # change placeholder in filename
                newLFN = fileSpec.lfn.replace('$JEDIFILEID',str(fileID))
                varMap = {}
                varMap[':jediTaskID'] = fileSpec.jediTaskID
                varMap[':datasetID'] = datasetID
                varMap[':fileID'] = fileID
                varMap[':newLFN'] = newLFN
                if not simul:
                    self.cur.execute(sqlFU+comment,varMap)
                # return IDs in a map since changes to jobSpec are not effective
                # since invoked in separate processes
                fileIdMap[fileSpec.lfn] = {'datasetID':datasetID,
                                           'fileID':fileID,
                                           'newLFN':newLFN,
                                           'scope':fileSpec.scope}
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("done")
            return True,fileIdMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,None



    # get sites used by a task
    def getSitesUsedByTask_JEDI(self,jediTaskID):
        comment = ' /* JediDBProxy.getSitesUsedByTask_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to insert dataset
            sqlDS  = "SELECT distinct site FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlDS += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':type1'] = 'output'
            varMap[':type2'] = 'log'
            # execute
            self.cur.execute(sqlDS+comment,varMap)
            resList = self.cur.fetchall()
            siteList = []
            for siteName in resList:
                siteList.append(siteName)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("done -> {0}".format(str(siteList)))
            return True,siteList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,None



    # get random seed
    def getRandomSeed_JEDI(self,jediTaskID,simul):
        comment = ' /* JediDBProxy.getRandomSeed_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get pseudo dataset for random seed
            sqlDS  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
            sqlDS += "FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlDS += "WHERE jediTaskID=:jediTaskID AND type=:type "
            # sql to get min random seed
            sqlFR  = "SELECT {0} ".format(JediFileSpec.columnNames())
            sqlFR += "FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlFR += "ORDER BY firstEvent "
            # sql to update file status
            sqlFU  = "UPDATE {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlFU += "SET status=:status "
            sqlFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # sql to get max random seed
            sqlLR  = "SELECT MAX(firstEvent) FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlLR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to insert file
            sqlFI  = "INSERT INTO {0}.JEDI_Dataset_Contents ({1}) ".format(jedi_config.db.schemaJEDI,JediFileSpec.columnNames())
            sqlFI += JediFileSpec.bindValuesExpression()
            sqlFI += " RETURNING fileID INTO :newFileID"
            # start transaction
            self.conn.begin()
            # get pseudo dataset for random seed
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':type'] = 'random_seed'
            self.cur.execute(sqlDS+comment,varMap)
            resDS = self.cur.fetchone()
            if resDS == None:
                # no random seed
                retVal = (None,None)
                tmpLog.debug('no random seed')
            else:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(resDS)
                # get min random seed
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':datasetID']  = datasetSpec.datasetID
                varMap[':status']     = 'ready'
                self.cur.execute(sqlFR+comment,varMap)
                resFR = self.cur.fetchone()
                if resFR != None:
                    # make FileSpec to reuse the row
                    tmpFileSpec = JediFileSpec()
                    tmpFileSpec.pack(resFR)
                    tmpLog.debug('reuse fileID={0} datasetID={1} rndmSeed={2}'.format(tmpFileSpec.fileID,
                                                                                      tmpFileSpec.datasetID,
                                                                                      tmpFileSpec.firstEvent))
                    # update status
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':datasetID']  = datasetSpec.datasetID
                    varMap[':fileID']     = tmpFileSpec.fileID
                    varMap[':status']     = 'picked'
                    self.cur.execute(sqlFU+comment,varMap)
                else:
                    # get max random seed
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':datasetID']  = datasetSpec.datasetID
                    self.cur.execute(sqlLR+comment,varMap)
                    resLR = self.cur.fetchone()
                    maxRndSeed = None
                    if resLR != None:
                        maxRndSeed, = resLR
                    if maxRndSeed == None:    
                        # first row
                        maxRndSeed = 1
                    else:
                        # increment
                        maxRndSeed += 1
                    # insert file
                    tmpFileSpec = JediFileSpec()
                    tmpFileSpec.jediTaskID   = jediTaskID
                    tmpFileSpec.datasetID    = datasetSpec.datasetID
                    tmpFileSpec.status       = 'picked'
                    tmpFileSpec.creationDate = datetime.datetime.utcnow()
                    tmpFileSpec.keepTrack    = 1
                    tmpFileSpec.type         = 'random_seed' 
                    tmpFileSpec.lfn          = "{0}".format(maxRndSeed)
                    tmpFileSpec.firstEvent   = maxRndSeed
                    if not simul:
                        varMap = tmpFileSpec.valuesMap(useSeq=True)
                        varMap[':newFileID'] = self.cur.var(cx_Oracle.NUMBER)
                        self.cur.execute(sqlFI+comment,varMap)
                        tmpFileSpec.fileID = long(varMap[':newFileID'].getvalue())
                        tmpLog.debug('insert fileID={0} datasetID={1} rndmSeed={2}'.format(tmpFileSpec.fileID,
                                                                                           tmpFileSpec.datasetID,
                                                                                           tmpFileSpec.firstEvent))
                    tmpFileSpec.status       = 'ready'
                # cannot return JobFileSpec due to owner.PandaID
                retVal = (tmpFileSpec,datasetSpec)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("done")
            return True,retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,(None,None)



    # get preprocess metadata
    def getPreprocessMetadata_JEDI(self,jediTaskID):
        comment = ' /* JediDBProxy.getPreprocessMetadata_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # sql to get jobPrams for runXYZ
        sqlSCF  = "SELECT tabF.fileID,tabF.datasetID,tabF.attemptNr "
        sqlSCF += "FROM {0}.JEDI_Datasets tabD, {0}.JEDI_Dataset_Contents tabF WHERE ".format(jedi_config.db.schemaJEDI)
        sqlSCF += "tabD.jediTaskID=tabF.jediTaskID AND tabD.jediTaskID=:jediTaskID AND tabF.status=:status "
        sqlSCF += "AND tabD.datasetID=tabF.datasetID "
        sqlSCF += "AND tabF.type=:type AND tabD.masterID IS NULL " 
        sqlSCP  = "SELECT PandaID FROM {0}.filesTable4 ".format(jedi_config.db.schemaPANDA)
        sqlSCP += "WHERE fileID=:fileID AND jediTaskID=:jediTaskID AND datasetID=:datasetID AND attemptNr=:attemptNr"
        sqlSCD  = "SELECT metaData FROM {0}.metaTable ".format(jedi_config.db.schemaPANDA)
        sqlSCD += "WHERE PandaID=:pandaID "
        failedRet = False,None
        retVal = failedRet
        try:
            # begin transaction
            self.conn.begin()
            # get files    
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':status'] = 'finished'
            varMap[':type']   = 'pp_input'
            self.cur.execute(sqlSCF+comment,varMap)
            tmpRes = self.cur.fetchone()
            if tmpRes == None:
                tmpLog.error('no successful input file')
            else:
                fileID,datasetID,attemptNr = tmpRes
                # get PandaID
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':datasetID']  = datasetID
                varMap[':fileID']     = fileID
                varMap[':attemptNr']  = attemptNr
                self.cur.execute(sqlSCP+comment,varMap)
                resPandaID = self.cur.fetchone()
                if resPandaID == None:
                    tmpLog.error('no PandaID for fileID={0}'.format(fileID))
                else:
                    pandaID, = resPandaID
                    # get metadata 
                    metaData = None
                    varMap = {}
                    varMap[':pandaID'] = pandaID
                    self.cur.execute(sqlSCD+comment,varMap)
                    for clobMeta, in self.cur:
                        metaData = clobMeta.read()
                        break
                    if metaData == None:
                        tmpLog.error('no metaData for PandaID={0}'.format(pandaID))
                    else:
                        retVal = True,metaData
                        tmpLog.debug('got metaData from PandaID={0}'.format(pandaID))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # get log dataset for preprocessing
    def getPreproLog_JEDI(self,jediTaskID,simul):
        comment = ' /* JediDBProxy.getPreproLog_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # sql to get dataset
        sqlDS  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
        sqlDS += "FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
        sqlDS += "WHERE jediTaskID=:jediTaskID AND type=:type "
        # sql to insert file
        sqlFI  = "INSERT INTO {0}.JEDI_Dataset_Contents ({1}) ".format(jedi_config.db.schemaJEDI,JediFileSpec.columnNames())
        sqlFI += JediFileSpec.bindValuesExpression()
        sqlFI += " RETURNING fileID INTO :newFileID"
        # sql to update dataset
        sqlUD  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
        sqlUD += "SET nFiles=nFiles+1 "
        sqlUD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        failedRet = False,None,None
        retVal = failedRet
        try:
            # begin transaction
            self.conn.begin()
            # get dataset   
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':type']       = 'pp_log'
            self.cur.execute(sqlDS+comment,varMap)
            resDS = self.cur.fetchone()
            if resDS == None:
                tmpLog.error('no dataset with type={0}'.format(varMap[':type']))
            else:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(resDS)
                # make file
                datasetSpec.nFiles = datasetSpec.nFiles + 1
                tmpFileSpec = JediFileSpec()
                tmpFileSpec.jediTaskID   = jediTaskID
                tmpFileSpec.datasetID    = datasetSpec.datasetID
                tmpFileSpec.status       = 'defined'
                tmpFileSpec.creationDate = datetime.datetime.utcnow()
                tmpFileSpec.keepTrack    = 1
                tmpFileSpec.type         = 'log'
                tmpFileSpec.lfn          = "{0}._{1:06d}.log.tgz".format(datasetSpec.datasetName,
                                                                         datasetSpec.nFiles)
                if not simul:
                    varMap = tmpFileSpec.valuesMap(useSeq=True)
                    varMap[':newFileID'] = self.cur.var(cx_Oracle.NUMBER)
                    self.cur.execute(sqlFI+comment,varMap)
                    tmpFileSpec.fileID = long(varMap[':newFileID'].getvalue())
                    # increment nFiles
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':datasetID']  = datasetSpec.datasetID
                    self.cur.execute(sqlUD+comment,varMap)
                # return value
                retVal = True,datasetSpec,tmpFileSpec
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug('done')
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet


    # get sites with best connections to source
    def getBestNNetworkSites_JEDI(self,source,protocol,nSites,threshold,cutoff,maxWeight):
        comment = ' /* JediDBProxy.getBestNNetworkSites_JEDI */'
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start for src={0} protocol={1} nSites={2} thr={3}'.format(source,protocol,
                                                                                nSites,threshold))
        # return for failure
        failedRet = False,None
        # check protocol
        if protocol in ['xrd','fax']:
            field = 'xrdcpval'
        else:
            tmpLog.error('unsupported protocol={0}'.format(protocol))
            return failedRet
        try:
            # sql
            sqlDS =  "SELECT * FROM "
            sqlDS += "(SELECT destination,CASE WHEN {0}>={1} THEN {2} ".format(field,cutoff,maxWeight)
            sqlDS += "ELSE ROUND({0}/{1}*{2},2) END AS {0} ".format(field,cutoff,maxWeight)
            sqlDS += "FROM {0}.sites_matrix_data tabM, {0}.schedconfig tabS ".format(jedi_config.db.schemaMETA)
            sqlDS += "WHERE source=:source AND tabM.destination=tabS.siteid "
            sqlDS += "AND wansinklimit IS NOT NULL AND wansinklimit<>0 "
            sqlDS += "AND xrdcp_last_update>=(SYSDATE-3/24) "
            sqlDS += "AND {0} IS NOT NULL AND {0}>:threshold ORDER BY {0} DESC) ".format(field)
            sqlDS += "WHERE rownum<=:nSites"
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 100
            varMap = {}
            varMap[':source']    = source
            varMap[':nSites']    = nSites
            varMap[':threshold'] = threshold
            # execute
            tmpLog.debug(sqlDS+comment+str(varMap))
            self.cur.execute(sqlDS+comment,varMap)
            resList = self.cur.fetchall()
            siteList = {}
            for siteName,costVal in resList:
                siteList[siteName] = costVal
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("done -> {0}".format(str(siteList)))
            return True,siteList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False,None



    # retry or incrementally execute a task
    def retryTask_JEDI(self,jediTaskID,commStr,maxAttempt=5,useCommit=True,statusCheck=True):
        comment = ' /* JediDBProxy.retryTask_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start command={0}'.format(commStr))
        newTaskStatus = None
        # check command
        if not commStr in ['retry','incexec']:
            tmpLog.debug('unknown command={0}'.format(commStr))
            return False,None
        try:
            # sql to retry files without maxFailure 
            sqlRFO  = "UPDATE {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlRFO += "SET maxAttempt=maxAttempt+:maxAttempt "
            sqlRFO += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlRFO += "AND keepTrack=:keepTrack AND maxAttempt IS NOT NULL AND maxAttempt<=attemptNr AND maxFailure IS NULL "
            # sql to retry files with maxFailure
            sqlRFF  = "UPDATE {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlRFF += "SET maxAttempt=maxAttempt+:maxAttempt,maxFailure=maxFailure+:maxAttempt "
            sqlRFF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlRFF += "AND keepTrack=:keepTrack AND maxAttempt IS NOT NULL AND maxFailure IS NOT NULL AND (maxAttempt<=attemptNr OR maxFailure<=failedAttempt) "
            # sql to count unprocessd files
            sqlCU  = "SELECT COUNT(*) FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlCU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlCU += "AND keepTrack=:keepTrack AND maxAttempt IS NOT NULL AND maxAttempt>attemptNr "
            sqlCU += "AND (maxFailure IS NULL OR maxFailure>failedAttempt) "
            # sql to retry/incexecute datasets
            sqlRD  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlRD += "SET status=:status,nFilesUsed=nFilesUsed-:nDiff-:nRun,nFilesFailed=nFilesFailed-:nDiff "
            sqlRD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update task status
            sqlUTB  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlUTB += "SET status=:status,oldStatus=NULL,modificationtime=:updateTime,errorDialog=:errorDialog,stateChangeTime=CURRENT_DATE "
            sqlUTB += "WHERE jediTaskID=:jediTaskID "
            sqlUTN  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlUTN += "SET status=:status,oldStatus=NULL,modificationtime=:updateTime,errorDialog=:errorDialog,stateChangeTime=CURRENT_DATE,startTime=CURRENT_DATE "
            sqlUTN += "WHERE jediTaskID=:jediTaskID "
            # sql to reset running files
            sqlRR  = "UPDATE {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlRR += "SET status=:newStatus,attemptNr=attemptNr+1,maxAttempt=maxAttempt+:maxAttempt " 
            sqlRR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status IN (:oldStatus1,:oldStatus2) "
            sqlRR += "AND keepTrack=:keepTrack AND maxAttempt IS NOT NULL "
            # sql to update output/lib/log datasets
            sqlUO  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlUO += "SET status=:status "
            sqlUO += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2,:type3,"
            for tmpType in JediDatasetSpec.getProcessTypes():
                if tmpType in JediDatasetSpec.getInputTypes():
                    continue
                mapKey = ':type_'+tmpType
                sqlUO += '{0},'.format(mapKey)
            sqlUO = sqlUO[:-1]
            sqlUO += ") "
            # start transaction
            if useCommit:
                self.conn.begin()
            self.cur.arraysize = 100000
            # check task status
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            sqlTK  = "SELECT status,oldStatus FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE ".format(jedi_config.db.schemaJEDI)
            self.cur.execute(sqlTK+comment,varMap)
            resTK = self.cur.fetchone()
            if resTK == None:
                # task not found
                msgStr = 'task not found'
                tmpLog.debug(msgStr)
            else:
                # check task status
                taskStatus,taskOldStatus = resTK
                newTaskStatus = None
                newErrorDialog = None
                if taskOldStatus == 'done' and commStr == 'retry' and statusCheck:
                    # no retry for finished task
                    msgStr = 'no {0} for task in {1} status'.format(commStr,taskOldStatus)
                    tmpLog.debug(msgStr)
                    newTaskStatus = taskOldStatus
                    newErrorDialog = msgStr
                elif not taskOldStatus in JediTaskSpec.statusToIncexec() and statusCheck:
                    # only tasks in a relevant final status 
                    msgStr = 'no {0} since not in relevant final status ({1})'.format(commStr,taskOldStatus)
                    tmpLog.debug(msgStr)
                    newTaskStatus = taskOldStatus
                    newErrorDialog = msgStr
                else:
                    # get input datasets
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    sqlDS  = "SELECT datasetID,masterID,nFiles,nFilesFinished,status,state "
                    sqlDS += "FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI) 
                    sqlDS += "WHERE jediTaskID=:jediTaskID AND type IN ("
                    for tmpType in JediDatasetSpec.getInputTypes():
                        mapKey = ':type_'+tmpType
                        sqlDS += '{0},'.format(mapKey)
                        varMap[mapKey] = tmpType
                    sqlDS  = sqlDS[:-1]
                    sqlDS += ") "
                    self.cur.execute(sqlDS+comment,varMap)
                    resDS = self.cur.fetchall()
                    changedMasterList = []
                    secMap  = {}
                    for datasetID,masterID,nFiles,nFilesFinished,status,state in resDS:
                        if masterID != None:
                            # keep secondary dataset info
                            if not secMap.has_key(masterID):
                                secMap[masterID] = []
                            secMap[masterID].append((datasetID,nFilesFinished,status,state))
                            # update dataset
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID']  = datasetID
                            varMap[':nDiff'] = 0
                            varMap[':nRun'] = 0
                            varMap[':status'] = 'ready'
                            tmpLog.debug('set status={0} for 2nd datasetID={1}'.format(varMap[':status'],datasetID))
                            self.cur.execute(sqlRD+comment,varMap)
                        else:
                            # set done if no more try is needed
                            if nFiles == nFilesFinished and status == 'failed':
                                # update dataset
                                varMap = {}
                                varMap[':jediTaskID'] = jediTaskID
                                varMap[':datasetID']  = datasetID
                                varMap[':nDiff'] = 0
                                varMap[':nRun'] = 0
                                varMap[':status'] = 'done'
                                tmpLog.debug('set status={0} for datasetID={1}'.format(varMap[':status'],datasetID))
                                self.cur.execute(sqlRD+comment,varMap)
                            # no retry if master dataset successfully finished
                            if commStr == 'retry' and nFiles == nFilesFinished:
                                tmpLog.debug('no {0} for datasetID={1} : nFiles==nFilesFinished'.format(commStr,datasetID))
                                continue
                            # count unprocessed files
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID']  = datasetID
                            varMap[':status']     = 'ready'
                            varMap[':keepTrack']  = 1
                            self.cur.execute(sqlCU+comment,varMap)
                            nUnp, = self.cur.fetchone()
                            # update files 
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID']  = datasetID
                            varMap[':status']     = 'ready'
                            varMap[':maxAttempt'] = maxAttempt
                            varMap[':keepTrack']  = 1
                            nDiff = 0
                            self.cur.execute(sqlRFO+comment,varMap)
                            nDiff += self.cur.rowcount
                            self.cur.execute(sqlRFF+comment,varMap)
                            nDiff += self.cur.rowcount
                            # reset running files
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID']  = datasetID
                            varMap[':oldStatus1'] = 'running'
                            varMap[':oldStatus2'] = 'picked'
                            varMap[':newStatus']  = 'ready'
                            varMap[':keepTrack']  = 1
                            varMap[':maxAttempt'] = maxAttempt
                            self.cur.execute(sqlRR+comment,varMap)
                            nRun = self.cur.rowcount
                            # no retry if no failed files
                            if commStr == 'retry' and nDiff == 0 and nUnp == 0 and nRun == 0:
                                tmpLog.debug('no {0} for datasetID={1} : nDiff/nReady/nRun=0'.format(commStr,datasetID))
                                continue
                            # update dataset
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID']  = datasetID
                            varMap[':nDiff'] = nDiff
                            varMap[':nRun'] = nRun
                            if commStr == 'retry':
                                varMap[':status'] = 'ready'
                            elif commStr == 'incexec':
                                varMap[':status'] = 'toupdate'
                            tmpLog.debug('set status={0} for datasetID={1} diff={2}'.format(varMap[':status'],datasetID,nDiff))
                            self.cur.execute(sqlRD+comment,varMap)
                            # collect masterIDs
                            changedMasterList.append(datasetID)
                    # update secondary
                    for masterID in changedMasterList:
                        # no seconday
                        if not secMap.has_key(masterID):
                            continue
                        # loop over all datasets
                        for datasetID,nFilesFinished,status,state in secMap[masterID]:
                            # update files
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID']  = datasetID
                            varMap[':status']     = 'ready'
                            varMap[':maxAttempt'] = maxAttempt
                            varMap[':keepTrack']  = 1
                            nDiff = 0
                            self.cur.execute(sqlRFO+comment,varMap)
                            nDiff += self.cur.rowcount
                            self.cur.execute(sqlRFF+comment,varMap)
                            nDiff += self.cur.rowcount
                            # reset running files
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID']  = datasetID
                            varMap[':oldStatus1'] = 'running'
                            varMap[':oldStatus2'] = 'picked'
                            varMap[':newStatus']  = 'ready'
                            varMap[':keepTrack']  = 1
                            varMap[':maxAttempt'] = maxAttempt
                            self.cur.execute(sqlRR+comment,varMap)
                            nRun = self.cur.rowcount
                            # update dataset
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID']  = datasetID
                            varMap[':nDiff'] = nDiff
                            varMap[':nRun'] = nRun
                            varMap[':status'] = 'ready'
                            self.cur.execute(sqlRD+comment,varMap)
                    # update task
                    if commStr == 'retry':
                        if changedMasterList != []:
                            newTaskStatus = JediTaskSpec.commandStatusMap()[commStr]['done']
                        else:
                            # to to finalization since no files left in ready status
                            msgStr = 'no {0} since no new/unprocessed files available'.format(commStr)
                            tmpLog.debug(msgStr)
                            newTaskStatus = taskOldStatus
                            newErrorDialog = msgStr
                    else:
                        # for incremental execution
                        newTaskStatus = JediTaskSpec.commandStatusMap()[commStr]['done']
                # update task
                varMap = {}
                varMap[':jediTaskID']  = jediTaskID
                varMap[':status']      = newTaskStatus
                varMap[':errorDialog'] = newErrorDialog
                if newTaskStatus != taskOldStatus:
                    tmpLog.debug('set taskStatus={0} for command={1}'.format(newTaskStatus,commStr))
                    # set old update time to trigger subsequent process
                    varMap[':updateTime'] = datetime.datetime.utcnow() - datetime.timedelta(hours=6)
                    self.cur.execute(sqlUTN+comment,varMap)
                else:
                    tmpLog.debug('back to taskStatus={0} for command={1}'.format(newTaskStatus,commStr))
                    varMap[':updateTime'] = datetime.datetime.utcnow()
                    self.cur.execute(sqlUTB+comment,varMap)
                # update output/lib/log
                if newTaskStatus != taskOldStatus:
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':type1']      = 'output'
                    varMap[':type2']      = 'lib'
                    varMap[':type3']      = 'log'
                    varMap[':status']     = 'done'
                    for tmpType in JediDatasetSpec.getProcessTypes():
                        if tmpType in JediDatasetSpec.getInputTypes():
                            continue
                        mapKey = ':type_'+tmpType
                        varMap[mapKey] = tmpType
                    self.cur.execute(sqlUO+comment,varMap)
                # retry or reactivate child tasks
                if newTaskStatus != taskOldStatus:
                    self.retryChildTasks_JEDI(jediTaskID,useCommit=False)
            if useCommit:
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("done")
            return True,newTaskStatus
        except:
            if useCommit:
                # roll back
                self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None,None



    # append input datasets for incremental execution
    def appendDatasets_JEDI(self,jediTaskID,inMasterDatasetSpecList,inSecDatasetSpecList):
        comment = ' /* JediDBProxy.appendDatasets_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        goDefined = False
        refreshContents = False 
        commandStr = 'incexec'
        try:
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            # check task status
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            sqlTK  = "SELECT status FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE ".format(jedi_config.db.schemaJEDI)
            self.cur.execute(sqlTK+comment,varMap)
            resTK = self.cur.fetchone()
            if resTK == None:
                # task not found
                msgStr = 'task not found'
                tmpLog.debug(msgStr)
            else:
                taskStatus, = resTK
                # invalid status
                if taskStatus != JediTaskSpec.commandStatusMap()[commandStr]['done']:
                    msgStr = 'invalid status={0} for dataset appending'.format(taskStatus)
                    tmpLog.debug(msgStr)
                else:
                    timeNow = datetime.datetime.utcnow()
                    # get existing input datasets
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    sqlDS  = "SELECT datasetName,status,nFilesTobeUsed,nFilesUsed,masterID "
                    sqlDS += "FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI) 
                    sqlDS += "WHERE jediTaskID=:jediTaskID AND type IN ("
                    for tmpType in JediDatasetSpec.getInputTypes():
                        mapKey = ':type_'+tmpType
                        sqlDS += '{0},'.format(mapKey)
                        varMap[mapKey] = tmpType
                    sqlDS  = sqlDS[:-1]
                    sqlDS += ") "
                    self.cur.execute(sqlDS+comment,varMap)
                    resDS = self.cur.fetchall()
                    existingDatasets = {}
                    for datasetName,datasetStatus,nFilesTobeUsed,nFilesUsed,masterID in resDS:
                        existingDatasets[datasetName] = datasetStatus
                        # remaining master files 
                        try:
                            if masterID == None and \
                                    (nFilesTobeUsed-nFilesUsed > 0 or datasetStatus in JediDatasetSpec.statusToUpdateContents()):
                                goDefined = True
                                if datasetStatus in JediDatasetSpec.statusToUpdateContents():
                                    refreshContents = True
                        except:
                            pass
                    # insert datasets
                    sqlID  = "INSERT INTO {0}.JEDI_Datasets ({1}) ".format(jedi_config.db.schemaJEDI,
                                                                           JediDatasetSpec.columnNames())
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
                        varMap[':newDatasetID'] = self.cur.var(cx_Oracle.NUMBER)            
                        # insert dataset
                        self.cur.execute(sqlID+comment,varMap)
                        datasetID = long(varMap[':newDatasetID'].getvalue())
                        masterID = datasetID
                        datasetSpec.datasetID = datasetID
                        # insert secondary datasets
                        for datasetSpec in inSecDatasetSpecList:
                            datasetSpec.creationTime = timeNow
                            datasetSpec.modificationTime = timeNow
                            datasetSpec.masterID = masterID
                            varMap = datasetSpec.valuesMap(useSeq=True)
                            varMap[':newDatasetID'] = self.cur.var(cx_Oracle.NUMBER)            
                            # insert dataset
                            self.cur.execute(sqlID+comment,varMap)
                            datasetID = long(varMap[':newDatasetID'].getvalue())
                            datasetSpec.datasetID = datasetID
                        goDefined = True
                    # update task
                    sqlUT  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
                    sqlUT += "SET status=:status,lockedBy=NULL,lockedTime=NULL,modificationtime=:updateTime,stateChangeTime=CURRENT_DATE "
                    sqlUT += "WHERE jediTaskID=:jediTaskID "
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    if goDefined:
                        # no new datasets
                        if inMasterDatasetSpecList == [] and not refreshContents:
                            # pass to JG
                            varMap[':status'] = 'ready'
                        else:
                            # pass to ContentsFeeder
                            varMap[':status'] = 'defined'
                    else:
                        # go to finalization since no datasets are appended
                        varMap[':status'] = 'prepared'
                    # set old update time to trigger subsequent process
                    varMap[':updateTime'] = datetime.datetime.utcnow() - datetime.timedelta(hours=6)
                    tmpLog.debug('set taskStatus={0}'.format(varMap[':status']))
                    self.cur.execute(sqlUT+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("done")
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # record retry history
    def recordRetryHistory_JEDI(self,jediTaskID,oldNewPandaIDs,relationType):
        comment = ' /* JediDBProxy.recordRetryHistory_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            sqlIN = "INSERT INTO {0}.JEDI_Job_Retry_History ".format(jedi_config.db.schemaJEDI) 
            if relationType == None:
                sqlIN += "(jediTaskID,oldPandaID,newPandaID,originPandaID) "
                sqlIN += "VALUES(:jediTaskID,:oldPandaID,:newPandaID,:originPandaID) "
            else:
                sqlIN += "(jediTaskID,oldPandaID,newPandaID,originPandaID,relationType) "
                sqlIN += "VALUES(:jediTaskID,:oldPandaID,:newPandaID,:originPandaID,:relationType) "
            # start transaction
            self.conn.begin()
            for newPandaID,oldPandaIDs in oldNewPandaIDs.iteritems():
                for oldPandaID in oldPandaIDs:
                    # get origin
                    originIDs = self.getOriginPandaIDsJEDI(oldPandaID,jediTaskID,self.cur)
                    for originID in originIDs:
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':oldPandaID'] = oldPandaID
                        varMap[':newPandaID'] = newPandaID
                        varMap[':originPandaID'] = originID
                        if relationType != None:
                            varMap[':relationType'] = relationType
                        self.cur.execute(sqlIN+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("done")
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # get JEDI tasks with a selection criteria
    def getTasksWithCriteria_JEDI(self,vo,prodSourceLabel,taskStatusList,taskCriteria,datasetCriteria,
                                  taskParamList,datasetParamList):
        comment = ' /* JediDBProxy.getTasksWithCriteria_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1}>'.format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start with tC={0} dC={1}'.format(str(taskCriteria),str(datasetCriteria)))
        # return value for failure
        failedRet = None
        try:
            # sql
            varMap = {}
            sqlRT  = "SELECT "
            for tmpPar in taskParamList:
                sqlRT += "tabT.{0},".format(tmpPar)
            for tmpPar in datasetParamList:
                sqlRT += "tabD.{0},".format(tmpPar)
            sqlRT  = sqlRT[:-1]
            sqlRT += " "
            sqlRT += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlRT += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
            sqlRT += "AND tabT.status IN ("
            for tmpStatus in taskStatusList:
                tmpKey = ':status_{0}'.format(tmpStatus)
                varMap[tmpKey] = tmpStatus
                sqlRT += '{0},'.format(tmpKey)
            sqlRT  = sqlRT[:-1]
            sqlRT += ") "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlRT += "AND tabT.vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlRT += "AND tabT.prodSourceLabel=:prodSourceLabel "
            for tmpKey,tmpVal in taskCriteria.iteritems():
                if isinstance(tmpVal,list):
                    sqlRT += "AND tabT.{0} IN (".format(tmpKey)
                    for tmpValItem in tmpVal:
                        sqlRT += ":{0}_{1},".format(tmpKey,tmpValItem)
                        varMap[':{0}_{1}'.format(tmpKey,tmpValItem)] = tmpValItem
                    sqlRT = sqlRT[:-1]   
                    sqlRT += ") "
                elif tmpVal != None:
                    sqlRT += "AND tabT.{0}=:{0} ".format(tmpKey)
                    varMap[':{0}'.format(tmpKey)] = tmpVal
                else:
                    sqlRT += "AND tabT.{0} IS NULL ".format(tmpKey)
            for tmpKey,tmpVal in datasetCriteria.iteritems():
                if isinstance(tmpVal,list):
                    sqlRT += "AND tabD.{0} IN (".format(tmpKey)
                    for tmpValItem in tmpVal:
                        sqlRT += ":{0}_{1},".format(tmpKey,tmpValItem)
                        varMap[':{0}_{1}'.format(tmpKey,tmpValItem)] = tmpValItem
                    sqlRT = sqlRT[:-1]   
                    sqlRT += ") "
                elif tmpVal != None:
                    sqlRT += "AND tabD.{0}=:{0} ".format(tmpKey)
                    varMap[':{0}'.format(tmpKey)] = tmpVal
                else:
                    sqlRT += "AND tabD.{0} IS NULL ".format(tmpKey)
            sqlRT += "ORDER BY tabT.jediTaskID "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            tmpLog.debug(sqlRT+comment+str(varMap))
            self.cur.execute(sqlRT+comment,varMap)
            resList = self.cur.fetchall()
            retTasks = []
            for resRT in resList:
                taskParMap = {}
                for tmpIdx,tmpPar in enumerate(taskParamList):
                    taskParMap[tmpPar] = resRT[tmpIdx]
                datasetParMap = {}
                for tmpIdx,tmpPar in enumerate(datasetParamList):
                    datasetParMap[tmpPar] = resRT[tmpIdx+len(taskParamList)]
                retTasks.append((taskParMap,datasetParMap))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('got {0} tasks'.format(len(retTasks)))
            return retTasks
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # check parent task status 
    def checkParentTask_JEDI(self,jediTaskID,useCommit=True):
        comment = ' /* JediDBProxy.checkParentTask_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            retVal = None
            sql = "SELECT status FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI) 
            sql += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            # start transaction
            if useCommit:
                self.conn.begin()
            self.cur.execute(sql+comment,varMap)
            resTK = self.cur.fetchone()
            if useCommit:
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            if resTK == None:
                tmpLog.error('parent not found')
                # set 1 (running) just in case
                retVal = 1
            else:
                # task status
                taskStatus, = resTK
                tmpLog.debug('parent status = {0}'.format(taskStatus))
                if taskStatus in ['done','finished']:
                    # parent is completed
                    retVal = 'completed'
                elif taskStatus in ['broken','aborted','failed']:
                    # parent is corrupted
                    retVal = 'corrupted'
                else:
                    # parent is running
                    retVal = 'running'
            # return
            tmpLog.debug("done with {0}".format(retVal))
            return retVal
        except:
            if useCommit:
                # roll back
                self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal



    # get task status 
    def getTaskStatus_JEDI(self,jediTaskID):
        comment = ' /* JediDBProxy.getTaskStatus_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            retVal = None
            sql = "SELECT status FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI) 
            sql += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            # start transaction
            self.conn.begin()
            self.cur.execute(sql+comment,varMap)
            resTK = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            if resTK != None:
                retVal, = resTK
            # return
            tmpLog.debug("done with {0}".format(retVal))
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal



    # get lib.tgz for waiting jobs
    def getLibForWaitingRunJob_JEDI(self,vo,prodSourceLabel,checkInterval):
        comment = ' /* JediDBProxy.getLibForWaitingRunJob_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1}>'.format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get the list of user/jobIDs 
            sqlL  = "SELECT prodUserName,jobsetID,jobDefinitionID,MAX(PandaID) "
            sqlL += "FROM {0}.jobsDefined4 ".format(jedi_config.db.schemaPANDA) 
            sqlL += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
            sqlL += "AND lockedBy=:lockedBy AND modificationTime<:timeLimit "
            sqlL += "GROUP BY prodUserName,jobsetID,jobDefinitionID "
            # sql to get data of lib.tgz
            sqlD  = "SELECT lfn,dataset,jediTaskID,datasetID,fileID "
            sqlD += "FROM {0}.filesTable4 ".format(jedi_config.db.schemaPANDA)
            sqlD += "WHERE PandaID=:PandaID AND type=:type AND status=:status "
            # sql to read file spec
            sqlF  = "SELECT {0} ".format(JediFileSpec.columnNames())
            sqlF += "FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # sql to update modificationTime
            sqlU  = "UPDATE {0}.jobsDefined4 ".format(jedi_config.db.schemaPANDA) 
            sqlU += "SET modificationTime=CURRENT_DATE "
            sqlU += "WHERE prodUserName=:prodUserName AND jobsetID=:jobsetID AND jobDefinitionID=:jobDefinitionID "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000000
            retList = []
            # get the list of waiting user/jobIDs
            varMap = {}
            varMap[':vo'] = vo
            varMap[':prodSourceLabel'] = prodSourceLabel
            varMap[':lockedBy'] = 'jedi'
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=checkInterval)
            self.cur.execute(sqlL+comment,varMap)
            resL = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # loop over all user/jobIDs
            for prodUserName,jobsetID,jobDefinitionID,pandaID in resL:
                self.conn.begin()
                # get data of lib.tgz
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':type'] = 'input'
                varMap[':status'] = 'unknown'
                self.cur.execute(sqlD+comment,varMap)
                resD = self.cur.fetchall()
                # loop over all files
                for lfn,datasetName,jediTaskID,datasetID,fileID in resD:
                    if re.search('\.lib\.tgz(\.\d+)*$',lfn) != None:
                        # read file spec
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':datasetID'] = datasetID
                        varMap[':fileID'] = fileID
                        self.cur.execute(sqlF+comment,varMap)
                        resF = self.cur.fetchone()
                        # make FileSpec
                        if resF != None:
                            tmpFileSpec = JediFileSpec()
                            tmpFileSpec.pack(resF)
                            retList.append((prodUserName,datasetName,tmpFileSpec))
                            break
                # update modificationTime
                varMap = {}
                varMap[':prodUserName'] = prodUserName
                varMap[':jobsetID'] = jobsetID
                varMap[':jobDefinitionID'] = jobDefinitionID
                self.cur.execute(sqlU+comment,varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("done with {0}".format(len(retList)))
            return retList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return []



    # get tasks to get reassigned
    def getTasksToReassign_JEDI(self,vo=None,prodSourceLabel=None):
        comment = ' /* JediDBProxy.getTasksToReassign_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <vo={0} label={1}>".format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retTasks = []
        try:
            # sql to get tasks to reassign
            varMap = {}
            varMap[':status'] = 'reassigning'
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
            sqlSCF  = "SELECT {0} ".format(JediTaskSpec.columnNames('tabT'))
            sqlSCF += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlSCF += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlSCF += "AND tabT.status=:status AND tabT.modificationTime<:timeLimit "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlSCF += "AND vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlSCF += "AND prodSourceLabel=:prodSourceLabel "
            sqlSCF += "FOR UPDATE"
            sqlSPC  = "UPDATE {0}.JEDI_Tasks SET modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlSPC += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # get tasks
            tmpLog.debug(sqlSCF+comment+str(varMap))
            self.cur.execute(sqlSCF+comment,varMap)
            resList = self.cur.fetchall()
            for resRT in resList:
                # make taskSpec
                taskSpec = JediTaskSpec()
                taskSpec.pack(resRT)
                # update modificationTime
                varMap = {}
                varMap[':jediTaskID'] = taskSpec.jediTaskID
                self.cur.execute(sqlSPC+comment,varMap)
                nRow = self.cur.rowcount
                if nRow > 0:
                    retTasks.append(taskSpec)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return    
            tmpLog.debug('got {0} tasks'.format(len(retTasks)))
            return retTasks
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return []



    # kill child tasks
    def killChildTasks_JEDI(self,jediTaskID,taskStatus,useCommit=True):
        comment = ' /* JediDBProxy.killChildTasks_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retTasks = []
        try:
            # sql to get child tasks
            sqlGT  = "SELECT jediTaskID,status FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlGT += "WHERE parent_tid=:jediTaskID AND parent_tid<>jediTaskID "
            # sql to change status
            sqlCT  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlCT += "SET status=:status,errorDialog=:errorDialog,stateChangeTime=CURRENT_DATE "
            sqlCT += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            if useCommit:
                self.conn.begin()
            # get tasks
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            self.cur.execute(sqlGT+comment,varMap)
            resList = self.cur.fetchall()
            for cJediTaskID,cTaskStatus in resList:
                # no more changes
                if cTaskStatus in JediTaskSpec.statusToRejectExtChange():
                    continue
                # change status
                cTaskStatus = 'toabort'
                varMap = {}
                varMap[':jediTaskID'] = cJediTaskID
                varMap[':status'] = cTaskStatus
                varMap[':errorDialog'] = 'parent task is {0}'.format(taskStatus)
                self.cur.execute(sqlCT+comment,varMap)
                tmpLog.debug('set {0} to jediTaskID={1}'.format(cTaskStatus,cJediTaskID))
                # kill child
                tmpStat = self.killChildTasks_JEDI(cJediTaskID,cTaskStatus,useCommit=False)
                if not tmpStat:
                    raise RuntimeError, 'Failed to kill child tasks'
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # return    
            tmpLog.debug('done')
            return True
        except:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # kick child tasks
    def kickChildTasks_JEDI(self,jediTaskID):
        comment = ' /* JediDBProxy.kickChildTasks_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retTasks = []
        try:
            # sql to get child tasks
            sqlGT  = "SELECT jediTaskID,status FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlGT += "WHERE parent_tid=:jediTaskID AND parent_tid<>jediTaskID "
            # sql to change modification time to the time just before pending tasks are reactivated
            timeLimitT = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
            sqlCT  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlCT += "SET modificationTime=CURRENT_DATE-1 "
            sqlCT += "WHERE jediTaskID=:jediTaskID AND modificationTime<:timeLimit "
            sqlCT += "AND status=:status AND lockedBy IS NULL "
            # sql to change state check time
            timeLimitD = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
            sqlCC  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlCC += "SET stateCheckTime=CURRENT_DATE-1 "
            sqlCC += "WHERE jediTaskID=:jediTaskID AND state=:dsState AND stateCheckTime<:timeLimit "
            # begin transaction
            self.conn.begin()
            # get tasks
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            self.cur.execute(sqlGT+comment,varMap)
            resList = self.cur.fetchall()
            for cJediTaskID,cTaskStatus in resList:
                # no more changes
                if cTaskStatus in JediTaskSpec.statusToRejectExtChange():
                    continue
                # change modification time for pending task
                varMap = {}
                varMap[':jediTaskID'] = cJediTaskID
                varMap[':status'] = 'pending'
                varMap[':timeLimit'] = timeLimitT
                self.cur.execute(sqlCT+comment,varMap)
                nRow = self.cur.rowcount
                tmpLog.debug('kicked jediTaskID={0} with {1}'.format(cJediTaskID,nRow))
                # change state check time for mutable datasets
                if not cTaskStatus in ['pending']:
                    varMap = {}
                    varMap[':jediTaskID'] = cJediTaskID
                    varMap[':dsState'] = 'mutable'
                    varMap[':timeLimit'] = timeLimitD
                    self.cur.execute(sqlCC+comment,varMap)
                    nRow = self.cur.rowcount
                    tmpLog.debug('kicked {0} mutable datasets for jediTaskID={1}'.format(nRow,cJediTaskID))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return    
            tmpLog.debug('done')
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # retry child tasks
    def retryChildTasks_JEDI(self,jediTaskID,useCommit=True):
        comment = ' /* JediDBProxy.retryChildTasks_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retTasks = []
        try:
            # sql to get output datasets of parent task
            sqlPD  = "SELECT datasetName FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlPD += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # sql to get child tasks
            sqlGT  = "SELECT jediTaskID,status FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlGT += "WHERE parent_tid=:jediTaskID AND parent_tid<>jediTaskID "
            # sql to get input datasets of child task
            sqlRD  = "SELECT datasetID,datasetName FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlRD += "WHERE jediTaskID=:jediTaskID AND type IN ("
            for tmpType in JediDatasetSpec.getProcessTypes():
                mapKey = ':type_'+tmpType
                sqlRD += '{0},'.format(mapKey)
            sqlRD  = sqlRD[:-1]
            sqlRD += ') '
            # sql to change task status
            sqlCT  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlCT += "SET status=:status,errorDialog=NULL,stateChangeTime=CURRENT_DATE "
            sqlCT += "WHERE jediTaskID=:jediTaskID "
            # sql to set mutable to dataset status
            sqlMD  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlMD += "SET state=:state,stateCheckTime=CURRENT_DATE "
            sqlMD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to set dataset status
            sqlCD  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlCD += "SET status=:status "
            sqlCD += "WHERE jediTaskID=:jediTaskID AND type=:type "
            # begin transaction
            if useCommit:
                self.conn.begin()
            # get output datasets of parent task
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':type1'] = 'output'
            varMap[':type2'] = 'log'
            self.cur.execute(sqlPD+comment,varMap)
            resList = self.cur.fetchall()
            parentDatasets = []
            for tmpDS, in resList:
                parentDatasets.append(tmpDS)
            # get child tasks
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            self.cur.execute(sqlGT+comment,varMap)
            resList = self.cur.fetchall()
            for cJediTaskID,cTaskStatus in resList:
                # not to retry if child task is aborted/broken
                if cTaskStatus in ['aborted','toabort','aborting','broken','tobroken']:
                    tmpLog.debug('not to retry child jediTaskID={0} in {1}'.format(cJediTaskID,cTaskStatus))
                    continue
                # get input datasets of child task
                varMap = {}
                varMap[':jediTaskID'] = cJediTaskID
                for tmpType in JediDatasetSpec.getProcessTypes():
                    mapKey = ':type_'+tmpType
                    varMap[mapKey] = tmpType
                self.cur.execute(sqlRD+comment,varMap)
                dsList = self.cur.fetchall()
                inputReady = False
                for datasetID,datasetName in dsList:
                    # set dataset status to mutable
                    if datasetName in parentDatasets or datasetName.split(':')[-1] in parentDatasets:
                        varMap = {}
                        varMap[':jediTaskID'] = cJediTaskID
                        varMap[':datasetID'] = datasetID
                        varMap[':state'] = 'mutable'
                        self.cur.execute(sqlMD+comment,varMap)
                        inputReady = True
                # set task status
                if not inputReady:
                    # set task status to registered since dataset is not ready
                    varMap = {}
                    varMap[':jediTaskID'] = cJediTaskID
                    varMap[':status'] = 'registered'
                    self.cur.execute(sqlCT+comment,varMap)
                    tmpLog.debug('set status of child jediTaskID={0} to {1}'.format(cJediTaskID,
                                                                                    varMap[':status']))
                elif not cTaskStatus in ['ready','running','scouting','scouted']:
                    # incexec child task
                    tmpLog.debug('incremental execution for child jediTaskID={0}'.format(cJediTaskID))
                    self.retryTask_JEDI(cJediTaskID,'incexec',useCommit=False,statusCheck=False)
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # return    
            tmpLog.debug('done')
            return True
        except:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # set super status
    def setSuperStatus_JEDI(self,jediTaskID,superStatus):
        comment = ' /* JediDBProxy.setSuperStatus_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        retTasks = []
        try:
            # sql to set super status
            sqlCT  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlCT += "SET superStatus=:superStatus "
            sqlCT += "WHERE jediTaskID=:jediTaskID "
            # set super status
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':superStatus'] = superStatus
            self.cur.execute(sqlCT+comment,varMap)
            return True
        except:
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # lock task
    def lockTask_JEDI(self,jediTaskID,pid):
        comment = ' /* JediDBProxy.lockTask_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0} pid={1}>".format(jediTaskID,pid)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to lock task
            sqlPD  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlPD += "SET lockedTime=CURRENT_DATE,modificationTime=CURRENT_DATE "
            sqlPD += "WHERE jediTaskID=:jediTaskID AND lockedBy=:lockedBy "
            # sql to check lock
            sqlCL  = "SELECT lockedBy,lockedTime FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlCL += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # lock
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':lockedBy'] = pid
            self.cur.execute(sqlPD+comment,varMap)
            nRow = self.cur.rowcount
            if nRow == 1:
                retVal = True
                tmpLog.debug('done with {0}'.format(retVal))
            else:
                retVal = False
                # check lock
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                self.cur.execute(sqlCL+comment,varMap)
                tmpLockedBy,tmpLockedTime = self.cur.fetchone()
                tmpLog.debug('done with {0} locked by another {1} at {2}'.format(retVal,tmpLockedBy,tmpLockedTime))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return    
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # get successful files
    def getSuccessfulFiles_JEDI(self,jediTaskID,datasetID):
        comment = ' /* JediDBProxy.getSuccessfulFiles_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0} datasetID={1}>".format(jediTaskID,datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get files
            sqlF  = "SELECT lfn FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            # begin transaction
            self.conn.begin()
            # lock
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':datasetID'] = datasetID
            varMap[':status'] = 'finished'
            self.cur.execute(sqlF+comment,varMap)
            res = self.cur.fetchall()
            lfnList = []
            for lfn, in res:
                lfnList.append(lfn)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return    
            tmpLog.debug('got {0} files'.format(len(lfnList)))
            return lfnList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # unlock a single task
    def unlockSingleTask_JEDI(self,jediTaskID,pid):
        comment = ' /* JediDBProxy.unlockSingleTask_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0} pid={1}>'.format(jediTaskID,pid)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to unlock
            sqlTU  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTU += "SET lockedBy=NULL,lockedTime=NULL "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND lockedBy=:pid "
            # begin transaction
            self.conn.begin()
            # unlock
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':pid'] = pid
            self.cur.execute(sqlTU+comment,varMap)
            nRow = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done with {0}'.format(nRow))
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # get JEDI tasks to be throttled
    def throttleTasks_JEDI(self,vo,prodSourceLabel,waitTime):
        comment = ' /* JediDBProxy.throttleTasks_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1}>'.format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start waitTime={0}min'.format(waitTime))
        try:
            # sql
            varMap = {}
            varMap[':taskStatus'] = 'running'
            varMap[':fileStat1']  = 'ready'
            varMap[':fileStat2']  = 'running'
            sqlRT  = "SELECT tabT.jediTaskID,tabT.numThrottled,AVG(tabC.attemptNr) "
            sqlRT += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA,".format(jedi_config.db.schemaJEDI)
            sqlRT += "{0}.JEDI_Datasets tabD,{0}.JEDI_Dataset_Contents tabC ".format(jedi_config.db.schemaJEDI)
            sqlRT += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlRT += "AND tabT.jediTaskID=tabD.jediTaskID AND tabT.jediTaskID=tabC.jediTaskID "
            sqlRT += "AND tabD.datasetID=tabC.datasetID "
            sqlRT += "AND tabT.status IN (:taskStatus) "
            sqlRT += "AND tabT.numThrottled IS NOT NULL "
            sqlRT += "AND tabD.type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sqlRT += '{0},'.format(mapKey)
                varMap[mapKey] = tmpType
            sqlRT  = sqlRT[:-1]
            sqlRT += ") AND tabD.masterID IS NULL "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlRT += "AND tabT.vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlRT += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sqlRT += "AND tabC.status IN (:fileStat1,:fileStat2) "
            sqlRT += "AND tabT.lockedBy IS NULL "
            sqlRT += "GROUP BY tabT.jediTaskID,tabT.numThrottled "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            tmpLog.debug(sqlRT+comment+str(varMap))
            self.cur.execute(sqlRT+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # sql to throttle tasks
            sqlTH  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTH += "SET throttledTime=:releaseTime,modificationTime=CURRENT_DATE,"
            sqlTH += "oldStatus=status,status=:newStatus,errorDialog=:errorDialog,"
            sqlTH += "numThrottled=:numThrottled "
            sqlTH += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus "
            sqlTH += "AND lockedBy IS NULL "
            attemptInterval = 5
            nTasks = 0
            for jediTaskID,numThrottled,largestAttemptNr in resList:
                # check threshold
                if int(largestAttemptNr/attemptInterval) <= numThrottled:
                    continue
                # begin transaction
                self.conn.begin()
                # check task
                try:
                    numThrottled += 1
                    throttledTime = datetime.datetime.utcnow()
                    releaseTime   = throttledTime + \
                        datetime.timedelta(minutes=waitTime*numThrottled*numThrottled)
                    errorDialog  = 'throttled due to many attempts {0}>={1}x{2} '.format(largestAttemptNr,
                                                                                         numThrottled,
                                                                                         attemptInterval)
                    errorDialog += 'from {0} '.format(throttledTime.strftime('%Y/%m/%d %H:%M:%S'))
                    errorDialog += 'till {0}'.format(releaseTime.strftime('%Y/%m/%d %H:%M:%S'))
                    varMap = {}
                    varMap[':jediTaskID']   = jediTaskID
                    varMap[':newStatus']    = 'throttled'
                    varMap[':oldStatus']    = 'running'
                    varMap[':releaseTime']  = releaseTime
                    varMap[':numThrottled'] = numThrottled
                    varMap[':errorDialog']  = errorDialog
                    tmpLog.debug(sqlTH+comment+str(varMap))
                    self.cur.execute(sqlTH+comment,varMap)
                    tmpLog.debug(errorDialog)
                    nTasks += 1
                except:
                    tmpLog.debug('skip locked jediTaskID={0}'.format(jediTaskID))
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return nTasks
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # release throttled tasks
    def releaseThrottledTasks_JEDI(self,vo,prodSourceLabel):
        comment = ' /* JediDBProxy.releaseThrottledTasks_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <vo={0} label={1}>".format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get tasks
            varMap = {}
            varMap[':status'] = 'throttled'
            sqlTL  = "SELECT jediTaskID "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += "AND tabT.status=:status AND tabT.throttledTime<CURRENT_DATE AND tabT.lockedBy IS NULL "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlTL += "AND vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlTL += "AND prodSourceLabel=:prodSourceLabel "
            # sql to update tasks    
            sqlTU  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTU += "SET status=oldStatus,oldStatus=NULL,errorDialog=NULL,modificationtime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID AND status=:oldStatus AND lockedBy IS NULL "
            # start transaction
            self.conn.begin()
            tmpLog.debug(sqlTL+comment+str(varMap))
            self.cur.execute(sqlTL+comment,varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            nRow = 0
            for jediTaskID, in resTL:
                timeoutFlag = False
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':oldStatus'] = 'throttled'
                self.cur.execute(sqlTU+comment,varMap)
                iRow = self.cur.rowcount
                tmpLog.debug('released jediTaskID={0} with {1}'.format(jediTaskID,iRow))
                nRow += iRow
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("updated {0} rows".format(nRow))
            return nRow
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # duplicate files for reuse
    def duplicateFilesForReuse_JEDI(self,datasetSpec):
        comment = ' /* JediDBProxy.duplicateFilesForReuse_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskId={0} datasetID={1}>".format(datasetSpec.jediTaskID,
                                                               datasetSpec.datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get unique files
            sqlCT  = "SELECT COUNT(*) FROM ("
            sqlCT += "SELECT distinct lfn,startEvent,endEvent "
            sqlCT += "FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlCT += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlCT += ") "
            # sql to read file spec
            defaultVales = {}
            defaultVales['status'] = 'ready'
            defaultVales['PandaID'] = None
            defaultVales['attemptNr']    = 0
            defaultVales['failedAttempt'] = 0
            sqlFR  = "INSERT INTO {0}.JEDI_Dataset_Contents ({1}) ".format(jedi_config.db.schemaJEDI,JediFileSpec.columnNames())
            sqlFR += "SELECT {0} FROM ( ".format(JediFileSpec.columnNames(useSeq=True,defaultVales=defaultVales))
            sqlFR += "SELECT {0} ".format(JediFileSpec.columnNames(defaultVales=defaultVales,skipDefaultAttr=True))
            sqlFR += "FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlFR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID IN ( "
            sqlFR += "SELECT MIN(fileID) minFileID "
            sqlFR += "FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlFR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlFR += "GROUP BY lfn,startEvent,endEvent) "
            sqlFR += "ORDER BY fileID) "
            # sql to update dataset record
            sqlDU  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlDU += "SET nFiles=nFiles+:iFiles,nFilesTobeUsed=nFilesTobeUsed+:iFiles "
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # read unique files
            varMap = {}
            varMap[':jediTaskID'] = datasetSpec.jediTaskID
            varMap[':datasetID'] = datasetSpec.datasetID
            self.cur.execute(sqlCT+comment,varMap)
            resCT = self.cur.fetchone()
            iFile, = resCT
            # insert files
            varMap = {}
            varMap[':jediTaskID'] = datasetSpec.jediTaskID
            varMap[':datasetID'] = datasetSpec.datasetID
            self.cur.execute(sqlFR+comment,varMap)
            # update dataset
            if iFile > 0:
                varMap = {}
                varMap[':jediTaskID'] = datasetSpec.jediTaskID
                varMap[':datasetID'] = datasetSpec.datasetID
                varMap[':iFiles'] = iFile
                self.cur.execute(sqlDU+comment,varMap)
            tmpLog.debug('inserted {0} files'.format(iFile))
            return iFile
        except:
            # error
            self.dumpErrorMessage(tmpLog)
            return 0



    # lock process
    def lockProcess_JEDI(self,vo,prodSourceLabel,cloud,workqueue_id,pid,forceOption):
        comment = ' /* JediDBProxy.lockProcess_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <vo={0} label={1} cloud={2} queue={3} pid={4}>".format(vo,prodSourceLabel,
                                                                               cloud,workqueue_id,pid)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            retVal = False
            # use non-null for cloud
            if cloud == None:
                cloud = "NULL"
            # sql to check
            sqlCT  = "SELECT lockedBy "
            sqlCT += "FROM {0}.JEDI_Process_Lock ".format(jedi_config.db.schemaJEDI)
            sqlCT += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud AND workqueue_id=:workqueue_id "
            sqlCT += "AND lockedTime>:timeLimit "
            sqlCT += "FOR UPDATE"
            # sql to delete
            sqlCD  = "DELETE FROM {0}.JEDI_Process_Lock ".format(jedi_config.db.schemaJEDI)
            sqlCD += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud AND workqueue_id=:workqueue_id "
            # sql to insert
            sqlFR  = "INSERT INTO {0}.JEDI_Process_Lock ".format(jedi_config.db.schemaJEDI)
            sqlFR += "(vo,prodSourceLabel,cloud,workqueue_id,lockedBy,lockedTime) "
            sqlFR += "VALUES(:vo,:prodSourceLabel,:cloud,:workqueue_id,:lockedBy,CURRENT_DATE) "
            # start transaction
            self.conn.begin()
            # check
            if not forceOption:
                varMap = {}
                varMap[':vo'] = vo
                varMap[':prodSourceLabel'] = prodSourceLabel
                varMap[':cloud'] = cloud
                varMap[':workqueue_id'] = workqueue_id
                varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
                self.cur.execute(sqlCT+comment,varMap)
                resCT = self.cur.fetchone()
            else:
                resCT = None
            if resCT != None:
                tmpLog.debug('skipped locked by {0}'.format(resCT[0]))
            else:
                # delete
                varMap = {}
                varMap[':vo'] = vo
                varMap[':prodSourceLabel'] = prodSourceLabel
                varMap[':cloud'] = cloud
                varMap[':workqueue_id'] = workqueue_id
                self.cur.execute(sqlCD+comment,varMap)
                # insert
                varMap = {}
                varMap[':vo'] = vo
                varMap[':prodSourceLabel'] = prodSourceLabel
                varMap[':cloud'] = cloud
                varMap[':workqueue_id'] = workqueue_id
                varMap[':lockedBy'] = pid
                self.cur.execute(sqlFR+comment,varMap)
                tmpLog.debug('successfully locked')
                retVal = True
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog,msgType='debug')
            return retVal



    # unlock process
    def unlockProcess_JEDI(self,vo,prodSourceLabel,cloud,workqueue_id,pid):
        comment = ' /* JediDBProxy.unlockProcess_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <vo={0} label={1} cloud={2} queue={3} pid={4}>".format(vo,prodSourceLabel,
                                                                               cloud,workqueue_id,pid)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            retVal = False
            # use non-null for cloud
            if cloud == None:
                cloud = "NULL"
            # sql to delete
            sqlCD  = "DELETE FROM {0}.JEDI_Process_Lock ".format(jedi_config.db.schemaJEDI)
            sqlCD += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud "
            sqlCD += "AND workqueue_id=:workqueue_id AND lockedBy=:lockedBy "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[':vo'] = vo
            varMap[':prodSourceLabel'] = prodSourceLabel
            varMap[':cloud'] = cloud
            varMap[':workqueue_id'] = workqueue_id
            varMap[':lockedBy'] = pid
            self.cur.execute(sqlCD+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            retVal = True
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal



    # unlock process with PID
    def unlockProcessWithPID_JEDI(self,vo,prodSourceLabel,workqueue_id,pid,useBase):
        comment = ' /* JediDBProxy.unlockProcessWithPID_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <vo={0} label={1} queue={2} pid={3} useBase={4}>".format(vo,prodSourceLabel,
                                                                                 workqueue_id,pid,useBase)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            retVal = False
            # sql to delete
            sqlCD  = "DELETE FROM {0}.JEDI_Process_Lock ".format(jedi_config.db.schemaJEDI)
            sqlCD += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
            sqlCD += "AND workqueue_id=:workqueue_id "
            if useBase:
                sqlCD += "AND lockedBy LIKE :lockedBy "
            else:
                sqlCD += "AND lockedBy=:lockedBy "
            # start transaction
            self.conn.begin()
            # delete
            varMap = {}
            varMap[':vo'] = vo
            varMap[':prodSourceLabel'] = prodSourceLabel
            varMap[':workqueue_id'] = workqueue_id
            if useBase:
                varMap[':lockedBy'] = pid + '%'
            else:
                varMap[':lockedBy'] = pid
            self.cur.execute(sqlCD+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            retVal = True
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal



    # check process lock
    def checkProcessLock_JEDI(self,vo,prodSourceLabel,cloud,workqueue_id,pid,checkBase):
        comment = ' /* JediDBProxy.checkProcessLock_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <vo={0} label={1} cloud={2} queue={3} pid={4}>".format(vo,prodSourceLabel,
                                                                               cloud,workqueue_id,pid)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            retVal = False
            # use non-null for cloud
            if cloud == None:
                cloud = "NULL"
            # sql to check
            sqlCT  = "SELECT lockedBy "
            sqlCT += "FROM {0}.JEDI_Process_Lock ".format(jedi_config.db.schemaJEDI)
            sqlCT += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud AND workqueue_id=:workqueue_id "
            sqlCT += "AND lockedTime>:timeLimit "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[':vo'] = vo
            varMap[':prodSourceLabel'] = prodSourceLabel
            varMap[':cloud'] = cloud
            varMap[':workqueue_id'] = workqueue_id
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
            self.cur.execute(sqlCT+comment,varMap)
            resCT = self.cur.fetchone()
            if resCT != None:
                lockedBy, = resCT
                if checkBase:
                    # check only base part
                    if not lockedBy.startswith(pid):
                        retVal = True
                else:
                    # check whole string
                    if lockedBy != pid:
                        retVal = True
                if retVal == True:
                    tmpLog.debug('skipped locked by {0}'.format(lockedBy))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done with {0}'.format(retVal))
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog,msgType='debug')
            return retVal



    # get JEDI tasks to be assessed
    def getAchievedTasks_JEDI(self,vo,prodSourceLabel,timeLimit,nTasks):
        comment = ' /* JediDBProxy.getAchievedTasks_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1}>'.format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # return value for failure
        failedRet = None
        try:
            # sql
            varMap = {}
            varMap[':status1'] = 'running'
            varMap[':status2'] = 'pending'
            sqlRT  = "SELECT tabT.jediTaskID,tabT.status,tabT.goal "
            sqlRT += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlRT += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlRT += "AND tabT.status IN (:status1,:status2) "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlRT += "AND tabT.vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlRT += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sqlRT += "AND goal IS NOT NULL "
            sqlRT += "AND (assessmentTime IS NULL OR assessmentTime<:timeLimit) "
            sqlRT += "AND rownum<{0} ".format(nTasks)
            sqlLK  = "UPDATE {0}.JEDI_Tasks SET assessmentTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlLK += "WHERE jediTaskID=:jediTaskID AND (assessmentTime IS NULL OR assessmentTime<:timeLimit) AND status=:status "
            sqlDS  = "SELECT datasetID,type,nEvents "
            sqlDS += "FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlDS += "WHERE jediTaskID=:jediTaskID AND ((type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sqlDS += '{0},'.format(mapKey)
            sqlDS  = sqlDS[:-1]
            sqlDS += ") AND masterID IS NULL) OR (type=:type1)) "
            sqlFC  = "SELECT COUNT(*) "
            sqlFC += "FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlFC += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status AND failedAttempt=:failedAttempt "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            timeToCheck = datetime.datetime.utcnow() - datetime.timedelta(minutes=timeLimit)
            varMap[':timeLimit'] = timeToCheck
            tmpLog.debug(sqlRT+comment+str(varMap))
            self.cur.execute(sqlRT+comment,varMap)
            resList = self.cur.fetchall()
            retTasks = []
            taskStatList = []
            for jediTaskID,taskStatus,taskGoal in resList:
                taskStatList.append((jediTaskID,taskStatus,taskGoal))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # get tasks and datasets    
            for jediTaskID,taskStatus,taskGoal in taskStatList:
                # begin transaction
                self.conn.begin()
                # lock task
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':timeLimit'] = timeToCheck
                varMap[':status'] = taskStatus
                self.cur.execute(sqlLK+comment,varMap)
                nRow = self.cur.rowcount
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                if nRow == 1:
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    for tmpType in JediDatasetSpec.getInputTypes():
                        mapKey = ':type_'+tmpType
                        varMap[mapKey] = tmpType
                    varMap[':type1'] = 'output'
                    # begin transaction
                    self.conn.begin()
                    # check datasets
                    self.cur.execute(sqlDS+comment,varMap)
                    resDS = self.cur.fetchall()
                    totalInputEvents = 0
                    totalOutputEvents = 0
                    firstOutput = True
                    # loop over all datasets
                    taskToFinish = True
                    for datasetID,datasetType,nEvents in resDS:
                        # counts events
                        if datasetType in JediDatasetSpec.getInputTypes():
                            # input
                            try:
                                totalInputEvents += nEvents
                            except:
                                pass
                            # check if there are unused files
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID'] = datasetID
                            varMap[':status'] = 'ready'
                            varMap[':failedAttempt'] = 0
                            self.cur.execute(sqlFC+comment,varMap)
                            nUnUsed, = self.cur.fetchone()
                            if nUnUsed != 0:
                                tmpLog.debug('skip jediTaskID={0} datasetID={1} has {2} unused files'.format(jediTaskID,
                                                                                                             datasetID,
                                                                                                             nUnUsed))
                                taskToFinish = False
                                break
                        else:
                            # only one output
                            if firstOutput:
                                # output
                                try:
                                    totalOutputEvents += nEvents
                                except:
                                    pass
                            firstOutput = False    
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # check number of events
                    if taskToFinish:
                        if totalInputEvents == 0:
                            # input has 0 events
                            tmpLog.debug('skip jediTaskID={0} input has 0 events'.format(jediTaskID))
                            taskToFinish = False
                        elif float(totalOutputEvents)/float(totalInputEvents)*1000.0 < taskGoal:
                            # goal is not yet reached
                            tmpLog.debug('skip jediTaskID={0} goal is not yet reached {1}.{2}%>{3}/{4}'.format(jediTaskID,
                                                                                                               taskGoal/10,
                                                                                                               taskGoal%10,
                                                                                                               totalOutputEvents,
                                                                                                               totalInputEvents))
                            taskToFinish = False
                        else:
                            tmpLog.debug('to finsh jediTaskID={0} goal is reached {1}.{2}%<={3}/{4}'.format(jediTaskID,
                                                                                                            taskGoal/10,
                                                                                                            taskGoal%10,
                                                                                                            totalOutputEvents,
                                                                                                            totalInputEvents))
                    # append
                    if taskToFinish:
                        retTasks.append(jediTaskID)
            tmpLog.debug('got {0} tasks'.format(len(retTasks)))
            return retTasks
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet


    # get inactive sites
    def getInactiveSites_JEDI(self,flag,timeLimit):
        comment = ' /* JediDBProxy.getInactiveSites_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <flag={0} timeLimit={1}>'.format(flag,timeLimit)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            retVal = set()
            # sql 
            sqlCD  = "SELECT site FROM {0}.SiteData ".format(jedi_config.db.schemaMETA)
            sqlCD += "WHERE flag=:flag AND hours=:hours AND laststart<:laststart "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[':flag'] = flag
            varMap[':hours'] = 3
            varMap[':laststart'] = datetime.datetime.utcnow() - datetime.timedelta(hours=timeLimit)
            self.cur.execute(sqlCD+comment,varMap)
            resCD = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            for tmpSiteID, in resCD:
                retVal.add(tmpSiteID)
            tmpLog.debug('done')
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return retVal



    # get total walltime
    def getTotalWallTime_JEDI(self,vo,prodSourceLabel,workQueue,cloud=None):
        comment = ' /* JediDBProxy.getTotalWallTime_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1} queue={2} cloud={3}>'.format(vo,prodSourceLabel,workQueue.queue_name,cloud)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql
            varMap = {}
            sql  = "SELECT SUM(NVL2(maxWalltime,maxWalltime,0)),SUM(NVL2(maxWalltime,1,0)),SUM(NVL2(maxWalltime,0,1)) "
            sql += "FROM {0}.".format(jedi_config.db.schemaPANDA) + "{0} "
            sql += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
            if cloud != None:
                sql += "AND cloud=:cloud "
                varMap[':cloud'] = cloud 
            sql += "AND workQueue_ID IN ("
            for tmpQueue_ID in workQueue.getIDs():
                tmpKey = ':queueID_{0}'.format(tmpQueue_ID)
                varMap[tmpKey] = tmpQueue_ID
                sql += '{0},'.format(tmpKey)
            sql  = sql[:-1]
            sql += ") "
            sqlA = "AND jobStatus IN (:jobStatus1,:jobStatus2) "
            # start transaction
            self.conn.begin()
            # get from jobsDefined
            varMap[':vo'] = vo
            varMap[':prodSourceLabel'] = prodSourceLabel
            self.cur.execute(sql.format('jobsDefined4')+comment,varMap)
            totWalltime,nHasVal,nNoVal = 0,0,0
            tmpTotWalltime,tmpHasVal,tmpNoVal = self.cur.fetchone()
            if tmpTotWalltime != None:
                totWalltime += tmpTotWalltime
            if tmpHasVal != None:
                nHasVal += tmpHasVal
            if tmpNoVal != None:
                nNoVal += tmpNoVal
            # get from jobsActive
            varMap[':jobStatus1'] = 'activated'
            varMap[':jobStatus2'] = 'starting'
            self.cur.execute(sql.format('jobsActive4')+sqlA+comment,varMap)
            tmpTotWalltime,tmpHasVal,tmpNoVal = self.cur.fetchone()
            if tmpTotWalltime != None:
                totWalltime += tmpTotWalltime
            if tmpHasVal != None:
                nHasVal += tmpHasVal
            if tmpNoVal != None:
                nNoVal += tmpNoVal
            tmpLog.debug('totWalltime={0} nHasVal={1} nNoVal={2}'.format(totWalltime,nHasVal,nNoVal))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            if nHasVal != 0:
                totWalltime = long(totWalltime*(1+float(nNoVal)/float(nHasVal)))
            else:
                totWalltime = None
            tmpLog.debug('done totWalltime={0}'.format(totWalltime))
            return totWalltime
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # check duplication with internal merge
    def checkDuplication_JEDI(self,jediTaskID):
        comment = ' /* JediDBProxy.checkDuplication_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # sql to get input datasetID
        sqlM  = "SELECT datasetID FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
        sqlM += "WHERE jediTaskID=:jediTaskID AND type IN ("
        for tmpType in JediDatasetSpec.getInputTypes():
            mapKey = ':type_'+tmpType
            sqlM += '{0},'.format(mapKey)
        sqlM  = sqlM[:-1]
        sqlM += ") AND masterID IS NULL "
        # sql to get output datasetID and templateID
        sqlO  = "SELECT datasetID,provenanceID FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
        sqlO += "WHERE jediTaskID=:jediTaskID AND type=:type "
        # sql to check duplication without internal merge
        sqlWM  = "SELECT COUNT(*) FROM ( "
        sqlWM += "SELECT distinct outPandaID "
        sqlWM += "FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
        sqlWM += "WHERE jediTaskID=:jediTaskID AND datasetID=:outDatasetID ANd status IN (:statT1,:statT2) "
        sqlWM += 'MINUS '
        sqlWM += "SELECT distinct PandaID "
        sqlWM += "FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
        sqlWM += "WHERE jediTaskID=:jediTaskID AND datasetID=:inDatasetID and status=:statI "
        sqlWM += ') '
        # sql to check duplication with internal merge
        sqlCM  = "SELECT COUNT(*) FROM ( "
        sqlCM += "SELECT distinct c1.outPandaID "
        sqlCM += "FROM {0}.JEDI_Dataset_Contents c1,{0}.JEDI_Dataset_Contents c2,{0}.JEDI_Datasets d ".format(jedi_config.db.schemaJEDI)
        sqlCM += "WHERE d.jediTaskID=:jediTaskID AND c1.jediTaskID=d.jediTaskID AND c1.datasetID=d.datasetID AND d.templateID=:templateID "
        sqlCM += "AND c1.jediTaskID=c2.jediTaskID AND c2.datasetID=:outDatasetID AND c1.pandaID=c2.pandaID and c2.status IN (:statT1,:statT2) "
        sqlCM += 'MINUS '
        sqlCM += "SELECT distinct PandaID "
        sqlCM += "FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
        sqlCM += "WHERE jediTaskID=:jediTaskID AND datasetID=:inDatasetID and status=:statI "
        sqlCM += ') '
        try:
            # start transaction
            self.conn.begin()
            # get input datasetID
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                varMap[mapKey] = tmpType
            self.cur.execute(sqlM+comment,varMap)
            resM = self.cur.fetchone()
            inDatasetID, = resM
            # get output datasetID and templateID
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':type'] = 'output'
            self.cur.execute(sqlO+comment,varMap)
            resO = self.cur.fetchone()
            if resO == None:
                # no output
                retVal = 0
            else:
                outDatasetID,templateID = resO
                # check duplication
                varMap = {}
                varMap[':jediTaskID']   = jediTaskID
                varMap[':inDatasetID']  = inDatasetID
                varMap[':outDatasetID'] = outDatasetID
                varMap[':statI']        = 'finished'
                varMap[':statT1']       = 'finished'
                varMap[':statT2']       = 'nooutput'
                if templateID != None:
                    # with internal merge
                    varMap[':templateID'] = templateID
                    self.cur.execute(sqlCM+comment,varMap)
                else:
                    # without internal merge
                    self.cur.execute(sqlWM+comment,varMap)
                retVal, = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('dup={0}'.format(retVal))
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None
