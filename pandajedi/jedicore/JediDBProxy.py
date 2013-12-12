import re
import sys
import copy
import math
import numpy
import datetime
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
    def dumpErrorMessage(self,tmpLog):
        # error
        errtype,errvalue = sys.exc_info()[:2]
        tmpLog.error(": %s %s" % (errtype.__name__,errvalue))



    # get work queue map
    def getWorkQueueMap(self):
        self.refreshWrokQueueMap()
        return self.workQueueMap

    

    # refresh work queue map
    def refreshWrokQueueMap(self):
        # avoid frequent lookup
        if self.updateTimeForWorkQueue != None and \
               (datetime.datetime.utcnow()-self.self.updateTimeForWorkQueue) < datetime.timedelta(hours=3):
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
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                varMap[mapKey] = tmpType
            varMap[':taskStatus']       = 'defined'
            varMap[':dsStatus_pending'] = 'pending'
            sql  = "SELECT {0} ".format(JediDatasetSpec.columnNames('tabD'))
            sql += 'FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA '.format(jedi_config.db.schemaJEDI)
            sql += 'WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID '
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sql += "AND tabT.vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sql += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sql += 'AND tabT.jediTaskID=tabD.jediTaskID AND tabT.status=:taskStatus '
            sql += 'AND type IN ('
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sql += '{0},'.format(mapKey)
            sql  = sql[:-1]    
            sql += ') AND tabD.status IN ('
            for tmpStat in JediDatasetSpec.statusToUpdateContents():
                mapKey = ':dsStatus_'+tmpStat
                sql += '{0},'.format(mapKey)
                varMap[mapKey] = tmpStat
            sql  = sql[:-1]    
            sql += ') AND tabT.lockedBy IS NULL AND tabD.lockedBy IS NULL '
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
            nDS = 0
            for res in resList:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(res)
                if not returnMap.has_key(datasetSpec.jediTaskID):
                    returnMap[datasetSpec.jediTaskID] = []
                returnMap[datasetSpec.jediTaskID].append(datasetSpec)
                nDS += 1
            jediTaskIDs = returnMap.keys()
            jediTaskIDs.sort()
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


                                                
    # feed files to the JEDI contents table
    def insertFilesForDataset_JEDI(self,datasetSpec,fileMap,datasetState,stateUpdateTime,
                                   nEventsPerFile,nEventsPerJob,maxAttempt,firstEventNumber,
                                   nMaxFiles,nMaxEvents,useScout,givenFileList,useFilesWithNewAttemptNr,
                                   nFilesPerJob,nEventsPerRange):
        comment = ' /* JediDBProxy.insertFilesForDataset_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0} datasetID={1}>'.format(datasetSpec.jediTaskID,
                                                           datasetSpec.datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start nEventsPerFile={0} nEventsPerJob={1} maxAttempt={2} '.format(nEventsPerFile,
                                                                                         nEventsPerJob,
                                                                                         maxAttempt))
        tmpLog.debug('firstEventNumber={0} nMaxFiles={1} nMaxEvents={2} useScout={3}'.format(firstEventNumber,
                                                                                             nMaxFiles,nMaxEvents,
                                                                                             useScout))
        tmpLog.debug('useFilesWithNewAttemptNr={0} nFilesPerJob={1} nEventsPerRange={2}'.format(useFilesWithNewAttemptNr,
                                                                                                nFilesPerJob,
                                                                                                nEventsPerRange))
        tmpLog.debug('len(fileMap)={0}'.format(len(fileMap)))
        if nFilesPerJob != None:
            nFilesForScout = 10 * nFilesPerJob 
        else:
            nFilesForScout = 10
        # return value for failure
        diagMap = {'errMsg':''}
        failedRet = False,0,None,diagMap
        # max number of file records per dataset
        maxFileRecords = 100000
        try:
            # current current date
            timeNow = datetime.datetime.utcnow()
            # loop over all files
            filelValMap = {}
            for guid,fileVal in fileMap.iteritems():
                filelValMap[fileVal['lfn']] = (guid,fileVal)
            # sort by LFN
            lfnList = filelValMap.keys()
            lfnList.sort()
            # truncate if nessesary
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
            uniqueLfnList = []
            totalNumEventsF = 0
            for tmpLFN in lfnList:
                # collect unique LFN list    
                if not tmpLFN in uniqueLfnList:
                    uniqueLfnList.append(tmpLFN)
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
                # set maxAttempt only for master
                if datasetSpec.isMaster():
                    fileSpec.maxAttempt = maxAttempt
                # this info will come from Rucio in the future
                if fileVal.has_key('nevents'):
                    fileSpec.nEvents = fileVal['nevents']
                else:
                    fileSpec.nEvents = nEventsPerFile
                # keep track
                if datasetSpec.toKeepTrack():
                    fileSpec.keepTrack = 1
                tmpFileSpecList = []
                if givenFileList != []:
                    # given file list
                    for fileItem in givenFileList:
                        # check file name
                        fileNamePatt = fileItem['lfn']
                        if useFilesWithNewAttemptNr:
                            # use files with different attempt numbers
                            fileNamePatt = re.sub('\.\d+$','',fileNamePatt)
                            fileNamePatt += '(\.\d+)*'
                        if re.search('^'+fileNamePatt+'$',fileSpec.lfn) == None:
                            continue
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
                elif nEventsPerJob == None or nEventsPerJob <= 0 or \
                       fileSpec.nEvents == None or fileSpec.nEvents <= 0 or \
                       nEventsPerFile == None or nEventsPerFile <= 0: 
                    if firstEventNumber != None and nEventsPerFile != None:
                        fileSpec.firstEvent = totalEventNumber
                        totalEventNumber += fileSpec.nEvents
                    # file-level splitting
                    tmpFileSpecList.append(fileSpec)
                else:
                    # event-level splitting
                    tmpStartEvent = 0
                    while nRemEvents > 0:
                        splitFileSpec = copy.copy(fileSpec)
                        if tmpStartEvent + nRemEvents >= splitFileSpec.nEvents:
                            splitFileSpec.startEvent = tmpStartEvent
                            splitFileSpec.endEvent = splitFileSpec.nEvents - 1
                            nRemEvents -= (splitFileSpec.nEvents - tmpStartEvent)
                            if nRemEvents == 0:
                                nRemEvents = nEventsPerJob
                            if firstEventNumber != None and nEventsPerFile != None:
                                splitFileSpec.firstEvent = totalEventNumber
                                totalEventNumber += (splitFileSpec.endEvent-splitFileSpec.startEvent+1)
                            tmpFileSpecList.append(splitFileSpec)
                            break
                        else:
                            splitFileSpec.startEvent = tmpStartEvent
                            splitFileSpec.endEvent   = tmpStartEvent + nRemEvents -1
                            tmpStartEvent += nRemEvents
                            nRemEvents = nEventsPerJob
                            if firstEventNumber != None and nEventsPerFile != None:
                                splitFileSpec.firstEvent = totalEventNumber
                                totalEventNumber += (splitFileSpec.endEvent-splitFileSpec.startEvent+1)
                            tmpFileSpecList.append(splitFileSpec)
                # append
                for fileSpec in tmpFileSpecList:
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
            # look for missing files if file list is specified
            missingFileList = []    
            for fileItem in givenFileList:
                if not fileItem['lfn'] in foundFileList:
                    missingFileList.append(fileItem['lfn'])
            # sql to check if task is locked
            sqlTL = "SELECT status,lockedBy FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE ".format(jedi_config.db.schemaJEDI)
            # sql to check dataset status
            sqlDs  = "SELECT status FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlDs += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID FOR UPDATE "
            # sql to get existing files
            sqlCh  = "SELECT fileID,lfn,status,startEvent,endEvent,boundaryID FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlCh += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID FOR UPDATE "
            # sql to count existing files
            sqlCo  = "SELECT count(*) FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlCo += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql for insert
            sqlIn  = "INSERT INTO {0}.JEDI_Dataset_Contents ({1}) ".format(jedi_config.db.schemaJEDI,JediFileSpec.columnNames())
            sqlIn += JediFileSpec.bindValuesExpression()
            # sql to update file status
            sqlFU  = "UPDATE {0}.JEDI_Dataset_Contents SET status=:status ".format(jedi_config.db.schemaJEDI)
            sqlFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # sql to update dataset
            sqlDU  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlDU += "SET status=:status,state=:state,stateCheckTime=:stateUpdateTime,"
            sqlDU += "nFiles=:nFiles,nFilesTobeUsed=:nFilesTobeUsed "
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            nInsert  = 0
            retVal = None,missingFileList,None,diagMap
            # begin transaction
            self.conn.begin()
            # check task
            varMap = {}
            varMap[':jediTaskID'] = datasetSpec.jediTaskID
            self.cur.execute(sqlTL+comment,varMap)
            resTask = self.cur.fetchone()
            if resTask == None:
                tmpLog.debug('task not found in Task table')
            else:
                taskStatus,taskLockedBy = resTask
                if taskLockedBy != None:
                    # task is locked
                    tmpLog.debug('task is locked by {0}'.format(taskLockedBy))
                elif not taskStatus in JediTaskSpec.statusToUpdateContents():
                    # task status is irrelevant
                    tmpLog.debug('task.status={0} is not for contents update'.format(taskStatus))
                else:
                    # check dataset status
                    nLost    = 0
                    nNewLost = 0
                    nExist   = 0
                    nReady   = 0
                    varMap = {}
                    varMap[':jediTaskID'] = datasetSpec.jediTaskID
                    varMap[':datasetID'] = datasetSpec.datasetID
                    self.cur.execute(sqlDs+comment,varMap)
                    resDs = self.cur.fetchone()
                    if resDs == None:
                        tmpLog.debug('dataset not found in Datasets table')
                    elif not resDs[0] in JediDatasetSpec.statusToUpdateContents():
                        tmpLog.debug('ds.status={0} is not for contents update'.format(resDs[0]))
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
                        # get existing file list
                        varMap = {}
                        varMap[':jediTaskID'] = datasetSpec.jediTaskID
                        varMap[':datasetID'] = datasetSpec.datasetID
                        self.cur.execute(sqlCh+comment,varMap)
                        tmpRes = self.cur.fetchall()
                        existingFiles = {}
                        for fileID,lfn,status,startEvent,endEvent,boundaryID in tmpRes:
                            uniqueFileKey = '{0}.{1}.{2}.{3}'.format(lfn,startEvent,endEvent,boundaryID)
                            existingFiles[uniqueFileKey] = {'fileID':fileID,'status':status}
                            if status == 'ready':
                                nReady += 1
                        # insert files
                        existingFileList = existingFiles.keys()
                        uniqueLfnList = []
                        totalNumEventsF = 0
                        totalNumEventsE = 0
                        escapeNextFile = False 
                        numUniqueLfn = 0
                        for uniqueFileKey in uniqueFileKeyList:
                            fileSpec = fileSpecMap[uniqueFileKey]
                            # count number of files 
                            if not fileSpec.lfn in uniqueLfnList:
                                # the limit is reached at the previous file
                                if escapeNextFile:
                                    break
                                uniqueLfnList.append(fileSpec.lfn)
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
                            if uniqueFileKey in existingFileList:
                                nExist += 1                        
                                continue
                            varMap = fileSpec.valuesMap(useSeq=True)
                            self.cur.execute(sqlIn+comment,varMap)
                            nInsert += 1
                        nReady += nInsert    
                        # lost or recovered files
                        for uniqueFileKey,fileVarMap in existingFiles.iteritems():
                            varMap = {}
                            varMap[':jediTaskID'] = datasetSpec.jediTaskID
                            varMap[':datasetID'] = datasetSpec.datasetID
                            varMap[':fileID'] = fileVarMap['fileID']
                            if not uniqueFileKey in uniqueFileKeyList:
                                varMap['status'] = 'lost'
                            elif fileVarMap['status'] in ['lost','missing'] and \
                                     fileSpecMap[uniqueFileKey].status != fileVarMap['status']:
                                varMap['status'] = fileSpecMap[uniqueFileKey].status
                            else:
                                continue
                            if varMap['status'] == 'ready':
                                nReady += 1
                            self.cur.execute(sqlFU+comment,varMap)
                        # updata dataset
                        varMap = {}
                        varMap[':jediTaskID'] = datasetSpec.jediTaskID
                        varMap[':datasetID'] = datasetSpec.datasetID
                        varMap[':nFiles'] = nInsert + len(existingFiles)
                        if taskStatus == 'defined' and nReady > nFilesForScout and useScout:
                            # set a fewer number for scout
                            varMap[':nFilesTobeUsed'] = nFilesForScout
                        else:
                            varMap[':nFilesTobeUsed'] = nReady
                        if missingFileList == []:    
                            varMap[':status' ] = 'ready'
                        else:
                            # don't change status when some files are missing
                            varMap[':status' ] = datasetSpec.status
                        varMap[':state' ] = datasetState
                        varMap[':stateUpdateTime'] = stateUpdateTime
                        self.cur.execute(sqlDU+comment,varMap)
                        # set return value
                        retVal = True,missingFileList,numUniqueLfn,diagMap
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('inserted {0} rows'.format(nInsert))
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
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
    def updateTaskStatusByContFeeder_JEDI(self,jediTaskID,taskSpec=None):
        comment = ' /* JediDBProxy.updateTaskStatusByContFeeder_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to check status
            sqlS  = "SELECT status,lockedBy,cloud,prodSourceLabel FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlS += "WHERE jediTaskID=:jediTaskID FOR UPDATE "
            # sql to update task
            sqlU  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlU += "SET status=:status,modificationTime=:updateTime,lockedBy=NULL,lockedTime=NULL"
            if taskSpec != None:
                sqlU += ",oldStatus=:oldStatus,errorDialog=:errorDialog"
            sqlU += " WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # check status
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            tmpLog.debug(sqlS+comment+str(varMap))
            self.cur.execute(sqlS+comment,varMap)            
            res = self.cur.fetchone()
            if res == None:
                tmpLog.debug('task is not found in Tasks table')
            else:
                taskStatus,lockedBy,cloudName,prodSourceLabel = res
                if lockedBy != None:
                    # task is locked
                    tmpLog('task is locked by {0}'.format(lockedBy))
                elif not taskStatus in JediTaskSpec.statusToUpdateContents():
                    # task status is irrelevant
                    tmpLog.debug('task.status={0} is not for contents update'.format(taskStatus))
                else:
                    # update task
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':updateTime'] = datetime.datetime.utcnow()
                    if taskSpec != None:
                        # new task status is specified 
                        varMap[':status']      = taskSpec.status
                        varMap[':oldStatus']   = taskSpec.oldStatus
                        varMap[':errorDialog'] = taskSpec.errorDialog
                    elif cloudName == None and prodSourceLabel in ['managed','test']:
                        # set assigning for TaskBrokerage
                        varMap[':status'] = 'assigning'
                        # set old update time to trigger TaskBrokerage immediately
                        varMap[':updateTime'] = datetime.datetime.utcnow() - datetime.timedelta(hours=6)
                    else:
                        # skip task brokerage since cloud is preassigned
                        varMap[':status'] = 'ready'
                    tmpLog.debug(sqlU+comment+str(varMap))
                    self.cur.execute(sqlU+comment,varMap)
                    tmpLog.debug('set to {0}'.format(varMap[':status']))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # update JEDI task
    def updateTask_JEDI(self,taskSpec,criteria,oldStatus=None):
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
            # values for UPDATE
            varMap = taskSpec.valuesMap(useSeq=False,onlyChanged=True)
            # sql
            sql  = "UPDATE {0}.JEDI_Tasks SET {1} WHERE ".format(jedi_config.db.schemaJEDI,
                                                                 taskSpec.bindUpdateChangesExpression())
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
            # update task
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
                sql += "FOR UPDATE NOWAIT"
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
            tmpLog.debug('done')
            return True,taskSpec
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # get JEDI task and datasets with ID and lock it
    def getTaskDatasetsWithID_JEDI(self,jediTaskID,pid):
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
            sqlRT  = "SELECT {0} ".format(JediTaskSpec.columnNames('tabT'))
            sqlRT += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlRT += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlRT += "AND tabT.status IN (:status1,:status2) "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlRT += "AND tabT.vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlRT += "AND tabT.prodSourceLabel=:prodSourceLabel "
            sqlRT += "AND (lockedBy IS NULL OR lockedTime<:timeLimit) "
            sqlRT += "AND rownum<{0} FOR UPDATE ".format(nTasks)
            sqlLK  = "UPDATE {0}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlLK += "WHERE jediTaskID=:jediTaskID "
            sqlDS  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
            sqlDS += "FROM {0}.JEDI_Datasets WHERE jediTaskID=:jediTaskID ".format(jedi_config.db.schemaJEDI)
            sqlSC  = "UPDATE {0}.JEDI_Tasks SET status=:status,modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlSC += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            #varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=10)
            tmpLog.debug(sqlRT+comment+str(varMap))
            self.cur.execute(sqlRT+comment,varMap)
            resList = self.cur.fetchall()
            retTasks = []
            allTasks = []
            for resRT in resList:
                taskSpec = JediTaskSpec()
                taskSpec.pack(resRT)
                allTasks.append(taskSpec)
            # get datasets    
            for taskSpec in allTasks:
                if taskSpec.status == 'prepared':
                    retTasks.append(taskSpec)
                    # lock task
                    varMap = {}
                    varMap[':jediTaskID'] = taskSpec.jediTaskID
                    varMap[':lockedBy'] = pid
                    self.cur.execute(sqlLK+comment,varMap)
                    # read datasets
                    varMap = {}
                    varMap[':jediTaskID'] = taskSpec.jediTaskID
                    self.cur.execute(sqlDS+comment,varMap)
                    resList = self.cur.fetchall()
                    for resDS in resList:
                        datasetSpec = JediDatasetSpec()
                        datasetSpec.pack(resDS)
                        taskSpec.datasetSpecList.append(datasetSpec)
                else:
                    # make avalanche
                    varMap = {}
                    varMap[':jediTaskID'] = taskSpec.jediTaskID
                    varMap[':status'] = 'running'
                    self.cur.execute(sqlSC+comment,varMap)
                    tmpLog.debug("changed status to {0} for jediTaskID={1}".format(varMap[':status'],taskSpec.jediTaskID))
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
    def getJobStatisticsWithWorkQueue_JEDI(self,vo,prodSourceLabel,minPriority=None):
        comment = ' /* DBProxy.getJobStatisticsWithWorkQueue_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1}>'.format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start minPriority={0}'.format(minPriority))
        sql0 = "SELECT computingSite,cloud,jobStatus,workQueue_ID,COUNT(*) FROM %s "
        sql0 += "WHERE vo=:vo and prodSourceLabel=:prodSourceLabel "
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
    def getOutputFiles_JEDI(self,jediTaskID,provenanceID,simul,instantiateTmpl,instantiatedSite,isUnMerging,
                            isPrePro):
        comment = ' /* JediDBProxy.getOutputFiles_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start with simul={0} instantiateTmpl={1} instantiatedSite={2}'.format(simul,
                                                                                            instantiateTmpl,
                                                                                            instantiatedSite))
        tmpLog.debug('isUnMerging={0} isPrePro={1}'.format(isUnMerging,isPrePro))
        try:
            outMap = {}
            datasetToRegister = []
            # sql to get dataset
            sqlD  = "SELECT datasetID,datasetName,vo,masterID,status FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
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
            self.cur.arraysize = 10000
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
            for datasetID,datasetName,vo,masterID,datsetStatus in resList:
                fileDatasetID = datasetID
                # instantiate template datasets
                if instantiateTmpl:
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
                            mstr_RelationMap[fileDatasetID] = masterID
                        # collect ID of dataset to be registered 
                        datasetToRegister.append(fileDatasetID)
                    # keep relation between template and concrete    
                    tmpl_RelationMap[datasetID] = fileDatasetID
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
                    fileSpec = JediFileSpec()
                    fileSpec.jediTaskID   = jediTaskID
                    fileSpec.datasetID    = fileDatasetID
                    nameTemplate = fileNameTemplate.replace('${SN}','{SN:06d}')
                    nameTemplate = nameTemplate.replace('${SN/P}','{SN:06d}')
                    nameTemplate = nameTemplate.replace('${SN','{SN')
                    fileSpec.lfn          = nameTemplate.format(SN=serialNr)
                    fileSpec.status       = 'defined'
                    fileSpec.creationDate = timeNow
                    fileSpec.type         = outType
                    fileSpec.keepTrack    = 1
                    if maxSerialNr == None or maxSerialNr < serialNr:
                        maxSerialNr = serialNr
                    # scope
                    if vo in jedi_config.ddm.voWithScope.split(','):
                        fileSpec.scope = datasetName.split('.')[0]
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
                    # append
                    outMap[streamName] = fileSpec
            # set masterID to concrete datasets 
            for fileDatasetID,masterID in mstr_RelationMap.iteritems():
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':datasetID']  = fileDatasetID
                if tmpl_RelationMap.has_key(masterID):
                    varMap[':masterID'] = tmpl_RelationMap[masterID]
                else:
                    varMap[':masterID'] = masterID
                self.cur.execute(sqlMC+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return outMap,maxSerialNr,datasetToRegister
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None,None,None


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
                                   fullSimulation=False):
        comment = ' /* JediDBProxy.getTasksToBeProcessed_JEDI */'
        methodName = self.getMethodName(comment)
        if simTasks != None:
            methodName += ' <jediTasks={0}>'.format(str(simTasks))
        elif workQueue == None:
            methodName += ' <vo={0} queue={1} cloud={2}>'.format(vo,None,cloudName)
        else:
            methodName += ' <vo={0} queue={1} cloud={2}>'.format(vo,workQueue.queue_name,cloudName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start label={0} nTasks={1} nFiles={2} minPriority={3}'.format(prodSourceLabel,nTasks,
                                                                                    nFiles,minPriority))
        tmpLog.debug('maxNumJobs={0} typicalNumFilesMap={1}'.format(maxNumJobs,str(typicalNumFilesMap)))
        # return value for failure
        failedRet = None
        # set max number of jobs if undefined
        if maxNumJobs == None:
            maxNumJobs = 100
            tmpLog.debug('set maxNumJobs={0} since undefined '.format(maxNumJobs))
        try:
            # sql to get tasks/datasets
            if simTasks == None:
                varMap = {}
                varMap[':vo']              = vo
                if not cloudName in [None,'','any']:
                    varMap[':cloud']       = cloudName
                varMap[':prodSourceLabel'] = prodSourceLabel
                varMap[':dsStatus']        = 'ready'            
                varMap[':dsOKStatus1']     = 'ready'
                varMap[':dsOKStatus2']     = 'done'
                varMap[':dsOKStatus3']     = 'defined'
                varMap[':dsOKStatus4']     = 'registered'
                sql  = "SELECT tabT.jediTaskID,datasetID,currentPriority,nFilesToBeUsed-nFilesUsed,tabD.type "
                sql += "FROM {0}.JEDI_Tasks tabT,ATLAS_PANDA.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE tabT.vo=:vo AND workQueue_ID IN ("
                for tmpQueue_ID in workQueue.getIDs():
                    tmpKey = ':queueID_{0}'.format(tmpQueue_ID)
                    varMap[tmpKey] = tmpQueue_ID
                    sql += '{0},'.format(tmpKey)
                sql  = sql[:-1]
                sql += ') '
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
                sql += "AND tabT.lockedBy IS NULL AND tabT.jediTaskID=tabD.jediTaskID "
                sql += "AND nFilesToBeUsed > nFilesUsed AND type IN ("
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
                for tmpType in JediDatasetSpec.getProcessTypes():
                    mapKey = ':type_'+tmpType
                    sql += '{0},'.format(mapKey)
                sql  = sql[:-1]
                sql += ') AND NOT status IN (:dsOKStatus1,:dsOKStatus2,:dsOKStatus3,:dsOKStatus4)) '
                sql += "ORDER BY currentPriority DESC,jediTaskID "
            else:
                varMap = {}
                if not fullSimulation:
                    sql  = "SELECT tabT.jediTaskID,datasetID,currentPriority,nFilesToBeUsed-nFilesUsed,tabD.type "
                else:
                    sql  = "SELECT tabT.jediTaskID,datasetID,currentPriority,nFilesToBeUsed,tabD.type "
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
            returnList  = []
            taskDatasetMap = {}
            jediTaskIDList = []
            for jediTaskID,datasetID,currentPriority,tmpNumFiles,datasetType in resList:
                tmpLog.debug('jediTaskID={0} datasetID={1} tmpNumFiles={2} type={3}'.format(jediTaskID,datasetID,
                                                                                            tmpNumFiles,datasetType))
                # just return the max priority
                if isPeeking:
                    return currentPriority
                # make task-dataset mapping
                if not taskDatasetMap.has_key(jediTaskID):
                    taskDatasetMap[jediTaskID] = []
                taskDatasetMap[jediTaskID].append((datasetID,tmpNumFiles,datasetType))
                if not jediTaskID in jediTaskIDList:
                    jediTaskIDList.append(jediTaskID)
            tmpLog.debug('got {0} tasks'.format(len(taskDatasetMap)))
            # sql to read task
            sqlRT  = "SELECT {0} ".format(JediTaskSpec.columnNames())
            sqlRT += "FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL FOR UPDATE NOWAIT".format(jedi_config.db.schemaJEDI)
            # sql to lock task
            sqlLock  = "UPDATE {0}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlLock += "WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL "
            # sql to read template
            sqlJobP = "SELECT jobParamsTemplate FROM {0}.JEDI_JobParams_Template WHERE jediTaskID=:jediTaskID ".format(jedi_config.db.schemaJEDI)
            # sql to read datasets
            sqlRD  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
            sqlRD += "FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlRD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID FOR UPDATE NOWAIT "
            # sql to read files
            sqlFR  = "SELECT * FROM (SELECT {0} ".format(JediFileSpec.columnNames())
            sqlFR += "FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID "
            if not fullSimulation:
                sqlFR += "AND status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt) "
            sqlFR += "ORDER BY lfn) "
            sqlFR += "WHERE rownum <= {0}"
            # sql to update file status
            sqlFU  = "UPDATE {0}.JEDI_Dataset_Contents SET status=:nStatus ".format(jedi_config.db.schemaJEDI)
            sqlFU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND status=:oStatus "
            # sql to update file usage info in dataset
            sqlDU  = "UPDATE {0}.JEDI_Datasets SET nFilesUsed=:nFilesUsed ".format(jedi_config.db.schemaJEDI)
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlDU += "RETURNING nFilesUsed,nFilesTobeUsed INTO :newnFilesUsed,:newnFilesTobeUsed "
            # sql to read DN
            sqlDN  = "SELECT dn FROM {0}.users WHERE name=:name ".format(jedi_config.db.schemaMETA)
            # loop over all tasks
            iTasks = 0
            for jediTaskID in jediTaskIDList:
                for datasetID,tmpNumFiles,datasetType in taskDatasetMap[jediTaskID]:
                    datasetIDs = [datasetID]
                    # begin transaction
                    self.conn.begin()
                    # read task
                    toSkip = False
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    try:
                        # select
                        self.cur.execute(sqlRT+comment,varMap)
                        resRT = self.cur.fetchone()
                        # locked by another
                        if resRT == None:
                            toSkip = True
                        else:
                            taskSpec = JediTaskSpec()
                            taskSpec.pack(resRT)
                            # make InputChunk
                            inputChunk = InputChunk(taskSpec)
                            # merging
                            if datasetType in JediDatasetSpec.getMergeProcessTypes():
                                inputChunk.isMerging = True
                            # for analysis use DN as userName
                            if taskSpec.prodSourceLabel in ['user']:
                                varMap = {}
                                varMap[':name'] = taskSpec.userName
                                tmpLog.debug(sqlDN+comment+str(varMap))
                                self.cur.execute(sqlDN+comment,varMap)
                                resDN = self.cur.fetchone()
                                tmpLog.debug(resDN)
                                if resDN == None:
                                    # no user info
                                    toSkip = True
                                    tmpLog.error('skipped since failed to get DN for {0}'.format(taskSpec.userName))
                                else:
                                    taskSpec.userName, = resDN
                                    if taskSpec.userName in ['',None]:
                                        # DN is empty
                                        toSkip = True
                                        tmpLog.error('skipped since DN is empty for {0}'.format(taskSpec.userName))
                                    else:
                                        # reset change to not update userName
                                        taskSpec.resetChangedAttr('userName')
                    except:
                        errType,errValue = sys.exc_info()[:2]
                        if self.isNoWaitException(errValue):
                            # resource busy and acquire with NOWAIT specified
                            toSkip = True
                            tmpLog.debug('skip locked jediTaskID={0}'.format(jediTaskID))
                        else:
                            # failed with something else
                            raise errType,errValue
                    # read secondary datasets
                    if not toSkip:
                        # sql to get seconday dataset list
                        sqlDS  = "SELECT datasetID FROM {0}.JEDI_Datasets WHERE jediTaskID=:jediTaskID ".format(jedi_config.db.schemaJEDI)
                        if not fullSimulation:
                            sqlDS += "AND nFilesToBeUsed > nFilesUsed AND type IN ("
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
                        sqlDS += ') AND status=:dsStatus AND masterID=:masterID '
                        varMap[':dsStatus']   = 'ready'
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
                    # read job params and files
                    if not toSkip:
                        # lock task
                        if simTasks == None:
                            varMap = {}
                            varMap[':lockedBy'] = pid
                            varMap[':jediTaskID'] = jediTaskID
                            self.cur.execute(sqlLock+comment,varMap)
                            nRow = self.cur.rowcount
                        else:
                            # set nRow for simulation
                            nRow = 1
                        if nRow != 1:
                            tmpLog.debug('failed to lock jediTaskID={0}'.format(jediTaskID))
                        else:
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
                            elif typicalNumFilesMap != None and typicalNumFilesMap.has_key(taskSpec.processingType) \
                                    and typicalNumFilesMap[taskSpec.processingType] > 0:
                                # typical usage
                                typicalNumFilesPerJob = typicalNumFilesMap[taskSpec.processingType]
                            # max number of files based on typical usage
                            typicalMaxNumFiles = typicalNumFilesPerJob * maxNumJobs
                            if typicalMaxNumFiles > nFiles:
                                maxNumFiles = typicalMaxNumFiles
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
                            for datasetID in datasetIDs:
                                # get DatasetSpec
                                tmpDatasetSpec = inputChunk.getDatasetWithID(datasetID)
                                # read files to make FileSpec
                                varMap = {}
                                varMap[':datasetID']  = datasetID
                                varMap[':jediTaskID'] = jediTaskID
                                if not fullSimulation:
                                    varMap[':status'] = 'ready'
                                # the number of files to be read
                                if tmpDatasetSpec.isMaster():
                                    maxFilesTobeRead = maxMasterFilesTobeRead
                                else:
                                    # set very large number for secondary to read all files
                                    maxFilesTobeRead = 1000000
                                tmpLog.debug('trying to read {0} files from datasetID={1}'.format(maxFilesTobeRead,datasetID))
                                self.cur.execute(sqlFR.format(maxFilesTobeRead)+comment,varMap)
                                resFileList = self.cur.fetchall()
                                iFiles = 0
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
                                    iFiles += 1
                                if iFiles == 0:
                                    # no input files
                                    tmpLog.debug('datasetID={0} has no files to be processed'.format(datasetID))
                                    toSkip = True
                                    break
                                elif simTasks == None and tmpDatasetSpec.toKeepTrack():
                                    # update nFilesUsed in DatasetSpec
                                    nFilesUsed = tmpDatasetSpec.nFilesUsed + iFiles
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
                                tmpLog.debug('datasetID={0} has {1} files to be processed'.format(datasetID,iFiles))
                                # set flag if it is a block read
                                if tmpDatasetSpec.isMaster():
                                    if readBlock:
                                        inputChunk.readBlock = True
                                    else:
                                        inputChunk.readBlock = False
                    # add to return
                    if not toSkip:
                        returnList.append((taskSpec,cloudName,inputChunk))
                        iTasks += 1
                        # reduce the number of jobs
                        maxNumJobs -= int(math.ceil(float(len(inputChunk.masterDataset.Files))/float(typicalNumFilesPerJob)))
                    if not toSkip:        
                        # commit
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                    else:
                        # roll back
                        self._rollback()
                    # enough tasks 
                    if iTasks >= nTasks:
                        break
                    # already read enough files to generate jobs 
                    if maxNumJobs <= 0:
                        break
                # enough tasks 
                if iTasks >= nTasks:
                    break
                # already read enough files to generate jobs 
                if maxNumJobs <= 0:
                    break
            tmpLog.debug('done for {0} tasks'.format(len(returnList)))
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
    def insertTaskParams_JEDI(self,metaTaskID,taskParams):
        comment = ' /* JediDBProxy.insertTaskParams_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += '<metaTaskID={0}>'.format(metaTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to insert task parameters
            sqlT  = "INSERT INTO {0}.DEFT_TASK (TASK_ID,TASK_PARAM) VALUES ".format(jedi_config.db.schemaDEFT)
            sqlT += "({0}.PRODSYS2_TASK_ID_SEQ.nextval,:param) ".format(jedi_config.db.schemaDEFT)
            sqlT += "RETURNING TASK_ID INTO :jediTaskID"
            # sql to insert command
            sqlC  = "INSERT INTO {0}.PRODSYS_COMM (COMM_TASK,COMM_OWNER,COMM_CMD) ".format(jedi_config.db.schemaDEFT)
            sqlC += "VALUES (:jediTaskID,:comm_owner,:comm_cmd) "
            # begin transaction
            self.conn.begin()
            # insert task parameters
            varMap = {}
            varMap[':param']  = taskParams
            varMap[':jediTaskID'] = self.cur.var(cx_Oracle.NUMBER)
            self.cur.execute(sqlT+comment,varMap)
            jediTaskID = long(varMap[':jediTaskID'].getvalue())
            # insert command
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':comm_cmd']  = 'submit'
            varMap[':comm_owner']  = 'DEFT'
            self.cur.execute(sqlC+comment,varMap)
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



    # insert TaskParams
    def insertUpdateTaskParams_JEDI(self,jediTaskID,updateTaskParams,insertTaskParamsList):
        comment = ' /* JediDBProxy.insertUpdateTaskParams_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += '<jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to insert task parameters
            sqlIT  = "INSERT INTO {0}.DEFT_TASK (TASK_ID,TASK_PARAM) VALUES ".format(jedi_config.db.schemaDEFT)
            sqlIT += "({0}.PRODSYS2_TASK_ID_SEQ.nextval,:param) ".format(jedi_config.db.schemaDEFT)
            sqlIT += "RETURNING TASK_ID INTO :jediTaskID"
            # sql to insert command
            sqlC  = "INSERT INTO {0}.PRODSYS_COMM (COMM_TASK,COMM_OWNER,COMM_CMD) ".format(jedi_config.db.schemaDEFT)
            sqlC += "VALUES (:jediTaskID,:comm_owner,:comm_cmd) "
            # sql to insert command
            sqlUT  = "UPDATE {0}.JEDI_TaskParams SET taskParams=:taskParams ".format(jedi_config.db.schemaJEDI)
            sqlUT += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # insert task parameters
            newJediTaskIDs = []
            for taskParams in insertTaskParamsList:
                varMap = {}
                varMap[':param']  = taskParams
                varMap[':jediTaskID'] = self.cur.var(cx_Oracle.NUMBER)
                self.cur.execute(sqlIT+comment,varMap)
                newJediTaskID = long(varMap[':jediTaskID'].getvalue())
                newJediTaskIDs.append(newJediTaskID)
                # insert command
                varMap = {}
                varMap[':jediTaskID'] = newJediTaskID
                varMap[':comm_cmd']  = 'submit'
                varMap[':comm_owner']  = 'DEFT'
                self.cur.execute(sqlC+comment,varMap)
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
            sqlTR  = "SELECT jediTaskID "
            sqlTR += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTR += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTR += "AND tabT.status IN (:status1,:status2,:status3,:status4) AND lockedBy IS NOT NULL AND lockedTime<:timeLimit "
            if vo != None:
                sqlTR += "AND vo=:vo "
            if prodSourceLabel != None:
                sqlTR += "AND prodSourceLabel=:prodSourceLabel " 
            # sql to get picked datasets
            sqlDP  = "SELECT datasetID FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlDP += "WHERE jediTaskID=:jediTaskID AND type=:type " 
            # sql to rollback files
            sqlF  = "UPDATE {0}.JEDI_Dataset_Contents SET status=:nStatus ".format(jedi_config.db.schemaJEDI)
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:oStatus AND keepTrack=:keepTrack "
            # sql to reset nFilesUsed
            sqlDU  = "UPDATE {0}.JEDI_Datasets SET nFilesUsed=nFilesUsed-:nFileRow ".format(jedi_config.db.schemaJEDI)
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID " 
            # sql to unlock tasks
            sqlTU  = "UPDATE {0}.JEDI_Tasks SET lockedBy=NULL,lockedTime=NULL ".format(jedi_config.db.schemaJEDI)
            sqlTU += "WHERE jediTaskID=:jediTaskID"
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get orphaned tasks
            varMap = {}
            varMap[':status1'] = 'ready'
            varMap[':status2'] = 'scouting'
            varMap[':status3'] = 'running'
            varMap[':status4'] = 'merging'
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=waitTime)
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
            for jediTaskID, in resTaskList:
                tmpLog.debug('[jediTaskID={0}] rescue'.format(jediTaskID))
                self.conn.begin()
                # get input datasets
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':type']   = 'input'
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
                    tmpLog.debug('[takID={0}] reset {1} rows for datasetID={2}'.format(jediTaskID,nFileRow,datasetID))
                    if nFileRow > 0:
                        # reset nFilesUsed
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':datasetID']  = datasetID
                        varMap[':nFileRow'] = nFileRow
                        self.cur.execute(sqlDU+comment,varMap)
                # unlock task
                tmpLog.debug('[jediTaskID={0}] ulock'.format(jediTaskID))        
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
                    varMap[':jobStatus2'] = 'dummy'
                else:
                    varMap[':jobStatus1'] = 'defined'
                    varMap[':jobStatus2'] = 'assigned'
                self.cur.arraysize = 100
                tmpLog.debug((sql0+comment).format(table))
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
            sqlC  = "SELECT comm_task FROM {0}.PRODSYS_COMM ".format(jedi_config.db.schemaDEFT)
            sqlC += "WHERE comm_owner=:comm_owner AND comm_cmd=:comm_cmd "
            varMap = {}
            varMap[':comm_owner']    = 'DEFT'
            varMap[':comm_cmd']      = 'submit'
            # FIXME once vo and prodSourceLabel are added to DEFT tables
            """
            if not vo in [None,'any']:
                varMap[':comm_vo'] = vo
                sqlC += "AND comm_vo=:comm_vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':comm_prodSourceLabel'] = prodSourceLabel
                sqlC += "AND comm_prodSourceLabel=:comm_prodSourceLabel "
            """    
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
            for jediTaskID, in resList:
                tmpLog.debug('start jediTaskID={0}'.format(jediTaskID))
               # start transaction
                self.conn.begin()
                # lock
                varMap = {}
                varMap[':comm_task'] = jediTaskID
                sqlLock  = "SELECT * FROM {0}.PRODSYS_COMM WHERE comm_task=:comm_task ".format(jedi_config.db.schemaDEFT)
                sqlLock += "FOR UPDATE NOWAIT "
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
                        sqlIT =  "INSERT INTO {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
                        sqlIT += "(jediTaskID,taskName,status,userName,creationDate,modificationtime"
                        if vo != None:
                            sqlIT += ',vo'
                        if prodSourceLabel != None:
                            sqlIT += ',prodSourceLabel'
                        sqlIT += ") "
                        sqlIT += "VALUES(:jediTaskID,:taskName,:status,:userName,CURRENT_DATE,CURRENT_DATE"
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
                        varMap[':task_id'] = jediTaskID
                        sqlTC = "SELECT task_id FROM {0}.DEFT_TASK WHERE task_id=:task_id ".format(jedi_config.db.schemaDEFT)
                        tmpLog.debug(sqlTC+comment+str(varMap))
                        self.cur.execute(sqlTC+comment,varMap)
                        resTC = self.cur.fetchone()
                        if resTC == None or resTC[0] == None:
                            tmpLog.error("task parameters not found in DEFT_TASK")
                            isOK = False
                    if isOK:        
                        # copy task parameters
                        varMap = {}
                        varMap[':task_id'] = jediTaskID
                        sqlCopy  = "INSERT INTO {0}.JEDI_TaskParams (jediTaskID,taskParams) ".format(jedi_config.db.schemaJEDI)
                        sqlCopy += "SELECT task_id,task_param FROM {0}.DEFT_TASK ".format(jedi_config.db.schemaDEFT)
                        sqlCopy += "WHERE task_id=:task_id "
                        try:
                            tmpLog.debug(sqlCopy+comment+str(varMap))
                            self.cur.execute(sqlCopy+comment,varMap)
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error("failed to insert param for jediTaskID={0} with {1} {2}".format(jediTaskID,errtype,errvalue))
                    # update
                    if isOK:        
                        varMap = {}
                        varMap[':comm_task'] = jediTaskID
                        varMap[':comm_cmd']  = 'submitted'
                        sqlUC = "UPDATE {0}.PRODSYS_COMM SET comm_cmd=:comm_cmd WHERE comm_task=:comm_task ".format(jedi_config.db.schemaDEFT)
                        tmpLog.debug(sqlUC+comment+str(varMap))
                        self.cur.execute(sqlUC+comment,varMap)
                    # append
                    if isOK:
                        retTaskIDs.append((jediTaskID,None,'registered'))
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # find orphaned tasks to rescue
            self.conn.begin()
            varMap = {}
            varMap[':status1'] = 'registered'
            varMap[':status2'] = JediTaskSpec.commandStatusMap()['incexec']['done']
            # FIXME
            #varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(seconds=10)
            sqlOrpS  = "SELECT tabT.jediTaskID,tabT.splitRule,tabT.status "
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
            for jediTaskID,splitRule,taskStatus in resList:
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                tmpLog.debug(sqlOrpU+comment+str(varMap))
                self.cur.execute(sqlOrpU+comment,varMap)
                nRow = self.cur.rowcount
                if nRow == 1 and not jediTaskID in retTaskIDs:
                    retTaskIDs.append((jediTaskID,splitRule,taskStatus))
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
            for tmpItem, in self.cur:
                retStr = tmpItem.read()
                break
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('end')            
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
                                   unmergeMasterDatasetSpec,unmergeDatasetSpecMap):
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
            # update task
            varMap = taskSpec.valuesMap(useSeq=False,onlyChanged=True)
            varMap[':jediTaskID'] = jediTaskID
            sql  = "UPDATE {0}.JEDI_Tasks SET {1} WHERE ".format(jedi_config.db.schemaJEDI,
                                                                 taskSpec.bindUpdateChangesExpression())
            sql += "jediTaskID=:jediTaskID "
            self.cur.execute(sql+comment,varMap)
            nRow = self.cur.rowcount
            tmpLog.debug('update {0} row in task table'.format(nRow))
            if nRow != 1:
                tmpLog.error('the task not found in task table')
            else:
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
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # get scout job data
    def getScoutJobData_JEDI(self,jediTaskID,useTransaction=False):
        comment = ' /* JediDBProxy.getScoutJobData_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        returnMap = {}
        # sql to get scout job data
        sqlSCF  = "SELECT tabF.fileID,tabF.datasetID,tabF.attemptNr "
        sqlSCF += "FROM {0}.JEDI_Datasets tabD, {0}.JEDI_Dataset_Contents tabF WHERE ".format(jedi_config.db.schemaJEDI)
        sqlSCF += "tabD.jediTaskID=tabF.jediTaskID AND tabD.jediTaskID=:jediTaskID AND tabF.status=:status "
        sqlSCF += "AND tabD.datasetID=tabF.datasetID "
        sqlSCF += "AND tabF.type IN ("
        for tmpType in JediDatasetSpec.getInputTypes():
            mapKey = ':type_'+tmpType
            sqlSCF += '{0},'.format(mapKey)
        sqlSCF  = sqlSCF[:-1]
        sqlSCF += ") AND tabD.masterID IS NULL " 
        sqlSCP  = "SELECT PandaID FROM {0}.filesTable4 ".format(jedi_config.db.schemaPANDA)
        sqlSCP += "WHERE fileID=:fileID AND jediTaskID=:jediTaskID AND datasetID=:datasetID AND attemptNr=:attemptNr"
        sqlSCD  = "SELECT jobStatus,outputFileBytes,jobMetrics,cpuConsumptionTime "
        sqlSCD += "FROM {0}.jobsArchived4 ".format(jedi_config.db.schemaPANDA)
        sqlSCD += "WHERE PandaID=:pandaID "
        sqlSCD += "UNION "
        sqlSCD += "SELECT jobStatus,outputFileBytes,jobMetrics,cpuConsumptionTime "
        sqlSCD += "FROM {0}.jobsArchived ".format(jedi_config.db.schemaPANDAARCH)
        sqlSCD += "WHERE PandaID=:pandaID AND modificationTime>(CURRENT_DATE-14) "
        if useTransaction:
            # begin transaction
            self.conn.begin()
        # get files    
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        varMap[':status'] = 'finished'
        for tmpType in JediDatasetSpec.getInputTypes():
            mapKey = ':type_'+tmpType
            varMap[mapKey] = tmpType
        self.cur.execute(sqlSCF+comment,varMap)
        resList = self.cur.fetchall()
        # the number of file records for normalization
        nFileRecords = len(resList)
        if nFileRecords == 0:
            scoutSucceeded = False
            nFileRecords = 1.0
        else:
            scoutSucceeded = True
            nFileRecords = float(nFileRecords)
        # loop over all files    
        outSizeList  = []
        walltimeList = []
        memSizeList  = []
        workSizeList = []
        finishedJobs = []
        for fileID,datasetID,attemptNr in resList:
            # get PandaID
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':datasetID']  = datasetID
            varMap[':fileID']     = fileID
            varMap[':attemptNr']  = attemptNr
            self.cur.execute(sqlSCP+comment,varMap)
            resPandaID = self.cur.fetchone()
            if resPandaID != None:
                pandaID, = resPandaID
                # get job data
                varMap = {}
                varMap[':pandaID'] = pandaID
                self.cur.execute(sqlSCD+comment,varMap)
                resData = self.cur.fetchone()
                if resData != None:
                    jobStatus,outputFileBytes,jobMetrics,cpuConsumptionTime = resData
                    if jobStatus != 'finished':
                        continue
                    finishedJobs.append(pandaID)
                    # output size
                    try:
                        outSizeList.append(long(outputFileBytes))
                    except:
                        pass
                    # execution time
                    try:
                        walltimeList.append(long(cpuConsumptionTime))
                    except:
                        pass
                    # VM size
                    try:
                        tmpMatch = re.search('vmPeakMax=(\d+)',jobMetrics)
                        memSizeList.append(long(tmpMatch.group(1)))
                    except:
                        pass
                    # workdir size
                    try:
                        tmpMatch = re.search('workDirSize=(\d+)',jobMetrics)
                        workSizeList.append(long(tmpMatch.group(1)))
                    except:
                        pass
            # normalization since job data is calculated per file record
            nFinisedJobs = len(finishedJobs)
            if nFinisedJobs == 0:
                nFinisedJobs = 1.0
            else:
                nFinisedJobs = float(nFinisedJobs)
            normFactor = nFileRecords / nFinisedJobs
            # calculate median values
            if outSizeList != []:
                median = numpy.median(outSizeList) 
                median /= (1024*1024)
                returnMap['outDiskCount'] = long(median/normFactor)
                returnMap['outDiskUnit']  = 'MB'
            if walltimeList != []:
                median = numpy.median(walltimeList)
                returnMap['walltime']     = long(median/normFactor)
                returnMap['walltimeUnit'] = 'kSI2kseconds'
            if memSizeList != []:
                median = numpy.median(memSizeList)
                median /= 1024
                returnMap['ramCount'] = long(median)
                returnMap['ramUnit']  = 'MB'
            if workSizeList != []:   
                median = numpy.median(workSizeList)
                returnMap['workDiskCount'] = long(median)
                returnMap['workDiskUnit']  = 'MB'
        if useTransaction:    
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
        # return    
        tmpLog.debug('succeeded={0} data->{1}'.format(scoutSucceeded,str(returnMap)))
        return scoutSucceeded,returnMap



    # prepare tasks to be finished
    def prepareTasksToBeFinished_JEDI(self,vo,prodSourceLabel,nTasks=50,simTasks=None):
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
                varMap[':dsEndStatus1'] = 'broken'
                varMap[':dsEndStatus2'] = 'done'
                if vo != None:
                    varMap[':vo'] = vo
                if prodSourceLabel != None:
                    varMap[':prodSourceLabel'] = prodSourceLabel
                sql  = "SELECT tabT.jediTaskID,tabT.status "
                sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
                sql += "AND tabT.status IN (:taskstatus1,:taskstatus2,:taskstatus3,:taskstatus4) "
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
                sql += ') AND NOT status IN (:dsEndStatus1,:dsEndStatus2) '
                sql += 'AND (nFilesToBeUsed<>nFilesUsed OR nFilesUsed=0 OR nFilesUsed>nFilesFinished+nFilesFailed)) '
                sql += 'AND rownum<={0}'.format(nTasks)
            else:
                varMap = {}
                sql  = "SELECT tabT.jediTaskID,tabT.status "
                sql += "FROM {0}.JEDI_Tasks tabT ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE "
                for tmpTaskIdx,tmpTaskID in enumerate(simTasks):
                    tmpKey = ':jediTaskID{0}'.format(tmpTaskIdx)
                    varMap[tmpKey] = tmpTaskID
                    sql += '{0},'.format(tmpKey)
                sql = sql[:-1]
                sql += ') '
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # select
            tmpLog.debug(sql+comment+str(varMap))
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # make list
            jediTaskIDstatusMap = {}
            jediTaskIDList = []
            for jediTaskID,taskStatus in resList:
                jediTaskIDstatusMap[jediTaskID] = taskStatus 
            jediTaskIDList = jediTaskIDstatusMap.keys()
            jediTaskIDList.sort()
            tmpLog.debug('got {0} tasks'.format(len(jediTaskIDList)))
            # sql to read task
            sqlRT  = "SELECT {0} ".format(JediTaskSpec.columnNames())
            sqlRT += "FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL FOR UPDATE NOWAIT ".format(jedi_config.db.schemaJEDI)
            # sql to read dataset status
            sqlRD  = "SELECT datasetID,status,nFiles,nFilesFinished,masterID "
            sqlRD += "FROM {0}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND status=:status AND type IN (".format(jedi_config.db.schemaJEDI)
            for tmpType in JediDatasetSpec.getProcessTypes():
                mapKey = ':type_'+tmpType
                sqlRD += '{0},'.format(mapKey)
            sqlRD  = sqlRD[:-1]
            sqlRD += ') '
            # sql to update input dataset status
            sqlDIU  = "UPDATE {0}.JEDI_Datasets SET status=:status,modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlDIU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update output/log dataset status
            sqlDOU  = "UPDATE {0}.JEDI_Datasets SET status=:status,modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlDOU += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # sql to update nFiles of dataset
            sqlFU  = "UPDATE {0}.JEDI_Datasets SET nFilesToBeUsed=nFiles,modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlFU += "WHERE type IN ("
            for tmpType in JediDatasetSpec.getInputTypes():
                mapKey = ':type_'+tmpType
                sqlFU += '{0},'.format(mapKey)
            sqlFU  = sqlFU[:-1]
            sqlFU += ") AND jediTaskID=:jediTaskID AND masterID IS NULL "
            # sql to update task status
            sqlTU  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTU += "SET status=:status,modificationTime=CURRENT_DATE,lockedBy=NULL,lockedTime=CURRENT_DATE,errorDialog=:errorDialog "
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
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                try:
                    # select
                    self.cur.execute(sqlRT+comment,varMap)
                    resRT = self.cur.fetchone()
                    # locked by another
                    if resRT == None:
                        toSkip = True
                    else:
                        taskSpec = JediTaskSpec()
                        taskSpec.pack(resRT)
                except:
                    errType,errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        # resource busy and acquire with NOWAIT specified
                        toSkip = True
                        tmpLog.debug('skip locked jediTaskID={0}'.format(jediTaskID))
                    else:
                        # failed with something else
                        raise errType,errValue
                # update dataset
                if not toSkip:
                    if taskSpec.status == 'scouting':
                        # set average job data
                        scoutSucceeded,scoutData = self.getScoutJobData_JEDI(jediTaskID)
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
                            self.cur.execute(sqlTSD+comment,varMap)
                        # update nFiles to be used
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        for tmpType in JediDatasetSpec.getInputTypes():
                            mapKey = ':type_'+tmpType
                            varMap[mapKey] = tmpType
                        self.cur.execute(sqlFU+comment,varMap)
                        # new task status
                        if scoutSucceeded:
                            newTaskStatus = 'scouted'
                        else:
                            newTaskStatus = 'broken'
                            errorDialog = 'no scout jobs succeeded'
                    elif taskSpec.status in ['running','merging','preprocessing']:
                        # update output datasets
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':type1']  = 'log'
                        varMap[':type2']  = 'output'
                        varMap[':status'] = 'prepared'
                        self.cur.execute(sqlDOU+comment,varMap)
                        # get input datasets
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':status'] = 'ready'
                        for tmpType in JediDatasetSpec.getProcessTypes():
                            mapKey = ':type_'+tmpType
                            varMap[mapKey] = tmpType
                        self.cur.execute(sqlRD+comment,varMap)
                        resRD = self.cur.fetchall()
                        preprocessedFlag = False
                        for datasetID,dsStatus,nFiles,nFilesFinished,masterID in resRD:
                            # update input datasets
                            varMap = {}
                            varMap[':datasetID']  = datasetID
                            varMap[':jediTaskID'] = jediTaskID
                            if masterID != None:
                                # seconday dataset
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
                                    varMap[':status'] = 'partial'
                            self.cur.execute(sqlDIU+comment,varMap)
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
            # FIXME
            #varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
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
            sqlSCF += "AND cloud IS NULL "
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
            sqlSCF += "AND cloud IS NULL "
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
                # sql to set cloud
                sql  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
                sql += "SET cloud=:cloud,status=:status,oldStatus=NULL "
                sql += "WHERE jediTaskID=:jediTaskID "
                for jediTaskID,cloudName in taskCloudMap.iteritems():
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':status']     = 'ready'
                    varMap[':cloud']      = cloudName
                    # begin transaction
                    self.conn.begin()
                    # set cloud
                    self.cur.execute(sql+comment,varMap)
                    nRow = self.cur.rowcount
                    tmpLog.debug('set cloud={0} for jediTaskID={1} with {2}'.format(cloudName,jediTaskID,nRow))
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
            varMap[':priority'] = priority
            sql  = "SELECT tabT.cloud,ROUND(SUM((nFiles-nFilesFinished-nFilesFailed)*walltime)/24/3600) "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sql += "AND tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
            sql += "AND tabT.vo=:vo AND prodSourceLabel=:prodSourceLabel AND currentPriority>=:priority "
            if workQueue != None:
                sql += "AND workQueue_ID IN (" 
                for tmpQueue_ID in workQueue.getIDs():
                    tmpKey = ':queueID_{0}'.format(tmpQueue_ID)
                    varMap[tmpKey] = tmpQueue_ID
                    sql += '{0},'.format(tmpKey)
                sql  = sql[:-1]    
                sql += ") "
            sql += "AND tabT.status IN (:status1,:status2,:status3,:status4,:status5) "
            varMap[':status1'] = 'ready'
            varMap[':status2'] = 'scouting'
            varMap[':status3'] = 'running'
            varMap[':status4'] = 'merging'
            varMap[':status5'] = 'pending'
            sql += "AND tabT.cloud IS NOT NULL "
            sql += "GROUP BY tabT.cloud "
            # begin transaction
            self.conn.begin()
            # set cloud
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            retMap = {}
            for cloudName,rwValue in resList:
                retMap[cloudName] = rwValue
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
                sqlLock  = "SELECT * FROM {0}.PRODSYS_COMM WHERE comm_task=:comm_task ".format(jedi_config.db.schemaDEFT)
                sqlLock += "FOR UPDATE NOWAIT "
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
                        sqlTC =  "SELECT status FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
                        sqlTC += "WHERE jediTaskID=:jediTaskID FOR UPDATE "
                        self.cur.execute(sqlTC+comment,varMap)
                        resTC = self.cur.fetchone()
                        if resTC == None or resTC[0] == None:
                            tmpLog.error("jediTaskID={0} is not found in JEDI_Tasks".format(jediTaskID))
                            isOK = False
                        else:
                            taskStatus = resTC[0]
                            if commandStr == 'retry':
                                if not taskStatus in JediTaskSpec.statusToRetry():
                                    # task is in a status which rejects retry
                                    tmpLog.error("jediTaskID={0} rejected command={1}. status={2} is not for retry".format(jediTaskID,
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
                        varMap[':status'] = newTaskStatus
                        varMap[':errDiag'] = comComment
                        sqlTU  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
                        sqlTU += "SET status=:status,oldStatus=status,modificationTime=CURRENT_DATE,errorDialog=:errDiag "
                        sqlTU += "WHERE jediTaskID=:jediTaskID "
                        self.cur.execute(sqlTU+comment,varMap)
                    # update command table
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
                        retTaskIDs[jediTaskID] = {'command':commandStr,'comment':comComment}
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # find orphaned tasks to rescue
            for commandStr,taskStatusMap in commandStatusMap.iteritems():
                self.conn.begin()
                varMap = {}
                varMap[':status'] = taskStatusMap['doing']
                # FIXME
                #varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
                varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
                sqlOrpS  = "SELECT jediTaskID,errorDialog "
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
                for jediTaskID,comComment in resList:
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    tmpLog.debug(sqlOrpU+comment+str(varMap))
                    self.cur.execute(sqlOrpU+comment,varMap)
                    nRow = self.cur.rowcount
                    if nRow == 1 and not retTaskIDs.has_key(jediTaskID):
                        retTaskIDs[jediTaskID] = {'command':commandStr,'comment':comComment}
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
    def reactivatePendingTasks_JEDI(self,vo,prodSourceLabel,timeLimit,timeoutLimit=None):
        comment = ' /* JediDBProxy.reactivatePendingTasks_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <vo={0} label={1} limit={2}min timeout={3}days>".format(vo,prodSourceLabel,timeLimit,timeoutLimit)
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
            sqlTL  = "SELECT jediTaskID,creationDate,errorDialog "
            sqlTL += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(jedi_config.db.schemaJEDI)
            sqlTL += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlTL += "AND tabT.status=:status AND tabT.modificationTime<:timeLimit AND tabT.oldStatus IS NOT NULL "
            if not vo in [None,'any']:
                varMap[':vo'] = vo
                sqlTL += "AND vo=:vo "
            if not prodSourceLabel in [None,'any']:
                varMap[':prodSourceLabel'] = prodSourceLabel
                sqlTL += "AND prodSourceLabel=:prodSourceLabel "
            # sql to update tasks    
            sqlTU  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTU += "SET status=oldStatus,oldStatus=NULL,errorDialog=NULL,modificationtime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID "
            # sql to timeout tasks    
            sqlTO  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTO += "SET status=:newStatus,errorDialog=:errorDialog,modificationtime=CURRENT_DATE "
            sqlTO += "WHERE jediTaskID=:jediTaskID "
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlTL+comment,varMap)
            resTL = self.cur.fetchall()
            # loop over all tasks
            nRow = 0
            for jediTaskID,creationDate,errorDialog in resTL:
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                if timeoutDate != None and creationDate < timeoutDate:
                    varMap[':newStatus'] = 'timeout'
                    if errorDialog == None:
                        errorDialog = ''
                    else:
                        errorDialog += '.'
                    errorDialog += 'timeout in pending'
                    varMap[':errorDialog'] = errorDialog
                    sql = sqlTO
                else:
                    sql = sqlTU
                self.cur.execute(sql+comment,varMap)
                nRow += self.cur.rowcount
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



    # get file spec of lib.tgz
    def getBuildFileSpec_JEDI(self,jediTaskID,siteName):
        comment = ' /* JediDBProxy.getBuildFileSpec_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += " <jediTaskID={0} siteName={1}>".format(jediTaskID,siteName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get dataset
            sqlRD  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
            sqlRD += "FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlRD += "WHERE jediTaskID=:jediTaskID AND type=:type AND site=:site "
            sqlRD += "ORDER BY creationTime DESC "
            # sql to read files
            sqlFR  = "SELECT {0} ".format(JediFileSpec.columnNames())
            sqlFR += "FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
            sqlFR += "jediTaskID=:jediTaskID AND datasetID=:datasetID AND type=:type AND status=:status "
            sqlFR += "ORDER BY creationDate DESC "
            # start transaction
            self.conn.begin()
            # get dataset
            varMap = {}
            varMap[':type'] = 'lib'
            varMap[':site'] = siteName
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
                varMap[':status']     = 'finished'
                self.cur.execute(sqlFR+comment,varMap)
                resFileList = self.cur.fetchall()
                for resFile in resFileList:
                    # make FileSpec
                    fileSpec = JediFileSpec()
                    fileSpec.pack(resFile)
                    break
                # no more dataset lookup
                if fileSpec != None:
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
                # return IDs in a map since changes to jobSpec are not effective
                # since invoked in separate processes
                fileIdMap[fileSpec.lfn] = {'datasetID':datasetID,
                                           'fileID':fileID}
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
        if protocol == 'xrd':
            field = 'xrdcpval'
        else:
            tmpLog.error('unsupported protocol={0}'.format(protocol))
            return failedRet
        try:
            # sql
            sqlDS =  "SELECT * FROM "
            sqlDS += "(SELECT destination,CASE WHEN {0}>={1} THEN {2} ".format(field,cutoff,maxWeight)
            sqlDS += "ELSE ROUND({0}/{1}*{2},2) END AS {0} ".format(field,cutoff,maxWeight)
            sqlDS += "FROM {0}.sites_matrix_data ".format(jedi_config.db.schemaMETA)
            sqlDS += "WHERE source=:source AND {0} IS NOT NULL AND {0}>:threshold ORDER BY {0} DESC) ".format(field)
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
    def retryTask_JEDI(self,jediTaskID,commStr,maxAttempt=5):
        comment = ' /* JediDBProxy.retryTask_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start command={0}'.format(commStr))
        updateTask = False
        # check command
        if not commStr in ['retry','incexec']:
            tmpLog.debug('unknown command={0}'.format(commStr))
            return updateTask
        try:
            # sql to retry files
            sqlRF  = "UPDATE {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlRF += "SET maxAttempt=maxAttempt+:maxAttempt "
            sqlRF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status "
            sqlRF += "AND keepTrack=:keepTrack AND maxAttempt IS NOT NULL AND maxAttempt=attemptNr "
            # sql to retry/incexecute datasets
            sqlRD  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlRD += "SET status=:status,nFilesFailed=nFilesFailed-:nDiff "
            sqlRD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update task status
            sqlUT  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlUT += " SET status=:status,oldStatus=NULL,modificationtime=CURRENT_DATE WHERE jediTaskID=:jediTaskID "
            # start transaction
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
                if taskOldStatus == 'finished' and commStr == 'retry':
                    # no retry for finished task
                    msgStr = 'no {0} for task in {1} status'.format(commStr,taskOldStatus)
                    tmpLog.debug(msgStr)
                    newTaskStatus = taskOldStatus
                elif not taskOldStatus in ['finished','failed','partial']:
                    # only tasks in a relevant final status 
                    msgStr = 'no {0} since not in relevant final status ({1})'.format(commStr,taskOldStatus)
                    tmpLog.debug(msgStr)
                    newTaskStatus = taskOldStatus
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
                        else:
                            # no retry if master dataset successfully finished
                            if commStr == 'retry' and nFiles == nFilesFinished:
                                tmpLog.debug('no {0} for datasetID={1} : nFiles==nFilesFinished'.format(commStr,datasetID))
                                continue
                            # no refresh of dataset contents if dataset is closed
                            if state == 'closed' and nFiles == nFilesFinished:
                                tmpLog.debug('no refresh for datasetID={0} : state={1}'.format(datasetID,state))
                                continue
                            # update files
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID']  = datasetID
                            varMap[':status']     = 'ready'
                            varMap[':maxAttempt'] = maxAttempt
                            varMap[':keepTrack']  = 1
                            self.cur.execute(sqlRF+comment,varMap)
                            nDiff = self.cur.rowcount
                            # no retry if no failed files
                            if commStr == 'retry' and nDiff == 0:
                                tmpLog.debug('no {0} for datasetID={1} : nDiff=0'.format(commStr,datasetID))
                                continue
                            # update dataset
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID']  = datasetID
                            varMap[':nDiff'] = nDiff
                            if commStr == 'retry' or state == 'closed':
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
                            self.cur.execute(sqlRF+comment,varMap)
                            nDiff = self.cur.rowcount
                            # update dataset
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':datasetID']  = datasetID
                            varMap[':nDiff'] = nDiff
                            if commStr == 'retry' or state == 'closed':
                                varMap[':status'] = 'ready'
                            elif commStr == 'incexec':
                                varMap[':status'] = 'refresh'
                            self.cur.execute(sqlRD+comment,varMap)
                    # update task
                    if commStr == 'retry':
                        if changedMasterList != []:
                            newTaskStatus = JediTaskSpec.commandStatusMap()[commStr]['done']
                        else:
                            # to to finalization since no files left in ready status
                            tmpLog.debug('no {0} since no files left'.format(commStr))
                            newTaskStatus = taskOldStatus
                    else:
                        # for incremental execution
                        newTaskStatus = JediTaskSpec.commandStatusMap()[commStr]['done']
                # update task
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':status'] = newTaskStatus
                if newTaskStatus != taskOldStatus:
                    tmpLog.debug('set taskStatus={0} for command={1}'.format(newTaskStatus,commStr))
                else:
                    tmpLog.debug('back to taskStatus={0} for command={1}'.format(newTaskStatus,commStr))
                self.cur.execute(sqlUT+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            tmpLog.debug("done")
            return updateTask
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None



    # append input datasets for incremental execution
    def appendDatasets_JEDI(self,jediTaskID,inMasterDatasetSpecList,inSecDatasetSpecList):
        comment = ' /* JediDBProxy.appendDatasets_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        goDefined = False
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
                    sqlDS  = "SELECT datasetName,status "
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
                    for datasetName,datasetStatus in resDS:
                        existingDatasets[datasetName] = datasetStatus
                    # insert datasets
                    sqlID  = "INSERT INTO {0}.JEDI_Datasets ({1}) ".format(jedi_config.db.schemaJEDI,
                                                                           JediDatasetSpec.columnNames())
                    sqlID += JediDatasetSpec.bindValuesExpression()
                    sqlID += " RETURNING datasetID INTO :newDatasetID"
                    for datasetSpec in inMasterDatasetSpecList:
                        # skip existing datasets
                        if datasetSpec.datasetName in existingDatasets:
                            # check dataset status
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
                    sqlUT += " SET status=:status,modificationtime=CURRENT_DATE WHERE jediTaskID=:jediTaskID "
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    if goDefined:
                        # pass to ContentsFeeder
                        varMap[':status'] = 'defined'
                    else:
                        # go to finalization since no datasets are appended
                        varMap[':status'] = 'prepared'
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
        
