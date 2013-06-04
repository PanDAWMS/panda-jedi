import re
import sys
import copy
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
    def getWrokQueueMap(self):
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
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False

                                            

    # get the list of datasets to feed contents to DB
    def getDatasetsToFeedContents_JEDI(self):
        comment = ' /* JediDBProxy.getDatasetsToFeedContents_JEDI */'
        methodName = self.getMethodName(comment)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # SQL
            varMap = {}
            varMap[':type']             = 'input'
            varMap[':taskStatus']       = 'defined'
            varMap[':dsStatus_pending'] = 'pending'
            sql  = "SELECT %s " % JediDatasetSpec.columnNames('tabD')
            sql += 'FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD '.format(jedi_config.db.schemaJEDI,
                                                                             jedi_config.db.schemaJEDI)
            sql += 'WHERE tabT.jediTaskID=tabD.jediTaskID '
            sql += 'AND type=:type AND tabT.status=:taskStatus '
            sql += 'AND tabD.status IN ('
            for tmpStat in JediDatasetSpec.statusToUpdateContents():
                mapKey = ':dsStatus_'+tmpStat
                sql += '{0},'.format(mapKey)
                varMap[mapKey] = tmpStat
            sql  = sql[:-1]    
            sql += ') AND tabT.lockedBy IS NULL AND tabD.lockedBy IS NULL '
            sql += 'AND NOT EXISTS '
            sql += '(SELECT 1 FROM {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
            sql += 'WHERE {0}.JEDI_Datasets.jediTaskID=tabT.jediTaskID '.format(jedi_config.db.schemaJEDI)
            sql += 'AND type=:type AND status=:dsStatus_pending) '
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
                                   nEventsPerFile,nEventsPerJob,maxAttempt):
        comment = ' /* JediDBProxy.insertFilesForDataset_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0} datasetID={1}>'.format(datasetSpec.jediTaskID,
                                                           datasetSpec.datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start nEventsPerFile={0} nEventsPerJob={1} maxAttempt={2}'.format(nEventsPerFile,
                                                                                        nEventsPerJob,
                                                                                        maxAttempt))
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
            # make file specs
            fileSpecMap = {}
            uniqueFileKeyList = []
            nRemEvents = nEventsPerJob
            for tmpLFN in lfnList:
                guid,fileVal = filelValMap[tmpLFN]
                fileSpec = JediFileSpec()
                fileSpec.jediTaskID       = datasetSpec.jediTaskID
                fileSpec.datasetID    = datasetSpec.datasetID
                fileSpec.GUID         = guid
                fileSpec.type         = 'input'
                fileSpec.status       = 'ready'            
                fileSpec.lfn          = fileVal['lfn']
                fileSpec.scope        = fileVal['scope']
                fileSpec.fsize        = fileVal['filesize']
                fileSpec.checksum     = fileVal['checksum']
                fileSpec.creationDate = timeNow
                # this info will come from Rucio in the future
                fileSpec.nEvents      = nEventsPerFile
                # keep track
                if datasetSpec.toKeepTrack():
                    fileSpec.keepTrack = 1
                tmpFileSpecList = []
                if nEventsPerJob == None or nEventsPerJob <= 0 or \
                       fileSpec.nEvents == None or fileSpec.nEvents <= 0 or \
                       nEventsPerFile == None or nEventsPerFile <= 0: 
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
                            tmpFileSpecList.append(splitFileSpec)
                            break
                        else:
                            splitFileSpec.startEvent = tmpStartEvent
                            splitFileSpec.endEvent   = tmpStartEvent + nRemEvents -1
                            tmpStartEvent += nRemEvents
                            nRemEvents = nEventsPerJob
                            tmpFileSpecList.append(splitFileSpec)
                # append
                for fileSpec in tmpFileSpecList:
                    uniqueFileKey = '{0}.{1}.{2}'.format(fileSpec.lfn,fileSpec.startEvent,fileSpec.endEvent)
                    uniqueFileKeyList.append(uniqueFileKey)                
                    fileSpecMap[uniqueFileKey] = fileSpec
            # too long list
            if len(uniqueFileKeyList) > 10000:
                tmpLog.error("too many file records {0}".format(len(uniqueFileKeyList)))
                return False
            # sql to check if task is locked
            sqlTL = "SELECT status,lockedBy FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE ".format(jedi_config.db.schemaJEDI)
            # sql to check dataset status
            sqlDs = "SELECT status FROM {0}.JEDI_Datasets WHERE datasetID=:datasetID FOR UPDATE ".format(jedi_config.db.schemaJEDI)
            # sql to get existing files
            sqlCh  = "SELECT fileID,lfn,status,startEvent,endEvent FROM {0}.JEDI_Dataset_Contents ".format(jedi_config.db.schemaJEDI)
            sqlCh += "WHERE datasetID=:datasetID FOR UPDATE"
            # sql for insert
            sqlIn  = "INSERT INTO {0}.JEDI_Dataset_Contents ({1}) ".format(jedi_config.db.schemaJEDI,JediFileSpec.columnNames())
            sqlIn += JediFileSpec.bindValuesExpression()
            # sql to update file status
            sqlFU = "UPDATE {0}.JEDI_Dataset_Contents SET status=:status WHERE fileID=:fileID ".format(jedi_config.db.schemaJEDI)
            # sql to update dataset
            sqlDU  = "UPDATE {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlDU += "SET status=:status,state=:state,stateCheckTime=:stateUpdateTime,"
            sqlDU += "nFiles=:nFiles,nFilesTobeUsed=:nFilesTobeUsed "
            sqlDU += "WHERE datasetID=:datasetID "
            nInsert  = 0
            retVal = None
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
                    varMap[':datasetID'] = datasetSpec.datasetID
                    self.cur.execute(sqlDs+comment,varMap)
                    resDs = self.cur.fetchone()
                    if resDs == None:
                        tmpLog.debug('dataset not found in Datasets table')
                    elif not resDs[0] in JediDatasetSpec.statusToUpdateContents():
                        tmpLog.debug('ds.status={0} is not for contents update'.format(resDs[0]))
                    else:    
                        # get existing file list
                        varMap = {}
                        varMap[':datasetID'] = datasetSpec.datasetID
                        self.cur.execute(sqlCh+comment,varMap)
                        tmpRes = self.cur.fetchall()
                        existingFiles = {}
                        for fileID,lfn,status,startEvent,endEvent in tmpRes:
                            uniqueFileKey = '{0}.{1}.{2}'.format(lfn,startEvent,endEvent)
                            existingFiles[uniqueFileKey] = {'fileID':fileID,'status':status}
                            if status == 'ready':
                                nReady += 1
                        # insert files
                        existingFileList = existingFiles.keys()
                        for uniqueFileKey in uniqueFileKeyList:
                            fileSpec = fileSpecMap[uniqueFileKey]
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
                            varMap['fileID'] = fileVarMap['fileID']
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
                        varMap[':datasetID'] = datasetSpec.datasetID
                        varMap[':nFiles'] = nInsert + len(existingFiles)
                        varMap[':nFilesTobeUsed'] = nReady
                        varMap[':status' ] = 'ready'
                        varMap[':state' ] = datasetState
                        varMap[':stateUpdateTime'] = stateUpdateTime
                        self.cur.execute(sqlDU+comment,varMap)
                    # set return value
                    retVal = True
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
            return False



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
            sql  = "SELECT * FROM (SELECT %s " % JediFileSpec.columnNames()
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
            self.cur.arraysize = 10000
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
            for tmpKey,tmpVal in criteria.iteritems():
                crKey = ':cr_%s' % tmpKey
                sql += '%s=%s' % (tmpKey,crKey)
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
    def getDatasetWithID_JEDI(self,datasetID):
        comment = ' /* JediDBProxy.getDatasetWithID_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <datasetID={0}>'.format(datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        # return value for failure
        failedRet = False,None
        try:
            # sql
            sql  = "SELECT %s " % JediDatasetSpec.columnNames()
            sql += "FROM {0}.JEDI_Datasets WHERE datasetID=:datasetID ".format(jedi_config.db.schemaJEDI)
            varMap = {}
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
    def updateTaskStatusByContFeeder_JEDI(self,jediTaskID,newStatus=None):
        comment = ' /* JediDBProxy.updateTaskStatusByContFeeder_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start newStat={0}'.format(newStatus))
        try:
            # sql to check status
            sqlS  = "SELECT status,oldStatus,lockedBy,cloud,prodSourceLabel FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlS += "WHERE jediTaskID=:jediTaskID "
            # sql to update task
            sqlU  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlU += "SET status=:status,oldStatus=:oldStatus,modificationTime=:updateTime "
            sqlU += "WHERE jediTaskID=:jediTaskID "
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
                taskStatus,oldStatus,lockedBy,cloudName,prodSourceLabel = res
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
                    varMap[':oldStatus'] = None
                    varMap[':updateTime'] = datetime.datetime.utcnow()
                    if newStatus != None:
                        varMap['status'] = newStatus
                    elif cloudName == None and prodSourceLabel in ['managed','test']:
                        # set pending for TaskBrokerage
                        varMap[':oldStatus'] = taskStatus
                        varMap['status'] = 'pending'
                        # set old update time to trigger TaskBrokerage immediately
                        varMap[':updateTime'] = datetime.datetime.utcnow() - datetime.timedelta(hours=6)
                    elif not oldStatus in ['',None]:
                        varMap['status'] = oldStatus
                    else:
                        varMap['status'] = 'ready'
                    tmpLog.debug(sqlU+comment+str(varMap))
                    self.cur.execute(sqlU+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('set to {0}'.format(varMap['status']))
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return failedRet



    # update JEDI task
    def updateTask_JEDI(self,taskSpec,criteria):
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
                sql += '{0}={1}'.format(tmpKey,crKey)
                varMap[crKey] = tmpVal
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
    def getTaskWithID_JEDI(self,jediTaskID,fullFlag,lockTask=False,pid=None):
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
            if lockTask:
                sql += "FOR UPDATE NOWAIT"
            sqlLock  = "UPDATE {0}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlLock += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            # begin transaction
            self.conn.begin()
            # select
            res = None
            try:
                self.cur.execute(sql+comment,varMap)
                res = self.cur.fetchone()
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



    # get JEDI task and tasks with ID and lock it
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
            sql  = "SELECT jediTaskID FROM {0}.JEDI_Tasks WHERE ".format(jedi_config.db.schemaJEDI)
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
            sqlRT  = "SELECT {0} ".format(JediTaskSpec.columnNames())
            sqlRT += "FROM {0}.JEDI_Tasks WHERE status=:status ".format(jedi_config.db.schemaJEDI)
            sqlRT += "AND (lockedBy IS NULL OR lockedTime<:timeLimit) "
            sqlRT += "AND rownum<{0} FOR UPDATE ".format(nTasks)
            sqlLK  = "UPDATE {0}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlLK += "WHERE jediTaskID=:jediTaskID "
            sqlDS  = "SELECT {0} ".format(JediDatasetSpec.columnNames())
            sqlDS += "FROM {0}.JEDI_Datasets WHERE jediTaskID=:jediTaskID ".format(jedi_config.db.schemaJEDI)
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get tasks
            varMap = {}
            varMap[':status'] = 'prepared'
            #varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=10)
            self.cur.execute(sqlRT+comment,varMap)
            resList = self.cur.fetchall()
            retTasks = []
            for resRT in resList:
                taskSpec = JediTaskSpec()
                taskSpec.pack(resRT)
                retTasks.append(taskSpec)
            # get datasets    
            for taskSpec in retTasks:
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



    # generate output files for task
    def getOutputFiles_JEDI(self,jediTaskID):
        comment = ' /* JediDBProxy.getOutputFiles_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            outMap = {}
            # sql to read template
            sqlR  = "SELECT outTempID,datasetID,fileNameTemplate,serialNr,outType,streamName "
            sqlR += "FROM {0}.JEDI_Output_Template ".format(jedi_config.db.schemaJEDI)
            sqlR += "WHERE jediTaskID=:jediTaskID FOR UPDATE"
            # sql to get dataset name and vo for scope
            sqlD  = "SELECT datasetName,vo FROM {0}.JEDI_Datasets WHERE datasetID=:datasetID ".format(jedi_config.db.schemaJEDI)
            # sql to insert files
            sqlI  = "INSERT INTO {0}.JEDI_Dataset_Contents ({1}) ".format(jedi_config.db.schemaJEDI,JediFileSpec.columnNames())
            sqlI += JediFileSpec.bindValuesExpression()
            sqlI += " RETURNING fileID INTO :newFileID"
            # sql to increment SN
            sqlU  = "UPDATE {0}.JEDI_Output_Template SET serialNr=serialNr+1 ".format(jedi_config.db.schemaJEDI)
            sqlU += "WHERE outTempID=:outTempID "
            # current current date
            timeNow = datetime.datetime.utcnow()
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # select
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            self.cur.execute(sqlR+comment,varMap)
            resList = self.cur.fetchall()
            maxSerialNr = None
            for resR in resList:
                # make FileSpec
                outTempID,datasetID,fileNameTemplate,serialNr,outType,streamName = resR
                fileSpec = JediFileSpec()
                fileSpec.jediTaskID       = jediTaskID
                fileSpec.datasetID    = datasetID
                fileSpec.lfn          = fileNameTemplate.replace('${SN}','{SN:06d}').format(SN=serialNr)
                fileSpec.status       = 'defined'
                fileSpec.creationDate = timeNow
                fileSpec.type         = outType
                fileSpec.keepTrack    = 1
                if maxSerialNr == None or maxSerialNr < serialNr:
                    maxSerialNr = serialNr
                # scope
                varMap = {}
                varMap[':datasetID'] = datasetID
                self.cur.execute(sqlD+comment,varMap)
                resD = self.cur.fetchone()
                if resD == None:
                    raise RuntimeError, 'Failed to get datasetName for outTempID={0}'.format(outTempID)
                datasetName,vo = resD
                if vo in jedi_config.ddm.voWithScope.split(','):
                    fileSpec.scope = datasetName.split('.')[0]
                # insert
                varMap = fileSpec.valuesMap(useSeq=True)
                varMap[':newFileID'] = self.cur.var(cx_Oracle.NUMBER)
                self.cur.execute(sqlI+comment,varMap)
                fileSpec.fileID = long(varMap[':newFileID'].getvalue())
                # increment SN
                varMap = {}
                varMap[':outTempID'] = outTempID
                self.cur.execute(sqlU+comment,varMap)
                nRow = self.cur.rowcount
                if nRow != 1:
                    raise RuntimeError, 'Failed to increment SN for outTempID={0}'.format(outTempID)
                # append
                outMap[streamName] = fileSpec
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return outMap,maxSerialNr
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None,None


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
                                   nTasks=50,nFiles=100,isPeeking=False,simTasks=None):
        comment = ' /* JediDBProxy.getTasksToBeProcessed_JEDI */'
        methodName = self.getMethodName(comment)
        if simTasks != None:
            methodName += ' <jediTasks={0}>'.format(str(simTasks))
        elif workQueue == None:
            methodName += ' <vo={0} queue={1} cloud={2}>'.format(vo,None,cloudName)
        else:
            methodName += ' <vo={0} queue={1} cloud={2}>'.format(vo,workQueue.queue_name,cloudName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start label={0} nTasks={1} nFiles={2}'.format(prodSourceLabel,nTasks,nFiles))
        # return value for failure
        failedRet = None
        try:
            # sql to get tasks/datasets
            if simTasks == None:
                varMap = {}
                varMap[':vo']              = vo
                varMap[':type']            = 'input'
                if cloudName != None:
                    varMap[':cloud']       = cloudName
                varMap[':taskstatus1']     = 'ready'
                varMap[':taskstatus2']     = 'running'
                varMap[':taskstatus3']     = 'merging'            
                varMap[':prodSourceLabel'] = prodSourceLabel
                varMap[':dsStatus']        = 'ready'            
                varMap[':dsOKStatus1']     = 'ready'
                varMap[':dsOKStatus2']     = 'done'
                sql  = "SELECT tabT.jediTaskID,datasetID,currentPriority "
                sql += "FROM {0}.JEDI_Tasks tabT,ATLAS_PANDA.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE tabT.vo=:vo AND workqueue_ID IN ("
                for tmpQueue_ID in workQueue.getIDs():
                    tmpKey = ':queueID_{0}'.format(tmpQueue_ID)
                    varMap[tmpKey] = tmpQueue_ID
                    sql += '{0},'.format(tmpKey)
                sql  = sql[:-1]
                sql += ') '
                sql += "AND prodSourceLabel=:prodSourceLabel "
                if cloudName != None:
                    sql += "AND tabT.cloud=:cloud "
                sql += "AND tabT.status IN (:taskstatus1,:taskstatus2,:taskstatus3) "
                sql += "AND tabT.lockedBy IS NULL AND tabT.jediTaskID=tabD.jediTaskID "
                sql += "AND nFilesToBeUsed > nFilesUsed AND type=:type AND tabD.status=:dsStatus "
                sql += 'AND NOT EXISTS '
                sql += '(SELECT 1 FROM {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
                sql += 'WHERE {0}.JEDI_Datasets.jediTaskID=tabT.jediTaskID '.format(jedi_config.db.schemaJEDI)
                sql += 'AND type=:type AND NOT status IN (:dsOKStatus1,:dsOKStatus2)) '
                sql += "ORDER BY currentPriority DESC, jediTaskID "
            else:
                varMap = {}
                sql  = "SELECT tabT.jediTaskID,datasetID,currentPriority "
                sql += "FROM {0}.JEDI_Tasks tabT,{1}.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI,
                                                                                 jedi_config.db.schemaJEDI)
                sql += "WHERE tabT.jediTaskID=tabD.jediTaskID AND tabT.jediTaskID IN ("
                for tmpTaskIdx,tmpTaskID in enumerate(simTasks):
                    tmpKey = ':jediTaskID{0}'.format(tmpTaskIdx)
                    varMap[tmpKey] = tmpTaskID
                    sql += '{0},'.format(tmpKey)
                sql = sql[:-1]
                sql += ') '
                sql += 'AND type=:type '
                varMap[':type'] = 'input'
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
            # no tasks
            if resList == [] and isPeeking:
                return 0
            # make return
            returnList  = []
            taskDatasetMap = {}
            jediTaskIDList = []
            for jediTaskID,datasetID,currentPriority in resList:
                # just return the max priority
                if isPeeking:
                    return currentPriority
                # make task-dataset mapping
                if not taskDatasetMap.has_key(jediTaskID):
                    taskDatasetMap[jediTaskID] = []
                taskDatasetMap[jediTaskID].append(datasetID)
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
            sqlRD += "FROM {0}.JEDI_Datasets WHERE datasetID=:datasetID FOR UPDATE NOWAIT ".format(jedi_config.db.schemaJEDI)
            # sql to read files
            sqlFR  = "SELECT * FROM (SELECT {0} ".format(JediFileSpec.columnNames())
            sqlFR += "FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
            sqlFR += "datasetID=:datasetID and status=:status AND (maxAttempt IS NULL OR attemptNr<maxAttempt) "
            sqlFR += "ORDER BY fileID) "
            sqlFR += "WHERE rownum <= %s" % nFiles
            # sql to update file status
            sqlFU  = "UPDATE {0}.JEDI_Dataset_Contents SET status=:nStatus ".format(jedi_config.db.schemaJEDI)
            sqlFU += "WHERE fileID=:fileID AND status=:oStatus "
            # sql to update file usage info in dataset
            sqlDU  = "UPDATE {0}.JEDI_Datasets SET nFilesUsed=:nFilesUsed WHERE datasetID=:datasetID ".format(jedi_config.db.schemaJEDI)
            # loop over all tasks
            iTasks = 0
            for jediTaskID in jediTaskIDList:
                datasetIDs = taskDatasetMap[jediTaskID]
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
                except:
                    errType,errValue = sys.exc_info()[:2]
                    if self.isNoWaitException(errValue):
                        # resource busy and acquire with NOWAIT specified
                        toSkip = True
                        tmpLog.debug('skip locked jediTaskID={0}'.format(jediTaskID))
                    else:
                        # failed with something else
                        raise errType,errValue
                # read dataset
                if not toSkip:
                    for datasetID in datasetIDs:
                        varMap = {}
                        varMap[':datasetID'] = datasetID
                        try:
                            # select
                            self.cur.execute(sqlRD+comment,varMap)
                            resRD = self.cur.fetchone()
                            datasetSpec = JediDatasetSpec()
                            datasetSpec.pack(resRD)
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
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':lockedBy'] = pid
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
                        # read files
                        for datasetID in datasetIDs:
                            # get DatasetSpec
                            tmpDatasetSpec = inputChunk.getDatasetWithID(datasetID)
                            # read files to make FileSpec
                            varMap = {}
                            varMap[':status']    = 'ready'
                            varMap[':datasetID'] = datasetID
                            self.cur.execute(sqlFR+comment,varMap)
                            resFileList = self.cur.fetchall()
                            iFiles = 0
                            for resFile in resFileList:
                                # make FileSpec
                                tmpFileSpec = JediFileSpec()
                                tmpFileSpec.pack(resFile)
                                # update file status
                                if simTasks == None:
                                    varMap = {}
                                    varMap[':fileID'] = tmpFileSpec.fileID
                                    varMap[':nStatus'] = 'picked'
                                    varMap[':oStatus'] = 'ready'                                
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
                            elif simTasks == None:
                                # update nFilesUsed in DatasetSpec
                                nFilesUsed = tmpDatasetSpec.nFilesUsed + iFiles
                                tmpDatasetSpec.nFilesUsed = nFilesUsed
                                varMap = {}
                                varMap[':datasetID']  = datasetID
                                varMap[':nFilesUsed'] = nFilesUsed
                                self.cur.execute(sqlDU+comment,varMap)
                # add to return
                if not toSkip:
                    returnList.append((taskSpec,cloudName,inputChunk))
                    iTasks += 1
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
            sqlT  = "INSERT INTO {0}.DEFT_TASK (TASK_ID,TASK_META,TASK_PARAM) VALUES ".format(jedi_config.db.schemaDEFT)
            sqlT += "({0}.PRODSYS2_TASK_ID_SEQ.nextval,:metaID,:param) ".format(jedi_config.db.schemaDEFT)
            sqlT += "RETURNING TASK_ID INTO :jediTaskID"
            # sql to insert command
            sqlC  = "INSERT INTO {0}.PRODSYS_COMM (COMM_TASK,COMM_META,COMM_OWNER,COMM_CMD) ".format(jedi_config.db.schemaDEFT)
            sqlC += "VALUES (:jediTaskID,:metaID,:comm_owner,:comm_cmd) "
            # begin transaction
            self.conn.begin()
            # insert task parameters
            varMap = {}
            varMap[':metaID'] = metaTaskID
            varMap[':param']  = taskParams
            varMap[':jediTaskID'] = self.cur.var(cx_Oracle.NUMBER)
            self.cur.execute(sqlT+comment,varMap)
            jediTaskID = long(varMap[':jediTaskID'].getvalue())
            # insert command
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':metaID'] = metaTaskID
            varMap[':comm_cmd']  = 'submit'
            varMap[':comm_owner']  = 'DEFT'
            self.cur.execute(sqlC+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done new jediTaskID={0}'.format(jediTaskID))
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return False



    # rollback files
    def rollbackFiles_JEDI(self,jediTaskID,inputChunk):
        comment = ' /* JediDBProxy.rollbackFiles_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to rollback files
            sql  = "UPDATE {0}.JEDI_Dataset_Contents SET status=:nStatus ".format(jedi_config.db.schemaJEDI)
            sql += "WHERE datasetID=:datasetID AND status=:oStatus "
            # sql to reset nFilesUsed
            sqlD  = "UPDATE {0}.JEDI_Datasets SET nFilesUsed=nFilesUsed-:nFileRow ".format(jedi_config.db.schemaJEDI)
            sqlD += "WHERE datasetID=:datasetID " 
            # begin transaction
            self.conn.begin()
            for datasetSpec in inputChunk.getDatasets():
                varMap = {}
                varMap[':datasetID']  = datasetSpec.datasetID
                varMap[':nStatus'] = 'ready'
                varMap[':oStatus'] = 'picked'
                # update contents
                self.cur.execute(sql+comment,varMap)
                nFileRow = self.cur.rowcount
                tmpLog.debug('reset {0} rows for datasetID={1}'.format(nFileRow,datasetSpec.datasetID))
                if nFileRow > 0:
                    varMap = {}
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



    # rescue picked files
    def rescuePickedFiles_JEDI(self,vo,prodSourceLabel,waitTime):
        comment = ' /* JediDBProxy.rescuePickedFiles_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <vo={0} label={1}>'.format(vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get orphaned tasks
            sqlTR  = "SELECT jediTaskID FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTR += "WHERE status IN (:status1,:status2,:status3) AND lockedBy IS NOT NULL AND lockedTime<:timeLimit "
            if vo != None:
                sqlTR += "AND vo=:vo "
            if prodSourceLabel != None:
                sqlTR += "AND prodSourceLabel=:prodSourceLabel " 
            # sql to get picked datasets
            sqlDP  = "SELECT datasetID FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
            sqlDP += "WHERE jediTaskID=:jediTaskID AND type=:type " 
            # sql to rollback files
            sqlF  = "UPDATE {0}.JEDI_Dataset_Contents SET status=:nStatus ".format(jedi_config.db.schemaJEDI)
            sqlF += "WHERE datasetID=:datasetID AND status=:oStatus "
            # sql to reset nFilesUsed
            sqlDU  = "UPDATE {0}.JEDI_Datasets SET nFilesUsed=nFilesUsed-:nFileRow ".format(jedi_config.db.schemaJEDI)
            sqlDU += "WHERE datasetID=:datasetID " 
            # sql to unlock tasks
            sqlTU  = "UPDATE {0}.JEDI_Tasks SET lockedBy=NULL,lockedTime=NULL ".format(jedi_config.db.schemaJEDI)
            sqlTU += "WHERE jediTaskID=:jediTaskID"
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get orphaned tasks
            varMap = {}
            varMap[':status1'] = 'ready'
            varMap[':status2'] = 'running'
            varMap[':status3'] = 'merging'
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
                    varMap[':datasetID']  = datasetID
                    varMap[':nStatus'] = 'ready'
                    varMap[':oStatus'] = 'picked'
                    self.cur.execute(sqlF+comment,varMap)
                    nFileRow = self.cur.rowcount
                    tmpLog.debug('[takID={0}] reset {1} rows for datasetID={2}'.format(jediTaskID,nFileRow,datasetID))
                    if nFileRow > 0:
                        # reset nFilesUsed
                        varMap = {}
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
            sql  = "SELECT SUM(inputFileBytes)/1024/1024/1024 FROM {0}.jobsDefined4 ".format(jedi_config.db.schemaJEDI)
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
            sqlC  = "SELECT comm_task,comm_meta FROM {0}.PRODSYS_COMM ".format(jedi_config.db.schemaDEFT)
            sqlC += "WHERE comm_owner=:comm_owner AND comm_cmd=:comm_cmd "
            varMap = {}
            varMap[':comm_owner']    = 'DEFT'
            #varMap[':comm_receiver'] = 'JEDI'        
            varMap[':comm_cmd']      = 'submit'
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
            for jediTaskID,metaTaskID in resList:
                tmpLog.debug('start jediTaskID={0} metaTaskID={1}'.format(jediTaskID,metaTaskID))                
               # start transaction
                self.conn.begin()
                # lock
                varMap = {}
                varMap[':comm_task'] = jediTaskID
                sqlLock  = "SELECT * FROM {0}.PRODSYS_COMM WHERE comm_task=:comm_task ".format(jedi_config.db.schemaDEFT)
                if metaTaskID != None:
                    varMap[':comm_meta'] = metaTaskID
                    sqlLock += "AND comm_meta=:comm_meta "
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
                        tmpLog.debug('skip locked jediTaskID={0} metaTaskID={1}'.format(jediTaskID,metaTaskID))
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
                    varMap = {}
                    varMap[':comm_task'] = jediTaskID
                    varMap[':comm_cmd']  = 'submitted'
                    sqlUC = "UPDATE {0}.PRODSYS_COMM SET comm_cmd=:comm_cmd WHERE comm_task=:comm_task ".format(jedi_config.db.schemaDEFT)
                    if metaTaskID != None:
                        varMap[':comm_meta'] = metaTaskID
                        sqlUC = "AND comm_meta=:comm_meta "
                    tmpLog.debug(sqlUC+comment+str(varMap))
                    self.cur.execute(sqlUC+comment,varMap)
                    # append
                    if isOK:
                        retTaskIDs.append(jediTaskID)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # find orphaned tasks to rescue
            self.conn.begin()
            varMap = {}
            varMap[':status'] = 'registered'
            # FIXME
            #varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(seconds=10)
            sqlOrpS  = "SELECT jediTaskID FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlOrpS += "WHERE status=:status AND modificationtime<:timeLimit "
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
            for jediTaskID, in resList:
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                tmpLog.debug(sqlOrpU+comment+str(varMap))
                self.cur.execute(sqlOrpU+comment,varMap)
                nRow = self.cur.rowcount
                if nRow == 1 and not jediTaskID in retTaskIDs:
                    retTaskIDs.append(jediTaskID)
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
    def registerTaskInOneShot_JEDI(self,jediTaskID,taskSpec,inMasterDatasetSpec,
                                   inSecDatasetSpecList,outDatasetSpecList,
                                   outputTemplateMap,jobParamsTemplate):
        comment = ' /* JediDBProxy.registerTaskInOneShot_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            timeNow = datetime.datetime.utcnow()
            # set attributes
            taskSpec.status = 'defined'
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
                # sql
                sql  = "INSERT INTO {0}.JEDI_Datasets ({1}) ".format(jedi_config.db.schemaJEDI,
                                                                     JediDatasetSpec.columnNames())
                sql += JediDatasetSpec.bindValuesExpression()
                sql += " RETURNING datasetID INTO :newDatasetID"
                # insert master dataset
                masterID = -1
                datasetIdMap = {}
                datasetSpec = inMasterDatasetSpec
                if datasetSpec != None:
                    datasetSpec.creationTime = timeNow
                    datasetSpec.modificationTime = timeNow
                    varMap = datasetSpec.valuesMap(useSeq=True)
                    varMap[':newDatasetID'] = self.cur.var(cx_Oracle.NUMBER)            
                    # insert dataset
                    self.cur.execute(sql+comment,varMap)
                    datasetID = long(varMap[':newDatasetID'].getvalue())
                    masterID = datasetID
                    datasetIdMap[datasetSpec.datasetName] = datasetID
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
                    datasetIdMap[datasetSpec.datasetName] = datasetID
                # insert output datasets
                for datasetSpec in outDatasetSpecList:
                    datasetSpec.creationTime = timeNow
                    datasetSpec.modificationTime = timeNow
                    varMap = datasetSpec.valuesMap(useSeq=True)
                    varMap[':newDatasetID'] = self.cur.var(cx_Oracle.NUMBER)            
                    # insert dataset
                    self.cur.execute(sql+comment,varMap)
                    datasetID = long(varMap[':newDatasetID'].getvalue())
                    datasetIdMap[datasetSpec.datasetName] = datasetID
                # insert outputTemplates
                tmpLog.debug('inserting outTmpl')
                for datasetName,outputTemplateList in outputTemplateMap.iteritems():
                    if not datasetIdMap.has_key(datasetName):
                        raise RuntimeError,'datasetID is not defined for {0}'.format(datasetName)
                    for outputTemplate in outputTemplateList:
                        sqlH = "INSERT INTO {0}.JEDI_Output_Template (outTempID,datasetID,".format(jedi_config.db.schemaJEDI)
                        sqlL = "VALUES({0}.JEDI_OUTPUT_TEMPLATE_ID_SEQ.nextval,:datasetID,".format(jedi_config.db.schemaJEDI) 
                        varMap = {}
                        varMap[':datasetID'] = datasetIdMap[datasetName]
                        for tmpAttr,tmpVal in outputTemplate.iteritems():
                            tmpKey = ':'+tmpAttr
                            sqlH += '{0},'.format(tmpAttr)
                            sqlL += '{0},'.format(tmpKey)
                            varMap[tmpKey] = tmpVal
                        sqlH = sqlH[:-1] + ') '
                        sqlL = sqlL[:-1] + ') '
                        sql = sqlH + sqlL
                        self.cur.execute(sql+comment,varMap)
                # insert job parameters
                tmpLog.debug('inserting jobParamsTmpl')
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':templ']  = jobParamsTemplate
                sql = "INSERT INTO {0}.JEDI_JobParams_Template (jediTaskID,jobParamsTemplate) VALUES (:jediTaskID,:templ) ".format(jedi_config.db.schemaJEDI)
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
        sqlSCF  = "SELECT fileID FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
        sqlSCF += "jediTaskID=:jediTaskID AND status=:status AND datasetID IN "
        sqlSCF += "(SELECT datasetID FROM {0}.JEDI_Datasets ".format(jedi_config.db.schemaJEDI)
        sqlSCF += "WHERE jediTaskID=:jediTaskID AND type=:type) " 
        sqlSCP  = "SELECT PandaID FROM {0}.filesTable4 WHERE fileID=:fileID ".format(jedi_config.db.schemaPANDA)
        sqlSCD  = "SELECT jobStatus,outputFileBytes,jobMetrics,cpuConsumptionTime "
        sqlSCD += "FROM {0}.jobsArchived4 ".format(jedi_config.db.schemaPANDA)
        sqlSCD += "WHERE PandaID=:pandaID "
        sqlSCD += "UNION "
        sqlSCD += "SELECT jobStatus,outputFileBytes,jobMetrics,cpuConsumptionTime "
        sqlSCD += "FROM {0}.jobsArchived ".format(jedi_config.db.schemaPANDAARCH)
        sqlSCD += "WHERE PandaID=:pandaID AND modificationTime>(CURRENT_DATE-14) "
        sqlSCC  = "SELECT count(*) FROM {0}.JEDI_Dataset_Contents WHERE ".format(jedi_config.db.schemaJEDI)
        sqlSCC += "jediTaskID=:jediTaskID AND status=:status AND type=:type "
        if useTransaction:
            # begin transaction
            self.conn.begin()
        # get files    
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        varMap[':status'] = 'finished'
        varMap[':type']   = 'log'
        self.cur.execute(sqlSCF+comment,varMap)
        resList = self.cur.fetchall()
        for fileID, in resList:
            # get PandaID
            varMap = {}
            varMap[':fileID'] = fileID
            self.cur.execute(sqlSCP+comment,varMap)
            resPandaIDs = self.cur.fetchall()
            outSizeList  = []
            walltimeList = []
            memSizeList  = []
            workSizeList = []
            # loop over all PandaIDs
            for pandaID, in resPandaIDs:
                # get job data
                varMap = {}
                varMap[':pandaID'] = pandaID
                self.cur.execute(sqlSCD+comment,varMap)
                resDataList = self.cur.fetchall()
                for jobStatus,outputFileBytes,jobMetrics,cpuConsumptionTime in resDataList:
                    if jobStatus != 'finished':
                        continue
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
            # get the number of file records for normalization
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':status'] = 'finished'
            varMap[':type']   = 'input'
            self.cur.execute(sqlSCC+comment,varMap)
            normFactor, = self.cur.fetchone()
            normFactor = float(normFactor)
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
        tmpLog.debug('data->{0}'.format(str(returnMap)))
        return returnMap



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
                varMap[':type']         = 'input'
                varMap[':taskstatus1']  = 'running'
                varMap[':taskstatus2']  = 'scouting'
                varMap[':taskstatus3']  = 'merging'
                varMap[':dsEndStatus1'] = 'broken'
                varMap[':dsEndStatus2'] = 'done'
                if vo != None:
                    varMap[':vo'] = vo
                if prodSourceLabel != None:
                    varMap[':prodSourceLabel'] = prodSourceLabel
                sql  = "SELECT tabT.jediTaskID,tabT.status "
                sql += "FROM {0}.JEDI_Tasks tabT ".format(jedi_config.db.schemaJEDI)
                sql += "WHERE tabT.status IN (:taskstatus1,:taskstatus2,:taskstatus3) "
                if vo != None:
                    sql += "AND tabT.vo=:vo "
                if prodSourceLabel != None:
                    sql += "AND prodSourceLabel=:prodSourceLabel "
                sql += "AND tabT.lockedBy IS NULL AND NOT EXISTS "
                sql += '(SELECT 1 FROM {0}.JEDI_Datasets tabD '.format(jedi_config.db.schemaJEDI)
                sql += 'WHERE tabD.jediTaskID=tabT.jediTaskID AND masterID IS NULL '
                sql += 'AND type=:type AND NOT status IN (:dsEndStatus1,:dsEndStatus2) '
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
            sqlRD += "FROM {0}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND type=:type AND status=:status ".format(jedi_config.db.schemaJEDI)
            # sql to update input dataset status
            sqlDIU  = "UPDATE {0}.JEDI_Datasets SET status=:status,modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlDIU += "WHERE datasetID=:datasetID "
            # sql to update output/log dataset status
            sqlDOU  = "UPDATE {0}.JEDI_Datasets SET status=:status,modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlDOU += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
            # sql to update nFiles of dataset
            sqlFU  = "UPDATE {0}.JEDI_Datasets SET nFilesToBeUsed=nFiles,modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlFU += "WHERE type=:type AND jediTaskID=:jediTaskID AND masterID IS NULL "
            # sql to update task status
            sqlTU  = "UPDATE {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
            sqlTU += "SET status=:status,modificationTime=CURRENT_DATE,lockedBy=NULL,lockedTime=CURRENT_DATE "
            sqlTU += "WHERE jediTaskID=:jediTaskID "
            # loop over all tasks
            iTasks = 0
            for jediTaskID in jediTaskIDList:
                taskStatus = jediTaskIDstatusMap[jediTaskID]
                tmpLog.debug('start jediTaskID={0} status={1}'.format(jediTaskID,taskStatus))
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
                        scoutData = self.getScoutJobData_JEDI(jediTaskID)
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
                        varMap[':type']   = 'input'
                        self.cur.execute(sqlFU+comment,varMap)
                        # new task status
                        newTaskStatus = 'running'
                    else:
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
                        varMap[':type']   = 'input'
                        varMap[':status'] = 'ready'
                        self.cur.execute(sqlRD+comment,varMap)
                        resRD = self.cur.fetchall()
                        for datasetID,dsStatus,nFiles,nFilesFinished,masterID in resRD:
                            # update input datasets
                            varMap = {}
                            varMap[':datasetID'] = datasetID
                            if masterID != None:
                                # seconday dataset
                                varMap[':status'] = 'done'
                            else:
                                # master dataset
                                if nFiles == nFilesFinished:
                                    # all succeeded
                                    varMap[':status'] = 'done'
                                elif nFilesFinished == 0:
                                    # all failed
                                    varMap[':status'] = 'failed'
                                else:
                                    # partially succeeded
                                    varMap[':status'] = 'partial'
                            self.cur.execute(sqlDIU+comment,varMap)
                        # new task status
                        newTaskStatus = 'prepared'    
                    # update tasks
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':status'] = newTaskStatus
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
            varMap[':vo'] = vo
            varMap[':status']    = 'pending'
            varMap[':oldStatus'] = 'defined'        
            # FIXME
            #varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
            varMap[':prodSourceLabel'] = prodSourceLabel
            sqlSCF  = "SELECT jediTaskID FROM {0}.JEDI_Tasks WHERE ".format(jedi_config.db.schemaJEDI)
            sqlSCF += "status=:status AND oldStatus=:oldStatus AND modificationTime<:timeLimit "
            sqlSCF += "AND vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud IS NULL "
            sqlSCF += "AND workQueue_ID IN (" 
            for tmpQueue_ID in workQueue.getIDs():
                tmpKey = ':queueID_{0}'.format(tmpQueue_ID)
                varMap[tmpKey] = tmpQueue_ID
                sqlSCF += '{0},'.format(tmpKey)
            sqlSCF  = sqlSCF[:-1]    
            sqlSCF += ") "
            sqlSCF += "ORDER BY currentPriority DESC FOR UPDATE"
            sqlSPC  = "UPDATE {0}.JEDI_Tasks SET modificationTime=CURRENT_DATE ".format(jedi_config.db.schemaJEDI)
            sqlSPC += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # get tasks
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
            varMap[':vo'] = vo
            varMap[':status']    = 'pending'
            varMap[':oldStatus'] = 'defined'        
            varMap[':prodSourceLabel'] = prodSourceLabel
            sqlSCF  = "SELECT jediTaskID FROM {0}.JEDI_Tasks WHERE ".format(jedi_config.db.schemaJEDI)
            sqlSCF += "status=:status AND oldStatus=:oldStatus "
            sqlSCF += "AND vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud IS NULL "
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
            sql  = "SELECT ROUND(SUM((nFiles-nFilesFinished-nFilesFailed)*walltime)/24/3600) "
            sql += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI,
                                                                             jedi_config.db.schemaJEDI)
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
            sql += "FROM {0}.JEDI_Tasks tabT,{1}.JEDI_Datasets tabD ".format(jedi_config.db.schemaJEDI,
                                                                             jedi_config.db.schemaJEDI)
            sql += "WHERE tabT.jediTaskID=tabD.jediTaskID AND masterID IS NULL "
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
        commandStatusMap = JediTaskSpec.terminateCommandStatusMap()
        try:
            # sql to get jediTaskIDs to exec a command from the command table
            varMap = {}
            varMap[':comm_owner'] = 'DEFT'
            # FIXME
            #sqlC  = "SELECT comm_task,comm_meta,comm_cmd,comm_comment FROM {0}.PRODSYS_COMM ".format(jedi_config.db.schemaDEFT)
            sqlC  = "SELECT comm_task,comm_meta,comm_cmd FROM {0}.PRODSYS_COMM ".format(jedi_config.db.schemaDEFT)
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
            # FIXME
            #for jediTaskID,metaTaskID,commandStr,comComment in resList:
            for jediTaskID,metaTaskID,commandStr in resList:
                comComment = '{0}ed by JEDI'.format(commandStr)
                tmpLog.debug('start jediTaskID={0} metaTaskID={1} command={2}'.format(jediTaskID,metaTaskID,commandStr))                
               # start transaction
                self.conn.begin()
                # lock
                varMap = {}
                varMap[':comm_task'] = jediTaskID
                sqlLock  = "SELECT * FROM {0}.PRODSYS_COMM WHERE comm_task=:comm_task ".format(jedi_config.db.schemaDEFT)
                if metaTaskID != None:
                    varMap[':comm_meta'] = metaTaskID
                    sqlLock += "AND comm_meta=:comm_meta "
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
                        tmpLog.debug('skip locked+nowauit jediTaskID={0} metaTaskID={1}'.format(jediTaskID,metaTaskID))
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
                            if taskStatus in JediTaskSpec.statusToRejectExtChange():
                                # task is in a status which rejects external changes
                                tmpLog.error("jediTaskID={0} rejected command={1} (status={2})".format(jediTaskID,commandStr,taskStatus))
                                isOK = False
                            else:
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
                        sqlTU += "SET status=:status,modificationTime=CURRENT_DATE,errorDialog=:errDiag "
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
                    if metaTaskID != None:
                        varMap[':comm_meta'] = metaTaskID
                        sqlUC = "AND comm_meta=:comm_meta "
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
                sqlOrpS  = "SELECT jediTaskID,errorDialog FROM {0}.JEDI_Tasks ".format(jedi_config.db.schemaJEDI)
                sqlOrpS += "WHERE status=:status AND modificationtime<:timeLimit "
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
