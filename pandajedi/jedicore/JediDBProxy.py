import re
import sys
import copy
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
        logger.debug('%s start' % methodName)
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
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
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
            sql += 'FROM ATLAS_PANDA.JEDI_Tasks tabT,ATLAS_PANDA.JEDI_Datasets tabD '
            sql += 'WHERE tabT.taskID=tabD.taskID '
            sql += 'AND type=:type AND tabT.status=:taskStatus '
            sql += 'AND tabD.status IN ('
            for tmpStat in JediDatasetSpec.statusToUpdateContents():
                mapKey = ':dsStatus_'+tmpStat
                sql += '{0},'.format(mapKey)
                varMap[mapKey] = tmpStat
            sql  = sql[:-1]    
            sql += ') AND tabT.lockedBy IS NULL AND tabD.lockedBy IS NULL '
            sql += 'AND NOT EXISTS '
            sql += '(SELECT 1 FROM ATLAS_PANDA.JEDI_Datasets '
            sql += 'WHERE ATLAS_PANDA.JEDI_Datasets.taskID=tabT.taskID '
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
                datasetSepc = JediDatasetSpec()
                datasetSepc.pack(res)
                if not returnMap.has_key(datasetSepc.taskID):
                    returnMap[datasetSepc.taskID] = []
                returnMap[datasetSepc.taskID].append(datasetSepc)
                nDS += 1
            taskIDs = returnMap.keys()
            taskIDs.sort()
            returnList  = []
            for taskID in taskIDs:
                returnList.append((taskID,returnMap[taskID]))
            tmpLog.debug('got {0} datasets for {1} tasks'.format(nDS,len(taskIDs)))
            return returnList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog)
            return None


                                                
    # feed files to the JEDI contents table
    def insertFilesForDataset_JEDI(self,datasetSpec,fileMap,datasetState,stateUpdateTime,
                                   nEventsPerFile,nEventsPerJob):
        comment = ' /* JediDBProxy.insertFilesForDataset_JEDI */'
        methodName = self.getMethodName(comment)
        methodName = '{0} taskID={1} datasetID={2}'.format(methodName,datasetSpec.taskID,
                                                           datasetSpec.datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start nEventsPerFile={0} nEventsPerJob={1}'.format(nEventsPerFile,nEventsPerJob))
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
                fileSpec.taskID       = datasetSpec.taskID
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
            sqlTL = "SELECT status,lockedBy FROM ATLAS_PANDA.JEDI_Tasks WHERE taskID=:taskID FOR UPDATE "
            # sql to check dataset status
            sqlDs = "SELECT status FROM ATLAS_PANDA.JEDI_Datasets WHERE datasetID=:datasetID FOR UPDATE "
            # sql to get existing files
            sqlCh  = "SELECT fileID,lfn,status,startEvent,endEvent FROM ATLAS_PANDA.JEDI_Dataset_Contents "
            sqlCh += "WHERE datasetID=:datasetID FOR UPDATE"
            # sql for insert
            sqlIn  = "INSERT INTO ATLAS_PANDA.JEDI_Dataset_Contents (%s) " % JediFileSpec.columnNames()
            sqlIn += JediFileSpec.bindValuesExpression()
            # sql to update file status
            sqlFU = "UPDATE ATLAS_PANDA.JEDI_Dataset_Contents SET status=:status WHERE fileID=:fileID "
            # sql to update dataset
            sqlDU  = "UPDATE ATLAS_PANDA.JEDI_Datasets "
            sqlDU += "SET status=:status,state=:state,stateCheckTime=:stateUpdateTime,"
            sqlDU += "nFiles=:nFiles,nFilesTobeUsed=:nFilesTobeUsed "
            sqlDU += "WHERE datasetID=:datasetID "
            nInsert  = 0
            retVal = None
            # begin transaction
            self.conn.begin()
            # check task
            varMap = {}
            varMap[':taskID'] = datasetSpec.taskID
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
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error("{0} {1}".format(errtype,errvalue))
            return False



    # get files from the JEDI contents table with taskID and/or datasetID
    def getFilesInDatasetWithID_JEDI(self,taskID,datasetID,nFiles,status):
        comment = ' /* JediDBProxy.getFilesInDataset_JEDI */'
        methodName = self.getMethodName(comment)
        methodName = '%s taskID=%s datasetID=%s nFiles=%s' % (methodName,taskID,datasetID,nFiles)
        logger.debug('%s start' % methodName)
        # return value for failure
        failedRet = False,0
        if taskID==None and datasetID==None:
            logger.error("%s : either taskID or datasetID is not defined" % methodName)
            return failedRet
        try:
            # sql 
            varMap = {}
            sql  = "SELECT * FROM (SELECT %s " % JediFileSpec.columnNames()
            sql += "FROM ATLAS_PANDA.JEDI_Dataset_Contents WHERE "
            useAND = False
            if taskID != None:    
                sql += "taskID=:taskID "
                varMap[':taskID'] = taskID
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
            logger.debug('%s got %s files' % (methodName,len(fileSpecList)))
            return True,fileSpecList
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            return failedRet



    # insert dataset to the JEDI datasets table
    def insertDataset_JEDI(self,datasetSpec):
        comment = ' /* JediDBProxy.insertDataset_JEDI */'
        methodName = self.getMethodName(comment)
        logger.debug('%s start' % methodName)
        try:
            # set attributes
            timeNow = datetime.datetime.utcnow()
            datasetSpec.creationTime = timeNow
            datasetSpec.modificationTime = timeNow
            # sql
            sql  = "INSERT INTO ATLAS_PANDA.JEDI_Datasets (%s) " % JediDatasetSpec.columnNames()
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
            logger.debug('%s done' % methodName)
            return True,long(varMap[':newDatasetID'].getvalue())
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            return False,None



    # update JEDI dataset
    def updateDataset_JEDI(self,datasetSpec,criteria,lockTask):
        comment = ' /* JediDBProxy.updateDataset_JEDI */'
        methodName = self.getMethodName(comment)
        for tmpKey,tmpVal in criteria.iteritems():
            methodName += ' %s=%s' % (tmpKey,tmpVal)
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
            sql  = "UPDATE ATLAS_PANDA.JEDI_Datasets SET %s WHERE " % datasetSpec.bindUpdateChangesExpression()
            for tmpKey,tmpVal in criteria.iteritems():
                crKey = ':cr_%s' % tmpKey
                sql += '%s=%s' % (tmpKey,crKey)
                varMap[crKey] = tmpVal
            # sql for loc
            varMapLock = {}
            varMapLock[':taskID'] = datasetSpec.taskID
            sqlLock = "SELECT 1 FROM ATLAS_PANDA.JEDI_Tasks WHERE taskID=:taskID FOR UPDATE"
            # begin transaction
            self.conn.begin()
            # lock task
            if lockTask:
                tmpLog.debug(sqlLock+comment+str(varMapLock))
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
        methodName += ' datasetID=%s ' % datasetID
        logger.debug('%s start ' % methodName)
        # return value for failure
        failedRet = False,None
        try:
            # sql
            sql  = "SELECT %s " % JediDatasetSpec.columnNames()
            sql += "FROM ATLAS_PANDA.JEDI_Datasets WHERE datasetID=:datasetID "
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
                datasetSepc = JediDatasetSpec()
                datasetSepc.pack(res)
            else:
                datasetSepc = None
            logger.debug('%s done' % methodName)
            return True,datasetSepc
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            return failedRet


        
    # insert task to the JEDI task table
    def insertTask_JEDI(self,taskSpec):
        comment = ' /* JediDBProxy.insertTask_JEDI */'
        methodName = self.getMethodName(comment)
        logger.debug('%s start' % methodName)
        try:
            # set attributes
            timeNow = datetime.datetime.utcnow()
            taskSpec.creationDate = timeNow
            taskSpec.modificationTime = timeNow
            # sql
            sql  = "INSERT INTO ATLAS_PANDA.JEDI_Tasks (%s) " % JediTaskSpec.columnNames()
            sql += JediTaskSpec.bindValuesExpression()
            varMap = taskSpec.valuesMap()
            # begin transaction
            self.conn.begin()
            # insert dataset
            self.cur.execute(sql+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            logger.debug('%s done' % methodName)
            return True
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            return False



    # update JEDI task status by ContentsFeeder
    def updateTaskStatusByContFeeder_JEDI(self,taskID,newStatus=None):
        comment = ' /* JediDBProxy.updateTaskStatusByContFeeder_JEDI */'
        methodName = self.getMethodName(comment)
        methodName ='{0} taskID={1}'.format(methodName,taskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start newStat={0}'.format(newStatus))
        try:
            # sql to check status
            sqlS = "SELECT status,lockedBy FROM ATLAS_PANDA.JEDI_Tasks WHERE taskID=:taskID "
            # sql to update task
            sqlU  = "UPDATE ATLAS_PANDA.JEDI_Tasks "
            sqlU += "SET status=:status,modificationTime=CURRENT_DATE "
            sqlU += "WHERE taskID=:taskID "
            # begin transaction
            self.conn.begin()
            # check status
            varMap = {}
            varMap[':taskID'] = taskID
            tmpLog.debug(sqlS+comment+str(varMap))
            self.cur.execute(sqlS+comment,varMap)            
            res = self.cur.fetchone()
            if res == None:
                tmpLog.debug('task is not found in Tasks table')
            else:
                taskStatus,lockedBy = res
                if lockedBy != None:
                    # task is locked
                    tmpLog('task is locked by {0}'.format(lockedBy))
                elif not taskStatus in JediTaskSpec.statusToUpdateContents():
                    # task status is irrelevant
                    tmpLog.debug('task.status={0} is not for contents update'.format(taskStatus))
                else:
                    # update task
                    varMap = {}
                    varMap[':taskID'] = taskID
                    if newStatus != None:
                        varMap['status'] = newStatus
                    elif taskStatus == 'holding':
                        varMap['status'] = 'running'
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
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error("{0} {1}".format(errtype,errvalue))
            return failedRet



    # update JEDI task
    def updateTask_JEDI(self,taskSpec,criteria):
        comment = ' /* JediDBProxy.updateTask_JEDI */'
        methodName = self.getMethodName(comment)
        logger.debug('%s start' % methodName)
        # return value for failure
        failedRet = False,0
        # no criteria
        if criteria == {}:
            logger.error('%s no selection criteria' % methodName)            
            return failedRet
        # check criteria
        for tmpKey in criteria.keys():
            if not hasattr(taskSpec,tmpKey):
                logger.error('%s unknown attribute %s is used in criteria' % (methodName,tmpKey))
                return failedRet
        try:
            # set attributes
            timeNow = datetime.datetime.utcnow()
            taskSpec.modificationTime = timeNow
            # values for UPDATE
            varMap = taskSpec.valuesMap(useSeq=False,onlyChanged=True)
            # sql
            sql  = "UPDATE ATLAS_PANDA.JEDI_Tasks SET %s WHERE " % taskSpec.bindUpdateChangesExpression()
            for tmpKey,tmpVal in criteria.iteritems():
                crKey = ':cr_%s' % tmpKey
                sql += '%s=%s' % (tmpKey,crKey)
                varMap[crKey] = tmpVal
            # begin transaction
            self.conn.begin()
            # update task
            logger.debug(sql+comment+str(varMap))
            self.cur.execute(sql+comment,varMap)
            # the number of updated rows
            nRows = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            logger.debug('%s updated %s rows' % (methodName,nRows))
            return True,nRows
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            return failedRet



    # get JEDI task with ID
    def getTaskWithID_JEDI(self,taskID,fullFlag):
        comment = ' /* JediDBProxy.getTaskWithID_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' taskID=%s ' % taskID
        logger.debug('%s start ' % methodName)
        # return value for failure
        failedRet = False,None
        try:
            # sql
            sql  = "SELECT %s " % JediTaskSpec.columnNames()
            sql += "FROM ATLAS_PANDA.JEDI_Tasks WHERE taskID=:taskID "
            varMap = {}
            varMap[':taskID'] = taskID
            # begin transaction
            self.conn.begin()
            # select
            self.cur.execute(sql+comment,varMap)
            res = self.cur.fetchone()
            # template to generate job parameters
            jobParamsTemplate = None
            if fullFlag:
                # sql to read template
                sqlJobP  = "SELECT jobParamsTemplate FROM ATLAS_PANDA.JEDI_JobParams_Template "
                sqlJobP += "WHERE taskID=:taskID "
                
                self.cur.execute(sqlJobP+comment,varMap)
                for clobJobP, in self.cur:
                    if clobJobP != None:
                        jobParamsTemplate = clobJobP.read()
                        break
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            if res != None:
                taskSepc = JediTaskSpec()
                taskSepc.pack(res)
                if jobParamsTemplate != None:
                    taskSepc.jobParamsTemplate = jobParamsTemplate
            else:
                taskSepc = None
            logger.debug('%s done' % methodName)
            return True,taskSepc
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            return failedRet



    # get job statistics with work queue
    def getJobStatisticsWithWorkQueue_JEDI(self,vo,prodSourceLabel,minPriority=None):
        comment = ' /* DBProxy.getJobStatisticsWithWorkQueue_JEDI */'
        methodName = self.getMethodName(comment)
        methodName = '%s vo=%s label=%s' % (methodName,vo,prodSourceLabel)
        logger.debug('%s start minPriority=%s' % (methodName,minPriority))
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
        tables = ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4']
        if minPriority != None:
            # read the number of running jobs with prio<=MIN
            tables.append('ATLAS_PANDA.jobsActive4')
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
                if table == 'ATLAS_PANDA.jobsActive4':
                    mvTableName = 'ATLAS_PANDA.MV_JOBSACTIVE4_STATS'
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
            logger.debug('%s end' % methodName)
            return True,returnMap
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            return False,{}



    # generate output files for task
    def getOutputFiles_JEDI(self,taskID):
        comment = ' /* JediDBProxy.getOutputFiles_JEDI */'
        methodName = self.getMethodName(comment)
        methodName = '{0} taskID={1}'.format(methodName,taskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            outMap = {}
            # sql to read template
            sqlR  = "SELECT outTempID,datasetID,fileNameTemplate,serialNr,outType,streamName FROM ATLAS_PANDA.JEDI_Output_Template "
            sqlR += "WHERE taskID=:taskID FOR UPDATE"
            # sql to get dataset name and vo for scope
            sqlD  = "SELECT datasetName,vo FROM ATLAS_PANDA.JEDI_Datasets WHERE datasetID=:datasetID "
            # sql to insert files
            sqlI  = "INSERT INTO ATLAS_PANDA.JEDI_Dataset_Contents (%s) " % JediFileSpec.columnNames()
            sqlI += JediFileSpec.bindValuesExpression()
            sqlI += " RETURNING fileID INTO :newFileID"
            # sql to increment SN
            sqlU  = "UPDATE ATLAS_PANDA.JEDI_Output_Template SET serialNr=serialNr+1 "
            sqlU += "WHERE outTempID=:outTempID "
            # current current date
            timeNow = datetime.datetime.utcnow()
            # begin transaction
            self.conn.begin()
            # select
            varMap = {}
            varMap[':taskID'] = taskID
            self.cur.execute(sqlR+comment,varMap)
            resList = self.cur.fetchall()
            maxSerialNr = None
            for resR in resList:
                # make FileSpec
                outTempID,datasetID,fileNameTemplate,serialNr,outType,streamName = resR
                fileSpec = JediFileSpec()
                fileSpec.taskID       = taskID
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
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error("{0} {1}".format(errtype,errvalue))
            return None,None


    # insert output file templates
    def insertOutputTemplate_JEDI(self,templates):
        comment = ' /* JediDBProxy.insertOutputTemplate_JEDI */'
        methodName = self.getMethodName(comment)
        logger.debug('%s start' % methodName)
        try:
            # begin transaction
            self.conn.begin()
            # loop over all templates
            for template in templates:
                # make sql
                varMap = {}
                sqlH = "INSERT INTO ATLAS_PANDA.JEDI_Output_Template (outTempID,"
                sqlL = "VALUES(ATLAS_PANDA.JEDI_OUTPUT_TEMPLATE_ID_SEQ.nextval," 
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
                logger.debug('%s done' % methodName)
                return True
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            return False



    # get tasks to be processed
    def getTasksToBeProcessed_JEDI(self,pid,vo,workQueue,prodSourceLabel,cloudName,
                                   nTasks=50,nFiles=100,isPeeking=False,simTasks=None):
        comment = ' /* JediDBProxy.getTasksToBeProcessed_JEDI */'
        methodName = self.getMethodName(comment)
        methodName = '{0} vo={1} queue={2} cloud={3}'.format(methodName,vo,workQueue.queue_name,cloudName)
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
                varMap[':cloud']           = cloudName
                varMap[':taskstatus1']     = 'ready'
                varMap[':taskstatus2']     = 'running'
                varMap[':taskstatus3']     = 'merging'            
                varMap[':prodSourceLabel'] = prodSourceLabel
                varMap[':dsStatus']        = 'ready'            
                varMap[':dsOKStatus1']     = 'ready'
                varMap[':dsOKStatus2']     = 'done'
                sql  = "SELECT tabT.taskID,datasetID,currentPriority "
                sql += "FROM ATLAS_PANDA.JEDI_Tasks tabT,ATLAS_PANDA.JEDI_Datasets tabD "
                sql += "WHERE tabT.vo=:vo AND workqueue_ID IN ("
                for tmpQueue_ID in workQueue.getIDs():
                    tmpKey = ':queueID_{0}'.format(tmpQueue_ID)
                    varMap[tmpKey] = tmpQueue_ID
                    sql += '{0},'.format(tmpKey)
                sql  = sql[:-1]
                sql += ') '
                sql += "AND prodSourceLabel=:prodSourceLabel AND tabD.cloud=:cloud "
                sql += "AND tabT.status IN (:taskstatus1,:taskstatus2,:taskstatus3) "
                sql += "AND tabT.lockedBy IS NULL AND tabT.taskID=tabD.taskID "
                sql += "AND nFilesToBeUsed > nFilesUsed AND type=:type AND tabD.status=:dsStatus "
                sql += 'AND NOT EXISTS '
                sql += '(SELECT 1 FROM ATLAS_PANDA.JEDI_Datasets '
                sql += 'WHERE ATLAS_PANDA.JEDI_Datasets.taskID=tabT.taskID '
                sql += 'AND type=:type AND NOT status IN (:dsOKStatus1,:dsOKStatus2)) '
                sql += "ORDER BY currentPriority DESC, taskID "
            else:
                varMap = {}
                sql  = "SELECT tabT.taskID,datasetID,currentPriority "
                sql += "FROM ATLAS_PANDA.JEDI_Tasks tabT,ATLAS_PANDA.JEDI_Datasets tabD "
                sql += "WHERE tabT.taskID=tabD.taskID AND tabT.taskID IN ("
                for tmpTaskIdx,tmpTaskID in enumerate(simTasks):
                    tmpKey = ':taskID{0}'.format(tmpTaskIdx)
                    varMap[tmpKey] = tmpTaskID
                    sql += '{0},'.format(tmpKey)
                sql = sql[:-1]
                sql += ') '
                sql += 'AND type=:type '
                varMap[':type'] = 'input'
            # begin transaction
            self.conn.begin()
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
            taskIDList = []
            for taskID,datasetID,currentPriority in resList:
                # just return the max priority
                if isPeeking:
                    return currentPriority
                # make task-dataset mapping
                if not taskDatasetMap.has_key(taskID):
                    taskDatasetMap[taskID] = []
                taskDatasetMap[taskID].append(datasetID)
                if not taskID in taskIDList:
                    taskIDList.append(taskID)
            tmpLog.debug('got {0} tasks'.format(len(taskDatasetMap)))
            # sql to read task
            sqlRT  = "SELECT %s " % JediTaskSpec.columnNames()
            sqlRT += "FROM ATLAS_PANDA.JEDI_Tasks WHERE taskID=:taskID AND lockedBy IS NULL FOR UPDATE NOWAIT"
            # sql to lock task
            sqlLock  = "UPDATE ATLAS_PANDA.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE "
            sqlLock += "WHERE taskID=:taskID AND lockedBy IS NULL "
            # sql to read template
            sqlJobP = "SELECT jobParamsTemplate FROM ATLAS_PANDA.JEDI_JobParams_Template WHERE taskID=:taskID "
            # sql to read datasets
            sqlRD  = "SELECT %s " % JediDatasetSpec.columnNames()
            sqlRD += "FROM ATLAS_PANDA.JEDI_Datasets WHERE datasetID=:datasetID FOR UPDATE NOWAIT"
            # sql to read files
            sqlFR  = "SELECT * FROM (SELECT %s " % JediFileSpec.columnNames()
            sqlFR += "FROM ATLAS_PANDA.JEDI_Dataset_Contents WHERE "
            sqlFR += "datasetID=:datasetID and status=:status "
            sqlFR += "ORDER BY fileID) "
            sqlFR += "WHERE rownum <= %s" % nFiles
            # sql to update file status
            sqlFU  = "UPDATE ATLAS_PANDA.JEDI_Dataset_Contents SET status=:nStatus "
            sqlFU += "WHERE fileID=:fileID AND status=:oStatus "
            # sql to update file usage info in dataset
            sqlDU  = "UPDATE ATLAS_PANDA.JEDI_Datasets SET nFilesUsed=:nFilesUsed WHERE datasetID=:datasetID "
            # loop over all tasks
            iTasks = 0
            for taskID in taskIDList:
                datasetIDs = taskDatasetMap[taskID]
                # begin transaction
                self.conn.begin()
                # read task
                toSkip = False
                varMap = {}
                varMap[':taskID'] = taskID
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
                        logger.debug('{0} skip locked taskID={1}'.format(methodName,taskID))
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
                                logger.debug('{0} skip locked taskID={1} datasetID={2}'.format(methodName,taskID,datasetID))
                            else:
                                # failed with something else
                                raise errType,errValue
                # read job params and files
                if not toSkip:
                    # lock task
                    if simTasks == None:
                        varMap = {}
                        varMap[':taskID'] = taskID
                        varMap[':lockedBy'] = pid
                        self.cur.execute(sqlLock+comment,varMap)
                        nRow = self.cur.rowcount
                    else:
                        # set nRow for simulation
                        nRow = 1
                    if nRow != 1:
                        logger.debug('{0} failed to lock taskID={1}'.format(methodName,taskID))
                    else:
                        # read template to generate job parameters
                        varMap = {}
                        varMap[':taskID'] = taskID
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
                                        logger.debug('{0} skip fileID={1} already used by another'.format(methodName,tmpFileSpec.fileID))
                                        continue
                                # add to InputChunk
                                tmpDatasetSpec.addFile(tmpFileSpec)
                                iFiles += 1
                            if iFiles == 0:
                                # no input files
                                logger.debug('{0} datasetID={1} has no files to be processed'.format(methodName,datasetID))
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
            logger.debug('{0} done for {1} tasks'.format(methodName,len(returnList)))
            return returnList
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            return failedRet


    # insert JobParamsTemplate
    def insertJobParamsTemplate_JEDI(self,taskID,templ):
        comment = ' /* JediDBProxy.insertJobParamsTemplate_JEDI */'
        methodName = self.getMethodName(comment)
        methodName = '{0} taskID={1}'.format(methodName,taskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # SQL
            sql  = "INSERT INTO ATLAS_PANDA.JEDI_JobParams_Template (taskID,jobParamsTemplate) VALUES (:taskID,:templ) "
            varMap = {}
            varMap[':taskID'] = taskID
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
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error("{0} {1}".format(errtype,errvalue))
            return False



    # insert TaskParams
    def insertTaskParams_JEDI(self,metaTaskID,taskID,taskParams):
        comment = ' /* JediDBProxy.insertTaskParams_JEDI */'
        methodName = self.getMethodName(comment)
        methodName = '{0} taskID={1} metaID={2}'.format(methodName,taskID,metaTaskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to insert task parameters
            sqlT  = "INSERT INTO ATLAS_PANDA.DEFT_TASK (TASK_ID,TASK_META,TASK_PARAM) VALUES (:taskID,:metaID,:param) "
            # sql to insert command
            sqlC  = "INSERT INTO ATLAS_PANDA.PRODSYS_COMM (COMM_TASK,COMM_META,COMM_OWNER,COMM_CMD) "
            sqlC += "VALUES (:taskID,:metaID,:comm_owner,:comm_cmd) "
            # begin transaction
            self.conn.begin()
            # insert task parameters
            varMap = {}
            varMap[':taskID'] = taskID
            varMap[':metaID'] = metaTaskID
            varMap[':param']  = taskParams
            self.cur.execute(sqlT+comment,varMap)
            # insert command
            varMap = {}
            varMap[':taskID'] = taskID
            varMap[':metaID'] = metaTaskID
            varMap[':comm_cmd']  = 'submit'
            varMap[':comm_owner']  = 'DEFT'
            self.cur.execute(sqlC+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug('done')
            return True
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error("{0} {1}".format(errtype,errvalue))
            return False



    # rollback files
    def rollbackFiles_JEDI(self,taskID,inputChunk):
        comment = ' /* JediDBProxy.rollbackFiles_JEDI */'
        methodName = self.getMethodName(comment)
        methodName = '{0} taskID={1}'.format(methodName,taskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to rollback files
            sql  = "UPDATE ATLAS_PANDA.JEDI_Dataset_Contents SET status=:nStatus "
            sql += "WHERE datasetID=:datasetID AND status=:oStatus "
            # sql to reset nFilesUsed
            sqlD  = "UPDATE ATLAS_PANDA.JEDI_Datasets SET nFilesUsed=nFilesUsed-:nFileRow "
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
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error("{0} {1}".format(errtype,errvalue))
            return False



    # get the size of input files which will be copied to the site
    def getMovingInputSize_JEDI(self,siteName):
        comment = ' /* JediDBProxy.getMovingInputSize_JEDI */'
        methodName = self.getMethodName(comment)
        methodName = '{0} site={1}'.format(methodName,siteName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql to get size
            sql  = "SELECT SUM(inputFileBytes)/1024/1024/1024 FROM ATLAS_PANDA.jobsDefined4 "
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
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error("{0} {1}".format(errtype,errvalue))
            return None
        


    # get highest prio jobs with workQueueID
    def getHighestPrioJobStat_JEDI(self,prodSourceLabel,cloudName,workQueue):
        comment = ' /* JediDBProxy.getHighestPrioJobStat_JEDI */'
        methodName = self.getMethodName(comment)
        methodName = "{0} cloud={1} workQueue={2}".format(methodName,cloudName,workQueue.queue_name)
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
        tables = ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4']
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
                if table == 'ATLAS_PANDA.jobsActive4':
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
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error("{0} {1}".format(errtype,errvalue))
            return False,None



    # get the list of tasks to refine
    def getTasksToRefine_JEDI(self,vo=None,prodSourceLabel=None):
        comment = ' /* JediDBProxy.getTasksToRefine_JEDI */'
        methodName = self.getMethodName(comment)
        methodName = "{0} vo={1} label={2}: ".format(methodName,vo,prodSourceLabel)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retTaskIDs = []
        try:
            # sql to get taskIDs to refine from the command table
            # FIXME ATLAS_PANDA -> ATLAS_DEFT
            sqlC  = "SELECT comm_task,comm_meta FROM ATLAS_PANDA.PRODSYS_COMM "
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
            for taskID,metaTaskID in resList:
                isOK = True
                tmpLog.debug('start taskID={0} metaTaskID={1}'.format(taskID,metaTaskID))                
               # start transaction
                self.conn.begin()
                # lock
                varMap = {}
                varMap[':comm_task'] = taskID
                varMap[':comm_meta'] = metaTaskID
                # FIXME ATLAS_PANDA -> ATLAS_DEFT
                sqlLock  = "SELECT * FROM ATLAS_PANDA.PRODSYS_COMM WHERE comm_task=:comm_task AND comm_meta=:comm_meta "
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
                        tmpLog.debug('skip locked taskID={0} metaTaskID={1}'.format(taskID,metaTaskID))
                    else:
                        # failed with something else
                        raise errType,errValue
                if not toSkip:     
                    # read task owner
                    varMap = {}
                    varMap[':meta_id'] = metaTaskID
                    # FIXME ATLAS_PANDA -> ATLAS_DEFT
                    sqlMeta = "SELECT meta_manager FROM ATLAS_PANDA.DEFT_META WHERE meta_id=:meta_id"
                    tmpLog.debug(sqlMeta+comment+str(varMap))
                    self.cur.execute(sqlMeta+comment,varMap)
                    resMeta = self.cur.fetchone()
                    if resMeta == None:
                        tmpLog.error('metaTask is not found for taskID={0} metaTaskID={1}'.format(taskID,metaTaskID))
                        isOK = False
                    if isOK:
                        # insert task to JEDI
                        varMap = {}
                        varMap[':taskID'] = taskID
                        # FIXME
                        #varMap[':taskName'] = 'unknown'
                        import uuid
                        varMap[':taskName'] = str(uuid.uuid4())
                        # FIXME
                        #varMap[':status'] = 'registered'
                        varMap[':status'] = 'defined'
                        varMap[':userName'] = resMeta[0]
                        sqlIT =  "INSERT INTO ATLAS_PANDA.JEDI_Tasks "
                        sqlIT += "(taskID,taskName,status,userName,creationDate,modificationtime"
                        if vo != None:
                            sqlIT += ',vo'
                        if prodSourceLabel != None:
                            sqlIT += ',prodSourceLabel'
                        sqlIT += ") "
                        sqlIT += "VALUES(:taskID,:taskName,:status,:userName,CURRENT_DATE,CURRENT_DATE"
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
                            tmpLog.error("failed to insert taskID={0} with {1} {2}".format(taskID,errtype,errvalue))
                            isOK = False
                    if isOK:
                        # check task parameters
                        varMap = {}
                        varMap[':task_id'] = taskID
                        sqlTC = "SELECT task_id FROM ATLAS_PANDA.DEFT_TASK WHERE task_id=:task_id "
                        tmpLog.debug(sqlTC+comment+str(varMap))
                        self.cur.execute(sqlTC+comment,varMap)
                        resTC = self.cur.fetchone()
                        if resTC == None or resTC[0] == None:
                            tmpLog.error("task parameters not found in DEFT_TASK")
                            isOK = False
                    if isOK:        
                        # copy task parameters
                        varMap = {}
                        varMap[':task_id'] = taskID
                        # FIXME ATLAS_PANDA -> ATLAS_DEFT
                        sqlCopy  = "INSERT INTO ATLAS_PANDA.JEDI_TaskParams (taskID,taskParams) "
                        sqlCopy += "SELECT task_id,task_param FROM ATLAS_PANDA.DEFT_TASK "
                        sqlCopy += "WHERE task_id=:task_id "
                        try:
                            tmpLog.debug(sqlCopy+comment+str(varMap))
                            self.cur.execute(sqlCopy+comment,varMap)
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error("failed to insert param for taskID={0} with {1} {2}".format(taskID,errtype,errvalue))
                    # update
                    varMap = {}
                    varMap[':comm_task'] = taskID
                    varMap[':comm_meta'] = metaTaskID
                    varMap[':comm_cmd']  = 'submitted'
                    # FIXME ATLAS_PANDA -> ATLAS_DEFT
                    sqlUC = "UPDATE ATLAS_PANDA.PRODSYS_COMM SET comm_cmd=:comm_cmd WHERE comm_task=:comm_task AND comm_meta=:comm_meta "
                    tmpLog.debug(sqlUC+comment+str(varMap))
                    self.cur.execute(sqlUC+comment,varMap)
                    # append
                    if isOK:
                        retTaskIDs.append(taskID)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # find orphaned tasks to rescue
            self.conn.begin()
            varMap = {}
            # FIXME
            #varMap[':status'] = 'registered'
            #varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
            varMap[':status'] = 'defined'
            varMap[':timeLimit'] = datetime.datetime.utcnow() - datetime.timedelta(seconds=10)
            sqlOrpS  = "SELECT taskID FROM ATLAS_PANDA.JEDI_Tasks "
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
            sqlOrpU  = "UPDATE ATLAS_PANDA.JEDI_Tasks SET modificationtime=CURRENT_DATE "
            sqlOrpU += "WHERE taskID=:taskID "
            for taskID, in resList:
                varMap = {}
                varMap[':taskID'] = taskID
                tmpLog.debug(sqlOrpU+comment+str(varMap))
                self.cur.execute(sqlOrpU+comment,varMap)
                nRow = self.cur.rowcount
                if nRow == 1 and not taskID in retTaskIDs:
                    retTaskIDs.append(taskID)
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
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error("{0} {1}".format(errtype,errvalue))
            return None



    # get task parameters with taskID
    def getTaskParamsWithID_JEDI(self,taskID):
        comment = ' /* JediDBProxy.getTaskParamsWithID_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' taskID={0}'.format(taskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # sql
            sql  = "SELECT taskParams FROM ATLAS_PANDA.JEDI_TASKPARAMS WHERE taskID=:taskID "
            varMap = {}
            varMap[':taskID'] = taskID
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
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error("{0} {1}".format(errtype,errvalue))
            return None



    # register task/dataset/templ/param in a single transaction
    def registerTaskInOneShot_JEDI(self,taskID,taskSpec,inMasterDatasetSpec,
                                   inSecDatasetSpecList,outDatasetSpecList,
                                   outputTemplateMap,jobParamsTemplate):
        comment = ' /* JediDBProxy.registerTaskInOneShot_JEDI */'
        methodName = self.getMethodName(comment)
        methodName += ' taskID={0}'.format(taskID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            timeNow = datetime.datetime.utcnow()
            # set attributes
            taskSpec.modificationTime = timeNow
            taskSpec.resetChangedAttr('taskID')
            # update task
            varMap = taskSpec.valuesMap(useSeq=False,onlyChanged=True)
            varMap[':taskID'] = taskID
            sql  = "UPDATE ATLAS_PANDA.JEDI_Tasks SET {0} WHERE ".format(taskSpec.bindUpdateChangesExpression())
            sql += "taskID=:taskID "
            self.cur.execute(sql+comment,varMap)
            nRow = self.cur.rowcount
            tmpLog.debug('update {0} row in task table'.format(nRow))
            if nRow != 1:
                tmpLog.error('the task not found in task table')
            else:
                tmpLog.debug('inserting datasets')
                # sql
                sql  = "INSERT INTO ATLAS_PANDA.JEDI_Datasets ({0}) ".format(JediDatasetSpec.columnNames())
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
                        sqlH = "INSERT INTO ATLAS_PANDA.JEDI_Output_Template (outTempID,datasetID,"
                        sqlL = "VALUES(ATLAS_PANDA.JEDI_OUTPUT_TEMPLATE_ID_SEQ.nextval,:datasetID," 
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
                varMap[':taskID'] = taskID
                varMap[':templ']  = jobParamsTemplate
                sql = "INSERT INTO ATLAS_PANDA.JEDI_JobParams_Template (taskID,jobParamsTemplate) VALUES (:taskID,:templ) "
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
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error("{0} {1}".format(errtype,errvalue))
            return False
