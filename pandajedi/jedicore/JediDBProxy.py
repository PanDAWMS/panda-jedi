import re
import sys
import datetime

from pandajedi.jediconfig import jedi_config

from pandaserver import taskbuffer
import taskbuffer.OraDBProxy
from WorkQueueMapper import WorkQueueMapper

from JediTaskSpec import JediTaskSpec
from JediFileSpec import JediFileSpec
from JediDatasetSpec import JediDatasetSpec

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])

#from taskbuffer.OraDBProxy import _logger

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
    def connect(self,dbhost=jedi_config.dbhost,dbpasswd=jedi_config.dbpasswd,
                dbuser=jedi_config.dbuser,dbname=jedi_config.dbname,
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
        logger.debug('%s start' % methodName)
        try:
            # SQL
            sql = 'SELECT taskID,datasetName,datasetID,vo FROM ATLAS_PANDA.JEDI_Datasets '
            sql += 'WHERE status=:status AND type=:type AND lockedBy IS NULL '
            varMap = {}
            varMap[':status'] = 'defined'
            varMap[':type']   = 'input'
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            self.cur.execute(sql+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            retList = self.cur.fetchall()
            logger.debug('%s got %s datasets' % (methodName,len(retList)))
            return retList
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            return None

                                                
    # feed files to the JEDI contents table
    def insertFilesForDataset_JEDI(self,taskID,datasetID,fileMap):
        comment = ' /* JediDBProxy.insertFilesForDataset_JEDI */'
        methodName = self.getMethodName(comment)
        methodName = '%s taskID=%s datasetID=%s' % (methodName,taskID,datasetID)
        logger.debug('%s start' % methodName)
        try:
            # current current date
            timeNow = datetime.datetime.utcnow()
            # loop over all files
            fileSpecMap = {}
            for guid,fileVal in fileMap.iteritems():
                fileSpec = JediFileSpec()
                fileSpec.taskID       = taskID
                fileSpec.datasetID    = datasetID
                fileSpec.guid         = guid
                fileSpec.type         = 'input'
                fileSpec.status       = 'ready'            
                fileSpec.lfn          = fileVal['lfn']
                fileSpec.scope        = fileVal['scope']
                fileSpec.fsize        = fileVal['filesize']
                fileSpec.checksum     = fileVal['checksum']
                fileSpec.creationDate = timeNow
                # FIXME : to be removed
                fileSpec.lastAttemptTime = timeNow                
                # append
                fileSpecMap[fileSpec.lfn] = fileSpec
            # sort by LFN
            lfnList = fileSpecMap.keys()
            lfnList.sort()
            # sql for check
            sqlCh  = "SELECT lfn FROM ATLAS_PANDA.JEDI_Dataset_Contents "
            sqlCh += "WHERE datasetID=:datasetID FOR UPDATE"
            # sql for insert
            sqlIn  = "INSERT INTO ATLAS_PANDA.JEDI_Dataset_Contents (%s) " % JediFileSpec.columnNames()
            sqlIn += JediFileSpec.bindValuesExpression()
            # begin transaction
            self.conn.begin()
            # get existing file list
            varMap = {}
            varMap[':datasetID'] = datasetID
            self.cur.execute(sqlCh+comment,varMap)
            tmpRes = self.cur.fetchall()
            existingFiles = []
            for lfn, in tmpRes:
                existingFiles.append(lfn)
            # insert files
            nInsert = 0
            for lfn in lfnList:
                # avoid duplication
                if lfn in existingFiles:
                    continue
                fileSpec = fileSpecMap[lfn]
                varMap = fileSpec.valuesMap(useSeq=True)
                self.cur.execute(sqlIn+comment,varMap)
                nInsert += 1
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            logger.debug('%s inserted %s rows' % (methodName,nInsert))
            return True
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
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
            varMap = datasetSpec.valuesMap(useSeq=True)
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


    # update JEDI dataset
    def updateDataset_JEDI(self,datasetSpec,criteria):
        comment = ' /* JediDBProxy.updateDataset_JEDI */'
        methodName = self.getMethodName(comment)
        for tmpKey,tmpVal in criteria.iteritems():
            methodName += ' %s=%s' % (tmpKey,tmpVal)
        logger.debug('%s start' % methodName)
        # return value for failure
        failedRet = False,0
        # no criteria
        if criteria == {}:
            logger.error('%s no selection criteria' % methodName)            
            return failedRet
        # check criteria
        for tmpKey in criteria.keys():
            if not hasattr(datasetSpec,tmpKey):
                logger.error('%s unknown attribute %s is used in criteria' % (methodName,tmpKey))
                return failedRet
        try:
            # set attributes
            timeNow = datetime.datetime.utcnow()
            datasetSpec.modificationTime = timeNow
            # values for UPDATE
            varMap = datasetSpec.valuesMap(useSeq=True,onlyChanged=True)
            # sql
            sql  = "UPDATE ATLAS_PANDA.JEDI_Datasets SET %s WHERE " % datasetSpec.bindUpdateChangesExpression()
            for tmpKey,tmpVal in criteria.iteritems():
                crKey = ':cr_%s' % tmpKey
                sql += '%s=%s' % (tmpKey,crKey)
                varMap[crKey] = tmpVal
            # begin transaction
            self.conn.begin()
            # update dataset
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
            varMap = taskSpec.valuesMap(useSeq=True)
            # sql
            sql  = "UPDATE ATLAS_PANDA.JEDI_Tasks SET %s WHERE " % taskSpec.bindUpdateChangesExpression()
            for tmpKey,tmpVal in criteria:
                crKey = ':cr_%s' % tmpKey
                sql += '%s=%s' % (tmpKey,crKey)
                varMap[crKey] = tmpVal
            # begin transaction
            self.conn.begin()
            # update task
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
    def getTaskWithID_JEDI(self,taskID):
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
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            if res != None:
                taskSepc = JediTaskSpec()
                taskSepc.pack(res)
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
    def getJobStatisticsWithWorkQueue_JEDI(self,vo,prodSourceLabel,minPriority):
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
        # FIXME
        #sqlMV = re.sub('COUNT\(\*\)','SUM(num_of_jobs)',sqlMV)
        #sqlMV = re.sub('SELECT ','SELECT /*+ RESULT_CACHE */ ',sqlMV)        
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
                    # FIXME
                    #mvTableName = 'ATLAS_PANDA.MV_JOBSACTIVE4_STATS'
                    mvTableName = 'ATLAS_PANDA.jobsActive4'
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
