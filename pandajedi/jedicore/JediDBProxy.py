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
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            # roll back
            self._rollback()
            return False
                                            

    # get the list of datasets to feed contents to DB
    def getDatasetsToFeedContentsJEDI(self):
        comment = ' /* JediDBProxy.getDatasetsToFeedContentsJEDI */'
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
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            # roll back
            self._rollback()
            return None

                                                
    # feed files to the JEDI contents table
    def insertFilesForDatasetJEDI(self,taskID,datasetID,fileMap):
        comment = ' /* JediDBProxy.insertFilesForDatasetJEDI */'
        methodName = self.getMethodName(comment)
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
            # sql
            sql  = "INSERT INTO ATLAS_PANDA.JEDI_Dataset_Contents (%s) " % JediFileSpec.columnNames()
            sql += JediFileSpec.bindValuesExpression()
            # begin transaction
            self.conn.begin()
            # insert files
            for lfn in lfnList:
                fileSpec = fileSpecMap[lfn]
                varMap = fileSpec.valuesMap(useSeq=True)
                self.cur.execute(sql+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            logger.debug('%s done' % methodName)
            return True
        except:
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            # roll back
            self._rollback()
            return False


    # insert dataset to the JEDI datasets table
    def insertDatasetJEDI(self,datasetSpec):
        comment = ' /* JediDBProxy.insertDatasetJEDI */'
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
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            # roll back
            self._rollback()
            return False


    # update JEDI dataset
    def updateDatasetJEDI(self,datasetSpec,criteria):
        comment = ' /* JediDBProxy.updateDatasetJEDI */'
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
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            # roll back
            self._rollback()
            return failedRet
                
        
    # insert task to the JEDI task table
    def insertTaskJEDI(self,taskSpec):
        comment = ' /* JediDBProxy.insertTaskJEDI */'
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
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            # roll back
            self._rollback()
            return False


    # update JEDI task
    def updateTaskJEDI(self,taskSpec,criteria):
        comment = ' /* JediDBProxy.updateTaskJEDI */'
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
            # error
            errtype,errvalue = sys.exc_info()[:2]
            logger.error("%s : %s %s" % (methodName,errtype,errvalue))
            # roll back
            self._rollback()
            return failedRet
