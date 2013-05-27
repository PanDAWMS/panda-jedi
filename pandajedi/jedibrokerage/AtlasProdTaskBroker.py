import re
import sys

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore import Interaction
from TaskBrokerBase import TaskBrokerBase

from pandaserver.userinterface import Client as PandaClient
# cannot use pandaserver.taskbuffer while Client is used
from taskbuffer.JobSpec import JobSpec


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# brokerage for ATLAS production
class AtlasProdTaskBroker (TaskBrokerBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        TaskBrokerBase.__init__(self,taskBufferIF,ddmIF)


    # main to check
    def doCheck(self,taskSpecList):
        # make logger
        tmpLog = MsgWrapper(logger)
        tmpLog.debug('start doCheck')
        # return for failure
        retFatal    = self.SC_FATAL,{}
        retTmpError = self.SC_FAILED,{}
        # get list of reqIDs wchih are mapped to taskID in Panda
        reqIdTaskIdMap = {}
        for taskSpec in taskSpecList:
            if taskSpec.reqID != None:
                if reqIdTaskIdMap.has_key(taskSpec.reqID):
                    tmpLog.error('reqID={0} is dubplicated in jediTaskID={1},{2}'.format(taskSpec.reqID,
                                                                                         taskSpec.jediTaskID,
                                                                                         reqIdTaskIdMap[taskSpec.reqID]))
                else:
                    reqIdTaskIdMap[taskSpec.reqID] = taskSpec.jediTaskID
                    tmpLog.debug('jediTaskID={0} has reqID={1}'.format(taskSpec.jediTaskID,taskSpec.reqID))
            else:
                tmpLog.error('jediTaskID={0} has undefined reqID'.format(taskSpec.jediTaskID)) 
        # check with panda
        tmpLog.debug('check with panda')
        tmpPandaStatus,cloudsInPanda = PandaClient.seeCloudTask(reqIdTaskIdMap.keys())
        if tmpPandaStatus != 0:
            tmpLog.error('failed to see clouds')
            return retTmpError
        # make return map
        retMap = {}
        for tmpReqID,tmpCloud in cloudsInPanda.iteritems():
            if not tmpCloud in ['NULL','',None]:
                tmpLog.debug('reqID={0} jediTaskID={1} -> {2}'.format(tmpReqID,reqIdTaskIdMap[tmpReqID],tmpCloud))
                retMap[reqIdTaskIdMap[tmpReqID]] = tmpCloud
        tmpLog.debug('ret {0}'.format(str(retMap)))
        # return
        tmpLog.debug('done')        
        return self.SC_SUCCEEDED,retMap



    # main to assign
    def doBrokerage(self,inputList,vo,prodSourceLabel,workQueue):
        # variables for submission
        maxBunchTask = 100
        # make logger
        tmpLog = MsgWrapper(logger)
        tmpLog.debug('start doBrokerage')
        # return for failure
        retFatal    = self.SC_FATAL
        retTmpError = self.SC_FAILED
        tmpLog.debug('vo={0} label={1} queue={2}'.format(vo,prodSourceLabel,
                                                         workQueue.queue_name))
        # loop over all tasks
        allRwMap    = {}
        prioMap     = {}
        tt2Map      = {}
        expRWs      = {}
        jobSpecList = []
        for taskSpec,cloudName,inputChunk in inputList:
            # make JobSpec to be submitted for TaskAssigner
            jobSpec = JobSpec()
            jobSpec.taskID     = taskSpec.reqID
            jobSpec.jediTaskID = taskSpec.jediTaskID
            jobSpec.prodSourceLabel  = taskSpec.prodSourceLabel
            jobSpec.processingType   = taskSpec.processingType
            jobSpec.workingGroup     = taskSpec.workingGroup
            jobSpec.metadata         = taskSpec.processingType
            jobSpec.assignedPriority = taskSpec.taskPriority
            jobSpec.currentPriority  = taskSpec.currentPriority
            jobSpec.maxDiskCount     = (taskSpec.getOutDiskSize() + taskSpec.getWorkDiskSize()) / 1024 / 1024
            for datasetSpec in inputChunk.getDatasets():
                if datasetSpec.isMaster():
                    jobSpec.prodDBlock = datasetSpec.datasetName
                for fileSpec in datasetSpec.Files:
                    tmpInFileSpec = fileSpec.convertToJobFileSpec(datasetSpec)
                    jobSpec.addFile(tmpInFileSpec)
            # append
            jobSpecList.append(jobSpec)
            prioMap[jobSpec.taskID] = jobSpec.currentPriority
            tt2Map[jobSpec.taskID]  = jobSpec.processingType
            # get RW for a priority
            if not allRwMap.has_key(jobSpec.currentPriority):
                tmpRW = self.taskBufferIF.calculateRWwithPrio_JEDI(vo,prodSourceLabel,workQueue,
                                                                   jobSpec.currentPriority) 
                if tmpRW == None:
                    tmpLog.error('failed to calculate RW with prio={0}'.format(jobSpec.currentPriority))
                    return retTmpError
                allRwMap[jobSpec.currentPriority] = tmpRW
            # get expected RW
            expRW = self.taskBufferIF.calculateTaskRW_JEDI(jobSpec.jediTaskID)
            if expRW == None:
                tmpLog.error('failed to calculate RW for jediTaskID={0}'.format(jobSpec.jediTaskID))
                return retTmpError
            expRWs[jobSpec.taskID] = expRW
        # get fullRWs
        fullRWs = self.taskBufferIF.calculateRWwithPrio_JEDI(vo,prodSourceLabel,None,None)
        if fullRWs == None:
            tmpLog.error('failed to calculate full RW')
            return retTmpError
        # set metadata
        for jobSpec in jobSpecList:
            rwValues = allRwMap[jobSpec.currentPriority]
            jobSpec.metadata = "%s;%s;%s;%s;%s;%s" % (jobSpec.metadata,
                                                      str(rwValues),str(expRWs),
                                                      str(prioMap),str(fullRWs),
                                                      str(tt2Map))
        tmpLog.debug('run task assigner for {0} tasks'.format(len(jobSpecList)))
        nBunchTask = 0
        while nBunchTask < len(jobSpecList):
            # get a bunch
            jobsBunch = jobSpecList[nBunchTask:nBunchTask+maxBunchTask]
            strIDs = 'taskID(reqID)='
            for tmpJobSpec in jobsBunch:
                strIDs += '{0},'.format(tmpJobSpec.taskID)
            strIDs = strIDs[:-1]
            tmpLog.debug(strIDs)
            # increment index
            nBunchTask += maxBunchTask
            # run task brokerge
            stS,outSs = PandaClient.runTaskAssignment(jobsBunch)
            tmpLog.debug('{0}:{1}'.format(stS,str(outSs)))
        # return
        tmpLog.debug('done')        
        return self.SC_SUCCEEDED
