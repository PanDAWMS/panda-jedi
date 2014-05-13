import re
import sys

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from WatchDogBase import WatchDogBase

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])



# watchdog for ATLAS production
class AtlasProdWatchDog (WatchDogBase):

    # constructor
    def __init__(self,ddmIF,taskBufferIF):
        WatchDogBase.__init__(self,ddmIF,taskBufferIF)



    # main
    def doAction(self):
        try:
            # get logger
            tmpLog = MsgWrapper(logger)
            tmpLog.debug('start')
            # action for priority boost
            self.doActionForPriorityBoost(tmpLog)
            # action for reassign
            self.doActionForReassgin(tmpLog)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('failed with {0} {1}'.format(errtype,errvalue))
        # return
        tmpLog.debug('done')
        return self.SC_SUCCEEDED
    


    # action for priority boost
    def doActionForPriorityBoost(self,gTmpLog):
        # get work queue mapper
        workQueueMapper = self.taskBufferIF.getWorkQueueMap()
        # get list of work queues
        workQueueList = workQueueMapper.getQueueListWithVoType(self.vo,self.prodSourceLabel)
        # loop over all work queues
        for workQueue in workQueueList:
            gTmpLog.debug('start workQueue={0}'.format(workQueue.queue_name))
            # get tasks
            taskVarList = self.taskBufferIF.getTasksWithCriteria_JEDI(self.vo,self.prodSourceLabel,['running'],
                                                                      taskCriteria={'workQueue_ID':workQueue.queue_id},
                                                                      datasetCriteria={'masterID':None},
                                                                      taskParamList=['jediTaskID','taskPriority','currentPriority'],
                                                                      datasetParamList=['nFiles','nFilesUsed','nFilesTobeUsed',
                                                                                        'nFilesFinished','nFilesFailed'])
            for taskParam,datasetParam in taskVarList:
                pass


        
    # action for reassignment
    def doActionForReassgin(self,gTmpLog):
        # get DDM I/F
        ddmIF = self.ddmIF.getInterface(self.vo)
        # get site mapper
        siteMapper = self.taskBufferIF.getSiteMapper()
        # get tasks to get reassigned
        taskList = self.taskBufferIF.getTasksToReassign_JEDI(self.vo,self.prodSourceLabel)
        gTmpLog.debug('got {0} tasks to reassign'.format(len(taskList)))
        for taskSpec in taskList:
            tmpLog = MsgWrapper(logger,'<jediTaskID={0}'.format(taskSpec.jediTaskID))
            tmpLog.debug('start to reassign')
            # update cloudtasks
            tmpStat = self.taskBufferIF.setCloudTaskByUser('jedi',taskSpec.jediTaskID,taskSpec.cloud,'assigned',True)
            if tmpStat != 'SUCCEEDED':
                tmpLog.error('failed to update CloudTasks')
                continue
            # get datasets
            tmpStat,datasetSpecList = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(taskSpec.jediTaskID,['output','log'])
            if tmpStat != True:
                tmpLog.error('failed to get datasets')
                continue
            # check cloud
            if not siteMapper.checkCloud(taskSpec.cloud):
                tmpLog.error("cloud={0} doesn't exist".format(taskSpec.cloud))
                continue
            # get T1
            t1SiteName = siteMapper.getCloud(taskSpec.cloud)['dest']
            t1Site = siteMapper.getSite(t1SiteName)
            # loop over all datasets
            isOK = True
            for datasetSpec in datasetSpecList:
                tmpLog.debug('dataset={0}'.format(datasetSpec.datasetName))
                # get location
                location = siteMapper.getDdmEndpoint(t1Site.sitename,datasetSpec.storageToken)
                # set origin metadata
                tmpLog.debug('setting metadata origin={0}'.format(location))
                tmpStat = ddmIF.setDatasetMetadata(datasetSpec.datasetName,'origin',location)
                if tmpStat != True:
                    tmpLog.error("failed to set origin")
                    isOK = False
                    break
                # make subscription
                tmpLog.debug('registering subscription to {0}'.format(location))
                tmpStat = ddmIF.registerDatasetSubscription(datasetSpec.datasetName,location,
                                                            activity='Production',ignoreUnknown=True)
                if tmpStat != True:
                    tmpLog.error("failed to make subscription")
                    isOK = False
                    break
            # succeeded
            if isOK:    
                # activate task
                taskSpec.status = taskSpec.oldStatus
                taskSpec.oldStatus = None
                self.taskBufferIF.updateTask_JEDI(taskSpec,{'jediTaskID':taskSpec.jediTaskID})
                tmpLog.debug('finished to reassign')
