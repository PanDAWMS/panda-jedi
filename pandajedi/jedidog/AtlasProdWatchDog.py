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
            # get work queue mapper
            workQueueMapper = self.taskBufferIF.getWorkQueueMap()
            # get list of work queues
            workQueueList = workQueueMapper.getQueueListWithVoType(self.vo,self.prodSourceLabel)
            # loop over all work queues
            for workQueue in workQueueList:
                tmpLog.debug('start workQueue={0}'.format(workQueue.queue_name))
                # get tasks
                taskVarList = self.taskBufferIF.getTasksWithCriteria_JEDI(self.vo,self.prodSourceLabel,['running'],
                                                                          taskCriteria={'workQueue_ID':workQueue.queue_id},
                                                                          datasetCriteria={'masterID':None},
                                                                          taskParamList=['jediTaskID','taskPriority','currentPriority'],
                                                                          datasetParamList=['nFiles','nFilesUsed','nFilesTobeUsed',
                                                                                            'nFilesFinished','nFilesFailed'])
                for taskParam,datasetParam in taskVarList:
                    pass
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('failed with {0} {1}'.format(errtype,errvalue))
        # return
        tmpLog.debug('done')
        return self.SC_SUCCEEDED
    
