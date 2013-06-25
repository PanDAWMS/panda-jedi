from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from JobThrottlerBase import JobThrottlerBase

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# class to throttle ATLAS production jobs
class AtlasProdJobThrottler (JobThrottlerBase):

    # constructor
    def __init__(self,taskBufferIF):
        JobThrottlerBase.__init__(self,taskBufferIF)


    # check if throttled
    def toBeThrottled(self,vo,prodSourceLabel,cloudName,workQueue,jobStat):
        # params
        nBunch = 4
        threshold = 2.0
        thresholdForSite = threshold - 1.0
        nJobsInBunchMax = 500
        nJobsInBunchMin = 300
        nJobsInBunchMaxES = 1000
        # make logger
        tmpLog = MsgWrapper(logger)
        tmpLog.debug('start vo={0} label={1} cloud={2} workQueue={3}'.format(vo,prodSourceLabel,cloudName,
                                                                             workQueue.queue_name))
        workQueueID = workQueue.queue_id
        # check cloud status
        cloudSpec = self.siteMapper.getCloud(cloudName)
        if cloudSpec['status'] in ['offline']:
            tmpLog.debug("  done : SKIP cloud.status={0}".format(cloudSpec['status']))
            return self.retThrottled
        if cloudSpec['status'] in ['test']:
            if workQueue.queue_name != 'test':
                tmpLog.debug("  done : SKIP cloud.status={0} for non test queue ({1})".format(cloudSpec['status'],
                                                                                              workQueue.queue_name))
                return self.retThrottled
        # check if unthrottled
        if workQueue.queue_share == None:
            tmpLog.debug("  done : unthrottled since share=None")
            return self.retUnThrottled
        # count number of jobs in each status
        nRunning = 0
        nNotRun  = 0
        nDefine  = 0
        nWaiting = 0
        if jobStat.has_key(cloudName) and \
               jobStat[cloudName].has_key(workQueueID):
            for pState,pNumber in jobStat[cloudName][workQueueID].iteritems():
                if pState in ['running']:
                    nRunning += pNumber
                elif pState in ['assigned','activated','starting']:
                    nNotRun  += pNumber
                elif pState in ['defined']:
                    nDefine  += pNumber
                elif pState in ['waiting']:
                    nWaiting += pNumber
        # check if higher prio tasks are waiting
        tmpStat,highestPrioJobStat = self.taskBufferIF.getHighestPrioJobStat_JEDI('managed',cloudName,workQueue)
        highestPrioInPandaDB = highestPrioJobStat['highestPrio']
        nNotRunHighestPrio   = highestPrioJobStat['nNotRun']
        # the highest priority of waiting tasks 
        highestPrioWaiting = self.taskBufferIF.checkWaitingTaskPrio_JEDI(vo,workQueue,
                                                                         'managed',cloudName)
        if highestPrioWaiting == None:
            tmpLog.error('failed to get the highest priority of waiting tasks')
            return self.retTmpError
        # high priority tasks are waiting
        highPrioQueued = False
        if highestPrioWaiting > highestPrioInPandaDB or (highestPrioWaiting == highestPrioInPandaDB and \
                                                         nNotRunHighestPrio < nJobsInBunchMin):
            highPrioQueued = True
        tmpLog.debug(" highestPrio waiting:{0} inPanda:{1} numNotRun:{2} -> highPrioQueued={3}".format(highestPrioWaiting,
                                                                                                       highestPrioInPandaDB,
                                                                                                       nNotRunHighestPrio,
                                                                                                       highPrioQueued))
        # set maximum number of jobs to be submitted
        tmpRemainingSlot = int(nRunning*threshold-nNotRun)
        if tmpRemainingSlot < nJobsInBunchMin:
            # use the lower limit to avoid creating too many _sub/_dis datasets
            nJobsInBunch = nJobsInBunchMin
        else:
            if workQueue.queue_name in ['evgensimul']:
                # use higher limit for evgensimul
                if tmpRemainingSlot < nJobsInBunchMaxES:
                    nJobsInBunch = tmpRemainingSlot
                else:
                    nJobsInBunch = nJobsInBunchMaxES
            else:
                if tmpRemainingSlot < nJobsInBunchMax:
                    nJobsInBunch = tmpRemainingSlot
                else:
                    nJobsInBunch = nJobsInBunchMax
        nQueueLimit = nJobsInBunch*nBunch
        # use special limit for CERN
        if cloudName == 'CERN':
            nQueueLimit = 2000
        # use nPrestage for reprocessing   
        if workQueue.queue_name in ['reprocessing']:
            if cloudSpec.has_key('nprestage') and cloudSpec['nprestage'] > 0:
                nQueueLimit = cloudSpec['nprestage']
                # reset nJobsInBunch
                if nQueueLimit > (nNotRun+nDefine):
                    tmpRemainingSlot = nQueueLimit - (nNotRun+nDefine)
                    if tmpRemainingSlot < nJobsInBunch:
                        pass
                    elif tmpRemainingSlot < nJobsInBunchMax:
                        nJobsInBunch = tmpRemainingSlot
                    else:
                        nJobsInBunch = nJobsInBunchMax
        # set number of jobs to be submitted
        self.setMaxNumJobs(nJobsInBunch)
        # check number of jobs when high priority jobs are not waiting. test jobs are sent without throttling
        limitPriority = False
        # check when high prio tasks are not waiting
        if not highPrioQueued:
            if nRunning == 0 and (nNotRun+nDefine) > nQueueLimit:
                limitPriority = True
                # pilot is not running or DDM has a problem
                tmpLog.debug("  done : SKIP no running and enough nQueued={0}>{1}".format(nNotRun+nDefine,nQueueLimit))
                return self.retThrottled
            elif nRunning != 0 and float(nNotRun)/float(nRunning) > threshold and (nNotRun+nDefine) > nQueueLimit:
                limitPriority = True
                # enough jobs in Panda
                tmpLog.debug("  done : SKIP nQueued/nRunning={0}>{1} & nQueued={2}>{3}".format(float(nNotRun)/float(nRunning),
                                                                                               threshold,nNotRun+nDefine,
                                                                                               nQueueLimit))
                return self.retThrottled
            elif nDefine > nQueueLimit:
                limitPriority = True
                # brokerage is stuck
                tmpLog.debug("  done : SKIP too many nDefin={0}>{1}".format(nDefine,nQueueLimit))
                return self.retThrottled
            elif nWaiting > nRunning*4 and nWaiting > nJobsInBunch*2:
                limitPriority = True
                # too many waiting
                tmpLog.debug("  done : SKIP too many nWaiting={0}>{1}".format(nWaiting,nRunning*4))
                return self.retThrottled
        # get jobs from prodDB
        limitPriorityValue = None
        if limitPriority:
            limitPriorityValue = highestPrioInPandaDB
            self.setMinPriority(limitPriorityValue)
        tmpLog.debug("   done : PASS - priority limit={0}".format(limitPriorityValue))
        return self.retUnThrottled
