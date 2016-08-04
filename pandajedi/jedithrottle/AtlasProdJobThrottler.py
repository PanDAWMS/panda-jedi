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
        nJobsInBunchMax = 600
        nJobsInBunchMin = 500
        nJobsInBunchMaxES = 1000
        if workQueue.criteria != None and 'site' in workQueue.criteria:
            minTotalWalltime = 10*1000*1000
        else:
            minTotalWalltime = 50*1000*1000
        nWaitingLimit = 4
        nWaitingBunchLimit = 2
        nParallel = 2
        # make logger
        tmpLog = MsgWrapper(logger)
        workQueueIDs = workQueue.getIDs()
        msgHeader = '{0}:{1} cloud={2} queue={3}:'.format(vo,prodSourceLabel,cloudName,workQueue.queue_name)
        tmpLog.debug(msgHeader+' start workQueueID={0}'.format(str(workQueueIDs)))
        # change threashold
        if workQueue.queue_name in ['mcore']:
            threshold = 5.0
        # check cloud status
        if not self.siteMapper.checkCloud(cloudName):
            msgBody = "SKIP cloud={0} undefined".format(cloudName)
            tmpLog.debug(msgHeader+" "+msgBody)
            tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning')
            return self.retThrottled
        cloudSpec = self.siteMapper.getCloud(cloudName)
        if cloudSpec['status'] in ['offline']:
            msgBody = "SKIP cloud.status={0}".format(cloudSpec['status'])
            tmpLog.debug(msgHeader+" "+msgBody)
            tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning')
            return self.retThrottled
        if cloudSpec['status'] in ['test']:
            if workQueue.queue_name != 'test':
                msgBody = "SKIP cloud.status={0} for non test queue ({1})".format(cloudSpec['status'],
                                                                                  workQueue.queue_name)
                tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning')
                tmpLog.debug(msgHeader+" "+msgBody)
                return self.retThrottled
        # check if unthrottled
        if workQueue.queue_share == None:
            msgBody = "PASS unthrottled since share=None"
            tmpLog.debug(msgHeader+" "+msgBody)
            return self.retUnThrottled
        # count number of jobs in each status
        nRunning = 0
        nNotRun  = 0
        nDefine  = 0
        nWaiting = 0
        for workQueueID in workQueueIDs:
            if jobStat.has_key(cloudName) and \
                   jobStat[cloudName].has_key(workQueueID):
                tmpLog.debug(msgHeader+" "+str(jobStat[cloudName][workQueueID]))
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
            msgBody = 'failed to get the highest priority of waiting tasks'
            tmpLog.error(msgHeader+" "+msgBody)
            return self.retTmpError
        # high priority tasks are waiting
        highPrioQueued = False
        if highestPrioWaiting > highestPrioInPandaDB or (highestPrioWaiting == highestPrioInPandaDB and \
                                                         nNotRunHighestPrio < nJobsInBunchMin):
            highPrioQueued = True
        tmpLog.debug(msgHeader+" highestPrio waiting:{0} inPanda:{1} numNotRun:{2} -> highPrioQueued={3}".format(highestPrioWaiting,
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
        if workQueue.queue_name == 'group':
            nQueueLimit = 40000
        if workQueue.queue_name == 'eventservice':
            nQueueLimit = 2000
        if workQueue.queue_name == 'opportunistic':
            nQueueLimit = 10000
        if workQueue.queue_name == 'titan':
            nQueueLimit = 5000
        # use nPrestage for reprocessing   
        if workQueue.queue_name in ['reprocessing','mcore_repro']:
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
        self.setMaxNumJobs(nJobsInBunch/nParallel)
        # get total walltime
        totWalltime = self.taskBufferIF.getTotalWallTime_JEDI(vo,prodSourceLabel,workQueue,cloudName)
        # check number of jobs when high priority jobs are not waiting. test jobs are sent without throttling
        limitPriority = False
        tmpLog.debug(msgHeader+" nQueueLimit:{0} nQueued:{1} nDefine:{2} nRunning:{3} totWalltime:{4}".format(nQueueLimit,
                                                                                                              nNotRun+nDefine,
                                                                                                              nDefine,
                                                                                                              nRunning,
                                                                                                              totWalltime))
        # check
        if nRunning == 0 and (nNotRun+nDefine) > nQueueLimit and (totWalltime == None or totWalltime > minTotalWalltime):
            limitPriority = True
            if not highPrioQueued:
                # pilot is not running or DDM has a problem
                msgBody = "SKIP no running and enough nQueued({0})>{1} totWalltime({2})>{3} ".format(nNotRun+nDefine,nQueueLimit,
                                                                                                     totWalltime,minTotalWalltime)
                tmpLog.debug(msgHeader+" "+msgBody)
                tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning',escapeChar=True)
                return self.retMergeUnThr
        elif nRunning != 0 and float(nNotRun+nDefine)/float(nRunning) > threshold and \
                (nNotRun+nDefine) > nQueueLimit and (totWalltime == None or totWalltime > minTotalWalltime):
            limitPriority = True
            if not highPrioQueued:
                # enough jobs in Panda
                msgBody = "SKIP nQueued({0})/nRunning({1})>{2} & nQueued({3})>{4} totWalltime({5})>{6}".format(nNotRun+nDefine,nRunning,
                                                                                                               threshold,nNotRun+nDefine,
                                                                                                               nQueueLimit,
                                                                                                               totWalltime,minTotalWalltime)
                tmpLog.debug(msgHeader+" "+msgBody)
                tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning',escapeChar=True)
                return self.retMergeUnThr
        elif nDefine > nQueueLimit:
            limitPriority = True
            if not highPrioQueued:
                # brokerage is stuck
                msgBody = "SKIP too many nDefined({0})>{1}".format(nDefine,nQueueLimit)
                tmpLog.debug(msgHeader+" "+msgBody)
                tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning',escapeChar=True)
                return self.retMergeUnThr
        elif nWaiting > nRunning*nWaitingLimit and nWaiting > nJobsInBunch*nWaitingBunchLimit:
            limitPriority = True
            if not highPrioQueued:
                # too many waiting
                msgBody = "SKIP too many nWaiting({0})>max(nRunning({1})x{2},{3}x{4})".format(nWaiting,nRunning,nWaitingLimit,
                                                                                              nJobsInBunch,nWaitingBunchLimit)
                tmpLog.debug(msgHeader+" "+msgBody)
                tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning',escapeChar=True)
                return self.retMergeUnThr
        # get jobs from prodDB
        limitPriorityValue = None
        if limitPriority:
            limitPriorityValue = highestPrioWaiting
            self.setMinPriority(limitPriorityValue)
        else:
            # not enough jobs are queued
            if nNotRun+nDefine < max(nQueueLimit,nRunning) or (totWalltime != None and totWalltime < minTotalWalltime):
                tmpLog.debug(msgHeader+" not enough jobs queued")
                self.notEnoughJobsQueued()
                self.setMaxNumJobs(max(self.maxNumJobs,nQueueLimit/20))
        msgBody = "PASS - priority limit={0}".format(limitPriorityValue)
        tmpLog.debug(msgHeader+" "+msgBody)
        return self.retUnThrottled
