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

    def __getConfiguration(self, queue_name, resource_name):

        # component name
        compName = 'prod_job_throttler'
        app = 'jedi'
        vo = 'atlas'

        # Avoid memory fragmentation
        if resource_name.startswith('MCORE'):
            resource_name = 'MCORE'
        elif resource_name.startswith('SCORE'):
            resource_name = 'SCORE'

        # QUEUE LIMIT
        # First try to get a wq + resource_name specific limit
        nQueueLimit = self.taskBufferIF.getConfigValue(compName, 'NQUEUELIMIT_{0}_{1}'.format(queue_name, resource_name),
                                                  app, vo)
        # Otherwise try to get a wq only specific limit
        if nQueueLimit is None:
            nQueueLimit = self.taskBufferIF.getConfigValue(compName, 'NQUEUELIMIT_{0}'.format(queue_name),
                                                      app, vo)

        # RUNNING CAP
        # First try to get a wq + resource_name specific limit
        nRunningCap = self.taskBufferIF.getConfigValue(compName, 'NRUNNINGCAP_{0}_{1}'.format(queue_name, resource_name),
                                                       app, vo)
        # Otherwise try to get a wq only specific limit
        if nRunningCap is None:
            nRunningCap = self.taskBufferIF.getConfigValue(compName, 'NRUNNINGCAP_{0}'.format(queue_name),
                                                           app, vo)

        # QUEUE CAP
        # First try to get a wq + resource_name specific limit
        nQueueCap = self.taskBufferIF.getConfigValue(compName, 'NQUEUECAP_{0}_{1}'.format(queue_name, resource_name),
                                                     app, vo)
        # Otherwise try to get a wq only specific limit
        if nQueueCap is None:
            nQueueCap = self.taskBufferIF.getConfigValue(compName, 'NQUEUECAP_{0}'.format(queue_name),
                                                         app, vo)

        return nQueueLimit, nRunningCap, nQueueCap

    # check if throttled
    def toBeThrottled(self, vo, prodSourceLabel, cloudName, workQueue, jobStat_agg, resource_name):
        # params
        nBunch = 4
        threshold = 2.0
        thresholdForSite = threshold - 1.0
        nJobsInBunchMax = 600
        nJobsInBunchMin = 500
        nJobsInBunchMaxES = 1000
        minTotalWalltime = 50*1000*1000
        nWaitingLimit = 4
        nWaitingBunchLimit = 2
        nParallel = 2
        nParallelCap = 5
        # make logger
        tmpLog = MsgWrapper(logger)

        workQueueID = workQueue.getID()
        workQueueName = workQueue.queue_name

        if workQueue.is_global_share:
            workQueueTag = workQueueName
        else:
            workQueueTag = workQueueID

        workQueueName = '_'.join(workQueue.queue_name.split(' '))
        msgHeader = '{0}:{1} cloud={2} queue={3} resource_type={4}:'.format(vo, prodSourceLabel, cloudName,
                                                                            workQueueName, resource_name)
        tmpLog.debug(msgHeader+' start workQueueID={0}'.format(workQueueID))

        # get central configuration values
        configQueueLimit, configQueueCap, configRunningCap = self.__getConfiguration(workQueue.queue_name, resource_name)
        tmpLog.debug(msgHeader + ' got configuration configQueueLimit={0}, configQueueCap={1}, configRunningCap={2}'
                     .format(configQueueLimit, configQueueCap, configRunningCap))

        # change threshold
        if workQueue.queue_name in ['mcore']:
            threshold = 5.0
        # check cloud status
        if not self.siteMapper.checkCloud(cloudName):
            msgBody = "SKIP cloud={0} undefined".format(cloudName)
            tmpLog.warning(msgHeader+" "+msgBody)
            tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning')
            return self.retThrottled
        cloudSpec = self.siteMapper.getCloud(cloudName)
        if cloudSpec['status'] in ['offline']:
            msgBody = "SKIP cloud.status={0}".format(cloudSpec['status'])
            tmpLog.warning(msgHeader+" "+msgBody)
            tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning')
            return self.retThrottled
        if cloudSpec['status'] in ['test']:
            if workQueue.queue_name != 'test':
                msgBody = "SKIP cloud.status={0} for non test queue ({1})".format(cloudSpec['status'],
                                                                                  workQueue.queue_name)
                tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning')
                tmpLog.warning(msgHeader+" "+msgBody)
                return self.retThrottled
        # check if unthrottled
        if not workQueue.throttled:
            msgBody = "PASS unthrottled since GS_throttled is False"
            tmpLog.debug(msgHeader+" "+msgBody)
            return self.retUnThrottled

        # count number of jobs in each status
        nRunning = 0
        nNotRun  = 0
        nDefine  = 0
        nWaiting = 0

        if jobStat_agg.has_key(workQueueTag):
            tmpLog.debug(msgHeader+" "+str(jobStat_agg[workQueueTag]))
            for pState,pNumber in jobStat_agg[workQueueTag].iteritems():
                if pState in ['running']:
                    nRunning += pNumber
                elif pState in ['assigned','activated','starting']:
                    nNotRun  += pNumber
                elif pState in ['defined']:
                    nDefine  += pNumber
                elif pState in ['waiting']:
                    nWaiting += pNumber

        # check if higher prio tasks are waiting
        tmpStat, highestPrioJobStat = self.taskBufferIF.getHighestPrioJobStat_JEDI('managed', cloudName, workQueue)
        highestPrioInPandaDB = highestPrioJobStat['highestPrio']
        nNotRunHighestPrio   = highestPrioJobStat['nNotRun']
        # the highest priority of waiting tasks 
        highestPrioWaiting = self.taskBufferIF.checkWaitingTaskPrio_JEDI(vo, workQueue, 'managed', cloudName)
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
        #    # TODO: review this case
        #    if workQueue.queue_name in ['evgensimul']:
        #        # use higher limit for evgensimul
        #        if tmpRemainingSlot < nJobsInBunchMaxES:
        #            nJobsInBunch = tmpRemainingSlot
        #        else:
        #            nJobsInBunch = nJobsInBunchMaxES
        #    else:
            if tmpRemainingSlot < nJobsInBunchMax:
                nJobsInBunch = tmpRemainingSlot
            else:
                nJobsInBunch = nJobsInBunchMax

        nQueueLimit = nJobsInBunch*nBunch
        if configQueueLimit is not None:
            nQueueLimit = configQueueLimit
        # use nPrestage for reprocessing
        if workQueue.queue_name in ['Heavy Ion', 'Reprocessing default']:
            # reset nJobsInBunch
            if nQueueLimit > (nNotRun + nDefine):
                tmpRemainingSlot = nQueueLimit - (nNotRun + nDefine)
                if tmpRemainingSlot < nJobsInBunch:
                    pass
                elif tmpRemainingSlot < nJobsInBunchMax:
                    nJobsInBunch = tmpRemainingSlot
                else:
                    nJobsInBunch = nJobsInBunchMax
        # get cap
        # set number of jobs to be submitted
        if configQueueCap is None:
            self.setMaxNumJobs(nJobsInBunch/nParallel)
        else:
            self.setMaxNumJobs(configQueueCap/nParallelCap)
        # get total walltime
        totWalltime = self.taskBufferIF.getTotalWallTime_JEDI(vo,prodSourceLabel,workQueue,cloudName)
        # check number of jobs when high priority jobs are not waiting. test jobs are sent without throttling
        limitPriority = False
        tmpStr = msgHeader+" nQueueLimit={0} nQueued={1} nDefine={2} nRunning={3} totWalltime={4} nRunCap={5} nQueueCap={6}"
        tmpLog.info(tmpStr.format(nQueueLimit, nNotRun+nDefine, nDefine, nRunning, totWalltime, configRunningCap, configQueueCap))
        # check
        if nRunning == 0 and (nNotRun+nDefine) > nQueueLimit and (totWalltime == None or totWalltime > minTotalWalltime):
            limitPriority = True
            if not highPrioQueued:
                # pilot is not running or DDM has a problem
                msgBody = "SKIP no running and enough nQueued({0})>{1} totWalltime({2})>{3} ".format(nNotRun+nDefine,nQueueLimit,
                                                                                                     totWalltime,minTotalWalltime)
                tmpLog.warning(msgHeader+" "+msgBody)
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
                tmpLog.warning(msgHeader+" "+msgBody)
                tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning',escapeChar=True)
                return self.retMergeUnThr
        elif nDefine > nQueueLimit:
            limitPriority = True
            if not highPrioQueued:
                # brokerage is stuck
                msgBody = "SKIP too many nDefined({0})>{1}".format(nDefine,nQueueLimit)
                tmpLog.warning(msgHeader+" "+msgBody)
                tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning',escapeChar=True)
                return self.retMergeUnThr
        elif nWaiting > nRunning*nWaitingLimit and nWaiting > nJobsInBunch*nWaitingBunchLimit:
            limitPriority = True
            if not highPrioQueued:
                # too many waiting
                msgBody = "SKIP too many nWaiting({0})>max(nRunning({1})x{2},{3}x{4})".format(nWaiting,nRunning,nWaitingLimit,
                                                                                              nJobsInBunch,nWaitingBunchLimit)
                tmpLog.warning(msgHeader+" "+msgBody)
                tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning',escapeChar=True)
                return self.retMergeUnThr
        elif configRunningCap is not None and nRunning > configRunningCap:
            # cap on running
            msgBody = "SKIP nRunning({0})>nRunningCap({1})".format(nRunning, configRunningCap)
            tmpLog.warning('{0} {1}'.format(msgHeader, msgBody))
            tmpLog.sendMsg('{0} {1}'.format(msgHeader, msgBody), self.msgType, msgLevel='warning', escapeChar=True)
            return self.retMergeUnThr
        elif configQueueCap is not None and nNotRun+nDefine > configQueueCap:
            limitPriority = True
            if not highPrioQueued:
                # cap on queued
                msgBody = "SKIP nQueue({0})>nQueueCap({1})".format(nNotRun+nDefine,configQueueCap)
                tmpLog.warning(msgHeader+" "+msgBody)
                tmpLog.sendMsg(msgHeader+' '+msgBody,self.msgType,msgLevel='warning',escapeChar=True)
                return self.retMergeUnThr
        # get jobs from prodDB
        limitPriorityValue = None
        if limitPriority:
            limitPriorityValue = highestPrioWaiting
            self.setMinPriority(limitPriorityValue)
        else:
            # not enough jobs are queued
            if nNotRun + nDefine < max(nQueueLimit*0.9, nRunning):
                tmpLog.debug(msgHeader+" not enough jobs queued")
                self.notEnoughJobsQueued()
                self.setMaxNumJobs(max(self.maxNumJobs,nQueueLimit/20))
        msgBody = "PASS - priority limit={0} maxNumJobs={1}".format(limitPriorityValue, self.maxNumJobs)
        tmpLog.debug(msgHeader+" "+msgBody)
        return self.retUnThrottled
