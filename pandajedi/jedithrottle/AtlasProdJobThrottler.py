from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from JobThrottlerBase import JobThrottlerBase

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])

LEVEL_None = 0 # There is no configuration defined
LEVEL_GS = 1 # There is a configuration defined at global share level
LEVEL_RT = 2 # There is a configuration defined at resource type level
NQUEUELIMIT = 'NQUEUELIMIT'
NRUNNINGCAP = 'NRUNNINGCAP'
NQUEUECAP = 'NQUEUECAP'

# class to throttle ATLAS production jobs
class AtlasProdJobThrottler (JobThrottlerBase):

    # constructor
    def __init__(self,taskBufferIF):
        JobThrottlerBase.__init__(self,taskBufferIF)

    def __getConfiguration(self, vo, queue_name, resource_name):

        # component name
        compName = 'prod_job_throttler'
        app = 'jedi'

        # Avoid memory fragmentation
        if resource_name.startswith('MCORE'):
            resource_name = 'MCORE'
        elif resource_name.startswith('SCORE'):
            resource_name = 'SCORE'

        # Read the WQ config values from the DB
        config_map = {
                        NQUEUELIMIT: {'value': None, 'level': LEVEL_None},
                        NRUNNINGCAP: {'value': None, 'level': LEVEL_None},
                        NQUEUECAP: {'value': None, 'level': LEVEL_None}
                      }
        for tag in (NQUEUELIMIT, NRUNNINGCAP, NQUEUECAP):
            # First try to get a wq + resource_name specific limit
            value = self.taskBufferIF.getConfigValue(compName, '{0}_{1}_{2}'.format(tag, queue_name, resource_name), app, vo)
            if value:
                config_map[tag] = {'value': value, 'level': LEVEL_RT}
            # Otherwise try to get a wq only specific limit
            else:
                value = self.taskBufferIF.getConfigValue(compName, '{0}_{1}'.format(tag, queue_name), app, vo)
                if value:
                    config_map[tag] = {'value': value, 'level': LEVEL_GS}

        return config_map


    def __prepareJobStats(self, workQueue, resource_name, config_map):
        """
        Calculates the jobs at resource level (SCORE or MCORE) and in total.

        :param workQueue: workqueue object
        :param resource_name: resource name, e.g. SCORE, MCORE, SCORE_HIMEM, MCORE_HIMEM
        :return: resource_level, nRunning, nRunning_level, nNotRun, nNotRun_level, nDefine, nDefine_level, nWaiting, nWaiting_level
        """
        # SCORE vs MCORE
        if resource_name.startswith('MCORE'):
            resource_level = 'MCORE'
        else:
            resource_level = 'SCORE'

        # get job statistics
        status, wq_stats = self.taskBufferIF.getJobStatisticsByResourceType(workQueue)
        if not status:
            raise RuntimeError, 'failed to get job statistics'

        # Count number of jobs in each status
        # We want to generate one value for the total, one value for the relevant MCORE/SCORE level
        # and one value for the full global share
        nRunning, nRunning_level, nRunning_gs = 0, 0, 0
        nNotRun, nNotRun_level, nNotRun_gs = 0, 0, 0
        nDefine, nDefine_level, nDefine_gs = 0, 0, 0
        nWaiting, nWaiting_level, nWaiting_gs = 0, 0, 0

        for status in wq_stats:
            nJobs, nJobs_level, nJobs_gs = 0, 0, 0
            for resource_type, count in wq_stats[status].items():
                if resource_type == resource_name:
                    nJobs = count
                if resource_type.startswith(resource_level):
                    nJobs_level += count
                nJobs_gs += count

            if status == 'running'
                nRunning = nJobs
                nRunning_level = nJobs_level
                nRunning_gs = nJobs_gs
            elif status == 'defined':
                nDefine = nJobs
                nDefine_level = nJobs_level
                nDefine_gs = nJobs_gs
            elif status == 'waiting':
                nWaiting = nJobs
                nWaiting_level = nJobs_level
                nWaiting_gs = nJobs_gs
            elif status in ['assigned','activated','starting']:
                nNotRun = nJobs
                nNotRun_level += nJobs_level
                nNotRun_gs += nJobs_gs

        # Get the job stats at the same level as the configured parameters
        # nRunning is compared with the nRunningCap
        if config_map[NRUNNINGCAP]['level']==LEVEL_GS:
            nRunning_runningcap = nRunning_gs
        elif config_map[NRUNNINGCAP]['level']==LEVEL_RT:
            nRunning_runningcap = nRunning_level
        else:
            nRunning_runningcap = nRunning

        # nNotRun and nDefine is compared with the nQueueLimit
        if config_map[NQUEUELIMIT]['level']==LEVEL_GS:
            nNotRun_queuelimit = nNotRun_gs
            nDefine_queuelimit = nDefine_gs
        elif config_map[NQUEUELIMIT]['level']==LEVEL_RT:
            nNotRun_queuelimit = nNotRun_level
            nDefine_queuelimit = nDefine_level
        else:
            nNotRun_queuelimit = nNotRun
            nDefine_queuelimit = nDefine

        # nNotRun and nDefine is compared with the nQueueCap
        if config_map[NQUEUECAP]['level']==LEVEL_GS:
            nNotRun_queuecap = nNotRun_gs
            nDefine_queuecap = nDefine_gs
        elif config_map[NQUEUECAP]['level']==LEVEL_RT:
            nNotRun_queuecap = nNotRun_gs
            nDefine_queuecap = nDefine_gs
        else:
            nNotRun_queuecap = nNotRun_gs
            nDefine_queuecap = nDefine_gs

        return_map = {'nRunning': nRunning,
                      'nRunning_runningcap': nRunning_runningcap,
                      'nNotRun': nNotRun,
                      'nNotRun_queuelimit': nNotRun_queuelimit,
                      'nNotRun_queuecap': nNotRun_queuecap,
                      'nDefine': nDefine,
                      'nDefine_queuelimit': nDefine_queuelimit,
                      'nDefine_queuecap': nDefine_queuecap,
                      'nWaiting': nWaiting
                      }

        return return_map



    # check if throttled
    def toBeThrottled(self, vo, prodSourceLabel, cloudName, workQueue, resource_name):
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
        tmpLog.debug('{0} start workQueueID={1}'.format(msgHeader, workQueueID))

        # get central configuration values
        configQueueLimit, configQueueCap, configRunningCap = self.__getConfiguration(vo, workQueue.queue_name, resource_name)
        tmpLog.debug(msgHeader + ' got configuration configQueueLimit={0}, configQueueCap={1}, configRunningCap={2}'
                     .format(configQueueLimit, configQueueCap, configRunningCap))

        # change threshold
        # OBSOLETE WITH GS-WQ ALIGNMENT
        # if workQueue.queue_name in ['mcore']:
        #    threshold = 5.0

        # check cloud status
        if not self.siteMapper.checkCloud(cloudName):
            msgBody = "SKIP cloud={0} undefined".format(cloudName)
            tmpLog.warning("{0} {1}".format(msgHeader, msgBody))
            tmpLog.sendMsg("{0} {1}".format(msgHeader, msgBody),self.msgType,msgLevel='warning')
            return self.retThrottled
        cloudSpec = self.siteMapper.getCloud(cloudName)
        if cloudSpec['status'] in ['offline']:
            msgBody = "SKIP cloud.status={0}".format(cloudSpec['status'])
            tmpLog.warning("{0} {1}".format(msgHeader, msgBody))
            tmpLog.sendMsg("{0} {1}".format(msgHeader, msgBody),self.msgType,msgLevel='warning')
            return self.retThrottled
        if cloudSpec['status'] in ['test']:
            if workQueue.queue_name != 'test':
                msgBody = "SKIP cloud.status={0} for non test queue ({1})".format(cloudSpec['status'],
                                                                                  workQueue.queue_name)
                tmpLog.sendMsg("{0} {1}".format(msgHeader, msgBody), self.msgType, msgLevel='warning')
                tmpLog.warning("{0} {1}".format(msgHeader, msgBody))
                return self.retThrottled
        # check if unthrottled
        if not workQueue.throttled:
            msgBody = "PASS unthrottled since GS_throttled is False"
            tmpLog.debug(msgHeader+" "+msgBody)
            return self.retUnThrottled

        # get the jobs statistics for our wq/gs and expand the stats map
        jobstats_map = self.__getJobStats(workQueue, resource_name)
        nRunning = jobstats_map['nRunning']
        nRunning_runningcap = jobstats_map['nRunning_runningcap']
        nNotRun = jobstats_map['nNotRun']
        nNotRun_queuelimit = jobstats_map['nNotRun_queuelimit']
        nNotRun_queuecap = jobstats_map['nNotRun_queuecap']
        nDefine = jobstats_map['nDefine']
        nDefine_queuelimit = jobstats_map['nDefine_queuelimit']
        nDefine_queuecap = jobstats_map['nDefine_queuecap']
        nWaiting = jobstats_map['nWaiting']

        # check if higher prio tasks are waiting
        tmpStat, highestPrioJobStat = self.taskBufferIF.getHighestPrioJobStat_JEDI('managed', cloudName, workQueue, resource_name)
        highestPrioInPandaDB = highestPrioJobStat['highestPrio']
        nNotRunHighestPrio   = highestPrioJobStat['nNotRun']
        # the highest priority of waiting tasks 
        highestPrioWaiting = self.taskBufferIF.checkWaitingTaskPrio_JEDI(vo, workQueue, 'managed', cloudName)
        if highestPrioWaiting == None:
            msgBody = 'failed to get the highest priority of waiting tasks'
            tmpLog.error("{0} {1}".format(msgHeader, msgBody))
            return self.retTmpError
        # high priority tasks are waiting
        highPrioQueued = False
        if highestPrioWaiting > highestPrioInPandaDB or (highestPrioWaiting == highestPrioInPandaDB and \
                                                         nNotRunHighestPrio < nJobsInBunchMin):
            highPrioQueued = True
        tmpLog.debug("{0} highestPrio waiting:{1} inPanda:{2} numNotRun:{3} -> highPrioQueued={4}".format(msgHeader,
                                                                                                          highestPrioWaiting,
                                                                                                          highestPrioInPandaDB,
                                                                                                          nNotRunHighestPrio,
                                                                                                          highPrioQueued))
        # set maximum number of jobs to be submitted
        tmpRemainingSlot = int(nRunning * threshold - nNotRun)
        # use the lower limit to avoid creating too many _sub/_dis datasets
        nJobsInBunch = min(max(nJobsInBunchMin, tmpRemainingSlot), nJobsInBunchMax)

        if configQueueLimit is not None:
            nQueueLimit = configQueueLimit
        else:
            nQueueLimit = nJobsInBunch * nBunch

        # use nPrestage for reprocessing
        if workQueue.queue_name in ['Heavy Ion', 'Reprocessing default']:
            # reset nJobsInBunch
            if nQueueLimit > (nNotRun + nDefine):
                tmpRemainingSlot = nQueueLimit - (nNotRun + nDefine)
                if tmpRemainingSlot > nJobsInBunch:
                    nJobsInBunch = min(tmpRemainingSlot, nJobsInBunchMax)

        # get cap
        # set number of jobs to be submitted
        if configQueueCap is None:
            self.setMaxNumJobs(nJobsInBunch / nParallel)
        else:
            self.setMaxNumJobs(configQueueCap / nParallelCap)

        # get total walltime
        totWalltime = self.taskBufferIF.getTotalWallTime_JEDI(vo, prodSourceLabel, workQueue, resource_name, cloudName)

        # log the current situation and limits
        tmpLog.info("{0} nQueueLimit={1} nRunCap={2} nQueueCap={3}".format(msgHeader, nQueueLimit,
                                                                           configRunningCap, configQueueCap))
        tmpLog.info("{0} nQueued={1} nDefine={2} nRunning={3} totWalltime={4}".format(msgHeader, nNotRun + nDefine,
                                                                                      nDefine, nRunning, totWalltime))

        # check number of jobs when high priority jobs are not waiting. test jobs are sent without throttling
        limitPriority = False
        if nRunning == 0 and (nNotRun_queuelimit + nDefine_queuelimit) > nQueueLimit \
                and (totWalltime == None or totWalltime > minTotalWalltime):
            limitPriority = True
            if not highPrioQueued:
                # pilot is not running or DDM has a problem
                msgBody = "SKIP no running and enough nQueued_queuelimit({0})>{1} totWalltime({2})>{3} ".format(nNotRun_queuelimit + nDefine_queuelimit,
                                                                                                     nQueueLimit,
                                                                                                     totWalltime, minTotalWalltime)
                tmpLog.warning("{0} {1}".format(msgHeader, msgBody))
                tmpLog.sendMsg("{0} {1}".format(msgHeader, msgBody),self.msgType, msgLevel='warning', escapeChar=True)
                return self.retMergeUnThr

        elif nRunning != 0 and float(nNotRun + nDefine) / float(nRunning) > threshold and \
                (nNotRun_queuelimit + nDefine_queuelimit) > nQueueLimit and (totWalltime == None or totWalltime > minTotalWalltime):
            limitPriority = True
            if not highPrioQueued:
                # enough jobs in Panda
                msgBody = "SKIP nQueued({0})/nRunning({1})>{2} & nQueued_queuelimit({3})>{4} totWalltime({5})>{6}".format(nNotRun + nDefine, nRunning,
                                                                                                               threshold, nNotRun_queuelimit + nDefine_queuelimit,
                                                                                                               nQueueLimit, totWalltime,
                                                                                                               minTotalWalltime)
                tmpLog.warning("{0} {1}".format(msgHeader, msgBody))
                tmpLog.sendMsg("{0} {1}".format(msgHeader, msgBody), self.msgType, msgLevel='warning', escapeChar=True)
                return self.retMergeUnThr

        elif nDefine_queuelimit > nQueueLimit:
            limitPriority = True
            if not highPrioQueued:
                # brokerage is stuck
                msgBody = "SKIP too many nDefined_queuelimit({0})>{1}".format(nDefine_queuelimit, nQueueLimit)
                tmpLog.warning("{0} {1}".format(msgHeader, msgBody))
                tmpLog.sendMsg("{0} {1}".format(msgHeader, msgBody), self.msgType, msgLevel='warning', escapeChar=True)
                return self.retMergeUnThr

        elif nWaiting > max(nRunning * nWaitingLimit, nJobsInBunch * nWaitingBunchLimit):
            limitPriority = True
            if not highPrioQueued:
                # too many waiting
                msgBody = "SKIP too many nWaiting({0})>max(nRunning({1})x{2},{3}x{4})".format(nWaiting, nRunning, nWaitingLimit,
                                                                                              nJobsInBunch, nWaitingBunchLimit)
                tmpLog.warning("{0} {1}".format(msgHeader, msgBody))
                tmpLog.sendMsg("{0} {1}".format(msgHeader, msgBody), self.msgType, msgLevel='warning', escapeChar=True)
                return self.retMergeUnThr

        elif configRunningCap and nRunning_runningcap > configRunningCap:
            # cap on running
            msgBody = "SKIP nRunning_runningcap({0})>nRunningCap({1})".format(nRunning_runningcap, configRunningCap)
            tmpLog.warning('{0} {1}'.format(msgHeader, msgBody))
            tmpLog.sendMsg('{0} {1}'.format(msgHeader, msgBody), self.msgType, msgLevel='warning', escapeChar=True)
            return self.retMergeUnThr

        elif configQueueCap and nNotRun_queuecap + nDefine_queuecap > configQueueCap:
            limitPriority = True
            if not highPrioQueued:
                # cap on queued
                msgBody = "SKIP nQueued_queuecap({0})>nQueueCap({1})".format(nNotRun_queuecap + nDefine_queuecap, configQueueCap)
                tmpLog.warning("{0} {1}".format(msgHeader, msgBody))
                tmpLog.sendMsg("{0} {1}".format(msgHeader, msgBody), self.msgType, msgLevel='warning', escapeChar=True)
                return self.retMergeUnThr

        # get jobs from prodDB
        limitPriorityValue = None
        if limitPriority:
            limitPriorityValue = highestPrioWaiting
            self.setMinPriority(limitPriorityValue)
        else:
            # not enough jobs are queued
            if nNotRun_queuelimit + nDefine_queuelimit < nQueueLimit * 0.9 or nNotRun + nDefine < nRunning:
                tmpLog.debug(msgHeader+" not enough jobs queued")
                self.notEnoughJobsQueued()
                self.setMaxNumJobs(max(self.maxNumJobs, nQueueLimit/20))

        msgBody = "PASS - priority limit={0} maxNumJobs={1}".format(limitPriorityValue, self.maxNumJobs)
        tmpLog.debug(msgHeader+" "+msgBody)
        return self.retUnThrottled
