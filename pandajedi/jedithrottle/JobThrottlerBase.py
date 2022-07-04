from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore import Interaction

# throttle level
THR_LEVEL5 = 5

LEVEL_None = 0 # There is no configuration defined
LEVEL_GS = 1 # There is a configuration defined at global share level
LEVEL_MS = 2 # There is a configuration defined at MCORE/SCORE level
LEVEL_RT = 3 # There is a configuration defined at resource type level

NQUEUELIMIT = 'NQUEUELIMIT'
NRUNNINGCAP = 'NRUNNINGCAP'
NQUEUECAP = 'NQUEUECAP'

# workqueues that do not work at resource type level.
# E.g. event service is a special case, since MCORE tasks generate SCORE jobs. Therefore we can't work at
# resource type level and need to go to the global level, in order to avoid over-generating jobs
non_rt_wqs = ['eventservice']

# base class for job throttle
class JobThrottlerBase(object):

    def __init__(self, taskBufferIF):
        self.taskBufferIF = taskBufferIF
        # returns
        self.retTmpError = self.SC_FAILED, True
        self.retThrottled = self.SC_SUCCEEDED, True
        self.retUnThrottled = self.SC_SUCCEEDED, False
        self.retMergeUnThr = self.SC_SUCCEEDED, THR_LEVEL5
        # limit
        self.refresh()
        self.msgType = 'jobthrottler'
        self.comp_name = 'base_job_throttler'
        self.app = 'jedi'

    # refresh
    def refresh(self):
        self.maxNumJobs = None
        self.minPriority = None
        self.underNqLimit = False
        self.siteMapper = self.taskBufferIF.getSiteMapper()

    # set maximum number of jobs to be submitted
    def setMaxNumJobs(self, maxNumJobs):
        self.maxNumJobs = maxNumJobs

    # set min priority of jobs to be submitted
    def setMinPriority(self, minPriority):
        self.minPriority = minPriority

    # check throttle level
    def mergeThrottled(self, thrLevel):
        # un-leveled flag
        if thrLevel in [True, False]:
            return thrLevel
        return thrLevel > THR_LEVEL5

    # check if lack of jobs
    def lackOfJobs(self):
        return self.underNqLimit

    # not enough jobs are queued
    def notEnoughJobsQueued(self):
        self.underNqLimit = True

    def __getConfiguration(self, vo, queue_name, resource_name):

        # component name
        comp_name = self.comp_name
        app = self.app

        # Avoid memory fragmentation
        resource_ms = None
        if resource_name.startswith('MCORE'):
            resource_ms = 'MCORE'
        elif resource_name.startswith('SCORE'):
            resource_ms = 'SCORE'

        # Read the WQ config values from the DB
        config_map = {
            NQUEUELIMIT: {'value': None, 'level': LEVEL_None},
            NRUNNINGCAP: {'value': None, 'level': LEVEL_None},
            NQUEUECAP: {'value': None, 'level': LEVEL_None}
        }

        for tag in (NQUEUELIMIT, NRUNNINGCAP, NQUEUECAP):
            # 1. try to get a wq + resource_type specific limit
            value = self.taskBufferIF.getConfigValue(comp_name, '{0}_{1}_{2}'.format(tag, queue_name, resource_name),
                                                     app, vo)
            if value:
                config_map[tag] = {'value': value, 'level': LEVEL_RT}
                continue

            # 2. try to get a wq + MCORE/SCORE specific limit
            value = self.taskBufferIF.getConfigValue(comp_name, '{0}_{1}_{2}*'.format(tag, queue_name, resource_ms),
                                                     app, vo)
            if value:
                config_map[tag] = {'value': value, 'level': LEVEL_MS}
                continue

            # 3. try to get a wq specific limit
            value = self.taskBufferIF.getConfigValue(comp_name, '{0}_{1}'.format(tag, queue_name), app, vo)
            if value:
                config_map[tag] = {'value': value, 'level': LEVEL_GS}

        return config_map

    def __prepareJobStats(self, work_queue, resource_name, config_map):
        """
        Calculates the jobs at resource level (SCORE or MCORE) and in total.

        :param work_queue: work_queue object
        :param resource_name: resource name, e.g. SCORE, MCORE, SCORE_HIMEM, MCORE_HIMEM
        :return: resource_level, nRunning, nRunning_level, nNotRun, nNotRun_level, nDefine, nDefine_level, nWaiting, nWaiting_level
        """
        # SCORE vs MCORE
        if resource_name.startswith('MCORE'):
            ms = 'MCORE'
        else:
            ms = 'SCORE'

        # get job statistics
        status, wq_stats = self.taskBufferIF.getJobStatisticsByResourceType(work_queue)
        if not status:
            raise RuntimeError('failed to get job statistics')

        # get the number of standby jobs which is used as the number of running jobs
        standby_num_static, standby_num_static_dynamic = self.taskBufferIF.getNumMapForStandbyJobs_JEDI(work_queue)

        # add running if the original stat doesn't have running and standby jobs are required
        if 'running' not in wq_stats and (len(standby_num_static) > 0 or len(standby_num_static_dynamic) > 0):
            wq_stats['running'] = {}

        # add dummy to subtract # of starting for dynamic number of standby jobs
        if len(standby_num_static_dynamic) > 0:
            wq_stats['dummy'] = standby_num_static_dynamic

        # Count number of jobs in each status
        # We want to generate one value for the total, one value for the relevant MCORE/SCORE level
        # and one value for the full global share
        nRunning_rt, nRunning_ms, nRunning_gs = 0, 0, 0
        nNotRun_rt, nNotRun_ms, nNotRun_gs = 0, 0, 0
        nDefine_rt, nDefine_ms, nDefine_gs = 0, 0, 0
        nWaiting_rt, nWaiting_gs = 0, 0

        for status in wq_stats:
            nJobs_rt, nJobs_ms, nJobs_gs = 0, 0, 0
            stats_list = list(wq_stats[status].items())
            # take into account the number of standby jobs
            if status == 'running':
                stats_list += list(standby_num_static.items())
                stats_list += list(standby_num_static_dynamic.items())
            for resource_type, count in stats_list:
                if resource_type == resource_name:
                    nJobs_rt = count
                if resource_type.startswith(ms):
                    nJobs_ms += count
                nJobs_gs += count

            if status == 'running':
                nRunning_rt = nJobs_rt
                nRunning_ms = nJobs_ms
                nRunning_gs = nJobs_gs
            elif status == 'defined':
                nDefine_rt = nJobs_rt
                nDefine_ms = nJobs_ms
                nDefine_gs = nJobs_gs
            elif status == 'waiting':
                nWaiting_rt = nJobs_rt
                nWaiting_gs = nJobs_gs
            elif status in ['assigned', 'activated', 'starting']:
                nNotRun_rt += nJobs_rt
                nNotRun_ms += nJobs_ms
                nNotRun_gs += nJobs_gs
            elif status == 'dummy':
                nNotRun_rt -= nJobs_rt
                nNotRun_ms -= nJobs_ms
                nNotRun_gs -= nJobs_gs


        # Get the job stats at the same level as the configured parameters
        # nRunning is compared with the nRunningCap
        if config_map[NRUNNINGCAP]['level'] == LEVEL_GS:
            nRunning_runningcap = nRunning_gs
        elif config_map[NRUNNINGCAP]['level'] == LEVEL_MS:
            nRunning_runningcap = nRunning_ms
        else:
            nRunning_runningcap = nRunning_rt

        # nNotRun and nDefine are compared with the nQueueLimit
        if config_map[NQUEUELIMIT]['level'] == LEVEL_GS:
            nNotRun_queuelimit = nNotRun_gs
            nDefine_queuelimit = nDefine_gs
        elif config_map[NQUEUELIMIT]['level'] == LEVEL_MS:
            nNotRun_queuelimit = nNotRun_ms
            nDefine_queuelimit = nDefine_ms
        else:
            nNotRun_queuelimit = nNotRun_rt
            nDefine_queuelimit = nDefine_rt

        # nNotRun and nDefine are compared with the nQueueCap
        if config_map[NQUEUECAP]['level'] == LEVEL_GS:
            nNotRun_queuecap = nNotRun_gs
            nDefine_queuecap = nDefine_gs
        elif config_map[NQUEUECAP]['level'] == LEVEL_MS:
            nNotRun_queuecap = nNotRun_ms
            nDefine_queuecap = nDefine_ms
        else:
            nNotRun_queuecap = nNotRun_rt
            nDefine_queuecap = nDefine_rt

        return_map = {'nRunning_rt': nRunning_rt, 'nRunning_gs': nRunning_gs,
                      'nRunning_runningcap': nRunning_runningcap,
                      'nNotRun_rt': nNotRun_rt, 'nNotRun_gs': nNotRun_gs,
                      'nNotRun_queuelimit': nNotRun_queuelimit, 'nNotRun_queuecap': nNotRun_queuecap,
                      'nDefine_rt': nDefine_rt, 'nDefine_gs': nDefine_gs,
                      'nDefine_queuelimit': nDefine_queuelimit,
                      'nDefine_queuecap': nDefine_queuecap,
                      'nWaiting_rt': nWaiting_rt, 'nWaiting_gs': nWaiting_gs
                      }

        return return_map

    # check if throttled
    def toBeThrottledBase(self, vo, prodSourceLabel, cloud_name, workQueue, resource_name, logger):

        workqueue_id = workQueue.getID()
        workqueue_name = '_'.join(workQueue.queue_name.split(' '))

        # params
        nBunch = 4
        threshold = self.taskBufferIF.getConfigValue(self.comp_name, 'THROTTLE_THRESHOLD', self.app, vo)
        if threshold is None:
            threshold = 2.0
        nJobsInBunchMax = 600
        nJobsInBunchMin = 500
        minTotalWalltime = 50 * 1000 * 1000
        nWaitingLimit = 4
        nWaitingBunchLimit = 2
        nParallel = 2
        nParallelCap = 5

        # make logger
        tmp_log = MsgWrapper(logger)
        msg_header = '{0}:{1} cloud={2} queue={3} resource_type={4}:'.format(vo, prodSourceLabel, cloud_name,
                                                                             workqueue_name, resource_name)
        tmp_log.debug('{} start workqueue_id={} threshold={}'.format(msg_header, workqueue_id, threshold))

        # check if unthrottled
        if not workQueue.throttled:
            msg_body = "PASS unthrottled since GS_throttled is False"
            tmp_log.info(msg_header+" "+msg_body)
            return self.retUnThrottled

        # get central configuration values
        config_map = self.__getConfiguration(vo, workQueue.queue_name, resource_name)
        configQueueLimit = config_map[NQUEUELIMIT]['value']
        configQueueCap = config_map[NQUEUECAP]['value']
        configRunningCap = config_map[NRUNNINGCAP]['value']

        tmp_log.debug(msg_header + ' got configuration configQueueLimit={0}, configQueueCap={1}, configRunningCap={2}'
                     .format(configQueueLimit, configQueueCap, configRunningCap))

        # get the jobs statistics for our wq/gs and expand the stats map
        jobstats_map = self.__prepareJobStats(workQueue, resource_name, config_map)

        tmp_log.debug('jobstats_map: {0}'.format(jobstats_map))
        nRunning_rt = jobstats_map['nRunning_rt']
        nRunning_gs = jobstats_map['nRunning_gs']
        nRunning_runningcap = jobstats_map['nRunning_runningcap']
        nNotRun_rt = jobstats_map['nNotRun_rt']
        nNotRun_gs = jobstats_map['nNotRun_gs']
        nNotRun_queuelimit = jobstats_map['nNotRun_queuelimit']
        nNotRun_queuecap = jobstats_map['nNotRun_queuecap']
        nDefine_rt = jobstats_map['nDefine_rt']
        nDefine_gs = jobstats_map['nDefine_gs']
        nDefine_queuelimit = jobstats_map['nDefine_queuelimit']
        nDefine_queuecap = jobstats_map['nDefine_queuecap']
        nWaiting_rt = jobstats_map['nWaiting_rt']
        nWaiting_gs = jobstats_map['nWaiting_gs']

        # check if higher prio tasks are waiting
        if workQueue.queue_name in non_rt_wqs:
            # find highest priority of currently defined jobs
            tmpStat, highestPrioJobStat = self.taskBufferIF.getHighestPrioJobStat_JEDI(prodSourceLabel, cloud_name, workQueue)
            # the highest priority of waiting tasks
            highestPrioWaiting = self.taskBufferIF.checkWaitingTaskPrio_JEDI(vo, workQueue, prodSourceLabel, cloud_name)
        else:
            # find highest priority of currently defined jobs
            tmpStat, highestPrioJobStat = self.taskBufferIF.getHighestPrioJobStat_JEDI(prodSourceLabel, cloud_name, workQueue, resource_name)
            # the highest priority of waiting tasks
            highestPrioWaiting = self.taskBufferIF.checkWaitingTaskPrio_JEDI(vo, workQueue, prodSourceLabel, cloud_name, resource_name)

        highestPrioInPandaDB = highestPrioJobStat['highestPrio']
        nNotRunHighestPrio   = highestPrioJobStat['nNotRun']
        if highestPrioWaiting is None:
            msg_body = 'failed to get the highest priority of waiting tasks'
            tmp_log.error("{0} {1}".format(msg_header, msg_body))
            return self.retTmpError

        # high priority tasks are waiting
        highPrioQueued = False
        if prodSourceLabel != 'user':
            if highestPrioWaiting > highestPrioInPandaDB \
                    or (highestPrioWaiting == highestPrioInPandaDB and nNotRunHighestPrio < nJobsInBunchMin):
                highPrioQueued = True
        tmp_log.debug("{0} highestPrio waiting:{1} inPanda:{2} numNotRun:{3} -> highPrioQueued={4}".format(msg_header,
                                                                                                          highestPrioWaiting,
                                                                                                          highestPrioInPandaDB,
                                                                                                          nNotRunHighestPrio,
                                                                                                          highPrioQueued))
        # set maximum number of jobs to be submitted
        if workQueue.queue_name in non_rt_wqs:
            tmpRemainingSlot = int(nRunning_gs * threshold - nNotRun_gs)
        else:
            tmpRemainingSlot = int(nRunning_rt * threshold - nNotRun_rt)
        # use the lower limit to avoid creating too many _sub/_dis datasets
        nJobsInBunch = min(max(nJobsInBunchMin, tmpRemainingSlot), nJobsInBunchMax)

        if configQueueLimit is not None:
            nQueueLimit = configQueueLimit
        else:
            nQueueLimit = nJobsInBunch * nBunch

        # use nPrestage for reprocessing
        if workQueue.queue_name in ['Heavy Ion', 'Reprocessing default']:
            # reset nJobsInBunch
            if nQueueLimit > (nNotRun_queuelimit + nDefine_queuelimit):
                tmpRemainingSlot = nQueueLimit - (nNotRun_queuelimit + nDefine_queuelimit)
                if tmpRemainingSlot > nJobsInBunch:
                    nJobsInBunch = min(tmpRemainingSlot, nJobsInBunchMax)

        # get cap
        # set number of jobs to be submitted
        if configQueueCap is None:
            self.setMaxNumJobs(nJobsInBunch // nParallel)
        else:
            self.setMaxNumJobs(configQueueCap // nParallelCap)

        # get total walltime
        totWalltime = self.taskBufferIF.getTotalWallTime_JEDI(vo, prodSourceLabel, workQueue, resource_name)

        # log the current situation and limits
        tmp_log.info("{0} nQueueLimit={1} nRunCap={2} nQueueCap={3}".format(msg_header, nQueueLimit,
                                                                           configRunningCap, configQueueCap))
        tmp_log.info("{0} at global share level: nQueued={1} nDefine={2} nRunning={3}".format(msg_header,
                                                                                             nNotRun_gs + nDefine_gs,
                                                                                             nDefine_gs, nRunning_gs))
        tmpMsg = ''
        if config_map[NQUEUECAP]['level'] == LEVEL_MS:
            tmpMsg = "{} at MCORE/SCORE level: ".format(msg_header)
            tmpMsg += "nQueued_ms={} ".format(nNotRun_queuecap)
        if config_map[NRUNNINGCAP]['level'] == LEVEL_MS:
            if not tmpMsg:
                tmpMsg = "{} at MCORE/SCORE level: ".format(msg_header)
            tmpMsg += "nRunning_ms={} ".format(nRunning_runningcap)
        if tmpMsg:
            tmp_log.info(tmpMsg)
        tmp_log.info("{0} at resource type level: nQueued_rt={1} nDefine_rt={2} nRunning_rt={3} totWalltime={4}".format(msg_header,
                                                                                                                nNotRun_rt + nDefine_rt,
                                                                                                                nDefine_rt, nRunning_rt,
                                                                                                                totWalltime))

        # check number of jobs when high priority jobs are not waiting. test jobs are sent without throttling
        limitPriority = False
        if workQueue.queue_name not in non_rt_wqs \
                and nRunning_rt == 0 and (nNotRun_queuelimit + nDefine_queuelimit) > nQueueLimit \
                and (totWalltime is None or totWalltime > minTotalWalltime):
            limitPriority = True
            if not highPrioQueued:
                # pilot is not running or DDM has a problem
                msg_body = "SKIP no running and enough nQueued_queuelimit({0})>{1} totWalltime({2})>{3} ".format(nNotRun_queuelimit + nDefine_queuelimit,
                                                                                                     nQueueLimit, totWalltime, minTotalWalltime)
                tmp_log.warning("{0} {1}".format(msg_header, msg_body))
                tmp_log.sendMsg("{0} {1}".format(msg_header, msg_body),self.msgType, msgLevel='warning', escapeChar=True)
                return self.retMergeUnThr

        elif workQueue.queue_name in non_rt_wqs \
                and nRunning_gs == 0 and (nNotRun_queuelimit + nDefine_queuelimit) > nQueueLimit:
            limitPriority = True
            if not highPrioQueued:
                # pilot is not running or DDM has a problem
                msg_body = "SKIP no running and enough nQueued_queuelimit({0})>{1} totWalltime({2})>{3} ".format(nNotRun_queuelimit + nDefine_queuelimit,
                                                                                                     nQueueLimit, totWalltime, minTotalWalltime)
                tmp_log.warning("{0} {1}".format(msg_header, msg_body))
                tmp_log.sendMsg("{0} {1}".format(msg_header, msg_body),self.msgType, msgLevel='warning', escapeChar=True)
                return self.retMergeUnThr

        elif workQueue.queue_name not in non_rt_wqs and  nRunning_rt != 0 \
                and float(nNotRun_rt + nDefine_rt) / float(nRunning_rt) > threshold and \
                (nNotRun_queuelimit + nDefine_queuelimit) > nQueueLimit and (totWalltime is None or totWalltime > minTotalWalltime):
            limitPriority = True
            if not highPrioQueued:
                # enough jobs in Panda
                msg_body = "SKIP nQueued_rt({0})/nRunning_rt({1})>{2} & nQueued_queuelimit({3})>{4} totWalltime({5})>{6}".format(nNotRun_rt + nDefine_rt, nRunning_rt,
                                                                                                               threshold, nNotRun_queuelimit + nDefine_queuelimit,
                                                                                                               nQueueLimit, totWalltime,
                                                                                                               minTotalWalltime)
                tmp_log.warning("{0} {1}".format(msg_header, msg_body))
                tmp_log.sendMsg("{0} {1}".format(msg_header, msg_body), self.msgType, msgLevel='warning', escapeChar=True)
                return self.retMergeUnThr

        elif workQueue.queue_name in non_rt_wqs and nRunning_gs != 0 \
                and float(nNotRun_gs + nDefine_gs) / float(nRunning_gs) > threshold and \
                (nNotRun_queuelimit + nDefine_queuelimit) > nQueueLimit:
            limitPriority = True
            if not highPrioQueued:
                # enough jobs in Panda
                msg_body = "SKIP nQueued_gs({0})/nRunning_gs({1})>{2} & nQueued_queuelimit({3})>{4}".format(nNotRun_gs + nDefine_gs, nRunning_gs,
                                                                                                               threshold, nNotRun_queuelimit + nDefine_queuelimit,
                                                                                                               nQueueLimit)
                tmp_log.warning("{0} {1}".format(msg_header, msg_body))
                tmp_log.sendMsg("{0} {1}".format(msg_header, msg_body), self.msgType, msgLevel='warning', escapeChar=True)
                return self.retMergeUnThr

        elif nDefine_queuelimit > nQueueLimit:
            limitPriority = True
            if not highPrioQueued:
                # brokerage is stuck
                msg_body = "SKIP too many nDefined_queuelimit({0})>{1}".format(nDefine_queuelimit, nQueueLimit)
                tmp_log.warning("{0} {1}".format(msg_header, msg_body))
                tmp_log.sendMsg("{0} {1}".format(msg_header, msg_body), self.msgType, msgLevel='warning', escapeChar=True)
                return self.retMergeUnThr

        elif nWaiting_rt > max(nRunning_rt * nWaitingLimit, nJobsInBunch * nWaitingBunchLimit):
            limitPriority = True
            if not highPrioQueued:
                # too many waiting
                msg_body = "SKIP too many nWaiting_rt({0})>max(nRunning_rt({1})x{2},{3}x{4})".format(nWaiting_rt, nRunning_rt, nWaitingLimit,
                                                                                                    nJobsInBunch, nWaitingBunchLimit)
                tmp_log.warning("{0} {1}".format(msg_header, msg_body))
                tmp_log.sendMsg("{0} {1}".format(msg_header, msg_body), self.msgType, msgLevel='warning', escapeChar=True)
                return self.retMergeUnThr

        elif configRunningCap and nRunning_runningcap > configRunningCap:
            # cap on running
            msg_body = "SKIP nRunning_runningcap({0})>nRunningCap({1})".format(nRunning_runningcap, configRunningCap)
            tmp_log.warning('{0} {1}'.format(msg_header, msg_body))
            tmp_log.sendMsg('{0} {1}'.format(msg_header, msg_body), self.msgType, msgLevel='warning', escapeChar=True)
            return self.retMergeUnThr

        elif configQueueCap and nNotRun_queuecap + nDefine_queuecap > configQueueCap:
            limitPriority = True
            if not highPrioQueued:
                # cap on queued
                msg_body = "SKIP nQueued_queuecap({0})>nQueueCap({1})".format(nNotRun_queuecap + nDefine_queuecap, configQueueCap)
                tmp_log.warning("{0} {1}".format(msg_header, msg_body))
                tmp_log.sendMsg("{0} {1}".format(msg_header, msg_body), self.msgType, msgLevel='warning', escapeChar=True)
                return self.retMergeUnThr

        # get jobs from prodDB
        limitPriorityValue = None
        if limitPriority:
            limitPriorityValue = highestPrioWaiting
            self.setMinPriority(limitPriorityValue)
        else:
            # not enough jobs are queued
            if (nNotRun_queuelimit + nDefine_queuelimit < nQueueLimit * 0.9) \
                    or (workQueue.queue_name in non_rt_wqs and nNotRun_gs + nDefine_gs < nRunning_gs) \
                    or (workQueue.queue_name not in non_rt_wqs and nNotRun_rt + nDefine_rt < nRunning_rt):
                tmp_log.debug(msg_header+" not enough jobs queued")
                self.notEnoughJobsQueued()
                self.setMaxNumJobs(max(self.maxNumJobs, nQueueLimit/20))

        msg_body = "PASS - priority limit={0} maxNumJobs={1}".format(limitPriorityValue, self.maxNumJobs)
        tmp_log.info(msg_header+" "+msg_body)
        return self.retUnThrottled


Interaction.installSC(JobThrottlerBase)
