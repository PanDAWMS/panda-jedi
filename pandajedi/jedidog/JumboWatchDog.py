import re
import os
import sys
import socket
import operator
import traceback

from six import iteritems

from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandajedi.jedirefine import RefinerUtils


# watchdog to take actions for jumbo jobs
class JumboWatchDog:

    # constructor
    def __init__(self, taskBufferIF, ddmIF, log, vo, prodSourceLabel):
        self.taskBufferIF = taskBufferIF
        self.ddmIF = ddmIF
        self.pid = '{0}-{1}_{2}-jumbo'.format(socket.getfqdn().split('.')[0], os.getpid(), os.getpgrp())
        self.log = log
        self.vo = vo
        self.prodSourceLabel = prodSourceLabel
        self.component = 'JumboWatchDog'
        self.dryRun = True


    # main
    def run(self):
        try:
            # get process lock
            locked = self.taskBufferIF.lockProcess_JEDI(vo=self.vo, prodSourceLabel=self.prodSourceLabel,
                                                        cloud=None, workqueue_id=None, resource_name=None,
                                                        component=self.component, pid=self.pid, timeLimit=10)
            if not locked:
                self.log.debug('component={0} skipped since locked by another'.format(self.component))
                return
            # get parameters for conversion
            self.log.debug('component={0} start'.format(self.component))
            maxTasks = self.taskBufferIF.getConfigValue(self.component, 'JUMBO_MAX_TASKS', 'jedi', self.vo)
            if maxTasks is None:
                maxTasks = 1
            nEventsToDisable = self.taskBufferIF.getConfigValue(self.component, 'JUMBO_MIN_EVENTS_DISABLE', 'jedi', self.vo)
            if nEventsToDisable is None:
                nEventsToDisable = 100000
            nEventsToEnable = self.taskBufferIF.getConfigValue(self.component, 'JUMBO_MIN_EVENTS_ENABLE', 'jedi', self.vo)
            if nEventsToEnable is None:
                nEventsToEnable = nEventsToDisable * 10
            maxEvents = self.taskBufferIF.getConfigValue(self.component, 'JUMBO_MAX_EVENTS', 'jedi', self.vo)
            if maxEvents is None:
                maxEvents = maxTasks * nEventsToEnable // 2
            nJumboPerTask = self.taskBufferIF.getConfigValue(self.component, 'JUMBO_PER_TASK', 'jedi', self.vo)
            if nJumboPerTask is None:
                nJumboPerTask = 1
            nJumboPerSite = self.taskBufferIF.getConfigValue(self.component, 'JUMBO_PER_SITE', 'jedi', self.vo)
            if nJumboPerSite is None:
                nJumboPerSite = 1
            maxPrio = self.taskBufferIF.getConfigValue(self.component, 'JUMBO_MAX_CURR_PRIO', 'jedi', self.vo)
            if maxPrio is None:
                maxPrio = 500
            progressToBoost = self.taskBufferIF.getConfigValue(self.component, 'JUMBO_PROG_TO_BOOST', 'jedi', self.vo)
            if progressToBoost is None:
                progressToBoost = 95
            maxFilesToBoost = self.taskBufferIF.getConfigValue(self.component, 'JUMBO_MAX_FILES_TO_BOOST', 'jedi', self.vo)
            if maxFilesToBoost is None:
                maxFilesToBoost = 500
            prioToBoost = 900
            prioWhenDisabled = self.taskBufferIF.getConfigValue(self.component, 'JUMBO_PRIO_DISABLED', 'jedi', self.vo)
            if prioWhenDisabled is None:
                prioWhenDisabled = 500
            # get current info
            tasksWithJumbo = self.taskBufferIF.getTaskWithJumbo_JEDI(self.vo, self.prodSourceLabel)
            totEvents = 0
            doneEvents = 0
            nTasks = 0
            for jediTaskID, taskData in iteritems(tasksWithJumbo):
                # disable jumbo
                if taskData['useJumbo'] != JediTaskSpec.enum_useJumbo['disabled'] and taskData['site'] is None:
                    if  taskData['nEvents'] - taskData['nEventsDone'] < nEventsToDisable:
                        # disable
                        self.log.info('component={0} disable jumbo in jediTaskID={1} due to n_events_to_process={2} < {3}'.format(self.component, jediTaskID,
                                                                                                                                  taskData['nEvents'] - taskData['nEventsDone'],
                                                                                                                                  nEventsToDisable))
                        self.taskBufferIF.enableJumboJobs(jediTaskID, 0, 0)
                    else:
                        # wait
                        nTasks += 1
                        totEvents += taskData['nEvents']
                        doneEvents += taskData['nEventsDone']
                        self.log.info('component={0} keep jumbo in jediTaskID={1} due to n_events_to_process={2} > {3}'.format(self.component, jediTaskID,
                                                                                                                               taskData['nEvents'] - taskData['nEventsDone'],
                                                                                                                               nEventsToDisable))
                # increase priority for jumbo disabled
                if taskData['useJumbo'] == JediTaskSpec.enum_useJumbo['disabled'] and taskData['currentPriority'] < prioWhenDisabled:
                    self.taskBufferIF.changeTaskPriorityPanda(jediTaskID, prioWhenDisabled)
                    self.log.info('component={0} priority boost to {1} after disabing jumbo in in jediTaskID={2}'.format(self.component, prioWhenDisabled, jediTaskID))
                # increase priority when close to completion
                if taskData['nEvents'] > 0 and (taskData['nEvents'] - taskData['nEventsDone']) * 100 // taskData['nEvents'] < progressToBoost \
                        and taskData['currentPriority'] < prioToBoost and (taskData['nFiles'] - taskData['nFilesDone']) < maxFilesToBoost:
                    # boost
                    tmpStr = 'component={0} priority boost to {5} for jediTaskID={1} due to n_events_done={2} > {3}*{4}% '.format(self.component, jediTaskID,
                                                                                                                                  taskData['nEventsDone'],
                                                                                                                                  taskData['nEvents'],
                                                                                                                                  progressToBoost,
                                                                                                                                  prioToBoost)
                    tmpStr += 'n_files_remaining={0} < {1}'.format(taskData['nFiles'] - taskData['nFilesDone'], maxFilesToBoost)
                    self.log.info(tmpStr)
                    self.taskBufferIF.changeTaskPriorityPanda(jediTaskID, prioToBoost)
                # kick pending
                if taskData['taskStatus'] in ['pending', 'running'] and taskData['useJumbo'] in [JediTaskSpec.enum_useJumbo['pending'], JediTaskSpec.enum_useJumbo['running']]:
                    nActiveJumbo = 0
                    for computingSite, jobStatusMap in iteritems(taskData['jumboJobs']):
                        for jobStatus, nJobs in iteritems(jobStatusMap):
                            if jobStatus in ['defined', 'assigned', 'activated', 'sent', 'starting', 'running', 'transferring', 'holding']:
                                nActiveJumbo += nJobs
                    if nActiveJumbo == 0:
                        self.log.info('component={0} kick jumbo in {2} jediTaskID={1}'.format(self.component,
                                                                                              jediTaskID,
                                                                                              taskData['taskStatus']))
                        self.taskBufferIF.kickPendingTasksWithJumbo_JEDI(jediTaskID)
                # reset input to re-generate co-jumbo
                if taskData['currentPriority'] >= prioToBoost:
                    nReset = self.taskBufferIF.resetInputToReGenCoJumbo_JEDI(jediTaskID)
                    if nReset is not None and nReset > 0:
                        self.log.info('component={0} reset {1} inputs to regenerate co-jumbo for jediTaskID={2}'.format(self.component, nReset, jediTaskID))
                    else:
                        self.log.debug('component={0} tried to reset inputs to regenerate co-jumbo with {1} for jediTaskID={2}'.format(self.component, nReset, jediTaskID))
            self.log.info('component={0} total_events={1} n_events_to_process={2} n_tasks={3} available for jumbo'.format(self.component, totEvents,
                                                                                                                          totEvents - doneEvents, nTasks))
            if True:
                # get list of releases and caches available at jumbo job enabled PQs
                jumboRels, jumboCaches = self.taskBufferIF.getRelCacheForJumbo_JEDI()
                # look for tasks to enable jumbo
                if self.dryRun:
                    self.log.info('component={0} look for tasks to enable jumbo in dry run mode'.format(self.component))
                else:
                    self.log.info('component={0} look for tasks to enable jumbo due to lack of tasks and events to meet max_tasks={1} max_events={2}'.format(self.component,
                                                                                                                                                             maxTasks,
                                                                                                                                                             maxEvents))
                tasksToEnableJumbo = self.taskBufferIF.getTaskToEnableJumbo_JEDI(self.vo, self.prodSourceLabel, maxPrio, nEventsToEnable)
                nGoodTasks = 0
                self.log.debug('component={0} got {1} tasks to check'.format(self.component, len(tasksToEnableJumbo)))
                # sort by nevents
                nEventsMap = dict()
                for jediTaskID, taskData in iteritems(tasksToEnableJumbo):
                    nEventsMap[jediTaskID] = taskData['nEvents']
                sortedList = sorted(list(nEventsMap.items()), key=operator.itemgetter(1))
                sortedList.reverse()
                for jediTaskID, nEvents in sortedList:
                    taskData = tasksToEnableJumbo[jediTaskID]
                    # get task parameters
                    try:
                        taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(jediTaskID)
                        taskParamMap = RefinerUtils.decodeJSON(taskParam)
                    except Exception:
                        self.log.error('component={0} failed to get task params for jediTaskID={1}'.format(self.component, jediTaskID))
                        continue
                    taskSpec = JediTaskSpec()
                    taskSpec.splitRule = taskData['splitRule']
                    # check if good for jumbo
                    if 'esConvertible' not in taskParamMap or taskParamMap['esConvertible'] is False:
                        self.log.info('component={0} skip to enable jumbo for jediTaskID={1} since not ES-convertible'.format(self.component, jediTaskID))
                        continue
                    if taskSpec.inFilePosEvtNum():
                        pass
                    elif taskSpec.getNumFilesPerJob() == 1:
                        pass
                    elif taskSpec.getNumEventsPerJob() is not None and 'nEventsPerInputFile' in taskParamMap \
                            and taskSpec.getNumEventsPerJob() <= taskParamMap['nEventsPerInputFile']:
                        pass
                    else:
                        self.log.info('component={0} skip to enable jumbo for jediTaskID={1} since not good for in-file positional event numbers'.format(self.component, jediTaskID))
                        continue
                    # check software
                    transHome = taskData['transHome']
                    cmtConfig = taskData['architecture']
                    if re.search('^\d+\.\d+\.\d+$', transHome.split('-')[-1]) is not None:
                        transHome = transHome.split('-')[-1]
                        swDict = jumboRels
                    else:
                        swDict = jumboCaches
                    key = (transHome, cmtConfig)
                    if key not in swDict:
                        self.log.info('component={0} skip to enable jumbo for jediTaskID={1} since {2}:{3} is unavailable at jumbo job enabled PQs'.format(
                                self.component,
                                jediTaskID,
                                transHome,
                                cmtConfig))
                        continue
                    if not self.dryRun and nTasks < maxTasks and (totEvents - doneEvents) < maxEvents:
                        self.log.info('component={0} enable jumbo in jediTaskID={1} with n_events_to_process={2}'.format(self.component, jediTaskID,
                                                                                                                         taskData['nEvents'] - taskData['nEventsDone']))
                        if taskData['eventService'] == 0:
                            tmpS, tmpO = self.taskBufferIF.enableEventService(taskData['jediTaskID'])
                            if tmpS != 0:
                                self.log.error('component={0} failed to enable ES in jediTaskID={1} with {2}'.format(self.component, jediTaskID, tmpO))
                                continue
                        self.taskBufferIF.enableJumboJobs(taskData['jediTaskID'], nJumboPerTask, nJumboPerSite)
                        nTasks += 1
                        totEvents += taskData['nEvents']
                        doneEvents += taskData['nEventsDone']
                    else:
                        nGoodTasks += 1
                        self.log.info('component={0} good to enable jumbo in jediTaskID={1} with n_events_to_process={2}'.format(self.component, jediTaskID,
                                                                                                                                 taskData['nEvents'] - taskData['nEventsDone']))
                self.log.info('component={0} there are n_good_tasks={1} tasks good for jumbo'.format(self.component, nGoodTasks))
            self.log.debug('component={0} done'.format(self.component))
        except Exception:
            # error
            errtype, errvalue = sys.exc_info()[:2]
            errStr = ": %s %s" % (errtype.__name__, errvalue)
            errStr.strip()
            errStr += traceback.format_exc()
            self.log.error(errStr)
