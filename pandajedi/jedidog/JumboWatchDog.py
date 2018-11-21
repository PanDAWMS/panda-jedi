import os
import sys
import socket
import operator
import traceback


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
        self.component = 'jumbo_dog'
        self.dryRun = True


    # main
    def run(self):
        try:
            # get process lock
            locked = self.taskBufferIF.lockProcess_JEDI(self.vo, self.prodSourceLabel, self.component, 0, '', self.pid, False, 10)
            if not locked:
                self.log.debug('skipped since locked by another')
                return
            # get parameters for conversion
            self.log.debug('start {0}'.format(self.component))
            maxTasks = self.taskBufferIF.getConfigValue(self.component, 'MAX_TASKS_WITH_JUMBO', 'jedi', self.vo)
            if maxTasks is None:
                maxTasks = 1
            nEventsToDisable = self.taskBufferIF.getConfigValue(self.component, 'MIN_EVENTS_DISABLE_JUMBO', 'jedi', self.vo)
            if nEventsToDisable is None:
                nEventsToDisable = 100000
            nEventsToEnable = self.taskBufferIF.getConfigValue(self.component, 'MIN_EVENTS_ENABLE_JUMBO', 'jedi', self.vo)
            if nEventsToEnable is None:
                nEventsToEnable = nEventsToDisable * 10
            maxEvents = self.taskBufferIF.getConfigValue(self.component, 'MAX_EVENTS_FOR_JUMBO', 'jedi', self.vo)
            if maxEvents is None:
                maxEvents = maxTasks * nEventsToEnable / 2
            nJumboPerTask = self.taskBufferIF.getConfigValue(self.component, 'N_JUMBO_PER_TASK', 'jedi', self.vo)
            if nJumboPerTask is None:
                nJumboPerTask = 1
            nJumboPerSite = self.taskBufferIF.getConfigValue(self.component, 'N_JUMBO_PER_SITE', 'jedi', self.vo)
            if nJumboPerSite is None:
                nJumboPerSite = 1
            maxPrio = self.taskBufferIF.getConfigValue(self.component, 'MAX_PRIO_FOR_JUMBO', 'jedi', self.vo)
            if maxPrio is None:
                maxPrio = 500
            # get current info
            tasksWithJumbo = self.taskBufferIF.getTaskWithJumbo_JEDI(self.vo, self.prodSourceLabel)
            totEvents = 0
            doneEvents = 0
            nTasks = 0
            for jediTaskID, taskData in tasksWithJumbo.iteritems():
                if taskData['nEvents'] - taskData['nEventsDone'] < nEventsToDisable:
                    # disable jumbo
                    self.log.debug('disable jumbo in jediTaskID={0} due to n_events={1} < {2}'.format(jediTaskID, taskData['nEvents'] - taskData['nEventsDone'],
                                                                                                      nEventsToDisable))
                    if not self.dryRun:
                        self.taskBufferIF.enableJumboJobs(jediTaskID, 0, 0)
                else:
                    nTasks += 1
                    totEvents += taskData['nEvents']
                    doneEvents += taskData['nEventsDone']
            self.log.debug('total_events={0} n_events_to_process={1} n_tasks={2} available for jumbo'.format(totEvents, totEvents - doneEvents, nTasks))
            if self.dryRun or (nTasks < maxTasks and (totEvents - doneEvents) < maxEvents):
                # look for tasks to enable jumbo
                self.log.debug('look for tasks to enable jumbo due to lack of tasks and events')
                tasksToEnableJumbo = self.taskBufferIF.getTaskToEnableJumbo_JEDI(self.vo, self.prodSourceLabel, maxPrio, nEventsToEnable)
                # sort by nevents
                nEventsMap = dict()
                for jediTaskID, taskData in tasksToEnableJumbo.iteritems():
                    nEventsMap[jediTaskID] = taskData['nEvents']
                sortedList = sorted(nEventsMap.items(), key=operator.itemgetter(1))
                sortedList.reverse()
                for jediTaskID, nEvents in sortedList:
                    taskData = tasksToEnableJumbo[jediTaskID]
                    self.log.debug('enable jumbo in jediTaskID={0} with n_events_to_process={1}'.format(jediTaskID, taskData['nEvents'] - taskData['nEventsDone']))
                    if not self.dryRun:
                        self.taskBufferIF.enableJumboJobs(taskData['jediTaskID'], nJumboPerTask, nJumboPerSite)
                    nTasks += 1
                    totEvents += taskData['nEvents']
                    doneEvents += taskData['nEventsDone']
                    if nTasks >= maxTasks or (totEvents - doneEvents) >= maxEvents:
                        break
            self.log.debug('done {0}'.format(self.component))
        except Exception:
            # error
            errtype, errvalue = sys.exc_info()[:2]
            errStr = ": %s %s" % (errtype.__name__, errvalue)
            errStr.strip()
            errStr += traceback.format_exc()
            self.log.error(errStr)
