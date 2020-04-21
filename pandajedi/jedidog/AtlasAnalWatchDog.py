import os
import sys
import socket
import traceback

from six import iteritems

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from .WatchDogBase import WatchDogBase
from pandaserver.dataservice.Activator import Activator

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# watchdog for ATLAS analysis
class AtlasAnalWatchDog (WatchDogBase):

    # constructor
    def __init__(self,ddmIF,taskBufferIF):
        WatchDogBase.__init__(self,ddmIF,taskBufferIF)
        self.pid = '{0}-{1}-dog'.format(socket.getfqdn().split('.')[0],os.getpid())
        self.cronActions = {'forPrestage':'atlas_prs'}

    # main
    def doAction(self):
        try:
            # get logger
            origTmpLog = MsgWrapper(logger)
            origTmpLog.debug('start')
            # handle waiting jobs
            self.doForWaitingJobs()
            # throttle tasks if so many prestaging requests
            self.doForPreStaging()
        except Exception:
            errtype,errvalue = sys.exc_info()[:2]
            origTmpLog.error('failed with {0} {1}'.format(errtype,errvalue))
        # return
        origTmpLog.debug('done')
        return self.SC_SUCCEEDED


    # handle waiting jobs
    def doForWaitingJobs(self):
        tmpLog = MsgWrapper(logger, 'doForWaitingJobs label=user')
        # check every 60 min
        checkInterval = 60
        # get lib.tgz for waiting jobs
        libList = self.taskBufferIF.getLibForWaitingRunJob_JEDI(self.vo,self.prodSourceLabel,checkInterval)
        tmpLog.debug('got {0} lib.tgz files'.format(len(libList)))
        # activate or kill orphan jobs which were submitted to use lib.tgz when the lib.tgz was being produced
        for prodUserName,datasetName,tmpFileSpec in libList:
            tmpLog = MsgWrapper(logger,'< #ATM #KV doForWaitingJobs jediTaskID={0} label=user >'.format(tmpFileSpec.jediTaskID))
            tmpLog.debug('start')
            # check status of lib.tgz
            if tmpFileSpec.status == 'failed':
                # get buildJob
                pandaJobSpecs = self.taskBufferIF.peekJobs([tmpFileSpec.PandaID],
                                                           fromDefined=False,
                                                           fromActive=False,
                                                           fromWaiting=False)
                pandaJobSpec = pandaJobSpecs[0]
                if pandaJobSpec is not None:
                    # kill
                    self.taskBufferIF.updateJobs([pandaJobSpec],False)
                    tmpLog.debug('  action=killed_downstream_jobs for user="{0}" with libDS={1}'.format(prodUserName,datasetName))
                else:
                    # PandaJobSpec not found
                    tmpLog.error('  cannot find PandaJobSpec for user="{0}" with PandaID={1}'.format(prodUserName,
                                                                                                     tmpFileSpec.PandaID))
            elif tmpFileSpec.status == 'finished':
                # set metadata
                self.taskBufferIF.setGUIDs([{'guid':tmpFileSpec.GUID,
                                             'lfn':tmpFileSpec.lfn,
                                             'checksum':tmpFileSpec.checksum,
                                             'fsize':tmpFileSpec.fsize,
                                             'scope':tmpFileSpec.scope,
                                             }])
                # get lib dataset
                dataset = self.taskBufferIF.queryDatasetWithMap({'name':datasetName})
                if dataset is not None:
                    # activate jobs
                    aThr = Activator(self.taskBufferIF,dataset)
                    aThr.start()
                    aThr.join()
                    tmpLog.debug('  action=activated_downstream_jobs for user="{0}" with libDS={1}'.format(prodUserName,datasetName))
                else:
                    # datasetSpec not found
                    tmpLog.error('  cannot find datasetSpec for user="{0}" with libDS={1}'.format(prodUserName,datasetName))
            else:
                # lib.tgz is not ready
                tmpLog.debug('  keep waiting for user="{0}" libDS={1}'.format(prodUserName,datasetName))


    # throttle tasks if so many prestaging requests
    def doForPreStaging(self):
        try:
            tmpLog = MsgWrapper(logger, ' #ATM #KV doForPreStaging label=user')
            # lock
            flagLocked = self.taskBufferIF.lockProcess_JEDI(self.vo, self.prodSourceLabel,
                                                            self.cronActions['forPrestage'],
                                                            0, 'NULL', self.pid, timeLimit=5)
            if not flagLocked:
                return
            tmpLog.debug('start')
            # get throttled users
            thrUserTasks = self.taskBufferIF.getThrottledUsersTasks_JEDI(self.vo,self.prodSourceLabel)
            # get dispatch datasets
            dispUserTasks = self.taskBufferIF.getDispatchDatasetsPerUser(self.vo,self.prodSourceLabel,True,True)
            # max size of prestaging requests in GB
            maxPrestaging = self.taskBufferIF.getConfigValue('anal_watchdog', 'USER_PRESTAGE_LIMIT', 'jedi', 'atlas')
            if maxPrestaging is None:
                maxPrestaging = 1024
            # max size of transfer requests in GB
            maxTransfer = self.taskBufferIF.getConfigValue('anal_watchdog', 'USER_TRANSFER_LIMIT', 'jedi', 'atlas')
            if maxTransfer is None:
                maxTransfer = 1024
            # throttle interval
            thrInterval = 120
            # loop over all users
            for userName, userDict in iteritems(dispUserTasks):
                # loop over all transfer types
                for transferType, maxSize in [('prestaging', maxPrestaging),
                                               ('transfer', maxTransfer)]:
                    if transferType not in userDict:
                        continue
                    userTotal = userDict[transferType]['size'] / 1024
                    tmpLog.debug('user={0} {1} total={2} GB'.format(userName, transferType, userTotal))
                    # too large
                    if userTotal > maxSize:
                        tmpLog.debug('user={0} has too large {1} total={2} GB > limit={3} GB'.
                                     format(userName, transferType, userTotal, maxSize))
                        # throttle tasks
                        for taskID in userDict[transferType]['tasks']:
                            if userName not in thrUserTasks or transferType not in thrUserTasks[userName] \
                                    or taskID not in thrUserTasks[userName][transferType]:
                                tmpLog.debug('action=throttle_{0} jediTaskID={1} for user={2}'.format(transferType,
                                                                                                      taskID, userName))
                                errDiag = 'throttled for {0} min due to large data motion type={0}'.format(thrInterval,
                                                                                                           transferType)
                                self.taskBufferIF.throttleTask_JEDI(taskID,thrInterval,errDiag)
                        # remove the user from the list
                        if userName in thrUserTasks and transferType in thrUserTasks[userName]:
                            del thrUserTasks[userName][transferType]
            # release users
            for userName, taskData in iteritems(thrUserTasks):
                for transferType, taskIDs in iteritems(taskData):
                    tmpLog.debug('user={0} release throttled tasks with {1}'.format(userName, transferType))
                    # unthrottle tasks
                    for taskID in taskIDs:
                        tmpLog.debug('action=release_{0} jediTaskID={1} for user={2}'.format(transferType,
                                                                                             taskID, userName))
                        self.taskBufferIF.releaseThrottledTask_JEDI(taskID)
            tmpLog.debug('done')
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error('failed with {0} {1} {2}'.format(errtype, errvalue, traceback.format_exc()))
