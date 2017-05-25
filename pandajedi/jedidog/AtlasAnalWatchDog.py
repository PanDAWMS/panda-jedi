import re
import os
import sys
import socket
import traceback

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from WatchDogBase import WatchDogBase
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
            tmpLog = MsgWrapper(logger)
            tmpLog.debug('start')
            origTmpLog = tmpLog
            # check every 60 min
            checkInterval = 60
            # get lib.tgz for waiting jobs
            libList = self.taskBufferIF.getLibForWaitingRunJob_JEDI(self.vo,self.prodSourceLabel,checkInterval)
            tmpLog.debug('got {0} lib.tgz files'.format(len(libList)))
            # activate or kill orphan jobs which were submitted to use lib.tgz when the lib.tgz was being produced
            for prodUserName,datasetName,tmpFileSpec in libList:
                tmpLog = MsgWrapper(logger,'<jediTaskID={0}>'.format(tmpFileSpec.jediTaskID))
                tmpLog.debug('start')
                # check status of lib.tgz
                if tmpFileSpec.status == 'failed':
                    # get buildJob 
                    pandaJobSpecs = self.taskBufferIF.peekJobs([tmpFileSpec.PandaID],
                                                               fromDefined=False,
                                                               fromActive=False,
                                                               fromWaiting=False)
                    pandaJobSpec = pandaJobSpecs[0]
                    if pandaJobSpec != None:
                        # kill
                        self.taskBufferIF.updateJobs([pandaJobSpec],False)
                        tmpLog.debug('  killed downstream jobs for user="{0}" with libDS={1}'.format(prodUserName,datasetName))
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
                    if dataset != None:
                        # activate jobs
                        aThr = Activator(self.taskBufferIF,dataset)
                        aThr.start()
                        aThr.join()
                        tmpLog.debug('  activated downstream jobs for user="{0}" with libDS={1}'.format(prodUserName,datasetName))
                    else:
                        # datasetSpec not found
                        tmpLog.error('  cannot find datasetSpec for user="{0}" with libDS={1}'.format(prodUserName,datasetName))
                else:
                    # lib.tgz is not ready
                    tmpLog.debug('  keep waiting for user="{0}" libDS={1}'.format(prodUserName,datasetName))
            # throttle tasks if so many prestaging requests
            self.doForPreStaging()
        except:
            tmpLog = origTmpLog
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('failed with {0} {1}'.format(errtype,errvalue))
        # return
        tmpLog = origTmpLog
        tmpLog.debug('done')
        return self.SC_SUCCEEDED
    


    # throttle tasks if so many prestaging requests
    def doForPreStaging(self):
        try:
            tmpLog = MsgWrapper(logger,'doForPreStaging')
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
            # max size of prestaging requests in MB
            maxPrestaging = self.taskBufferIF.getConfigValue('anal_watchdog', 'PRESTAGE_LIMIT', 'jedi', 'atlas')
            if maxPrestaging == None:
                maxPrestaging = 1
            maxPrestaging *= 1024*1024
            # throttle interval
            thrInterval = 120
            # loop over all users
            for userName,userDict in dispUserTasks.iteritems():
                tmpLog.debug('{0} {1} GB'.format(userName, userDict['size']/1024))
                # too large
                if userDict['size'] > maxPrestaging:
                    tmpLog.debug('{0} has too large prestaging {1}>{2} GB'.format(userName,
                                                                                  userDict['size']/1024,
                                                                                  maxPrestaging/1024))
                    # throttle tasks
                    for taskID in userDict['tasks']:
                        if not userName in thrUserTasks or not taskID in thrUserTasks[userName]:
                            tmpLog.debug('thottle jediTaskID={0}'.format(taskID))
                            errDiag = 'throttled for {0} min due to too large prestaging from TAPE'.format(thrInterval)
                            self.taskBufferIF.throttleTask_JEDI(taskID,thrInterval,errDiag)
                    # remove the user from the list
                    if userName in thrUserTasks:
                        del thrUserTasks[userName]
            # release users
            for userName,taskIDs in thrUserTasks.items():
                tmpLog.debug('{0} release throttled tasks'.format(userName))
                # unthrottle tasks
                for taskID in taskIDs:
                    tmpLog.debug('unthottle jediTaskID={0}'.format(taskID))
                    self.taskBufferIF.releaseThrottledTask_JEDI(taskID)
            tmpLog.debug('done')
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('failed with {0} {1} {2}'.format(errtype,errvalue,traceback.format_exc()))
            
