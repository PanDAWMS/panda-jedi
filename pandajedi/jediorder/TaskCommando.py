import os
import sys
import time
import socket
import datetime

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.ThreadUtils import ListWithLock,ThreadPool,WorkerThread
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from JediKnight import JediKnight
from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandajedi.jedirefine import RefinerUtils


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# worker class to kill/amend tasks
class TaskCommando (JediKnight):

    # constructor
    def __init__(self,commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.pid = '{0}-{1}-dog'.format(socket.getfqdn().split('.')[0],os.getpid())
        JediKnight.__init__(self,commuChannel,taskBufferIF,ddmIF,logger)


    # main
    def start(self):
        # start base classes
        JediKnight.start(self)
        # go into main loop
        while True:
            startTime = datetime.datetime.utcnow()
            try:
                # get logger
                tmpLog = MsgWrapper(logger)
                tmpLog.debug('start')
                # loop over all vos
                for vo in self.vos:
                    # loop over all sourceLabels
                    for prodSourceLabel in self.prodSourceLabels:
                        # get the list of tasks to exec command
                        tmpList = self.taskBufferIF.getTasksToExecCommand_JEDI(vo,prodSourceLabel)
                        if tmpList == None:
                            # failed
                            tmpLog.error('failed to get the task list for vo={0} label={1}'.format(vo,prodSourceLabel))
                        else:
                            tmpLog.debug('got {0} tasks'.format(len(tmpList)))
                            # put to a locked list
                            taskList = ListWithLock(tmpList)
                            # make thread pool
                            threadPool = ThreadPool()
                            # make workers
                            nWorker = jedi_config.taskrefine.nWorkers
                            for iWorker in range(nWorker):
                                thr = TaskCommandoThread(taskList,threadPool,
                                                         self.taskBufferIF,
                                                         self.ddmIF)
                                thr.start()
                            # join
                            threadPool.join()
                tmpLog.debug('done')
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed in {0}.start() with {1} {2}'.format(self.__class__.__name__,errtype.__name__,errvalue))
            # sleep if needed
            loopCycle = jedi_config.tcommando.loopCycle
            timeDelta = datetime.datetime.utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)



# thread for real worker
class TaskCommandoThread (WorkerThread):

    # constructor
    def __init__(self,taskList,threadPool,taskbufferIF,ddmIF):
        # initialize woker with no semaphore
        WorkerThread.__init__(self,None,threadPool,logger)
        # attributres
        self.taskList = taskList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF


    # main
    def runImpl(self):
        while True:
            try:
                # get a part of list
                nTasks = 10
                taskList = self.taskList.get(nTasks)
                # no more datasets
                if len(taskList) == 0:
                    self.logger.debug('{0} terminating since no more items'.format(self.__class__.__name__))
                    return
                # loop over all tasks
                for jediTaskID,commandMap in taskList:
                    # make logger
                    tmpLog = MsgWrapper(self.logger,' <jediTaskID={0}>'.format(jediTaskID))
                    commandStr = commandMap['command']
                    commentStr = commandMap['comment']
                    tmpLog.info('start for {0}'.format(commandStr))
                    tmpStat = Interaction.SC_SUCCEEDED
                    if commandStr in ['kill','finish']:
                        # get active PandaIDs to be killed
                        pandaIDs = self.taskBufferIF.getPandaIDsWithTask_JEDI(jediTaskID,True)
                        if pandaIDs == None:
                            tmpLog.error('failed to get PandaIDs for jediTaskID={0}'.format(jediTaskID))
                            tmpStat = Interaction.SC_FAILED
                        # kill jobs or update task
                        if tmpStat == Interaction.SC_SUCCEEDED:
                            if pandaIDs == []:
                                # done since no active jobs
                                tmpLog.info('completed the command')
                                tmpTaskSpec = JediTaskSpec()
                                tmpTaskSpec.jediTaskID = jediTaskID
                                tmpTaskSpec.forceUpdate('oldStatus')
                                tmpTaskSpec.status = JediTaskSpec.commandStatusMap()[commandStr]['done']
                                tmpRet = self.taskBufferIF.updateTask_JEDI(tmpTaskSpec,{'jediTaskID':jediTaskID})
                            else:
                                tmpLog.info('sending kill command')
                                tmpRet = self.taskBufferIF.killJobs(pandaIDs,commentStr,'50',True)
                            tmpLog.info('done with {0}'.format(str(tmpRet)))
                    elif commandStr in ['retry','incexec']:
                        # change task params for incexec
                        if commandStr == 'incexec':
                            try:
                                # read task params
                                taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(jediTaskID)
                                taskParamMap = RefinerUtils.decodeJSON(taskParam)
                                # convert new params
                                newParamMap = RefinerUtils.decodeJSON(commentStr)
                                # change params
                                for newKey,newVal in newParamMap.iteritems():
                                    if newVal == None:
                                        # delete 
                                        del taskParamMap[newKey]
                                    else:
                                        # change
                                        taskParamMap[newKey] = newVal
                                # encode new param
                                strTaskParams = RefinerUtils.encodeJSON(taskParamMap)
                                tmpRet = self.taskBufferIF.updateTaskParams_JEDI(jediTaskID,strTaskParams)
                                if tmpRet != True:
                                    tmpLog.error('failed to update task params')
                                    continue
                            except:
                                errtype,errvalue = sys.exc_info()[:2]
                                tmpLog.error('failed to change task params with {0}:{1}'.format(errtype.__name__,errvalue))
                                continue
                        # retry failed files
                        tmpRet = self.taskBufferIF.retryTask_JEDI(jediTaskID,commandStr)
                        tmpLog.info('done with {0}'.format(tmpRet))
                    else:
                        tmpLog.error('unknown command')
            except:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('{0} failed in runImpl() with {1}:{2}'.format(self.__class__.__name__,errtype.__name__,errvalue))
        


########## launch 
                
def launcher(commuChannel,taskBufferIF,ddmIF,vos=None,prodSourceLabels=None):
    p = TaskCommando(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels)
    p.start()
