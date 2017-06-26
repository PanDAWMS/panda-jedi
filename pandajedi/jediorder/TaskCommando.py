import os
import re
import sys
import time
import socket
import datetime
import traceback

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
            # randomize cycle
            self.randomSleep()



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
        self.msgType = 'taskcommando'


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
                    tmpLog = MsgWrapper(self.logger,' < jediTaskID={0} >'.format(jediTaskID))
                    commandStr = commandMap['command']
                    commentStr = commandMap['comment']
                    oldStatus  = commandMap['oldStatus']
                    tmpLog.info('start for {0}'.format(commandStr))
                    tmpStat = Interaction.SC_SUCCEEDED
                    if commandStr in ['kill','finish','reassign']:
                        tmpMsg = 'executing {0}'.format(commandStr)
                        tmpLog.info(tmpMsg)
                        tmpLog.sendMsg(tmpMsg,self.msgType)
                        # loop twice to see immediate result
                        for iLoop in range(2):
                            # get active PandaIDs to be killed
                            if commandStr == 'reassign' and commentStr != None and 'soft reassign' in commentStr:
                                pandaIDs = self.taskBufferIF.getQueuedPandaIDsWithTask_JEDI(jediTaskID)
                            elif commandStr == 'reassign' and commentStr != None and 'nokill reassign' in commentStr:
                                pandaIDs = []
                            else:
                                pandaIDs = self.taskBufferIF.getPandaIDsWithTask_JEDI(jediTaskID,True)
                            if pandaIDs == None:
                                tmpLog.error('failed to get PandaIDs for jediTaskID={0}'.format(jediTaskID))
                                tmpStat = Interaction.SC_FAILED
                            # kill jobs or update task
                            if tmpStat == Interaction.SC_SUCCEEDED:
                                if pandaIDs == []:
                                    # done since no active jobs
                                    tmpMsg = 'completed cleaning jobs'
                                    tmpLog.sendMsg(tmpMsg,self.msgType)
                                    tmpLog.info(tmpMsg)
                                    tmpTaskSpec = JediTaskSpec()
                                    tmpTaskSpec.jediTaskID = jediTaskID
                                    updateTaskStatus = True
                                    if commandStr != 'reassign':
                                        # reset oldStatus
                                        # keep oldStatus for task reassignment since it is reset when actually reassigned
                                        tmpTaskSpec.forceUpdate('oldStatus')
                                    else:
                                        # extract cloud or site
                                        if commentStr != None:
                                            tmpItems = commentStr.split(':')
                                            if tmpItems[0] == 'cloud':
                                                tmpTaskSpec.cloud = tmpItems[1]
                                            elif tmpItems[0] == 'nucleus':
                                                tmpTaskSpec.nucleus = tmpItems[1]
                                            else:
                                                tmpTaskSpec.site = tmpItems[1]
                                            tmpMsg = 'set {0}={1}'.format(tmpItems[0],tmpItems[1])
                                            tmpLog.sendMsg(tmpMsg,self.msgType)
                                            tmpLog.info(tmpMsg)
                                            # back to oldStatus if necessary 
                                            if tmpItems[2] == 'y':
                                                tmpTaskSpec.status = oldStatus
                                                tmpTaskSpec.forceUpdate('oldStatus')
                                                updateTaskStatus = False
                                    if commandStr == 'reassign':
                                        tmpTaskSpec.forceUpdate('errorDialog')
                                    if commandStr == 'finish':
                                        # ignore failGoalUnreached when manually finished
                                        tmpStat,taskSpec = self.taskBufferIF.getTaskWithID_JEDI(jediTaskID)
                                        tmpTaskSpec.splitRule = taskSpec.splitRule
                                        tmpTaskSpec.unsetFailGoalUnreached()
                                    if updateTaskStatus:
                                        tmpTaskSpec.status = JediTaskSpec.commandStatusMap()[commandStr]['done']
                                    tmpMsg = 'set task_status={0}'.format(tmpTaskSpec.status)
                                    tmpLog.sendMsg(tmpMsg,self.msgType)
                                    tmpLog.info(tmpMsg)
                                    tmpRet = self.taskBufferIF.updateTask_JEDI(tmpTaskSpec,{'jediTaskID':jediTaskID},
                                                                               setOldModTime=True)
                                    tmpLog.info('done with {0}'.format(str(tmpRet)))
                                    break
                                else:
                                    # kill only in the first loop
                                    if iLoop > 0:
                                        break
                                    # wait or kill jobs 
                                    if 'soft finish' in commentStr:
                                        tmpMsg = "wating {0} jobs for soft finish".format(len(pandaIDs))
                                        tmpLog.info(tmpMsg)
                                        tmpRet = True
                                        tmpLog.info('done with {0}'.format(str(tmpRet)))
                                        break
                                    else:
                                        tmpMsg = "trying to kill {0} jobs".format(len(pandaIDs))
                                        tmpLog.info(tmpMsg)
                                        tmpLog.sendMsg(tmpMsg,self.msgType)
                                        if commandStr in ['finish']:
                                            # force kill
                                            tmpRet = self.taskBufferIF.killJobs(pandaIDs,commentStr,'52',True)
                                        elif commandStr in ['reassign']:
                                            # force kill
                                            tmpRet = self.taskBufferIF.killJobs(pandaIDs,commentStr,'51',True)
                                        else:
                                            # normal kill
                                            tmpRet = self.taskBufferIF.killJobs(pandaIDs,commentStr,'50',True)
                                        tmpLog.info('done with {0}'.format(str(tmpRet)))
                    elif commandStr in ['retry','incexec']:
                        tmpMsg = 'executing {0}'.format(commandStr)
                        tmpLog.info(tmpMsg)
                        tmpLog.sendMsg(tmpMsg,self.msgType)
                        # change task params for incexec
                        if commandStr == 'incexec':
                            try:
                                # read task params
                                taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(jediTaskID)
                                taskParamMap = RefinerUtils.decodeJSON(taskParam)
                                # remove some params
                                for newKey in ['nFiles','fixedSandbox']:
                                    try:
                                        del taskParamMap[newKey]
                                    except:
                                        pass
                                # convert new params
                                newParamMap = RefinerUtils.decodeJSON(commentStr)
                                # change params
                                for newKey,newVal in newParamMap.iteritems():
                                    if newVal == None:
                                        # delete
                                        if newKey in taskParamMap:
                                            del taskParamMap[newKey]
                                    else:
                                        # change
                                        taskParamMap[newKey] = newVal
                                # overwrite sandbox
                                if 'fixedSandbox' in taskParamMap:
                                    # noBuild
                                    for tmpParam in taskParamMap['jobParameters']:
                                        if tmpParam['type'] == 'constant' and re.search('^-a [^ ]+$',tmpParam['value']) != None:
                                            tmpParam['value'] = '-a {0}'.taskParamMap['fixedSandbox']
                                    # build
                                    if taskParamMap.has_key('buildSpec'):
                                        taskParamMap['buildSpec']['archiveName'] = taskParamMap['fixedSandbox']
                                    # merge
                                    if taskParamMap.has_key('mergeSpec'):
                                        taskParamMap['mergeSpec']['jobParameters'] = \
                                            re.sub('-a [^ ]+','-a {0}'.format(taskParamMap['fixedSandbox']),taskParamMap['mergeSpec']['jobParameters'])
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
                        tmpRet,newTaskStatus = self.taskBufferIF.retryTask_JEDI(jediTaskID,commandStr)
                        if tmpRet == True:
                            tmpMsg = 'set task_status={0}'.format(newTaskStatus)
                            tmpLog.sendMsg(tmpMsg,self.msgType)
                            tmpLog.info(tmpMsg)
                        tmpLog.info('done with {0}'.format(tmpRet))
                    else:
                        tmpLog.error('unknown command')
            except:
                errtype,errvalue = sys.exc_info()[:2]
                errStr  = '{0} failed in runImpl() with {1}:{2} '.format(self.__class__.__name__,errtype.__name__,errvalue)
                errStr += traceback.format_exc()
                logger.error(errStr)
        


########## launch 
                
def launcher(commuChannel,taskBufferIF,ddmIF,vos=None,prodSourceLabels=None):
    p = TaskCommando(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels)
    p.start()
