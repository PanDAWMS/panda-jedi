import sys
import time
import datetime
import traceback

from pandajedi.jedicore.ThreadUtils import ListWithLock,ThreadPool,WorkerThread
from pandajedi.jedicore import Interaction
from pandajedi.jedicore import JediException
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.FactoryBase import FactoryBase
from pandajedi.jedirefine import RefinerUtils
from .JediKnight import JediKnight

from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandajedi.jedicore.JediDatasetSpec import JediDatasetSpec
from pandajedi.jediconfig import jedi_config


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# worker class to refine TASK_PARAM to fill JEDI tables
class TaskRefiner (JediKnight,FactoryBase):

    # constructor
    def __init__(self, commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        JediKnight.__init__(self,commuChannel,taskBufferIF,ddmIF,logger)
        FactoryBase.__init__(self, self.vos, self.prodSourceLabels, logger,
                             jedi_config.taskrefine.modConfig)


    # main
    def start(self):
        # start base classes
        JediKnight.start(self)
        FactoryBase.initializeMods(self, self.taskBufferIF, self.ddmIF)
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
                        # get the list of tasks to refine
                        tmpList = self.taskBufferIF.getTasksToRefine_JEDI(vo, prodSourceLabel)
                        if tmpList is None:
                            # failed
                            tmpLog.error('failed to get the list of tasks to refine')
                        else:
                            tmpLog.debug('got {0} tasks'.format(len(tmpList)))
                            # put to a locked list
                            taskList = ListWithLock(tmpList)
                            # make thread pool
                            threadPool = ThreadPool()
                            # get work queue mapper
                            workQueueMapper = self.taskBufferIF.getWorkQueueMap()
                            # make workers
                            nWorker = jedi_config.taskrefine.nWorkers
                            for iWorker in range(nWorker):
                                thr = TaskRefinerThread(taskList, threadPool,
                                                        self.taskBufferIF,
                                                        self.ddmIF,
                                                        self, workQueueMapper)
                                thr.start()
                            # join
                            threadPool.join()
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                tmpLog.error('failed in {0}.start() with {1} {2}'.format(self.__class__.__name__,
                                                                         errtype.__name__, errvalue))
                tmpLog.error('Traceback: {0}'.format(traceback.format_exc()))
            # sleep if needed
            loopCycle = jedi_config.taskrefine.loopCycle
            timeDelta = datetime.datetime.utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep(max_val=loopCycle)



# thread for real worker
class TaskRefinerThread (WorkerThread):

    # constructor
    def __init__(self, taskList, threadPool, taskbufferIF, ddmIF, implFactory, workQueueMapper):
        # initialize woker with no semaphore
        WorkerThread.__init__(self,None,threadPool,logger)
        # attributres
        self.taskList = taskList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.implFactory = implFactory
        self.workQueueMapper = workQueueMapper
        self.msgType = 'taskrefiner'



    # main
    def runImpl(self):
        while True:
            try:
                # get a part of list
                nTasks = 10
                taskList = self.taskList.get(nTasks)
                # no more datasets
                if len(taskList) == 0:
                    self.logger.info('{0} terminating since no more items'.format(self.__class__.__name__))
                    return
                # loop over all tasks
                for jediTaskID,splitRule,taskStatus,parent_tid in taskList:
                    # make logger
                    tmpLog = MsgWrapper(self.logger,'< jediTaskID={0} >'.format(jediTaskID))
                    tmpLog.debug('start')
                    tmpStat = Interaction.SC_SUCCEEDED
                    errStr = ''
                    # read task parameters
                    try:
                        taskParam = None
                        taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(jediTaskID)
                        taskParamMap = RefinerUtils.decodeJSON(taskParam)
                    except Exception:
                        errtype,errvalue = sys.exc_info()[:2]
                        errStr = 'conversion to map from json failed with {0}:{1}'.format(errtype.__name__,errvalue)
                        tmpLog.debug(taskParam)
                        tmpLog.error(errStr)
                        continue
                        tmpStat = Interaction.SC_FAILED
                    # get impl
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info('getting Impl')
                        try:
                            # get VO and sourceLabel
                            vo = taskParamMap['vo']
                            prodSourceLabel = taskParamMap['prodSourceLabel']
                            taskType = taskParamMap['taskType']
                            tmpLog.info('vo={0} sourceLabel={1} taskType={2}'.format(vo,prodSourceLabel,taskType))
                            # get impl
                            impl = self.implFactory.instantiateImpl(vo, prodSourceLabel, taskType,
                                                                    self.taskBufferIF, self.ddmIF)
                            if impl is None:
                                # task refiner is undefined
                                errStr = 'task refiner is undefined for vo={0} sourceLabel={1}'.format(vo,prodSourceLabel)
                                tmpLog.error(errStr)
                                tmpStat = Interaction.SC_FAILED
                        except Exception:
                            errtype,errvalue = sys.exc_info()[:2]
                            errStr = 'failed to get task refiner with {0}:{1}'.format(errtype.__name__,errvalue)
                            tmpLog.error(errStr)
                            tmpStat = Interaction.SC_FAILED
                    # extract common parameters
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info('extracting common')
                        try:
                            # initalize impl
                            impl.initializeRefiner(tmpLog)
                            impl.oldTaskStatus = taskStatus
                            # extract common parameters
                            impl.extractCommon(jediTaskID, taskParamMap, self.workQueueMapper, splitRule)
                            # set parent tid
                            if parent_tid not in [None,jediTaskID]:
                                impl.taskSpec.parent_tid = parent_tid
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            # on hold in case of external error
                            if errtype == JediException.ExternalTempError:
                                tmpErrStr = 'pending due to external problem. {0}'.format(errvalue)
                                setFrozenTime = True
                                impl.taskSpec.status = taskStatus
                                impl.taskSpec.setOnHold()
                                impl.taskSpec.setErrDiag(tmpErrStr)
                                # not to update some task attributes
                                impl.taskSpec.resetRefinedAttrs()
                                tmpLog.info(tmpErrStr)
                                self.taskBufferIF.updateTask_JEDI(impl.taskSpec,{'jediTaskID':impl.taskSpec.jediTaskID},
                                                                  oldStatus=[taskStatus],
                                                                  insertUnknown=impl.unknownDatasetList,
                                                                  setFrozenTime=setFrozenTime)
                                continue
                            errStr = 'failed to extract common parameters with {0}:{1} {2}'.format(errtype.__name__,errvalue,
                                                                                                   traceback.format_exc())
                            tmpLog.error(errStr)
                            tmpStat = Interaction.SC_FAILED
                    # check attribute length
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info('checking attribute length')
                        if not impl.taskSpec.checkAttrLength():
                            tmpLog.error(impl.taskSpec.errorDialog)
                            tmpStat = Interaction.SC_FAILED
                    # staging
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        if 'toStaging' in taskParamMap and taskStatus not in ['staged', 'rerefine']:
                            errStr = 'wait until staging is done'
                            impl.taskSpec.status = 'staging'
                            impl.taskSpec.oldStatus = taskStatus
                            impl.taskSpec.setErrDiag(errStr)
                            # not to update some task attributes
                            impl.taskSpec.resetRefinedAttrs()
                            tmpLog.info(errStr)
                            self.taskBufferIF.updateTask_JEDI(impl.taskSpec, {'jediTaskID':impl.taskSpec.jediTaskID},
                                                              oldStatus=[taskStatus], updateDEFT=False, setFrozenTime=False)
                            continue
                    # check parent
                    noWaitParent = False
                    parentState = None
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        if parent_tid not in [None,jediTaskID]:
                            tmpLog.info('check parent task')
                            try:
                                tmpStat = self.taskBufferIF.checkParentTask_JEDI(parent_tid)
                                parentState = tmpStat
                                if tmpStat == 'completed':
                                    # parent is done
                                    tmpStat = Interaction.SC_SUCCEEDED
                                elif tmpStat is None or tmpStat == 'running':
                                    if not impl.taskSpec.noWaitParent():
                                        # parent is running
                                        errStr = 'pending until parent task {0} is done'.format(parent_tid)
                                        impl.taskSpec.status = taskStatus
                                        impl.taskSpec.setOnHold()
                                        impl.taskSpec.setErrDiag(errStr)
                                        # not to update some task attributes
                                        impl.taskSpec.resetRefinedAttrs()
                                        tmpLog.info(errStr)
                                        self.taskBufferIF.updateTask_JEDI(impl.taskSpec,{'jediTaskID':impl.taskSpec.jediTaskID},
                                                                          oldStatus=[taskStatus],setFrozenTime=False)
                                        continue
                                    else:
                                        # not wait for parent
                                        tmpStat = Interaction.SC_SUCCEEDED
                                        noWaitParent = True
                                else:
                                    # parent is corrupted
                                    tmpStat = Interaction.SC_FAILED
                                    tmpErrStr = 'parent task {0} failed to complete'.format(parent_tid)
                                    impl.taskSpec.setErrDiag(tmpErrStr)
                            except Exception:
                                errtype,errvalue = sys.exc_info()[:2]
                                errStr = 'failed to check parent task with {0}:{1}'.format(errtype.__name__,errvalue)
                                tmpLog.error(errStr)
                                tmpStat = Interaction.SC_FAILED
                    # refine
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info('refining with {0}'.format(impl.__class__.__name__))
                        try:
                            tmpStat = impl.doRefine(jediTaskID,taskParamMap)
                        except Exception:
                            errtype,errvalue = sys.exc_info()[:2]
                            # wait unknown input if noWaitParent or waitInput
                            if ((impl.taskSpec.noWaitParent() or impl.taskSpec.waitInput()) \
                                    and errtype == JediException.UnknownDatasetError) or parentState == 'running' \
                                    or errtype == Interaction.JEDITemporaryError:
                                if impl.taskSpec.noWaitParent() and errtype == JediException.UnknownDatasetError \
                                    and parentState != 'running':
                                    tmpErrStr = 'pending due to missing input while parent is {}'.format(parentState)
                                    setFrozenTime = True
                                elif impl.taskSpec.noWaitParent() or parentState == 'running':
                                    tmpErrStr = 'pending until parent produces input. parent is {}'.format(parentState)
                                    setFrozenTime=False
                                elif errtype == Interaction.JEDITemporaryError:
                                    tmpErrStr = 'pending due to DDM problem. {0}'.format(errvalue)
                                    setFrozenTime=True
                                else:
                                    tmpErrStr = 'pending until input is staged'
                                    setFrozenTime=True
                                impl.taskSpec.status = taskStatus
                                impl.taskSpec.setOnHold()
                                impl.taskSpec.setErrDiag(tmpErrStr)
                                # not to update some task attributes
                                impl.taskSpec.resetRefinedAttrs()
                                tmpLog.info(tmpErrStr)
                                self.taskBufferIF.updateTask_JEDI(impl.taskSpec,{'jediTaskID':impl.taskSpec.jediTaskID},
                                                                  oldStatus=[taskStatus],
                                                                  insertUnknown=impl.unknownDatasetList,
                                                                  setFrozenTime=setFrozenTime)
                                continue
                            else:
                                errStr  = 'failed to refine task with {0}:{1}'.format(errtype.__name__,errvalue)
                                tmpLog.error(errStr)
                                tmpStat = Interaction.SC_FAILED
                    # register
                    if tmpStat != Interaction.SC_SUCCEEDED:
                        tmpLog.error('failed to refine the task')
                        if impl is None or impl.taskSpec is None:
                            tmpTaskSpec = JediTaskSpec()
                            tmpTaskSpec.jediTaskID = jediTaskID
                        else:
                            tmpTaskSpec = impl.taskSpec
                        tmpTaskSpec.status = 'tobroken'
                        if errStr != '':
                            tmpTaskSpec.setErrDiag(errStr,True)
                        self.taskBufferIF.updateTask_JEDI(tmpTaskSpec,{'jediTaskID':tmpTaskSpec.jediTaskID},oldStatus=[taskStatus])
                    else:
                        tmpLog.info('registering')
                        # fill JEDI tables
                        try:
                            # enable protection against task duplication
                            if 'uniqueTaskName' in taskParamMap and taskParamMap['uniqueTaskName'] and \
                                    not impl.taskSpec.checkPreProcessed():
                                uniqueTaskName = True
                            else:
                                uniqueTaskName = False
                            strTaskParams = None
                            if impl.updatedTaskParams is not None:
                                strTaskParams = RefinerUtils.encodeJSON(impl.updatedTaskParams)
                            if taskStatus in ['registered', 'staged']:
                                # unset pre-process flag
                                if impl.taskSpec.checkPreProcessed():
                                    impl.taskSpec.setPostPreProcess()
                                # full registration
                                tmpStat,newTaskStatus = self.taskBufferIF.registerTaskInOneShot_JEDI(jediTaskID,impl.taskSpec,
                                                                                                     impl.inMasterDatasetSpec,
                                                                                                     impl.inSecDatasetSpecList,
                                                                                                     impl.outDatasetSpecList,
                                                                                                     impl.outputTemplateMap,
                                                                                                     impl.jobParamsTemplate,
                                                                                                     strTaskParams,
                                                                                                     impl.unmergeMasterDatasetSpec,
                                                                                                     impl.unmergeDatasetSpecMap,
                                                                                                     uniqueTaskName,
                                                                                                     taskStatus)
                                if not tmpStat:
                                    tmpErrStr = 'failed to register the task to JEDI in a single shot'
                                    tmpLog.error(tmpErrStr)
                                    tmpTaskSpec = JediTaskSpec()
                                    tmpTaskSpec.status = newTaskStatus
                                    tmpTaskSpec.errorDialog = impl.taskSpec.errorDialog
                                    tmpTaskSpec.setErrDiag(tmpErrStr, True)
                                    self.taskBufferIF.updateTask_JEDI(tmpTaskSpec,
                                                                      {'jediTaskID': impl.taskSpec.jediTaskID},
                                                                      oldStatus=[taskStatus])
                                tmpMsg = 'set task_status={0}'.format(newTaskStatus)
                                tmpLog.info(tmpMsg)
                                tmpLog.sendMsg(tmpMsg,self.msgType)
                            else:
                                # disable scouts if previous attempt didn't use it
                                if not impl.taskSpec.useScout(splitRule):
                                    impl.taskSpec.setUseScout(False)
                                # disallow to reset some attributes
                                impl.taskSpec.reserve_old_attributes()
                                # update task with new params
                                self.taskBufferIF.updateTask_JEDI(impl.taskSpec,{'jediTaskID':impl.taskSpec.jediTaskID},
                                                                  oldStatus=[taskStatus])
                                # appending for incremetnal execution
                                tmpStat = self.taskBufferIF.appendDatasets_JEDI(jediTaskID,impl.inMasterDatasetSpec,
                                                                                impl.inSecDatasetSpecList)
                                if not tmpStat:
                                    tmpLog.error('failed to append datasets for incexec')
                        except Exception:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpErrStr = 'failed to register the task to JEDI with {0}:{1}'.format(errtype.__name__,errvalue)
                            tmpLog.error(tmpErrStr)
                        else:
                            tmpLog.info('done')
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('{0} failed in runImpl() with {1}:{2}'.format(self.__class__.__name__,errtype.__name__,errvalue))



########## lauch

def launcher(commuChannel,taskBufferIF,ddmIF,vos=None,prodSourceLabels=None):
    p = TaskRefiner(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels)
    p.start()
