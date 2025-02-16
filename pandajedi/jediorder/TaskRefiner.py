import datetime
import sys
import time
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction, JediException
from pandajedi.jedicore.DataCarousel import DataCarouselInterface
from pandajedi.jedicore.FactoryBase import FactoryBase
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.ThreadUtils import ListWithLock, ThreadPool, WorkerThread
from pandajedi.jedirefine import RefinerUtils

from .JediKnight import JediKnight

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# worker class to refine TASK_PARAM to fill JEDI tables
class TaskRefiner(JediKnight, FactoryBase):
    # constructor
    def __init__(self, commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        JediKnight.__init__(self, commuChannel, taskBufferIF, ddmIF, logger)
        FactoryBase.__init__(self, self.vos, self.prodSourceLabels, logger, jedi_config.taskrefine.modConfig)

    # main
    def start(self):
        # start base classes
        JediKnight.start(self)
        FactoryBase.initializeMods(self, self.taskBufferIF, self.ddmIF)
        # go into main loop
        while True:
            startTime = naive_utcnow()
            try:
                # get logger
                tmpLog = MsgWrapper(logger)
                tmpLog.debug("start")
                # loop over all vos
                for vo in self.vos:
                    # loop over all sourceLabels
                    for prodSourceLabel in self.prodSourceLabels:
                        # get the list of tasks to refine
                        tmpList = self.taskBufferIF.getTasksToRefine_JEDI(vo, prodSourceLabel)
                        if tmpList is None:
                            # failed
                            tmpLog.error("failed to get the list of tasks to refine")
                        else:
                            tmpLog.debug(f"got {len(tmpList)} tasks")
                            # put to a locked list
                            taskList = ListWithLock(tmpList)
                            # make thread pool
                            threadPool = ThreadPool()
                            # get work queue mapper
                            workQueueMapper = self.taskBufferIF.getWorkQueueMap()
                            # make workers
                            nWorker = jedi_config.taskrefine.nWorkers
                            for _ in range(nWorker):
                                thr = TaskRefinerThread(taskList, threadPool, self.taskBufferIF, self.ddmIF, self, workQueueMapper)
                                thr.start()
                            # join
                            threadPool.join()
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                tmpLog.error(f"failed in {self.__class__.__name__}.start() with {errtype.__name__} {errvalue}")
                tmpLog.error(f"Traceback: {traceback.format_exc()}")
            # sleep if needed
            loopCycle = jedi_config.taskrefine.loopCycle
            timeDelta = naive_utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep(max_val=loopCycle)


# thread for real worker
class TaskRefinerThread(WorkerThread):
    # constructor
    def __init__(self, taskList, threadPool, taskbufferIF, ddmIF, implFactory, workQueueMapper):
        # initialize worker with no semaphore
        WorkerThread.__init__(self, None, threadPool, logger)
        # attributes
        self.taskList = taskList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.implFactory = implFactory
        self.workQueueMapper = workQueueMapper
        self.msgType = "taskrefiner"

    # main
    def runImpl(self):
        while True:
            try:
                # get a part of list
                nTasks = 10
                taskList = self.taskList.get(nTasks)
                # no more datasets
                if len(taskList) == 0:
                    self.logger.info(f"{self.__class__.__name__} terminating since no more items")
                    return
                # loop over all tasks
                for jediTaskID, splitRule, taskStatus, parent_tid in taskList:
                    # make logger
                    tmpLog = MsgWrapper(self.logger, f"< jediTaskID={jediTaskID} >")
                    tmpLog.debug("start")
                    tmpStat = Interaction.SC_SUCCEEDED
                    errStr = ""
                    # read task parameters
                    try:
                        taskParam = None
                        taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(jediTaskID)
                        taskParamMap = RefinerUtils.decodeJSON(taskParam)
                    except Exception:
                        errtype, errvalue = sys.exc_info()[:2]
                        errStr = f"conversion to map from json failed with {errtype.__name__}:{errvalue}"
                        tmpLog.debug(taskParam)
                        tmpLog.error(errStr)
                        continue
                        tmpStat = Interaction.SC_FAILED
                    # get impl
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info("getting Impl")
                        try:
                            # get VO and sourceLabel
                            vo = taskParamMap["vo"]
                            prodSourceLabel = taskParamMap["prodSourceLabel"]
                            taskType = taskParamMap["taskType"]
                            tmpLog.info(f"vo={vo} sourceLabel={prodSourceLabel} taskType={taskType}")
                            # get impl
                            impl = self.implFactory.instantiateImpl(vo, prodSourceLabel, taskType, self.taskBufferIF, self.ddmIF)
                            if impl is None:
                                # task refiner is undefined
                                errStr = f"task refiner is undefined for vo={vo} sourceLabel={prodSourceLabel}"
                                tmpLog.error(errStr)
                                tmpStat = Interaction.SC_FAILED
                            # get data carousel interface
                            data_carousel_interface = DataCarouselInterface(self.taskBufferIF, self.ddmIF.getInterface(vo))
                            if data_carousel_interface is None:
                                # data carousel interface is undefined
                                errStr = f"data carousel interface is undefined for vo={vo}"
                                tmpLog.error(errStr)
                                tmpStat = Interaction.SC_FAILED
                            # get data carousel config map
                            dc_config_map = data_carousel_interface.dc_config_map
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            errStr = f"failed to get task refiner with {errtype.__name__}:{errvalue}"
                            tmpLog.error(errStr)
                            tmpStat = Interaction.SC_FAILED
                    # adjust task parameters
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info("adjusting task parameters")
                        try:
                            if "inputPreStaging" not in taskParamMap:
                                if dc_config_map and (user_name := taskParamMap.get("userName")) in dc_config_map.early_access_users:
                                    # enable input pre-staging for early access user
                                    taskParamMap["inputPreStaging"] = True
                                    tmpLog.debug(f"set inputPreStaging for data carousel early access user {user_name}")
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            errStr = f"failed to adjust task parameters with {errtype.__name__}:{errvalue}"
                            tmpLog.error(errStr)
                            tmpStat = Interaction.SC_FAILED
                    # extract common parameters
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info("extracting common")
                        try:
                            # initialize impl
                            impl.initializeRefiner(tmpLog)
                            impl.oldTaskStatus = taskStatus
                            # extract common parameters
                            impl.extractCommon(jediTaskID, taskParamMap, self.workQueueMapper, splitRule)
                            # set parent tid
                            if parent_tid not in [None, jediTaskID]:
                                impl.taskSpec.parent_tid = parent_tid
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            # on hold in case of external error
                            if errtype == JediException.ExternalTempError:
                                tmpErrStr = f"pending due to external problem. {errvalue}"
                                setFrozenTime = True
                                impl.taskSpec.status = taskStatus
                                impl.taskSpec.setOnHold()
                                impl.taskSpec.setErrDiag(tmpErrStr)
                                # not to update some task attributes
                                impl.taskSpec.resetRefinedAttrs()
                                tmpLog.info(tmpErrStr)
                                self.taskBufferIF.updateTask_JEDI(
                                    impl.taskSpec,
                                    {"jediTaskID": impl.taskSpec.jediTaskID},
                                    oldStatus=[taskStatus],
                                    insertUnknown=impl.unknownDatasetList,
                                    setFrozenTime=setFrozenTime,
                                )
                                continue
                            errStr = f"failed to extract common parameters with {errtype.__name__}:{errvalue} {traceback.format_exc()}"
                            tmpLog.error(errStr)
                            tmpStat = Interaction.SC_FAILED
                    # check attribute length
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info("checking attribute length")
                        if not impl.taskSpec.checkAttrLength():
                            tmpLog.error(impl.taskSpec.errorDialog)
                            tmpStat = Interaction.SC_FAILED
                    # data carousel (input pre-staging) ; currently only for analysis tasks
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        # set of datasets not requiring staging
                        no_staging_datasets = set()
                        # check datasets to pre-stage
                        if taskParamMap.get("inputPreStaging") and taskParamMap.get("taskType") == "anal" and taskParamMap.get("prodSourceLabel") == "user":
                            tmpLog.info("checking about data carousel")
                            try:
                                ds_list_to_prestage, ds_on_disk_list = data_carousel_interface.get_input_datasets_to_prestage(jediTaskID, taskParamMap)
                                if ds_list_to_prestage is None:
                                    # error to get datasets to prestage
                                    tmpLog.debug("nothing found to prestage; skipped")
                                elif not ds_list_to_prestage:
                                    # found no datasets on tape to prestage
                                    tmpLog.debug("no need to prestage, try to resume task from staging")
                                    # no dataset needs pre-staging; resume task from staging
                                    self.taskBufferIF.sendCommandTaskPanda(jediTaskID, "TaskRefiner. No need to prestage. Resumed from staging", True, "resume")
                                else:
                                    # submit data carousel requests for dataset to pre-stage
                                    tmpLog.info("to prestage, submitting data carousel requests")
                                    tmp_ret = data_carousel_interface.submit_data_carousel_requests(jediTaskID, ds_list_to_prestage)
                                    if tmp_ret:
                                        taskParamMap["toStaging"] = True
                                        tmpLog.info("submitted data carousel requests; set toStaging")
                                    else:
                                        tmpLog.error("failed to submit data carousel requests")
                                # update no_staging_datasets with datasets already on datadisks
                                no_staging_datasets.update(set(ds_on_disk_list))
                            except Exception:
                                errtype, errvalue = sys.exc_info()[:2]
                                errStr = f"failed to check about data carousel with {errtype.__name__}:{errvalue}"
                                tmpLog.error(errStr)
                                tmpStat = Interaction.SC_FAILED
                    # staging
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        if "toStaging" in taskParamMap and taskStatus not in ["staged", "rerefine"]:
                            errStr = "wait until staging is done"
                            impl.taskSpec.status = "staging"
                            impl.taskSpec.oldStatus = taskStatus
                            impl.taskSpec.setErrDiag(errStr)
                            # not to update some task attributes
                            impl.taskSpec.resetRefinedAttrs()
                            tmpLog.info(errStr)
                            self.taskBufferIF.updateTask_JEDI(
                                impl.taskSpec, {"jediTaskID": impl.taskSpec.jediTaskID}, oldStatus=[taskStatus], updateDEFT=False, setFrozenTime=False
                            )
                            continue
                    # check parent
                    noWaitParent = False
                    parentState = None
                    if tmpStat == Interaction.SC_SUCCEEDED and parent_tid not in [None, jediTaskID]:
                        tmpLog.info("check parent task")
                        try:
                            tmpStat = self.taskBufferIF.checkParentTask_JEDI(parent_tid)
                            parentState = tmpStat
                            if tmpStat == "completed":
                                # parent is done
                                tmpStat = Interaction.SC_SUCCEEDED
                            elif tmpStat is None or tmpStat == "running":
                                if not impl.taskSpec.noWaitParent():
                                    # parent is running
                                    errStr = f"pending until parent task {parent_tid} is done"
                                    impl.taskSpec.status = taskStatus
                                    impl.taskSpec.setOnHold()
                                    impl.taskSpec.setErrDiag(errStr)
                                    # not to update some task attributes
                                    impl.taskSpec.resetRefinedAttrs()
                                    tmpLog.info(errStr)
                                    self.taskBufferIF.updateTask_JEDI(
                                        impl.taskSpec, {"jediTaskID": impl.taskSpec.jediTaskID}, oldStatus=[taskStatus], setFrozenTime=False
                                    )
                                    continue
                                else:
                                    # not wait for parent
                                    tmpStat = Interaction.SC_SUCCEEDED
                                    noWaitParent = True
                            else:
                                # parent is corrupted
                                tmpStat = Interaction.SC_FAILED
                                tmpErrStr = f"parent task {parent_tid} failed to complete"
                                impl.taskSpec.setErrDiag(tmpErrStr)
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            errStr = f"failed to check parent task with {errtype.__name__}:{errvalue}"
                            tmpLog.error(errStr)
                            tmpStat = Interaction.SC_FAILED

                    # refine
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info(f"refining with {impl.__class__.__name__}")
                        try:
                            tmpStat = impl.doRefine(jediTaskID, taskParamMap)
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            # wait unknown input if noWaitParent or waitInput
                            toFinish = False
                            if (
                                ((impl.taskSpec.noWaitParent() or impl.taskSpec.waitInput()) and errtype == JediException.UnknownDatasetError)
                                or parentState == "running"
                                or errtype == Interaction.JEDITemporaryError
                            ):
                                if impl.taskSpec.noWaitParent() and errtype == JediException.UnknownDatasetError and parentState != "running":
                                    if impl.taskSpec.allowEmptyInput():
                                        tmpErrStr = f"finishing due to missing input while parent is {parentState}"
                                        toFinish = True
                                        setFrozenTime = False
                                    else:
                                        tmpErrStr = f"pending due to missing input while parent is {parentState}"
                                        setFrozenTime = True
                                elif impl.taskSpec.noWaitParent() or parentState == "running":
                                    tmpErrStr = f"pending until parent produces input. parent is {parentState}"
                                    setFrozenTime = False
                                elif errtype == Interaction.JEDITemporaryError:
                                    tmpErrStr = f"pending due to DDM problem. {errvalue}"
                                    setFrozenTime = True
                                else:
                                    tmpErrStr = "pending until input is staged"
                                    setFrozenTime = True
                                if toFinish:
                                    impl.taskSpec.status = "finishing"
                                else:
                                    impl.taskSpec.status = taskStatus
                                    impl.taskSpec.setOnHold()
                                impl.taskSpec.setErrDiag(tmpErrStr)
                                # not to update some task attributes
                                impl.taskSpec.resetRefinedAttrs()
                                tmpLog.info(tmpErrStr)
                                self.taskBufferIF.updateTask_JEDI(
                                    impl.taskSpec,
                                    {"jediTaskID": impl.taskSpec.jediTaskID},
                                    oldStatus=[taskStatus],
                                    insertUnknown=impl.unknownDatasetList,
                                    setFrozenTime=setFrozenTime,
                                )
                                continue
                            elif (
                                not (impl.taskSpec.noWaitParent() or impl.taskSpec.waitInput())
                                and errtype == JediException.UnknownDatasetError
                                and impl.taskSpec.allowEmptyInput()
                            ):
                                impl.taskSpec.status = "finishing"
                                tmpErrStr = f"finishing due to missing input after parent is {parentState}"
                                impl.taskSpec.setErrDiag(tmpErrStr)
                                # not to update some task attributes
                                impl.taskSpec.resetRefinedAttrs()
                                tmpLog.info(tmpErrStr)
                                self.taskBufferIF.updateTask_JEDI(
                                    impl.taskSpec, {"jediTaskID": impl.taskSpec.jediTaskID}, oldStatus=[taskStatus], insertUnknown=impl.unknownDatasetList
                                )
                                continue
                            else:
                                errStr = f"failed to refine task with {errtype.__name__}:{errvalue}"
                                tmpLog.error(errStr)
                                tmpStat = Interaction.SC_FAILED
                    # adjust specs after refining
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        try:
                            if no_staging_datasets:
                                # set no_staging attribute (for datasets not requiring staging
                                tmp_ds_set = set()
                                for dataset_spec in impl.inMasterDatasetSpec:
                                    if dataset_spec.datasetName in no_staging_datasets:
                                        dataset_spec.set_no_staging(True)
                                        tmp_ds_set.add(dataset_spec.datasetName)
                                if tmp_ds_set:
                                    tmpLog.debug(f"set no_staging for datasets on DISKs: {list(tmp_ds_set)}")
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            errStr = f"failed to adjust spect after refining {errtype.__name__}:{errvalue}"
                            tmpLog.error(errStr)
                            tmpStat = Interaction.SC_FAILED
                    # register
                    if tmpStat != Interaction.SC_SUCCEEDED:
                        tmpLog.error("failed to refine the task")
                        if impl is None or impl.taskSpec is None:
                            tmpTaskSpec = JediTaskSpec()
                            tmpTaskSpec.jediTaskID = jediTaskID
                        else:
                            tmpTaskSpec = impl.taskSpec
                        tmpTaskSpec.status = "tobroken"
                        if errStr != "":
                            tmpTaskSpec.setErrDiag(errStr, True)
                        self.taskBufferIF.updateTask_JEDI(tmpTaskSpec, {"jediTaskID": tmpTaskSpec.jediTaskID}, oldStatus=[taskStatus])
                    else:
                        tmpLog.info("registering")
                        # fill JEDI tables
                        try:
                            # enable protection against task duplication
                            if "uniqueTaskName" in taskParamMap and taskParamMap["uniqueTaskName"] and not impl.taskSpec.checkPreProcessed():
                                uniqueTaskName = True
                            else:
                                uniqueTaskName = False
                            strTaskParams = None
                            if impl.updatedTaskParams is not None:
                                strTaskParams = RefinerUtils.encodeJSON(impl.updatedTaskParams)
                            if taskStatus in ["registered", "staged"]:
                                # unset pre-process flag
                                if impl.taskSpec.checkPreProcessed():
                                    impl.taskSpec.setPostPreProcess()
                                # full registration
                                tmpStat, newTaskStatus = self.taskBufferIF.registerTaskInOneShot_JEDI(
                                    jediTaskID,
                                    impl.taskSpec,
                                    impl.inMasterDatasetSpec,
                                    impl.inSecDatasetSpecList,
                                    impl.outDatasetSpecList,
                                    impl.outputTemplateMap,
                                    impl.jobParamsTemplate,
                                    strTaskParams,
                                    impl.unmergeMasterDatasetSpec,
                                    impl.unmergeDatasetSpecMap,
                                    uniqueTaskName,
                                    taskStatus,
                                )
                                if not tmpStat:
                                    tmpErrStr = "failed to register the task to JEDI in a single shot"
                                    tmpLog.error(tmpErrStr)
                                    tmpTaskSpec = JediTaskSpec()
                                    tmpTaskSpec.status = newTaskStatus
                                    tmpTaskSpec.errorDialog = impl.taskSpec.errorDialog
                                    tmpTaskSpec.setErrDiag(tmpErrStr, True)
                                    self.taskBufferIF.updateTask_JEDI(tmpTaskSpec, {"jediTaskID": impl.taskSpec.jediTaskID}, oldStatus=[taskStatus])
                                tmpMsg = f"set task_status={newTaskStatus}"
                                tmpLog.info(tmpMsg)
                                tmpLog.sendMsg(tmpMsg, self.msgType)
                                # send message to contents feeder if the task is registered
                                if tmpStat and impl.taskSpec.is_msg_driven():
                                    push_ret = self.taskBufferIF.push_task_trigger_message("jedi_contents_feeder", jediTaskID, task_spec=impl.taskSpec)
                                    if push_ret:
                                        tmpLog.debug("pushed trigger message to jedi_contents_feeder")
                                    else:
                                        tmpLog.warning("failed to push trigger message to jedi_contents_feeder")
                            else:
                                # disable scouts if previous attempt didn't use it
                                if not impl.taskSpec.useScout(splitRule):
                                    impl.taskSpec.setUseScout(False)
                                # disallow to reset some attributes
                                impl.taskSpec.reserve_old_attributes()
                                # update task with new params
                                self.taskBufferIF.updateTask_JEDI(impl.taskSpec, {"jediTaskID": impl.taskSpec.jediTaskID}, oldStatus=[taskStatus])
                                # appending for incremental execution
                                tmpStat = self.taskBufferIF.appendDatasets_JEDI(jediTaskID, impl.inMasterDatasetSpec, impl.inSecDatasetSpecList)
                                if not tmpStat:
                                    tmpLog.error("failed to append datasets for incexec")
                        except Exception:
                            errtype, errvalue = sys.exc_info()[:2]
                            tmpErrStr = f"failed to register the task to JEDI with {errtype.__name__}:{errvalue}"
                            tmpLog.error(tmpErrStr)
                        else:
                            tmpLog.info("done")
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                logger.error(f"{self.__class__.__name__} failed in runImpl() with {errtype.__name__}:{errvalue}")


def launcher(commuChannel, taskBufferIF, ddmIF, vos=None, prodSourceLabels=None):
    p = TaskRefiner(commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels)
    p.start()
