import sys
import traceback

from six import iteritems

from .TypicalWatchDogBase import TypicalWatchDogBase
from .JumboWatchDog import JumboWatchDog

from pandajedi.jedicore import JediCoreUtils
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jediconfig import jedi_config
from pandajedi.jedibrokerage import AtlasBrokerUtils

from pandaserver.taskbuffer import JobUtils
from pandaserver.dataservice import DataServiceUtils

from pandacommon.pandalogger.PandaLogger import PandaLogger

logger = PandaLogger().getLogger(__name__.split('.')[-1])

# watchdog for ATLAS production
class AtlasProdWatchDog(TypicalWatchDogBase):

    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        TypicalWatchDogBase.__init__(self, taskBufferIF, ddmIF)

    # main
    def doAction(self):
        try:
            # get logger
            tmpLog = MsgWrapper(logger)
            tmpLog.debug('start')

            # action for priority boost
            self.doActionForPriorityBoost(tmpLog)

            # action for reassign
            self.doActionForReassign(tmpLog)

            # action for throttled
            self.doActionForThrottled(tmpLog)

            # action for high prio pending
            for minPriority, timeoutVal in [(950, 10),
                                            (900, 30),
                                            ]:
                self.doActionForHighPrioPending(tmpLog, minPriority, timeoutVal)

            # action to set scout job data w/o scouts
            self.doActionToSetScoutJobData(tmpLog)

            # action to throttle jobs in paused tasks
            self.doActionToThrottleJobInPausedTasks(tmpLog)

            # action for jumbo
            jumbo = JumboWatchDog(self.taskBufferIF, self.ddmIF, tmpLog, 'atlas', 'managed')
            jumbo.run()
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error('failed with {0}:{1} {2}'.format(errtype.__name__, errvalue, traceback.format_exc()))
        # return
        tmpLog.debug('done')
        return self.SC_SUCCEEDED

    # action for priority boost
    def doActionForPriorityBoost(self, gTmpLog):
        # get work queue mapper
        workQueueMapper = self.taskBufferIF.getWorkQueueMap()
        # get list of work queues
        workQueueList = workQueueMapper.getAlignedQueueList(self.vo, self.prodSourceLabel)
        resource_types = self.taskBufferIF.load_resource_types()
        # loop over all work queues
        for workQueue in workQueueList:
            break_loop = False # for special workqueues we only need to iterate once
            for resource_type in resource_types:
                gTmpLog.debug('start workQueue={0}'.format(workQueue.queue_name))
                # get tasks to be boosted
                if workQueue.is_global_share:
                    task_criteria = {'gshare': workQueue.queue_name,
                                     'resource_type': resource_type.resource_name}
                else:
                    break_loop = True
                    task_criteria = {'workQueue_ID': workQueue.queue_id}
                dataset_criteria = {'masterID': None, 'type': ['input', 'pseudo_input']}
                task_param_list = ['jediTaskID', 'taskPriority', 'currentPriority', 'parent_tid', 'gshare']
                dataset_param_list = ['nFiles', 'nFilesUsed', 'nFilesTobeUsed', 'nFilesFinished', 'nFilesFailed']
                taskVarList = self.taskBufferIF.getTasksWithCriteria_JEDI(self.vo, self.prodSourceLabel, ['running'],
                                                                          taskCriteria= task_criteria,
                                                                          datasetCriteria=dataset_criteria,
                                                                          taskParamList=task_param_list,
                                                                          datasetParamList=dataset_param_list,
                                                                          taskLockColumn='throttledTime',
                                                                          taskLockInterval=20)
                boostedPrio = 900
                toBoostRatio = 0.95
                for taskParam,datasetParam in taskVarList:
                    jediTaskID = taskParam['jediTaskID']
                    taskPriority = taskParam['taskPriority']
                    currentPriority = taskParam['currentPriority']
                    parent_tid = taskParam['parent_tid']
                    gshare = taskParam['gshare']
                    # check parent
                    parentState = None
                    if parent_tid not in [None, jediTaskID]:
                        parentState = self.taskBufferIF.checkParentTask_JEDI(parent_tid)
                        if parentState != 'completed':
                            gTmpLog.info('#ATM #KV label=managed jediTaskID={0} skip prio boost since parent_id={1} has parent_status={2}'
                                         .format(jediTaskID, parent_tid, parentState))
                            continue
                    nFiles = datasetParam['nFiles']
                    nFilesFinished = datasetParam['nFilesFinished']
                    nFilesFailed = datasetParam['nFilesFailed']
                    # get num jobs
                    nJobs = self.taskBufferIF.getNumJobsForTask_JEDI(jediTaskID)
                    nRemJobs = None
                    if nJobs is not None:
                        try:
                            nRemJobs = int(float(nFiles-nFilesFinished-nFilesFailed) * float(nJobs) / float(nFiles))
                        except Exception:
                            pass
                    tmpStr = 'jediTaskID={0} nFiles={1} nFilesFinishedFailed={2} '.format(jediTaskID,nFiles,nFilesFinished+nFilesFailed)
                    tmpStr += 'nJobs={0} nRemJobs={1} parent_tid={2} parentStatus={3}'.format(nJobs, nRemJobs, parent_tid, parentState)
                    gTmpLog.debug(tmpStr)

                    try:
                        if nRemJobs is not None and float(nFilesFinished+nFilesFailed) / float(nFiles) >= toBoostRatio and nRemJobs <= 100:
                            # skip high enough
                            if currentPriority < boostedPrio:
                                gTmpLog.info(' >>> action=priority_boosting of jediTaskID={0} to priority={1} #ATM #KV label=managed '.
                                             format(jediTaskID, boostedPrio))
                                self.taskBufferIF.changeTaskPriorityPanda(jediTaskID, boostedPrio)

                            # skip express or non global share
                            newShare = 'Express'
                            newShareType = 'managed'
                            if gshare != newShare and workQueue.is_global_share and \
                                    workQueue.queue_type == newShareType:
                                gTmpLog.info(' >>> action=gshare_reassignment jediTaskID={0} from gshare_old={2} to gshare_new={1} #ATM #KV label=managed'.
                                             format(jediTaskID, newShare, gshare))
                                self.taskBufferIF.reassignShare([jediTaskID], newShare, True)
                            gTmpLog.info('>>> done jediTaskID={0}'.format(jediTaskID))
                    except Exception:
                        pass

                if break_loop:
                    break

    # action for reassignment
    def doActionForReassign(self,gTmpLog):
        # get DDM I/F
        ddmIF = self.ddmIF.getInterface(self.vo)
        # get site mapper
        siteMapper = self.taskBufferIF.getSiteMapper()
        # get tasks to get reassigned
        taskList = self.taskBufferIF.getTasksToReassign_JEDI(self.vo,self.prodSourceLabel)

        gTmpLog.debug('got {0} tasks to reassign'.format(len(taskList)))
        for taskSpec in taskList:
            tmpLog = MsgWrapper(logger, '< jediTaskID={0} >'.format(taskSpec.jediTaskID))
            tmpLog.debug('start to reassign')
            # DDM backend
            ddmBackEnd = taskSpec.getDdmBackEnd()
            # get datasets
            tmpStat,datasetSpecList = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(taskSpec.jediTaskID,['output','log'])
            if tmpStat is not True:
                tmpLog.error('failed to get datasets')
                continue
            # update DB
            if not taskSpec.useWorldCloud():
                # update cloudtasks
                tmpStat = self.taskBufferIF.setCloudTaskByUser('jedi',taskSpec.jediTaskID,taskSpec.cloud,'assigned',True)
                if tmpStat != 'SUCCEEDED':
                    tmpLog.error('failed to update CloudTasks')
                    continue
                # check cloud
                if not siteMapper.checkCloud(taskSpec.cloud):
                    tmpLog.error("cloud={0} doesn't exist".format(taskSpec.cloud))
                    continue
            else:
                # re-run task brokerage
                if taskSpec.nucleus in [None,'']:
                    taskSpec.status = 'assigning'
                    taskSpec.oldStatus = None
                    taskSpec.setToRegisterDatasets()
                    self.taskBufferIF.updateTask_JEDI(taskSpec,{'jediTaskID': taskSpec.jediTaskID},
                                                      setOldModTime=True)
                    tmpLog.debug('#ATM #KV label=managed action=trigger_new_brokerage by setting task_status={0}'.
                                 format(taskSpec.status))
                    continue

                # get nucleus
                nucleusSpec = siteMapper.getNucleus(taskSpec.nucleus)
                if nucleusSpec is None:
                    tmpLog.error("nucleus={0} doesn't exist".format(taskSpec.nucleus))
                    continue

                # set nucleus
                retMap = {taskSpec.jediTaskID: AtlasBrokerUtils.getDictToSetNucleus(nucleusSpec,datasetSpecList)}
                tmpRet = self.taskBufferIF.setCloudToTasks_JEDI(retMap)

            # get T1/nucleus
            if not taskSpec.useWorldCloud():
                t1SiteName = siteMapper.getCloud(taskSpec.cloud)['dest']
            else:
                t1SiteName = nucleusSpec.getOnePandaSite()
            t1Site = siteMapper.getSite(t1SiteName)

            # loop over all datasets
            isOK = True
            for datasetSpec in datasetSpecList:
                tmpLog.debug('dataset={0}'.format(datasetSpec.datasetName))
                if DataServiceUtils.getDistributedDestination(datasetSpec.storageToken) is not None:
                    tmpLog.debug('skip {0} is distributed'.format(datasetSpec.datasetName))
                    continue
                # get location
                location = siteMapper.getDdmEndpoint(t1Site.sitename, datasetSpec.storageToken, taskSpec.prodSourceLabel,
                                                     JobUtils.translate_tasktype_to_jobtype(taskSpec.taskType))
                # make subscription
                try:
                    tmpLog.debug('registering subscription to {0} with backend={1}'.format(location,
                                                                                           ddmBackEnd))
                    tmpStat = ddmIF.registerDatasetSubscription(datasetSpec.datasetName,location,
                                                                'Production Output',asynchronous=True)
                    if tmpStat is not True:
                        tmpLog.error("failed to make subscription")
                        isOK = False
                        break
                except Exception:
                    errtype,errvalue = sys.exc_info()[:2]
                    tmpLog.warning('failed to make subscription with {0}:{1}'.format(errtype.__name__,errvalue))
                    isOK = False
                    break
            # succeeded
            if isOK:
                # activate task
                if taskSpec.oldStatus in ['assigning','exhausted',None]:
                    taskSpec.status = 'ready'
                else:
                    taskSpec.status = taskSpec.oldStatus
                taskSpec.oldStatus = None
                self.taskBufferIF.updateTask_JEDI(taskSpec,{'jediTaskID':taskSpec.jediTaskID},
                                                  setOldModTime=True)
                tmpLog.debug('finished to reassign')

    # action for throttled tasks
    def doActionForThrottled(self,gTmpLog):
        # release tasks
        nTasks = self.taskBufferIF.releaseThrottledTasks_JEDI(self.vo,self.prodSourceLabel)
        gTmpLog.debug('released {0} tasks'.format(nTasks))

        # throttle tasks
        nTasks = self.taskBufferIF.throttleTasks_JEDI(self.vo,self.prodSourceLabel,
                                                      jedi_config.watchdog.waitForThrottled)
        gTmpLog.debug('throttled {0} tasks'.format(nTasks))

    # action for high priority pending tasks
    def doActionForHighPrioPending(self, gTmpLog, minPriority, timeoutVal):
        timeoutForPending = None
        # try to get the timeout from the config files
        if hasattr(jedi_config.watchdog,'timeoutForPendingVoLabel'):
            timeoutForPending = JediCoreUtils.getConfigParam(jedi_config.watchdog.timeoutForPendingVoLabel,self.vo,self.prodSourceLabel)
        if timeoutForPending is None:
            timeoutForPending = jedi_config.watchdog.timeoutForPending
        timeoutForPending = int(timeoutForPending) * 24
        tmpRet = self.taskBufferIF.reactivatePendingTasks_JEDI(self.vo, self.prodSourceLabel,
                                                               timeoutVal, timeoutForPending,
                                                               minPriority=minPriority)
        if tmpRet is None:
            # failed
            gTmpLog.error('failed to reactivate high priority (>{0}) tasks'.format(minPriority))
        else:
            gTmpLog.info('reactivated high priority (>{0}) {1} tasks'.format(minPriority,tmpRet))

    # action to set scout job data w/o scouts
    def doActionToSetScoutJobData(self,gTmpLog):
        tmpRet = self.taskBufferIF.setScoutJobDataToTasks_JEDI(self.vo,self.prodSourceLabel)
        if tmpRet is None:
            # failed
            gTmpLog.error('failed to set scout job data')
        else:
            gTmpLog.info('set scout job data successfully')

    # action to throttle jobs in paused tasks
    def doActionToThrottleJobInPausedTasks(self,gTmpLog):
        tmpRet = self.taskBufferIF.throttleJobsInPausedTasks_JEDI(self.vo,self.prodSourceLabel)
        if tmpRet is None:
            # failed
            gTmpLog.error('failed to thottle jobs in paused tasks')
        else:
            for jediTaskID, pandaIDs in iteritems(tmpRet):
                gTmpLog.info('throttled jobs in paused jediTaskID={0} successfully'.format(jediTaskID))
                tmpRet = self.taskBufferIF.killJobs(pandaIDs,'reassign','51',True)
                gTmpLog.info('reassigned {0} jobs in paused jediTaskID={1} with {2}'.format(len(pandaIDs), jediTaskID, tmpRet))
