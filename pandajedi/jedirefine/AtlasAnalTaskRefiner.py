import re
import sys
import random

from .TaskRefinerBase import TaskRefinerBase
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandaserver.config import panda_config

from pandaserver.taskbuffer import JobUtils
from pandaserver.dataservice import DataServiceUtils

# brokerage for ATLAS analysis
class AtlasAnalTaskRefiner (TaskRefinerBase):

    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        TaskRefinerBase.__init__(self, taskBufferIF, ddmIF)


    # extract common parameters
    def extractCommon(self,jediTaskID,taskParamMap,workQueueMapper,splitRule):
        processingTypes = taskParamMap['processingType'].split('-')
        # set ddmBackEnd
        if 'ddmBackEnd' not in taskParamMap:
            taskParamMap['ddmBackEnd'] = 'rucio'
        # set sourceURL
        try:
            if 'sourceURL' in taskParamMap:
                for tmpItem in taskParamMap['jobParameters']:
                    if 'value' in tmpItem:
                        tmpItem['value'] = re.sub('\$\{SURL\}',taskParamMap['sourceURL'],tmpItem['value'])
        except Exception:
            pass
        # set transPath
        if 'transPath' not in taskParamMap:
            if 'athena' in processingTypes:
                # athena
                taskParamMap['transPath'] = 'http://{0}:{1}/trf/user/runAthena-00-00-12'.format(panda_config.pserveralias,
                                                                                                panda_config.pserverportcache)
            elif 'cont' in processingTypes:
                # container
                taskParamMap['transPath'] = 'http://{0}:{1}/trf/user/runcontainer'.format(panda_config.pserveralias,
                                                                                          panda_config.pserverportcache)
            else:
                # general executable
                taskParamMap['transPath'] = 'http://{0}:{1}/trf/user/runGen-00-00-02'.format(panda_config.pserveralias,
                                                                                             panda_config.pserverportcache)
                # shorter base walltime
                if 'baseWalltime' not in taskParamMap:
                    taskParamMap['baseWalltime'] = 60
        # set transPath for build
        if 'buildSpec' in taskParamMap and 'transPath' not in taskParamMap['buildSpec']:
            if 'athena' in processingTypes:
                # athena
                taskParamMap['buildSpec']['transPath'] = 'http://{0}:{1}/trf/user/buildJob-00-00-03'.format(panda_config.pserveralias,
                                                                                                            panda_config.pserverportcache)
            else:
                # general executable
                taskParamMap['buildSpec']['transPath'] = 'http://{0}:{1}/trf/user/buildGen-00-00-01'.format(panda_config.pserveralias,
                                                                                                            panda_config.pserverportcache)
        # set transPath for preprocessing
        if 'preproSpec' in taskParamMap and 'transPath' not in taskParamMap['preproSpec']:
            if 'evp' in processingTypes:
                # event picking
                taskParamMap['preproSpec']['transPath'] = 'http://{0}:{1}/trf/user/preEvtPick-00-00-01'.format(panda_config.pserveralias,
                                                                                                               panda_config.pserverportcache)
            elif 'grl' in processingTypes:
                # good run list
                taskParamMap['preproSpec']['transPath'] = 'http://{0}:{1}/trf/user/preGoodRunList-00-00-01'.format(panda_config.pserveralias,
                                                                                                                   panda_config.pserverportcache)
        # set transPath for merge
        if 'mergeSpec' in taskParamMap and 'transPath' not in taskParamMap['mergeSpec']:
            taskParamMap['mergeSpec']['transPath'] = 'http://{0}:{1}/trf/user/runMerge-00-00-02'.format(panda_config.pserveralias,
                                                                                                        panda_config.pserverportcache)
        # min ram count
        if 'ramCount' not in taskParamMap:
            taskParamMap['ramCount'] = 2000
            taskParamMap['ramUnit'] = 'MBPerCore'
        # disk count
        if 'outDiskCount' not in taskParamMap:
            out_disk_count_default = self.taskBufferIF.getConfigValue('taskrefiner', 'OUTDISKCOUNT_ANALY_KB', 'jedi', 'atlas')
            if out_disk_count_default is None or out_disk_count_default < 0:
                out_disk_count_default = 500
            taskParamMap['outDiskCount'] = out_disk_count_default
            taskParamMap['outDiskUnit'] = 'kB'
        # set cpu time unit to use HS06
        if 'cpuTimeUnit' not in taskParamMap:
            taskParamMap['cpuTimeUnit'] = 'HS06sPerEvent'
        # use local IO for ancient releases since inputfilepeeker+xrootd is problematic
        if 'transUses' in taskParamMap and taskParamMap['transUses']:
            try:
                ver = taskParamMap['transUses'].split('-')[1]
                m = re.search('^(\d{2})\.(\d{2})\.', ver)
                if m is not None:
                    major = int(m.group(1))
                    minor = int(m.group(2))
                    if major < 20 or (major == 20 and minor <= 20):
                        taskParamMap['useLocalIO'] = 1
            except Exception:
                pass
        # scout success rate
        if 'scoutSuccessRate' not in taskParamMap:
            taskParamMap['scoutSuccessRate'] = 5
        # directIO
        if 'useLocalIO' not in taskParamMap and 'allowInputLAN' not in taskParamMap:
            taskParamMap['allowInputLAN'] = 'use'
        # current priority
        if 'currentPriority' in taskParamMap and (taskParamMap['currentPriority'] < 900 or taskParamMap['currentPriority'] > 1100):
            taskParamMap['currentPriority'] = 1000
        isSU, isSG = self.taskBufferIF.isSuperUser(taskParamMap['userName'])
        if isSU or (isSG and 'workingGroup' in taskParamMap):
            # super high priority to jump over others
            if 'currentPriority' not in taskParamMap or taskParamMap['currentPriority'] < JobUtils.priorityTasksToJumpOver:
                taskParamMap['currentPriority'] = JobUtils.priorityTasksToJumpOver
        # max attempts
        if 'maxAttempt' not in taskParamMap:
            taskParamMap['maxAttempt'] = 10
        if 'maxFailure' not in taskParamMap:
            taskParamMap['maxFailure'] = 3
        # target walltime
        if 'maxWalltime' not in taskParamMap:
            tgtWalltime = self.taskBufferIF.getConfigValue('taskrefiner', 'USER_JOB_TARGET_WALLTIME', 'jedi', 'atlas')
            if tgtWalltime:
                taskParamMap['maxWalltime'] = tgtWalltime
        # choose N % of tasks to enable input data motion
        fracTaskWithDataMotion = self.taskBufferIF.getConfigValue('taskrefiner', 'USER_TASKS_MOVE_INPUT', 'jedi', 'atlas')
        if fracTaskWithDataMotion is not None and fracTaskWithDataMotion > 0:
            if random.randint(1, 100) <= fracTaskWithDataMotion:
                if 'currentPriority' not in taskParamMap:
                    taskParamMap['currentPriority'] = taskParamMap['taskPriority']
                taskParamMap['taskPriority'] = 1001
        # image name
        if 'container_name' not in taskParamMap:
            try:
                for tmpItem in taskParamMap['jobParameters']:
                    if 'value' in tmpItem:
                        tmpM = re.search('--containerImage\s+([^\s]+)', tmpItem['value'])
                        if tmpM is not None:
                            taskParamMap['container_name'] = tmpM.group(1)
                            break
            except Exception:
                pass
        # update task parameters
        self.updatedTaskParams = taskParamMap
        # call base method
        TaskRefinerBase.extractCommon(self,jediTaskID,taskParamMap,workQueueMapper,splitRule)



    # main
    def doRefine(self,jediTaskID,taskParamMap):
        # make logger
        tmpLog = self.tmpLog
        tmpLog.debug('start taskType={0}'.format(self.taskSpec.taskType))
        try:
            # preprocessing
            tmpStat,taskParamMap = self.doPreProRefine(taskParamMap)
            if tmpStat is True:
                tmpLog.debug('done for preprocessing')
                return self.SC_SUCCEEDED
            if tmpStat is False:
                # failed
                tmpLog.error('doPreProRefine failed')
                return self.SC_FAILED
            # normal refine
            self.doBasicRefine(taskParamMap)
            # set nosplit+repeat for DBR
            for datasetSpec in self.inSecDatasetSpecList:
                # get the latest version of DBR
                if datasetSpec.datasetName == 'DBR_LATEST':
                    tmpLog.debug('resolving real name for {0}'.format(datasetSpec.datasetName))
                    datasetSpec.datasetName = self.ddmIF.getInterface(self.taskSpec.vo).getLatestDBRelease(useResultCache=3600)
                    datasetSpec.containerName = datasetSpec.datasetName
                # set attributes to DBR
                if DataServiceUtils.isDBR(datasetSpec.datasetName):
                    datasetSpec.attributes = 'repeat,nosplit'
            # check invalid characters
            for datasetSpec in self.outDatasetSpecList:
                if not DataServiceUtils.checkInvalidCharacters(datasetSpec.datasetName):
                    errStr = "invalid characters in {0}".format(datasetSpec.datasetName)
                    tmpLog.error(errStr)
                    self.taskSpec.setErrDiag(errStr,None)
                    return self.SC_FATAL
            # destination
            if 'destination' in taskParamMap:
                for datasetSpec in self.outDatasetSpecList:
                    datasetSpec.destination = taskParamMap['destination']
            # use build
            if 'buildSpec' in taskParamMap:
                self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['useBuild'])
            # use template dataset
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['instantiateTmpl'])
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['instantiateTmplSite'])
            for datasetSpec in self.outDatasetSpecList:
                datasetSpec.type = "tmpl_{0}".format(datasetSpec.type)
            # get jobsetID
            tmpStat,tmpJobID = self.taskBufferIF.getUserJobsetID_JEDI(self.taskSpec.userName)
            if not tmpStat:
                tmpLog.error('failed to get jobsetID failed')
                return self.SC_FAILED
            self.taskSpec.reqID = tmpJobID
            # site limitation
            if 'excludedSite' in taskParamMap and 'includedSite' in taskParamMap:
                self.taskSpec.setLimitedSites('incexc')
            elif 'excludedSite' in taskParamMap:
                self.taskSpec.setLimitedSites('exc')
            elif 'includedSite' in taskParamMap:
                self.taskSpec.setLimitedSites('inc')
        except Exception:
            errtype,errvalue = sys.exc_info()[:2]
            errStr = 'doRefine failed with {0}:{1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errStr)
            self.taskSpec.setErrDiag(errStr,None)
            raise errtype(errvalue)
        tmpLog.debug('done')
        return self.SC_SUCCEEDED
