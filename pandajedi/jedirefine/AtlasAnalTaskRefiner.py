import re
import sys

from pandajedi.jedicore import Interaction
from TaskRefinerBase import TaskRefinerBase
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandaserver.config import panda_config

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
        if not 'ddmBackEnd' in taskParamMap:
            taskParamMap['ddmBackEnd'] = 'rucio'
        # set sourceURL
        try:
            if taskParamMap.has_key('sourceURL'):
                for tmpItem in taskParamMap['jobParameters']:
                    if tmpItem.has_key('value'):
                        tmpItem['value'] = re.sub('\$\{SURL\}',taskParamMap['sourceURL'],tmpItem['value'])
        except:
            pass
        # set transPath
        if not taskParamMap.has_key('transPath'):
            if 'athena' in processingTypes:
                # athena
                taskParamMap['transPath'] = 'http://{0}:{1}/trf/user/runAthena-00-00-12'.format(panda_config.pserveralias,
                                                                                                panda_config.pserverportcache)
            else:
                # general executable
                taskParamMap['transPath'] = 'http://{0}:{1}/trf/user/runGen-00-00-02'.format(panda_config.pserveralias,
                                                                                             panda_config.pserverportcache)
        # set transPath for build
        if taskParamMap.has_key('buildSpec') and not taskParamMap['buildSpec'].has_key('transPath'):
            if 'athena' in processingTypes:
                # athena
                taskParamMap['buildSpec']['transPath'] = 'http://{0}:{1}/trf/user/buildJob-00-00-03'.format(panda_config.pserveralias,
                                                                                                            panda_config.pserverportcache)
            else:
                # general executable
                taskParamMap['buildSpec']['transPath'] = 'http://{0}:{1}/trf/user/buildGen-00-00-01'.format(panda_config.pserveralias,
                                                                                                            panda_config.pserverportcache)
        # set transPath for preprocessing
        if taskParamMap.has_key('preproSpec') and not taskParamMap['preproSpec'].has_key('transPath'):
            if 'evp' in processingTypes:
                # event picking
                taskParamMap['preproSpec']['transPath'] = 'http://{0}:{1}/trf/user/preEvtPick-00-00-01'.format(panda_config.pserveralias,
                                                                                                               panda_config.pserverportcache)
            elif 'grl' in processingTypes:
                # good run list
                taskParamMap['preproSpec']['transPath'] = 'http://{0}:{1}/trf/user/preGoodRunList-00-00-01'.format(panda_config.pserveralias,
                                                                                                                   panda_config.pserverportcache)
        # set transPath for merge
        if taskParamMap.has_key('mergeSpec') and not taskParamMap['mergeSpec'].has_key('transPath'):
            taskParamMap['mergeSpec']['transPath'] = 'http://{0}:{1}/trf/user/runMerge-00-00-02'.format(panda_config.pserveralias,
                                                                                                        panda_config.pserverportcache)
        # min ram count
        if not 'ramCount' in taskParamMap:
            taskParamMap['ramCount'] = 2000
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
            if tmpStat == True:
                tmpLog.debug('done for preprocessing')
                return self.SC_SUCCEEDED
            if tmpStat == False:
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
            if taskParamMap.has_key('destination'):
                for datasetSpec in self.outDatasetSpecList:
                    datasetSpec.destination = taskParamMap['destination']
            # use build
            if taskParamMap.has_key('buildSpec'):
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
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errStr = 'doRefine failed with {0}:{1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errStr)
            self.taskSpec.setErrDiag(errStr,None)
            raise errtype,errvalue
        tmpLog.debug('done')
        return self.SC_SUCCEEDED
            
    
