import re
import sys

from pandajedi.jedicore import Interaction
from TaskRefinerBase import TaskRefinerBase
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec


# brokerage for ATLAS analysis
class AtlasAnalTaskRefiner (TaskRefinerBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        TaskRefinerBase.__init__(self,taskBufferIF,ddmIF)


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
                if datasetSpec.datasetName.startswith('ddo.'):
                    datasetSpec.attributes = 'repeat,nosplit'
            # use build
            if taskParamMap.has_key('buildSpec'):
                self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['useBuild'])
            # use template dataset
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['instantiateTmpl'])
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['instantiateTmplSite'])
            for datasetSpec in self.outDatasetSpecList:
                datasetSpec.type = "tmpl_{0}".format(datasetSpec.type) 
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doRefine failed with {0}:{1}'.format(errtype.__name__,errvalue))
            return self.SC_FAILED
        tmpLog.debug('done')
        return self.SC_SUCCEEDED
            
    
