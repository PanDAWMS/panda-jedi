import re
import sys

from pandajedi.jedicore import Interaction
from TaskRefinerBase import TaskRefinerBase



# brokerage for ATLAS production
class AtlasProdTaskRefiner (TaskRefinerBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        TaskRefinerBase.__init__(self,taskBufferIF,ddmIF)


    # main
    def doRefine(self,jediTaskID,taskType,taskParamMap):
        # make logger
        tmpLog = self.tmpLog
        tmpLog.debug('start taskType={0}'.format(taskType))
        try:
            self.doBasicRefine(taskParamMap)
            # set nosplit+repeat for DBR
            for datasetSpec in self.inSecDatasetSpecList:
                if datasetSpec.datasetName.startswith('ddo.'):
                    datasetSpec.attributes = 'repeat,nosplit'
            # set default walltime
            if self.taskSpec.walltime == 0:
                if self.taskSpec.processingType == 'evgen':
                    self.taskSpec.walltime = 30000
                else:
                    self.taskSpec.walltime = 100000
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doBasicRefine failed with {0}:{1}'.format(errtype.__name__,errvalue))
            return self.SC_FAILED
        tmpLog.debug('done')
        return self.SC_SUCCEEDED
            
    
