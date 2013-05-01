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
    def doRefine(self,taskID,taskType,taskParamMap):
        # make logger
        tmpLog = self.tmpLog
        tmpLog.debug('start taskType={0}'.format(taskType))
        try:
            self.doBasicRefine(taskParamMap)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doBasicRefine failed with {0}:{1}'.format(errtype.__name__,errvalue))
            return self.SC_FAILED
        tmpLog.debug('done')
        return self.SC_SUCCEEDED
            
    
