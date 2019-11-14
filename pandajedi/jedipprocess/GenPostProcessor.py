import re
import sys

from pandajedi.jedicore import Interaction
from .PostProcessorBase import PostProcessorBase



# post processor for general purpose
class GenPostProcessor (PostProcessorBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        PostProcessorBase.__init__(self,taskBufferIF,ddmIF)


    # main
    def doPostProcess(self,taskSpec,tmpLog):
        try:
            self.doBasicPostProcess(taskSpec,tmpLog)
        except Exception:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doBasicPostProcess failed with {0}:{1}'.format(errtype.__name__,errvalue))
            return self.SC_FATAL
        return self.SC_SUCCEEDED
            
    
