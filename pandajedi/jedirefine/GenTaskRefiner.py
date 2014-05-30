import re
import sys

from pandajedi.jedicore import Interaction
from TaskRefinerBase import TaskRefinerBase
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandaserver.config import panda_config


# refiner for general purpose
class GenTaskRefiner (TaskRefinerBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        TaskRefinerBase.__init__(self,taskBufferIF,ddmIF)


    # main
    def doRefine(self,jediTaskID,taskParamMap):
        # normal refine
        self.doBasicRefine(taskParamMap)
        return self.SC_SUCCEEDED
            
    
