import re
import sys

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from .WatchDogBase import WatchDogBase

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])



# watchdog for general purpose
class GenWatchDog (WatchDogBase):

    # constructor
    def __init__(self,ddmIF,taskBufferIF):
        WatchDogBase.__init__(self,ddmIF,taskBufferIF)



    # main
    def doAction(self):
        # get logger
        tmpLog = MsgWrapper(logger)
        tmpLog.debug('start')
        # return
        tmpLog.debug('done')
        return self.SC_SUCCEEDED
