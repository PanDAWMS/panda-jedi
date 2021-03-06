from pandajedi.jedicore.MsgWrapper import MsgWrapper

from .DDMClientBase import DDMClientBase

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])

# class to access to general DDM
class GenDDMClient(DDMClientBase):

    # constructor
    def __init__(self,con):
        # initialize base class
        DDMClientBase.__init__(self,con)
