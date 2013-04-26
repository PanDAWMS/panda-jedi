from pandajedi.jedicore.FactoryBase import FactoryBase
from pandajedi.jediconfig import jedi_config

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# factory class for job brokerage
class JobBroker (FactoryBase):

    # constructor
    def __init__(self,vo,sourceLabel):
        FactoryBase.__init__(self,vo,sourceLabel,logger,
                             jedi_config.jobbroker.modConfig)


    # main
    def doBrokerage(self,taskSpec,cloudName,inputChunk):
        return self.impl.doBrokerage(taskSpec,cloudName,inputChunk)
