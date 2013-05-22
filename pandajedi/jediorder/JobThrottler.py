from pandajedi.jedicore.FactoryBase import FactoryBase
from pandajedi.jediconfig import jedi_config

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# factory class for throttling
class JobThrottler (FactoryBase):

    # constructor
    def __init__(self,vo,sourceLabel):
        FactoryBase.__init__(self,vo,sourceLabel,logger,
                             jedi_config.jobthrottle.modConfig)


    # main
    def toBeThrottled(self,vo,sourceLabel,cloudName,workQueue,jobStat):
        return self.getImpl(vo,sourceLabel).toBeThrottled(vo,cloudName,workQueue,jobStat)
                                                            
            
