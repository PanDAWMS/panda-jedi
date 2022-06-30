from pandajedi.jedicore.MsgWrapper import MsgWrapper
from .JobThrottlerBase import JobThrottlerBase

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])

LEVEL_None = 0 # There is no configuration defined
LEVEL_GS = 1 # There is a configuration defined at global share level
LEVEL_MS = 2 # There is a configuration defined at MCORE/SCORE level
LEVEL_RT = 3 # There is a configuration defined at resource type level

NQUEUELIMIT = 'NQUEUELIMIT'
NRUNNINGCAP = 'NRUNNINGCAP'
NQUEUECAP = 'NQUEUECAP'

# workqueues that do not work at resource type level.
# E.g. event service is a special case, since MCORE tasks generate SCORE jobs. Therefore we can't work at
# resource type level and need to go to the global level, in order to avoid over-generating jobs
non_rt_wqs = ['eventservice']

# class to throttle ATLAS production jobs
class AtlasProdJobThrottler (JobThrottlerBase):

    # constructor
    def __init__(self,taskBufferIF):
        JobThrottlerBase.__init__(self, taskBufferIF)
        self.comp_name = 'prod_job_throttler'
        self.app = 'jedi'

    # check if throttled
    def toBeThrottled(self, vo, prodSourceLabel, cloudName, workQueue, resource_name):
        return self.toBeThrottledBase(vo, prodSourceLabel, cloudName, workQueue, resource_name, logger)
