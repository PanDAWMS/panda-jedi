from .JobThrottlerBase import JobThrottlerBase

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger

logger = PandaLogger().getLogger(__name__.split('.')[-1])


# class to throttle ATLAS analysis jobs
class AtlasAnalJobThrottler(JobThrottlerBase):

    # constructor
    def __init__(self, taskBufferIF):
        JobThrottlerBase.__init__(self, taskBufferIF)
        self.logger_name = __name__.split('.')[-1]
        self.comp_name = 'anal_job_throttler'
        self.app = 'jedi'

    # check if throttled
    def toBeThrottled(self, vo, prodSourceLabel, cloudName, workQueue, resource_name):
        return self.toBeThrottledBase(vo, prodSourceLabel, cloudName, workQueue, resource_name, logger)

    # check if throttled
    def toBeThrottled_old(self, vo, prodSourceLabel, cloudName, workQueue, resource_name):
        # make logger
        tmpLog = MsgWrapper(logger)
        tmp_log.debug('start vo={0} label={1} cloud={2} workQueue={3}'.format(vo, prodSourceLabel, cloudName,
                                                                             workQueue.queue_name))

        # check if unthrottled
        if not workQueue.throttled:
            tmpLog.debug("  done : unthrottled since throttled is False")
            return self.retUnThrottled

        tmpLog.debug("  done : SKIP")
        return self.retThrottled
