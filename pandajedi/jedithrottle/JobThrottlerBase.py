from pandajedi.jedicore import Interaction

# base class for job throttle
class JobThrottlerBase (object):

    def __init__(self,taskBufferIF):
        self.taskBufferIF = taskBufferIF
        self.siteMapper = taskBufferIF.getSiteMapper()
        # returns
        self.retTmpError    = self.SC_FAILED,True
        self.retThrottled   = self.SC_SUCCEEDED,True
        self.retUnThrottled = self.SC_SUCCEEDED,False


Interaction.installSC(JobThrottlerBase)
