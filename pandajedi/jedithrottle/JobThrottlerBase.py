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
        # limit
        self.maxNumJobs  = None
        self.minPriority = None

        
    # set maximum number of jobs to be submitted    
    def setMaxNumJobs(self,maxNumJobs):
        self.maxNumJobs = maxNumJobs


    # set min priority of jobs to be submitted
    def setMinPriority(self,minPriority):
        self.minPriority = minPriority
        


Interaction.installSC(JobThrottlerBase)
