from pandajedi.jedicore import Interaction

# base class for job brokerge
class JobBrokerBase (object):

    def __init__(self,ddmIF,taskBufferIF):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.liveCounter = None
        self.lockID = None
        self.baseLockID = None
        self.useLock = False
        self.testMode = False
        self.refresh()



    def refresh(self):
        self.siteMapper = self.taskBufferIF.getSiteMapper()


    
    def setLiveCounter(self,liveCounter):
        self.liveCounter = liveCounter



    def getLiveCount(self,siteName):
        if self.liveCounter == None:
            return 0
        return self.liveCounter.get(siteName)



    def setLockID(self,pid,tid):
        self.baseLockID = '{0}-jbr'.format(pid)
        self.lockID = '{0}-{1}'.format(self.baseLockID,tid)



    def getBaseLockID(self):
        if self.useLock:
            return self.baseLockID
        return None

    def releaseSiteLock(self, vo, prodSourceLabel, queue_id):
        if self.useLock:
            self.taskBufferIF.unlockProcessWithPID_JEDI(vo, prodSourceLabel, queue_id, self.lockID, False)


    def lockSite(self, vo, prodSourceLabel, siteName, queue_id):
        if not self.useLock:
            self.useLock = True
        self.taskBufferIF.lockProcess_JEDI(vo, prodSourceLabel, siteName, queue_id, self.lockID, True)


    def checkSiteLock(self, vo, prodSourceLabel, siteName, queue_id, resource_name):
        return self.taskBufferIF.checkProcessLock_JEDI(vo, prodSourceLabel, siteName, queue_id, resource_name, self.baseLockID, True)


    def setTestMode(self):
        self.testMode = True



Interaction.installSC(JobBrokerBase)
