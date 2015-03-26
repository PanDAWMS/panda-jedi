from pandajedi.jedicore import Interaction

# base class for job brokerge
class JobBrokerBase (object):

    def __init__(self,ddmIF,taskBufferIF):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.liveCounter = None
        self.refresh()



    def refresh(self):
        self.siteMapper = self.taskBufferIF.getSiteMapper()


    
    def setLiveCounter(self,liveCounter):
        self.liveCounter = liveCounter



    def getLiveCount(self,siteName):
        if self.liveCounter == None:
            return 0
        return self.liveCounter.get(siteName)



Interaction.installSC(JobBrokerBase)                        
