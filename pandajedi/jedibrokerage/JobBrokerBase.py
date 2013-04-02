from pandajedi.jedicore import Interaction

# base class for job brokerge
class JobBrokerBase (object):

    def __init__(self,ddmIF,taskBufferIF,siteMapper):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.siteMapper = siteMapper



Interaction.installSC(JobBrokerBase)                        
