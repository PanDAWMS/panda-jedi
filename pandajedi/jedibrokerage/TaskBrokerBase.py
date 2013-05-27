from pandajedi.jedicore import Interaction

# base class for task brokerge
class TaskBrokerBase (object):

    def __init__(self,taskBufferIF,ddmIF):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.siteMapper = taskBufferIF.getSiteMapper()



Interaction.installSC(TaskBrokerBase)                        
