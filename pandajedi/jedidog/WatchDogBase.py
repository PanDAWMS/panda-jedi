from pandajedi.jedicore import Interaction

# base class for watchdog
class WatchDogBase (object):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.siteMapper = taskBufferIF.getSiteMapper()


    
Interaction.installSC(WatchDogBase)
