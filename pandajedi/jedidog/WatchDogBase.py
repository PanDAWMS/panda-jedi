from pandajedi.jedicore import Interaction


# base class for watchdog
class WatchDogBase(object):

    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        self.taskBufferIF = taskBufferIF
        self.ddmIF = ddmIF
        self.refresh()

    # refresh
    def refresh(self):
        self.siteMapper = self.taskBufferIF.getSiteMapper()

    # pre-action
    def pre_action(self, tmpLog, vo, prodSourceLabel, pid, *args, **kwargs):
        pass


Interaction.installSC(WatchDogBase)
