from pandajedi.jedicore import Interaction

# base class for task setup
class TaskSetupperBase (object):

    def __init__(self,taskBufferIF,ddmIF):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF



Interaction.installSC(TaskSetupperBase)
