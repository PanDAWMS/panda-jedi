from pandajedi.jedicore import Interaction

# base class for task generator
class TaskGeneratorBase (object):

    def __init__(self,taskBufferIF,ddmIF):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF



Interaction.installSC(TaskGeneratorBase)
