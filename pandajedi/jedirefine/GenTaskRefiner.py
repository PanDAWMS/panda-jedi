from .TaskRefinerBase import TaskRefinerBase

# refiner for general purpose
class GenTaskRefiner (TaskRefinerBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        TaskRefinerBase.__init__(self,taskBufferIF,ddmIF)

    # extract common parameters
    def extractCommon(self, jediTaskID, taskParamMap, workQueueMapper, splitRule):
        if 'cloud' not in taskParamMap and 'workingGroup' in taskParamMap:
            taskParamMap['cloud'] = taskParamMap['workingGroup']
        if 'transPath' not in taskParamMap:
            taskParamMap['transPath'] = 'https://atlpan.web.cern.ch/atlpan/runGen-00-00-02'
        # update task parameters
        self.updatedTaskParams = taskParamMap
        # call base method
        TaskRefinerBase.extractCommon(self, jediTaskID, taskParamMap, workQueueMapper, splitRule)

    # main
    def doRefine(self,jediTaskID,taskParamMap):
        # normal refine
        self.doBasicRefine(taskParamMap)
        return self.SC_SUCCEEDED
