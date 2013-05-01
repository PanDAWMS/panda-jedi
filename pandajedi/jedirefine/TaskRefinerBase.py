import re

import RefinerUtils
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandajedi.jedicore.JediDatasetSpec import JediDatasetSpec



# base class for task refine
class TaskRefinerBase (object):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.siteMapper = taskBufferIF.getSiteMapper()
        self.initializeRefiner(None)



    # initialize
    def initializeRefiner(self,tmpLog):
        self.taskSpec = None
        self.inMasterDatasetSpec = None
        self.inSecDatasetSpecList = []        
        self.outDatasetSpecList = []
        self.outputTemplateMap = {}
        self.jobParamsTemplate = None
        self.cloud = None
        self.tmpLog = tmpLog



    # set jobParamsTemplate
    def setJobParamsTemplate(self,jobParamsTemplate):
        self.jobParamsTemplate = jobParamsTemplate

        
        
    # extract common parameters
    def extractCommon(self,taskID,taskParamMap,workQueueMapper):
        # make task spec
        taskSpec = JediTaskSpec()
        taskSpec.taskID = taskID
        taskSpec.taskName = taskParamMap['taskName']
        taskSpec.userName = taskParamMap['userName']
        taskSpec.vo = taskParamMap['vo']     
        taskSpec.prodSourceLabel = taskParamMap['prodSourceLabel']
        taskSpec.taskPriority = taskParamMap['taskPriority']
        taskSpec.currentPriority = taskSpec.taskPriority
        taskSpec.architecture = taskParamMap['architecture']
        taskSpec.transUses = taskParamMap['transUses']
        taskSpec.transHome = taskParamMap['transHome']
        taskSpec.transPath = taskParamMap['transPath']
        taskSpec.processingType = taskParamMap['processingType']
        taskSpec.taskType = taskParamMap['taskType']
        taskSpec.workingGroup = taskParamMap['workingGroup']
        if taskParamMap.has_key('coreCount'):
            taskSpec.coreCount = taskParamMap['coreCount']
        else:
            taskSpec.coreCount = 1
        if taskParamMap.has_key('walltime'):
            taskSpec.walltime = taskParamMap['walltime']
        else:
            taskSpec.walltime = 0
        if taskParamMap.has_key('outDiskCount'):
            taskSpec.outDiskCount = taskParamMap['outDiskCount']
        else:
            taskSpec.outDiskCount = 0
        if taskParamMap.has_key('workDiskCount'):
            taskSpec.outDiskCount = taskParamMap['workDiskCount']
        else:
            taskSpec.outDiskCount = 0
        if taskParamMap.has_key('ramCount'):
            taskSpec.ramCount = taskParamMap['ramCount']
        else:
            taskSpec.ramCount = 0
        workQueue,tmpStr = workQueueMapper.getQueueWithSelParams(taskSpec.vo,
                                                                 taskSpec.prodSourceLabel,
                                                                 processingType=taskSpec.processingType,
                                                                 workingGroup=taskSpec.workingGroup,
                                                                 coreCount=taskSpec.coreCount)
        if workQueue == None:
            raise RuntimeError,'workqueue is undefined for vo={0} labal={1} param={2}'.format(taskSpec.vo,
                                                                                              taskSpec.prodSourceLabel,
                                                                                              str(workQueueParam))
        taskSpec.workQueue_ID = workQueue.queue_id
        self.taskSpec = taskSpec
        # cloud
        if taskParamMap.has_key('cloud'):
            self.cloudName = taskParamMap['cloud']
        # return
        return
    
        
            
    # basic refinement procedure
    def doBasicRefine(self,taskParamMap):
        # get input/output/log dataset specs
        nIn  = 0
        nOut = 0
        for tmpItem in taskParamMap['jobParameters'] + [taskParamMap['log']]:
            # look for datasets
            if tmpItem.has_key('dataset'):
                datasetSpec = JediDatasetSpec()
                datasetSpec.datasetName = tmpItem['dataset']
                datasetSpec.taskID = self.taskSpec.taskID
                datasetSpec.type = tmpItem['param_type']
                if tmpItem.has_key('token'):
                    datasetSpec.storageToken = tmpItem['token']
                if tmpItem.has_key('destination'):
                    datasetSpec.destination = tmpItem['destination']
                datasetSpec.vo = self.taskSpec.vo
                datasetSpec.nFiles = 0
                datasetSpec.nFilesUsed = 0
                datasetSpec.nFilesFinished = 0
                datasetSpec.nFilesFailed = 0
                datasetSpec.status = 'defined'
                if self.cloudName != None:
                    datasetSpec.cloud = self.cloudName
                if datasetSpec.type == 'input':
                    datasetSpec.streamName = RefinerUtils.extractStreamName(tmpItem['value'])
                    if nIn == 0:
                        # master
                        self.inMasterDatasetSpec = datasetSpec
                    else:
                        # secondary
                        self.inSecDatasetSpecList.append(datasetSpec)
                    nIn += 1    
                    continue
                if datasetSpec.type in ['output','log']:
                    # make stream name
                    datasetSpec.streamName = "{0}{1}".format(datasetSpec.type.upper(),nOut)
                    nOut += 1
                    # extract output filename template and change the value field
                    outFileTemplate,tmpItem['value'] = RefinerUtils.extractReplaceOutFileTemplate(tmpItem['value'],
                                                                                                  datasetSpec.streamName)
                    # make output template
                    if outFileTemplate != None:
                        outTemplateMap = {'taskID' : self.taskSpec.taskID,
                                          'serialNr' : 1,
                                          'streamName' : datasetSpec.streamName,
                                          'filenameTemplate' : outFileTemplate,
                                          'outtype' : datasetSpec.type,
                                          }
                        if self.outputTemplateMap.has_key(datasetSpec.datasetName):
                            # multiple files are associated to the same output datasets
                            self.outputTemplateMap[datasetSpec.datasetName].append(outTemplateMap)
                            # don't insert the same output dataset
                            continue
                        self.outputTemplateMap[datasetSpec.datasetName] = [outTemplateMap]
                    # append
                    self.outDatasetSpecList.append(datasetSpec)
        # make job parameters
        jobParameters = ''
        for tmpItem in taskParamMap['jobParameters']:
            if tmpItem.has_key('value'):
                jobParameters += '{0} '.format(tmpItem['value'])
        jobParameters = jobParameters[:-1]
        self.setJobParamsTemplate(jobParameters)
        # return
        return


    
Interaction.installSC(TaskRefinerBase)
