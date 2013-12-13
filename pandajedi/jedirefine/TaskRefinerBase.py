import re
import sys
import uuid
import copy
import types

import RefinerUtils
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandajedi.jedicore.JediDatasetSpec import JediDatasetSpec
from pandajedi.jedicore.JediFileSpec import JediFileSpec



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
        self.inMasterDatasetSpec = []
        self.inSecDatasetSpecList = []        
        self.outDatasetSpecList = []
        self.outputTemplateMap = {}
        self.jobParamsTemplate = None
        self.cloudName = None
        self.siteName = None
        self.tmpLog = tmpLog
        self.updatedTaskParams = None
        self.unmergeMasterDatasetSpec = {}
        self.unmergeDatasetSpecMap = {}



    # set jobParamsTemplate
    def setJobParamsTemplate(self,jobParamsTemplate):
        self.jobParamsTemplate = jobParamsTemplate


    
    # extract common parameters
    def extractCommon(self,jediTaskID,taskParamMap,workQueueMapper,splitRule):
        # make task spec
        taskSpec = JediTaskSpec()
        taskSpec.jediTaskID = jediTaskID
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
        taskSpec.splitRule = splitRule
        if taskParamMap.has_key('workingGroup'):
            taskSpec.workingGroup = taskParamMap['workingGroup']
        if taskParamMap.has_key('reqID'):
            taskSpec.reqID = taskParamMap['reqID']
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
        # cloud
        if taskParamMap.has_key('cloud'):
            self.cloudName = taskParamMap['cloud']
            taskSpec.cloud = self.cloudName
        # site
        if taskParamMap.has_key('site'):
            self.siteName = taskParamMap['site']
            taskSpec.site = self.siteName
        workQueue,tmpStr = workQueueMapper.getQueueWithSelParams(taskSpec.vo,
                                                                 taskSpec.prodSourceLabel,
                                                                 processingType=taskSpec.processingType,
                                                                 workingGroup=taskSpec.workingGroup,
                                                                 coreCount=taskSpec.coreCount)
        if workQueue == None:
            errStr  = 'workqueue is undefined for vo={0} labal={1} '.format(taskSpec.vo,taskSpec.prodSourceLabel)
            errStr += 'processingType={0} workingGroup={1} coreCount={2} '.format(taskSpec.processingType,
                                                                                  taskSpec.workingGroup,
                                                                                  taskSpec.coreCount)
            raise RuntimeError,errStr
        taskSpec.workQueue_ID = workQueue.queue_id
        self.taskSpec = taskSpec
        # set split rule    
        self.setSplitRule(taskParamMap,'nFilesPerJob',   JediTaskSpec.splitRuleToken['nFilesPerJob'])
        self.setSplitRule(taskParamMap,'nEventsPerJob',  JediTaskSpec.splitRuleToken['nEventsPerJob'])
        self.setSplitRule(taskParamMap,'nGBPerJob',      JediTaskSpec.splitRuleToken['nGBPerJob'])
        self.setSplitRule(taskParamMap,'nMaxFilesPerJob',JediTaskSpec.splitRuleToken['nMaxFilesPerJob'])
        self.setSplitRule(taskParamMap,'useLocalIO',     JediTaskSpec.splitRuleToken['useLocalIO'])
        # return
        return
    


    # basic refinement procedure
    def doBasicRefine(self,taskParamMap):
        # get input/output/log dataset specs
        nIn  = 0
        nOutMap = {}
        if isinstance(taskParamMap['log'],dict):
            itemList = taskParamMap['jobParameters'] + [taskParamMap['log']]
        else:
            itemList = taskParamMap['jobParameters'] + taskParamMap['log']
        # pseudo input
        if taskParamMap.has_key('noInput') and taskParamMap['noInput'] == True:
            tmpItem = {}
            tmpItem['type']       = 'template'
            tmpItem['value']      = ''
            tmpItem['dataset']    = 'pseudo_dataset'
            tmpItem['param_type'] = 'pseudo_input'
            itemList = [tmpItem] + itemList
        # random seed
        if RefinerUtils.useRandomSeed(taskParamMap):
            tmpItem = {}
            tmpItem['type']       = 'template'
            tmpItem['value']      = ''
            tmpItem['dataset']    = 'RNDMSEED'
            tmpItem['param_type'] = 'random_seed'
            itemList.append(tmpItem)
        # loop over all items
        allDsList = []   
        for tmpItem in itemList:
            # look for datasets
            if tmpItem['type'] == 'template' and tmpItem.has_key('dataset'):
                # avoid duplication
                if not tmpItem['dataset'] in allDsList:
                    allDsList.append(tmpItem['dataset'])
                else:
                    continue
                datasetSpec = JediDatasetSpec()
                datasetSpec.datasetName = tmpItem['dataset']
                datasetSpec.jediTaskID = self.taskSpec.jediTaskID
                datasetSpec.type = tmpItem['param_type']
                if tmpItem.has_key('container'):
                    datasetSpec.containerName = tmpItem['container']
                if tmpItem.has_key('token'):
                    datasetSpec.storageToken = tmpItem['token']
                if tmpItem.has_key('destination'):
                    datasetSpec.destination = tmpItem['destination']
                if tmpItem.has_key('attributes'):
                    datasetSpec.setDatasetAttribute(tmpItem['attributes'])
                if tmpItem.has_key('ratio'):
                    datasetSpec.setDatasetAttribute('ratio={0}'.format(tmpItem['ratio']))
                if tmpItem.has_key('offset'):
                    datasetSpec.setOffset(tmpItem['offset'])
                datasetSpec.vo = self.taskSpec.vo
                datasetSpec.nFiles = 0
                datasetSpec.nFilesUsed = 0
                datasetSpec.nFilesFinished = 0
                datasetSpec.nFilesFailed = 0
                datasetSpec.nFilesOnHold = 0
                datasetSpec.status = 'defined'
                if datasetSpec.type in JediDatasetSpec.getInputTypes() + ['random_seed']:
                    datasetSpec.streamName = RefinerUtils.extractStreamName(tmpItem['value'])
                    inDatasetSpecList = []
                    # exapand dataset container
                    if tmpItem.has_key('expand') and tmpItem['expand'] == True:
                        tmpDatasetNameList = self.ddmIF.getInterface(self.taskSpec.vo).expandContainer(datasetSpec.datasetName)
                        for datasetName in tmpDatasetNameList:
                            inDatasetSpec = copy.copy(datasetSpec)
                            inDatasetSpec.datasetName = datasetName
                            if datasetName != datasetSpec.datasetName:
                                inDatasetSpec.containerName = datasetSpec.datasetName
                            inDatasetSpecList.append(inDatasetSpec)
                    elif ',' in datasetSpec.datasetName:
                        # comma-separated dataset names
                        for datasetName in datasetSpec.datasetName.split(','):
                            inDatasetSpec = copy.copy(datasetSpec)
                            inDatasetSpec.datasetName = datasetName
                            inDatasetSpecList.append(inDatasetSpec)
                    else:
                        # normal dataset name
                        inDatasetSpecList = [datasetSpec]
                    for inDatasetSpec in inDatasetSpecList:    
                        if nIn == 0:
                            # master
                            self.inMasterDatasetSpec.append(inDatasetSpec)
                        else:
                            # secondary
                            self.inSecDatasetSpecList.append(inDatasetSpec)
                    nIn += 1    
                    continue
                if datasetSpec.type in ['output','log']:
                    if not nOutMap.has_key(datasetSpec.type):
                        nOutMap[datasetSpec.type] = 0
                    # make stream name
                    datasetSpec.streamName = "{0}{1}".format(datasetSpec.type.upper(),nOutMap[datasetSpec.type])
                    nOutMap[datasetSpec.type] += 1
                    # extract output filename template and change the value field
                    outFileTemplate,tmpItem['value'] = RefinerUtils.extractReplaceOutFileTemplate(tmpItem['value'],
                                                                                                  datasetSpec.streamName)
                    # make output template
                    if outFileTemplate != None:
                        if tmpItem.has_key('offset'):
                            offsetVal = 1 + tmpItem['offset']
                        else:
                            offsetVal = 1
                        outTemplateMap = {'jediTaskID' : self.taskSpec.jediTaskID,
                                          'serialNr' : offsetVal,
                                          'streamName' : datasetSpec.streamName,
                                          'filenameTemplate' : outFileTemplate,
                                          'outtype' : datasetSpec.type,
                                          }
                        if self.outputTemplateMap.has_key(datasetSpec.outputMapKey()):
                            # multiple files are associated to the same output datasets
                            self.outputTemplateMap[datasetSpec.outputMapKey()].append(outTemplateMap)
                            # don't insert the same output dataset
                            continue
                        self.outputTemplateMap[datasetSpec.outputMapKey()] = [outTemplateMap]
                    # append
                    self.outDatasetSpecList.append(datasetSpec)
                    # make unmerged dataset
                    if taskParamMap.has_key('mergeOutput') and taskParamMap['mergeOutput'] == True:
                        umDatasetSpec = JediDatasetSpec()
                        umDatasetSpec.datasetName = 'panda.um.' + datasetSpec.datasetName
                        umDatasetSpec.jediTaskID = self.taskSpec.jediTaskID
                        umDatasetSpec.storageToken = 'TOMERGE'
                        umDatasetSpec.vo = datasetSpec.vo
                        umDatasetSpec.type = "tmpl_trn_" + datasetSpec.type
                        umDatasetSpec.nFiles = 0
                        umDatasetSpec.nFilesUsed = 0
                        umDatasetSpec.nFilesToBeUsed = 0
                        umDatasetSpec.nFilesFinished = 0
                        umDatasetSpec.nFilesFailed = 0
                        umDatasetSpec.nFilesOnHold = 0
                        umDatasetSpec.status = 'defined'
                        umDatasetSpec.streamName = datasetSpec.streamName
                        # make unmerged output template 
                        if outFileTemplate != None:
                            umOutTemplateMap = {'jediTaskID' : self.taskSpec.jediTaskID,
                                                'serialNr' : 1,
                                                'streamName' : umDatasetSpec.streamName,
                                                'filenameTemplate' : 'panda.um.' + outFileTemplate,
                                                'outtype' : datasetSpec.type,
                                                }
                            if self.outputTemplateMap.has_key(umDatasetSpec.outputMapKey()):
                                # multiple files are associated to the same output datasets
                                self.outputTemplateMap[umDatasetSpec.outputMapKey()].append(umOutTemplateMap)
                                # don't insert the same output dataset
                                continue
                            self.outputTemplateMap[umDatasetSpec.outputMapKey()] = [umOutTemplateMap]
                        # use log as master for merging
                        if datasetSpec.type == 'log':
                            self.unmergeMasterDatasetSpec[datasetSpec.outputMapKey()] = umDatasetSpec
                        else:
                            # append
                            self.unmergeDatasetSpecMap[datasetSpec.outputMapKey()] = umDatasetSpec
        # set attributes for merging
        if taskParamMap.has_key('mergeOutput') and taskParamMap['mergeOutput'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['mergeOutput'])
        # make job parameters
        rndmSeedOffset = None
        firstEventOffset = None
        jobParameters = ''
        for tmpItem in taskParamMap['jobParameters']:
            if tmpItem.has_key('value'):
                # hidden parameter
                if tmpItem.has_key('hidden') and tmpItem['hidden'] == True:
                    continue
                jobParameters += '{0}'.format(tmpItem['value'])
                # padding
                if tmpItem.has_key('padding') and tmpItem['padding'] == False:
                    pass
                else:
                    jobParameters += ' '
                # get offset for random seed and first event
                if tmpItem['type'] == 'template' and tmpItem['param_type'] == 'number':
                    if '${RNDMSEED}' in tmpItem['value']:
                        if tmpItem.has_key('offset'):
                            rndmSeedOffset = tmpItem['offset']
                        else:
                            rndmSeedOffset = 0
                    elif '${FIRSTEVENT}' in tmpItem['value']:
                        if tmpItem.has_key('offset'):
                            firstEventOffset = tmpItem['offset']    
        jobParameters = jobParameters[:-1]
        self.setJobParamsTemplate(jobParameters)
        # set random seed offset
        if rndmSeedOffset != None:
            self.setSplitRule(None,rndmSeedOffset,JediTaskSpec.splitRuleToken['randomSeed'])
        if firstEventOffset != None:
            self.setSplitRule(None,firstEventOffset,JediTaskSpec.splitRuleToken['firstEvent'])
        # return
        return


    
    # replace placeholder with dict provided by prepro job
    def replacePlaceHolders(self,paramItem,placeHolderName,newValue):
        if isinstance(paramItem,types.DictType):
            # loop over all dict params
            for tmpParName,tmpParVal in paramItem.iteritems():
                if tmpParVal == placeHolderName:
                    # replace placeholder
                    paramItem[tmpParName] = newValue
                elif isinstance(tmpParVal,types.DictType) or \
                        isinstance(tmpParVal,types.ListType):
                    # recursive execution
                    self.replacePlaceHolders(tmpParVal,placeHolderName,newValue)
        elif isinstance(paramItem,types.ListType):
            # loop over all list items
            for tmpItem in paramItem:
                self.replacePlaceHolders(tmpItem,placeHolderName,newValue)

    

    # refinement procedure for preprocessing
    def doPreProRefine(self,taskParamMap):
        # no preprocessing
        if not taskParamMap.has_key('preproSpec'):
            return None,taskParamMap
        # already preprocessed
        if self.taskSpec.checkPreProcessed():
            # get replaced task params
            tmpStat,tmpJsonStr = self.taskBufferIF.getPreprocessMetadata_JEDI(self.taskSpec.jediTaskID)
            try:
                # replace placeholders 
                replaceParams = RefinerUtils.decodeJSON(tmpJsonStr)
                for tmpKey,tmpVal in replaceParams.iteritems():
                    self.replacePlaceHolders(taskParamMap,tmpKey,tmpVal)
            except:
                errtype,errvalue = sys.exc_info()[:2]
                self.tmpLog.error('{0} failed to get additional task params with {1}:{2}'.format(self.__class__.__name__,
                                                                                                 errtype.__name__,errvalue))
                return False,taskParamMap
            # succeeded
            self.updatedTaskParams = taskParamMap
            return None,taskParamMap
        # make dummy dataset to keep track of preprocessing
        datasetSpec = JediDatasetSpec()
        datasetSpec.datasetName = 'panda.pp.in.{0}.{1}'.format(uuid.uuid4(),self.taskSpec.jediTaskID)
        datasetSpec.jediTaskID = self.taskSpec.jediTaskID
        datasetSpec.type = 'pp_input'
        datasetSpec.vo = self.taskSpec.vo
        datasetSpec.nFiles = 1
        datasetSpec.nFilesUsed = 0
        datasetSpec.nFilesToBeUsed = 1
        datasetSpec.nFilesFinished = 0
        datasetSpec.nFilesFailed = 0
        datasetSpec.nFilesOnHold = 0
        datasetSpec.status = 'ready'
        self.inMasterDatasetSpec.append(datasetSpec)
        # make file 
        fileSpec = JediFileSpec()
        fileSpec.jediTaskID   = datasetSpec.jediTaskID
        fileSpec.type         = datasetSpec.type
        fileSpec.status       = 'ready'            
        fileSpec.lfn          = 'pseudo_lfn'
        fileSpec.attemptNr    = 0
        fileSpec.maxAttempt   = 3
        fileSpec.keepTrack    = 1
        datasetSpec.addFile(fileSpec)
        # make log dataset
        logDatasetSpec = JediDatasetSpec()
        logDatasetSpec.datasetName = 'panda.pp.log.{0}.{1}'.format(uuid.uuid4(),self.taskSpec.jediTaskID)
        logDatasetSpec.jediTaskID = self.taskSpec.jediTaskID
        logDatasetSpec.type = 'tmpl_pp_log'
        logDatasetSpec.streamName = 'PP_LOG'
        logDatasetSpec.vo = self.taskSpec.vo
        logDatasetSpec.nFiles = 0
        logDatasetSpec.nFilesUsed = 0
        logDatasetSpec.nFilesToBeUsed = 0
        logDatasetSpec.nFilesFinished = 0
        logDatasetSpec.nFilesFailed = 0
        logDatasetSpec.nFilesOnHold = 0
        logDatasetSpec.status = 'defined'
        self.outDatasetSpecList.append(logDatasetSpec)
        # make output template for log
        outTemplateMap = {'jediTaskID' : self.taskSpec.jediTaskID,
                          'serialNr' : 1,
                          'streamName' : logDatasetSpec.streamName,
                          'filenameTemplate' : "{0}._${{SN}}.log.tgz".format(logDatasetSpec.datasetName),
                          'outtype' : re.sub('^tmpl_','',logDatasetSpec.type),
                          }
        self.outputTemplateMap[logDatasetSpec.outputMapKey()] = [outTemplateMap]
        # set split rule to use preprocessing
        self.taskSpec.setPrePro()
        # set task status
        self.taskSpec.status = 'topreprocess'
        # return
        return True,taskParamMap



    # set split rule
    def setSplitRule(self,taskParamMap,keyName,valName):
        if taskParamMap != None:
            if not taskParamMap.has_key(keyName):
                return
            tmpStr = '{0}={1}'.format(valName,taskParamMap[keyName])
        else:
            tmpStr = '{0}={1}'.format(valName,keyName)
        if self.taskSpec.splitRule in [None,'']:
            self.taskSpec.splitRule = tmpStr
        else:
            self.taskSpec.splitRule += ',{0}'.format(tmpStr)
        return    


    
Interaction.installSC(TaskRefinerBase)
