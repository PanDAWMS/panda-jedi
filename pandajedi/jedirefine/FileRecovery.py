import re
import sys
import copy

from .import RefinerUtils
from pandajedi.jedicore import Interaction
from .TaskRefinerBase import TaskRefinerBase
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandajedi.jedicore.JediDatasetSpec import JediDatasetSpec

# refiner for lost file recovery
class FileRecovery (TaskRefinerBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        TaskRefinerBase.__init__(self,taskBufferIF,ddmIF)


    # extract common parameters
    def extractCommon(self,jediTaskID,taskParamMap,workQueueMapper):
        return self.SC_SUCCEEDED


    # check matching of dataset names
    def checkDatasetNameMatching(self,datasetName,patternList):
        # name list is not given
        if patternList is None:
            return False
        # loop over all names
        for namePattern in patternList:
            if re.search('^'+namePattern+'$',datasetName) is not None:
                return True
        return False


    # main
    def doRefine(self,jediTaskID,taskParamMap):
        try:
            # make logger
            tmpLog = self.tmpLog
            tmpLog.debug('start jediTaskID={0}'.format(jediTaskID))
            # old dataset name
            oldDatasetName = taskParamMap['oldDatasetName']
            # accompany datasets
            if 'oldAccompanyDatasetNames' in taskParamMap:
                oldAccDatasetNames = taskParamMap['oldAccompanyDatasetNames']
            else:
                oldAccDatasetNames = None
            # use first file to get task and dataset info
            lostFileName = taskParamMap['lostFiles'][0]
            # get ole jediTaskID and datasetIDs
            tmpStat,oldIDs = self.taskBufferIF.getIDsWithFileDataset_JEDI(oldDatasetName,lostFileName,'output')
            if tmpStat is not True or oldIDs is None:
                tmpLog.error('failed to get jediTaskID and DatasetID for {0}:{1}'.format(oldDatasetName,
                                                                                         lostFileName))
                return self.SC_FAILED
            # get task
            oldJediTaskID = oldIDs['jediTaskID']
            oldDatasetID  = oldIDs['datasetID']
            tmpStat,oldTaskSpec = self.taskBufferIF.getTaskWithID_JEDI(oldJediTaskID,True)
            if tmpStat is not True:
                tmpLog.error('failed to get TaskSpec for old jediTaskId={0}'.format(oldJediTaskID))
                return self.SC_FAILED
            # make task spec
            taskSpec = JediTaskSpec()
            taskSpec.copyAttributes(oldTaskSpec)
            # reset attributes
            taskSpec.jediTaskID   = jediTaskID
            taskSpec.taskType     = taskParamMap['taskType']
            taskSpec.taskPriority = taskParamMap['taskPriority']
            self.taskSpec = taskSpec
            # get datasets
            tmpStat,datasetSpecList = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(oldJediTaskID)
            if tmpStat is not True:
                tmpLog.error('failed to get datasetSpecs')
                return self.SC_FAILED
            # loop over all datasets
            provenanceID = None
            dummyStreams = []
            outDatasetSpec = None
            datasetNameSpecMap = {}
            for datasetSpec in datasetSpecList:
                # for output datasets
                if datasetSpec.type not in JediDatasetSpec.getInputTypes():
                    # collect output with the same provenanceID
                    if provenanceID is not None and datasetSpec.provenanceID != provenanceID:
                        continue
                    # set provenanceID if undefined
                    if provenanceID is None and datasetSpec.provenanceID is not None:
                        provenanceID = datasetSpec.provenanceID
                    # collect dummy streams
                    if datasetSpec.type != 'log' and (datasetSpec.datasetID != oldDatasetID and \
                                                          not self.checkDatasetNameMatching(datasetSpec.datasetName,oldAccDatasetNames)):
                        if datasetSpec.streamName not in dummyStreams:
                            dummyStreams.append(datasetSpec.streamName)
                        continue
                # reset attributes
                datasetSpec.status = 'defined'
                datasetSpec.datasetID  = None
                datasetSpec.jediTaskID = jediTaskID
                datasetSpec.nFiles = 0
                datasetSpec.nFilesUsed = 0
                datasetSpec.nFilesToBeUsed = 0
                datasetSpec.nFilesFinished = 0
                datasetSpec.nFilesFailed   = 0
                datasetSpec.nFilesOnHold   = 0
                # remove nosplit and repeat since even the same file is made for each bounaryID
                datasetSpec.remNoSplit()
                datasetSpec.remRepeat()
                # append to map
                datasetNameSpecMap[datasetSpec.datasetName] = datasetSpec
                # set master and secondary for input
                if datasetSpec.type in JediDatasetSpec.getInputTypes():
                    if datasetSpec.isMaster():
                        # master
                        self.inMasterDatasetSpec = datasetSpec
                    else:
                        # secondary
                        self.inSecDatasetSpecList.append(datasetSpec)
                elif datasetSpec.type == 'log':
                    # set new attributes
                    tmpItem = taskParamMap['log']
                    datasetSpec.datasetName = tmpItem['dataset']
                    if 'container' in tmpItem:
                        datasetSpec.containerName = tmpItem['container']
                    if 'token' in tmpItem:
                        datasetSpec.storageToken = tmpItem['token']
                    if 'destination' in tmpItem:
                        datasetSpec.destination = tmpItem['destination']
                    # extract output filename template and change the value field
                    outFileTemplate,tmpItem['value'] = RefinerUtils.extractReplaceOutFileTemplate(tmpItem['value'],
                                                                                                  datasetSpec.streamName)
                    # make output template
                    if outFileTemplate is not None:
                        if 'offset' in tmpItem:
                            offsetVal = 1 + tmpItem['offset']
                        else:
                            offsetVal = 1
                        outTemplateMap = {'jediTaskID' : self.taskSpec.jediTaskID,
                                          'serialNr' : offsetVal,
                                          'streamName' : datasetSpec.streamName,
                                          'filenameTemplate' : outFileTemplate,
                                          'outtype' : datasetSpec.type,
                                          }
                        self.outputTemplateMap[datasetSpec.outputMapKey()] = [outTemplateMap]
                    # append
                    self.outDatasetSpecList.append(datasetSpec)
                else:
                    # output dataset to make copies later
                    outDatasetSpec = datasetSpec
            # replace redundant output streams with dummy files
            for dummyStream in dummyStreams:
                self.taskSpec.jobParamsTemplate = self.taskSpec.jobParamsTemplate.replace('${'+dummyStream+'}',
                                                                                          dummyStream.lower()+'.tmp')
            self.setJobParamsTemplate(self.taskSpec.jobParamsTemplate)
            # loop over all lost files
            datasetIDSpecMap = {}
            for lostFileName in taskParamMap['lostFiles']:
                # get FileID
                tmpStat,tmpIDs = self.taskBufferIF.getIDsWithFileDataset_JEDI(oldDatasetName,lostFileName,'output')
                if tmpStat is not True or tmpIDs is None:
                    tmpLog.error('failed to get FileID for {0}:{1}'.format(oldDatasetName,
                                                                           lostFileName))
                    return self.SC_FAILED
                # get PandaID
                tmpStat,pandaID = self.taskBufferIF.getPandaIDWithFileID_JEDI(tmpIDs['jediTaskID'],
                                                                              tmpIDs['datasetID'],
                                                                              tmpIDs['fileID'])
                if tmpStat is not True or pandaID is None:
                    tmpLog.error('failed to get PandaID for {0}'.format(str(tmpIDs)))
                    return self.SC_FAILED
                # get files
                tmpStat,fileSpecList = self.taskBufferIF.getFilesWithPandaID_JEDI(pandaID)
                if tmpStat is not True or fileSpecList == []:
                    tmpLog.error('failed to get files for PandaID={0}'.format(pandaID))
                    return self.SC_FAILED
                # append
                for fileSpec in fileSpecList:
                    # only input types
                    if fileSpec.type not in JediDatasetSpec.getInputTypes():
                        continue
                    # get original datasetSpec
                    if fileSpec.datasetID not in datasetIDSpecMap:
                        tmpStat,tmpDatasetSpec = self.taskBufferIF.getDatasetWithID_JEDI(fileSpec.jediTaskID,fileSpec.datasetID)
                        if tmpStat is not True or tmpDatasetSpec is None:
                            tmpLog.error('failed to get dataset for jediTaskID={0} datasetID={1}'.format(fileSpec.jediTaskID,
                                                                                                         fileSpec.datasetID))
                            return self.SC_FAILED
                        datasetIDSpecMap[fileSpec.datasetID] = tmpDatasetSpec
                    origDatasetSpec = datasetIDSpecMap[fileSpec.datasetID]
                    if origDatasetSpec.datasetName not in datasetNameSpecMap:
                        tmpLog.error('datasetName={0} is missing in new datasets'.format(origDatasetSpec.datasetName))
                        return self.SC_FAILED
                    # not target or accompany datasets
                    if origDatasetSpec.datasetID != oldDatasetID and \
                            not self.checkDatasetNameMatching(origDatasetSpec.datasetName,oldAccDatasetNames):
                        continue
                    newDatasetSpec = datasetNameSpecMap[origDatasetSpec.datasetName]
                    # set new attributes
                    fileSpec.fileID = None
                    fileSpec.datasetID = None
                    fileSpec.jediTaskID = None
                    fileSpec.boundaryID = pandaID
                    fileSpec.keepTrack = 1
                    fileSpec.attemptNr = 1
                    fileSpec.status = 'ready'
                    # append
                    newDatasetSpec.addFile(fileSpec)
                # make one output dataset per file
                datasetSpec = copy.copy(outDatasetSpec)
                # set new attributes
                tmpItem = taskParamMap['output']
                datasetSpec.datasetName = tmpItem['dataset']
                if 'container' in tmpItem:
                    datasetSpec.containerName = tmpItem['container']
                if 'token' in tmpItem:
                    datasetSpec.storageToken = tmpItem['token']
                if 'destination' in tmpItem:
                    datasetSpec.destination = tmpItem['destination']
                # use PandaID of original job as provenanceID
                datasetSpec.provenanceID = pandaID
                # append
                self.outDatasetSpecList.append(datasetSpec)
                # extract attempt number from original filename
                tmpMatch = re.search('\.(\d+)$',lostFileName)
                if tmpMatch is None:
                    offsetVal = 1
                else:
                    offsetVal = 1 + int(tmpMatch.group(1))
                # filename without attempt number
                baseFileName = re.sub('\.(\d+)$','',lostFileName)
                # make output template
                outTemplateMap = {'jediTaskID' : self.taskSpec.jediTaskID,
                                  'serialNr' : offsetVal,
                                  'streamName' : datasetSpec.streamName,
                                  'filenameTemplate' : baseFileName + '.${SN:d}',
                                  'outtype' : datasetSpec.type,
                                  }
                self.outputTemplateMap[datasetSpec.outputMapKey()] = [outTemplateMap]
            # append datasets to task parameters
            for datasetSpec in datasetNameSpecMap.values():
                if datasetSpec.Files == []:
                    continue
                fileList = []
                for fileSpec in datasetSpec.Files:
                    fileList.append({'lfn':fileSpec.lfn,
                                     'firstEvent':fileSpec.firstEvent,
                                     'startEvent':fileSpec.startEvent,
                                     'endEvent':fileSpec.endEvent,
                                     'keepTrack':fileSpec.keepTrack,
                                     'boundaryID':fileSpec.boundaryID,
                                     })
                taskParamMap = RefinerUtils.appendDataset(taskParamMap,datasetSpec,fileList)
                self.updatedTaskParams = taskParamMap
            # grouping with boundaryID
            self.setSplitRule(None,4,JediTaskSpec.splitRuleToken['groupBoundaryID'])
        except Exception:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doRefine failed with {0}:{1}'.format(errtype.__name__,errvalue))
            return self.SC_FAILED
        tmpLog.debug('done')
        return self.SC_SUCCEEDED
