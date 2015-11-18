import re
import sys

from pandajedi.jedicore import Interaction
from PostProcessorBase import PostProcessorBase



# post processor for ATLAS production
class AtlasProdPostProcessor (PostProcessorBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        PostProcessorBase.__init__(self,taskBufferIF,ddmIF)


    # main
    def doPostProcess(self,taskSpec,tmpLog):
        # pre-check
        try:
            tmpStat = self.doPreCheck(taskSpec,tmpLog)
            if tmpStat:
                return self.SC_SUCCEEDED
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doPreCheck failed with {0}:{1}'.format(errtype.__name__,errvalue))
            return self.SC_FATAL
        # get DDM I/F
        ddmIF = self.ddmIF.getInterface(taskSpec.vo)
        # loop over all datasets
        for datasetSpec in taskSpec.datasetSpecList:
            try:
                # remove wrong files
                if datasetSpec.type in ['output']:
                    # get successful files
                    okFiles = self.taskBufferIF.getSuccessfulFiles_JEDI(datasetSpec.jediTaskID,datasetSpec.datasetID)
                    if okFiles == None:
                        tmpLog.warning('failed to get successful files for {0}'.format(datasetSpec.datasetName))
                        return self.SC_FAILED
                    # get files in dataset
                    ddmFiles = ddmIF.getFilesInDataset(datasetSpec.datasetName,skipDuplicate=False,ignoreUnknown=True)
                    tmpLog.info('datasetID={0}:Name={1} has {2} files in DB, {3} files in DDM'.format(datasetSpec.datasetID,
                                                                                                      datasetSpec.datasetName,
                                                                                                      len(okFiles),len(ddmFiles)))
                    # check all files
                    toDelete = []
                    for tmpGUID,attMap in ddmFiles.iteritems():
                        if attMap['lfn'] not in okFiles:
                            did = {'scope':attMap['scope'], 'name':attMap['lfn']}
                            toDelete.append(did)
                            tmpLog.info('delete {0} from {1}'.format(attMap['lfn'],datasetSpec.datasetName))
                    # delete
                    if toDelete != []:
                        ddmIF.deleteFilesFromDataset(datasetSpec.datasetName,toDelete)
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.warning('failed to remove wrong files with {0}:{1}'.format(errtype.__name__,errvalue))
                return self.SC_FAILED
            try:
                # freeze output and log datasets
                if datasetSpec.type in ['output','log','trn_log']:
                    tmpLog.info('freezing datasetID={0}:Name={1}'.format(datasetSpec.datasetID,datasetSpec.datasetName))
                    ddmIF.freezeDataset(datasetSpec.datasetName,ignoreUnknown=True)
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.warning('failed to freeze datasets with {0}:{1}'.format(errtype.__name__,errvalue))
                return self.SC_FAILED
            try:
                # delete transient datasets
                if datasetSpec.type in ['trn_output']:
                    tmpLog.info('deleting datasetID={0}:Name={1}'.format(datasetSpec.datasetID,datasetSpec.datasetName))
                    retStr = ddmIF.deleteDataset(datasetSpec.datasetName,False,ignoreUnknown=True)
                    tmpLog.info(retStr)
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.warning('failed to delete datasets with {0}:{1}'.format(errtype.__name__,errvalue))
        # check duplication
        nDup = self.taskBufferIF.checkDuplication_JEDI(taskSpec.jediTaskID)
        tmpLog.info('checked duplication with {0}'.format(nDup))
        try:
            self.doBasicPostProcess(taskSpec,tmpLog)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doBasicPostProcess failed with {0}:{1}'.format(errtype.__name__,errvalue))
            return self.SC_FATAL
        return self.SC_SUCCEEDED



    # final procedure
    def doFinalProcedure(self,taskSpec,tmpLog):
        tmpLog.info('final procedure for status={0} processingType={1}'.format(taskSpec.status,
                                                                               taskSpec.processingType))
        if taskSpec.status in ['done','finished']:
            trnLifeTime = 30*24*60*60
            ddmIF = self.ddmIF.getInterface(taskSpec.vo)
            # set lifetime to transient datasets
            metaData = {'lifetime':trnLifeTime}
            for datasetSpec in taskSpec.datasetSpecList:
                if datasetSpec.type in ['log','output']:
                    tmpMetadata = ddmIF.getDatasetMetaData(datasetSpec.datasetName)
                    if tmpMetadata['transient'] == True:
                        tmpLog.info('set metadata={0} to datasetID={1}:Name={2}'.format(str(metaData),
                                                                                        datasetSpec.datasetID,
                                                                                        datasetSpec.datasetName))
                        for metadataName,metadaValue in metaData.iteritems():
                            ddmIF.setDatasetMetadata(datasetSpec.datasetName,metadataName,metadaValue)
            # set lifetime to parent transient datasets
            if taskSpec.processingType in ['merge']:
                # get parent task
                if not taskSpec.parent_tid in [None,taskSpec.jediTaskID]:
                    # get parent
                    tmpStat,parentTaskSpec = self.taskBufferIF.getTaskDatasetsWithID_JEDI(taskSpec.parent_tid,None,False)
                    if tmpStat and parentTaskSpec != None:
                        # set lifetime to parent datasets if they are transient
                        metaData = {'lifetime':trnLifeTime}
                        for datasetSpec in parentTaskSpec.datasetSpecList:
                            if datasetSpec.type in ['log','output']:
                                tmpMetadata = ddmIF.getDatasetMetaData(datasetSpec.datasetName)
                                if tmpMetadata['transient'] == True:
                                    tmpLog.info('set metadata={0} to parent jediTaskID={1}:datasetID={2}:Name={3}'.format(str(metaData),
                                                                                                                          taskSpec.parent_tid,
                                                                                                                          datasetSpec.datasetID,
                                                                                                                          datasetSpec.datasetName))
                                    for metadataName,metadaValue in metaData.iteritems():
                                        ddmIF.setDatasetMetadata(datasetSpec.datasetName,metadataName,metadaValue)
        # delete empty datasets
        if taskSpec.status == 'done':
            ddmIF = self.ddmIF.getInterface(taskSpec.vo)
            # loop over all datasets
            for datasetSpec in taskSpec.datasetSpecList:
                try:
                    if datasetSpec.type == 'output' and datasetSpec.nFilesFinished == 0:
                        tmpStat = True #ddmIF.deleteDataset(datasetSpec.datasetName,True,True)
                        tmpLog.info('delete empty prod dataset {0} with {1}'.format(datasetSpec.datasetName,tmpStat))
                except:
                    errtype,errvalue = sys.exc_info()[:2]
                    tmpLog.warning('failed to delete dataset with {0}:{1}'.format(errtype.__name__,errvalue))
        return self.SC_SUCCEEDED
