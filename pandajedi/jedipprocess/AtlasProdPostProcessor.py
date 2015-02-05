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
        try:
            self.doBasicPostProcess(taskSpec,tmpLog)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doBasicPostProcess failed with {0}:{1}'.format(errtype.__name__,errvalue))
            return self.SC_FATAL
        return self.SC_SUCCEEDED
            
    
