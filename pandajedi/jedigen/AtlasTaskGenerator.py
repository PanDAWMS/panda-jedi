import re
import sys
import uuid
import json

from six import iteritems

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore import Interaction
from .TaskGeneratorBase import TaskGeneratorBase


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# task generator for ATLAS
class AtlasTaskGenerator (TaskGeneratorBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        TaskGeneratorBase.__init__(self,taskBufferIF,ddmIF)


    # main to generate task
    def doGenerate(self,taskSpec,taskParamMap,**varMap):
        # make logger
        tmpLog = MsgWrapper(logger,"<jediTaskID={0}>".format(taskSpec.jediTaskID))
        tmpLog.info('start taskType={0}'.format(taskSpec.taskType))
        tmpLog.info(str(varMap))
        # returns
        retFatal    = self.SC_FATAL
        retTmpError = self.SC_FAILED
        retOK       = self.SC_SUCCEEDED
        try:
            # check prodSourceLabel
            if taskSpec.prodSourceLabel in ['managed','test']:
                # check taskType
                if taskSpec.taskType == 'recov':
                    # generate parent tasks for lost file recovery if it is not yet generated
                    if 'parentGenerated' in taskParamMap:
                        tmpLog.info('skip since already generated parent tasks')
                    else:
                        tmpLog.info('generating parent tasks for lost file recovery')
                        # missing files are undefined
                        if 'missingFilesMap' not in varMap:
                            tmpLog.error('missing files are undefined')
                            return retFatal
                        missingFilesMap = varMap['missingFilesMap']
                        # check datasets
                        for datasetName,datasetValMap in iteritems(missingFilesMap):
                            # dataset needs specify container
                            datasetSpec = datasetValMap['datasetSpec']
                            if datasetSpec.containerName in ['',None]:
                                errStr = 'cannot make parent tasks due to undefined container for datasetID={0}:{1}'.format(datasetSpec.datasetID,
                                                                                                                            datasetName)
                                taskSpec.setErrDiag(errStr)
                                tmpLog.error(errStr)
                                return retFatal
                        # make parameters for new task
                        newJsonStrList = []
                        for datasetName,datasetValMap in iteritems(missingFilesMap):
                            datasetSpec = datasetValMap['datasetSpec']
                            newTaskParamMap = {}
                            newTaskParamMap['oldDatasetName']  = datasetName
                            newTaskParamMap['lostFiles']       = datasetValMap['missingFiles']
                            newTaskParamMap['vo']              = taskSpec.vo
                            newTaskParamMap['cloud']           = taskSpec.cloud
                            newTaskParamMap['taskPriority']    = taskSpec.taskPriority
                            newTaskParamMap['taskType']        = taskSpec.taskType
                            newTaskParamMap['prodSourceLabel'] = taskSpec.prodSourceLabel
                            logDatasetName = 'panda.jedi{0}.log.{1}'.format(taskSpec.taskType,uuid.uuid4())
                            newTaskParamMap['log'] = {'dataset': logDatasetName,
                                                      'type':'template',
                                                      'param_type':'log',
                                                      'token':'ATLASDATADISK',
                                                      'value':'{0}.${{SN}}.log.tgz'.format(logDatasetName)}
                            # make new datasetname
                            outDatasetName = datasetName
                            # remove /
                            outDatasetName = re.sub('/$','',outDatasetName)
                            # remove extension
                            outDatasetName = re.sub('\.{0}\d+$'.format(taskSpec.taskType),'',outDatasetName)
                            # add extension
                            outDatasetName = outDatasetName + '.{0}{1}'.format(taskSpec.taskType,taskSpec.jediTaskID)
                            newTaskParamMap['output'] = {'dataset': outDatasetName}
                            if datasetSpec.containerName not in ['',None]:
                                newTaskParamMap['output']['container'] = datasetSpec.containerName
                            # make json
                            jsonStr = json.dumps(newTaskParamMap)
                            newJsonStrList.append(jsonStr)
                        # change original task parameters to not repeat the same procedure and to use newly produced files
                        taskParamMap['parentGenerated']         = True
                        taskParamMap['useInFilesInContainer']   = True
                        taskParamMap['useInFilesWithNewAttemptNr'] = True
                        jsonStr = json.dumps(taskParamMap)
                        # insert and update task parameters
                        sTmp,newJediTaskIDs = self.taskBufferIF.insertUpdateTaskParams_JEDI(taskSpec.jediTaskID,
                                                                                            taskSpec.vo,
                                                                                            taskSpec.prodSourceLabel,
                                                                                            jsonStr,newJsonStrList)
                        if sTmp:
                            tmpLog.info('inserted/updated tasks in DB : new jediTaskIDs={0}'.format(str(newJediTaskIDs)))
                        else:
                            tmpLog.error('failed to insert/update tasks in DB')
                            return retFatal
            # return
            tmpLog.info('done')
            return retOK
        except Exception:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doGenerate failed with {0}:{1}'.format(errtype.__name__,errvalue))
            return retFatal
