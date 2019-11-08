import sys
try:
    metaID = sys.argv[1]
except Exception:
    metaID = None
import json
import uuid

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

inFileList = ['file1','file2','file3']

logDatasetName = 'panda.jeditest.log.{0}'.format(uuid.uuid4())

taskParamMap = {}

taskParamMap['nFilesPerJob'] = 1
taskParamMap['nFiles'] = len(inFileList)
#taskParamMap['nEventsPerInputFile']  = 10000
#taskParamMap['nEventsPerJob'] = 10000
#taskParamMap['nEvents'] = 25000
taskParamMap['noInput'] = True
taskParamMap['pfnList'] = inFileList
#taskParamMap['mergeOutput'] = True
taskParamMap['taskName'] = str(uuid.uuid4())
taskParamMap['userName'] = 'someone'
taskParamMap['vo'] = 'nonatlas'
taskParamMap['taskPriority'] = 900
#taskParamMap['reqID'] = reqIdx
taskParamMap['architecture'] = 'i686-slc5-gcc43-opt'
taskParamMap['transUses'] = 'Atlas-17.2.2'
taskParamMap['transHome'] = 'AtlasProduction-17.2.2.2'
taskParamMap['transPath'] = 'Generate_trf.py'
taskParamMap['processingType'] = 'evgen'
taskParamMap['prodSourceLabel'] = 'test'
taskParamMap['taskType'] = 'test'
taskParamMap['workingGroup'] = 'AP_Higgs'
#taskParamMap['coreCount'] = 1
#taskParamMap['walltime'] = 1
taskParamMap['cloud'] = 'FR'
taskParamMap['site'] = 'FZK-LCG2'
taskParamMap['log'] = {'dataset': logDatasetName,
                       'type':'template',
                       'param_type':'log',
                       'value':'{0}.${{SN}}.log.tgz'.format(logDatasetName)}
outDatasetName = 'panda.jeditest.EVNT.{0}'.format(uuid.uuid4())


taskParamMap['jobParameters'] = [
    {'type':'constant',
     'value':'-i "${IN/T}"',
     },
    {'type':'constant',
     'value': 'ecmEnergy=8000 runNumber=12345'
     },
    {'type':'template',
     'param_type':'output',
     'token':'ATLASDATADISK',     
     'value':'outputEVNTFile={0}.${{SN}}.pool.root'.format(outDatasetName),
     'dataset':outDatasetName,
     'offset':1000,
     },
    {'type':'constant',
     'value':'evgenJobOpts=MC12JobOpts-00-01-06_v1.tar.gz',
     },
    ]

#taskParamMap['mergeSpec'] = {}
#taskParamMap['mergeSpec']['transPath'] = 'Merge_trf.py'
#taskParamMap['mergeSpec']['jobParameters'] = "inputFile=${INPUT0} inputLogFile=${INLOG0} outputFile=${OUTPUT0}"

jonStr = json.dumps(taskParamMap)

tbIF.insertTaskParams_JEDI(taskParamMap['vo'],taskParamMap['prodSourceLabel'],taskParamMap['userName'],taskParamMap['taskName'],jonStr)
