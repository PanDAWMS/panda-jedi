import uuid
try:
    from urllib.parse import quote
except ImportError:
    from urllib import quote

from userinterface import Client

taskParamMap = {}

taskParamMap['noInput'] = True
taskParamMap['nEventsPerJob'] = 1
taskParamMap['nEvents'] = 5
taskParamMap['taskName'] = str(uuid.uuid4())
taskParamMap['userName'] = 'pandasrv1'
taskParamMap['vo'] = 'atlas'
taskParamMap['taskPriority'] = 1000
taskParamMap['reqID'] = 12345
taskParamMap['architecture'] = ''
taskParamMap['hpoWorkflow'] = True
taskParamMap['transUses'] = ''
taskParamMap['transHome'] = ''
taskParamMap['transPath'] = 'http://pandaserver.cern.ch:25080/trf/user/runHPO-00-00-01'
taskParamMap['processingType'] = 'simul'
taskParamMap['prodSourceLabel'] = 'test'
taskParamMap['useLocalIO'] = 1
taskParamMap['taskType'] = 'prod'
taskParamMap['workingGroup'] = 'AP_HPO'
taskParamMap['coreCount'] = 1
taskParamMap['site'] = 'BNL_PROD_UCORE'
taskParamMap['nucleus'] = 'CERN-PROD'
taskParamMap['cloud'] = 'WORLD'

logDatasetName = 'panda.jeditest.log.{0}'.format(uuid.uuid4())
outDatasetName = 'panda.jeditest.HPO.{0}'.format(uuid.uuid4())

taskParamMap['log'] = {'dataset': logDatasetName,
                       'type':'template',
                       'param_type':'log',
                       'token': 'ddd:.*DATADISK',
                       'destination':'(type=DATADISK)\(dontkeeplog=True)',
                       'offset':1000,
                       'value':'{0}.${{SN}}.log.tgz'.format(logDatasetName)}

taskParamMap['hpoRequestData'] = {'sandbox': None,
                                  'method': 'bayesian',
                                  'opt_space': {'A': (1, 4), 'B': (1, 10)},
                                  'initial_points': [({'A': 1, 'B': 2}, 0.3), ({'A': 1, 'B': 3}, None)],
                                  'max_points': 5,
                                  'num_points_per_generation': 2,
                                  }

taskParamMap['jobParameters'] = [
    {'type':'constant',
     'value': '-o out.json -j "" -p "{0}"'.format(quote('cp xxx.json out.json; tar cvfz metrics.tgz xxx.json'))
     },
    {'type': 'constant',
     'value': '--writeInputToTxt IN_DATA:input_ds.json --inSampleFile input_sample.json'
     },
    {'type': 'constant',
     'value': '-a aaa.tgz --sourceURL https://aipanda048.cern.ch:25443'
     },
    {'type': 'constant',
     'value': '--inMap "{\'IN_DATA\': ${IN_DATA/T}}"'
     },
    {'type':'template',
     'param_type':'input',
     'value':'-i "${IN_DATA/T}"',
     'dataset':'mc16_13TeV.501103.MGPy8EG_StauStauDirect_220p0_1p0_TFilt.merge.EVNT.e8102_e7400_tid21342682_00',
     'attributes': 'nosplit,repeat',
     },
    {'type': 'template',
     'param_type': 'output',
     'token': 'ATLASDATADISK',
     'value': '$JEDITASKID.metrics.${SN}.tgz',
     'dataset': outDatasetName,
     'hidden': True,
     },
    {'type': 'constant',
     'value': '--outMetricsFile=${OUTPUT0}^metrics.tgz',
     },
    ]

print(Client.insertTaskParams(taskParamMap))
