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
taskParamMap['architecture'] = 'power9'
taskParamMap['hpoWorkflow'] = True
taskParamMap['transUses'] = ''
taskParamMap['transHome'] = ''
taskParamMap['transPath'] = 'https://pandaserver.cern.ch:25443/trf/user/runHPO-00-00-01'
taskParamMap['processingType'] = 'simul'
taskParamMap['prodSourceLabel'] = 'test'
taskParamMap['taskType'] = 'prod'
taskParamMap['workingGroup'] = 'AP_HPO'
taskParamMap['coreCount'] = 1
taskParamMap['site'] = 'BNL_PROD_UCORE'
taskParamMap['nucleus'] = 'CERN-PROD'
taskParamMap['cloud'] = 'WORLD'

logDatasetName = 'panda.jeditest.log.{0}'.format(uuid.uuid4())

taskParamMap['log'] = {'dataset': logDatasetName,
                       'type':'template',
                       'param_type':'log',
                       'token': 'ddd:.*DATADISK',
                       'destination':'type=DATADISK,s3=1',
                       'offset':1000,
                       'value':'{0}.${{SN}}.log.tgz'.format(logDatasetName)}

taskParamMap['hpoRequestData'] = {'sandbox': None,
                                  'method': 'bayesian',
                                  'opt_space': {'A': (1, 4), 'B': (1, 10)},
                                  'initial_points': [({'A': 1, 'B': 2}, 0.3), ({'A': 1, 'B': 3}, None)],
                                  'max_points': 20,
                                  'num_points_per_generation': 10}

taskParamMap['jobParameters'] = [
    {'type':'constant',
     'value': '-o out.json -j "{0}"'.format(quote('<<<command to execute the container>>>'))
     },
    {'type': 'constant',
     'value': '--writeInputToTxt IN_DATA:input.txt'
     },
    {'type': 'constant',
     'value': '--inMap "{\'IN_DATA\': ${IN_DATA/T}}"'
     },
    {'type':'template',
     'param_type':'input',
     'value':'-i "${IN_DATA/T}"',
     'dataset':'<<<training dataset name>>>',
     'attributes': 'nosplit,repeat',
     },
    ]

print(Client.insertTaskParams(taskParamMap))
