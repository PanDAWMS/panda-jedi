import uuid

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
taskParamMap['transUses'] = 'some_A'
taskParamMap['transHome'] = 'some_B'
taskParamMap['transPath'] = 'training.py'
taskParamMap['processingType'] = 'simul'
taskParamMap['prodSourceLabel'] = 'test'
taskParamMap['taskType'] = 'prod'
taskParamMap['workingGroup'] = 'AP_HPO'
taskParamMap['coreCount'] = 1
taskParamMap['site'] = 'BNL_PROD_UCORE'
taskParamMap['nucleus'] = 'CERN-PROD'
taskParamMap['cloud'] = 'WORLD'
#taskParamMap['mergeOutput'] = True
logDatasetName = 'panda.jeditest.log.{0}'.format(uuid.uuid4())
taskParamMap['log'] = {'dataset': logDatasetName,
                       'type':'template',
                       'param_type':'log',
                       'token': 'ddd:.*DATADISK',
                       'destination':'type=DATADISK,s3=1',
                       'offset':1000,
                       'value':'{0}.${{SN}}.log.tgz'.format(logDatasetName)}
taskParamMap['jobParameters'] = [
    {'type':'constant',
     'value': 'some args'
     },
    {'type':'constant',
     'value':'AMITag=p1462'
     },
    {'type':'template',
     'param_type':'input',
     'value':'ds=${IN_DATA}',
     'dataset':'mc16_13TeV.398184.MGPy8EG_A14N23LO_SlepSlep_direct_150p0_130p0.simul.HITS.e7506_e5984_a875_tid18097791_00',
     'attributes': 'nosplit,repeat',
     },
    ]

print(Client.insertTaskParams(taskParamMap))
