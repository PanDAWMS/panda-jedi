import re
import json

import six
from six import iteritems


# convert UTF-8 to ASCII in json dumps
def unicodeConvert(input):
    if isinstance(input, dict):
        retMap = {}
        for tmpKey,tmpVal in iteritems(input):
            retMap[unicodeConvert(tmpKey)] = unicodeConvert(tmpVal)
        return retMap
    elif isinstance(input, list):
        retList = []
        for tmpItem in input:
            retList.append(unicodeConvert(tmpItem))
        return retList
    elif six.PY2 and isinstance(input, unicode):
        return input.encode('ascii', 'ignore')
    return input



# decode
def decodeJSON(inputStr):
    return json.loads(inputStr, object_hook=unicodeConvert)



# encode
def encodeJSON(inputMap):
    return json.dumps(inputMap)



# extract stream name
def extractStreamName(valStr):
    tmpMatch = re.search('\$\{([^\}]+)\}',valStr)
    if tmpMatch is None:
        return None
    # remove decorators
    streamName = tmpMatch.group(1)
    streamName = streamName.split('/')[0]
    return streamName



# extract output filename template and replace the value field
def extractReplaceOutFileTemplate(valStr,streamName):
    outFileTempl = valStr.split('=')[-1]
    outFileTempl = outFileTempl.replace("'","")
    valStr = valStr.replace(outFileTempl,'${{{0}}}'.format(streamName))
    return outFileTempl,valStr



# extract file list
def extractFileList(taskParamMap,datasetName):
    baseDatasetName = datasetName.split(':')[-1]
    if 'log' in taskParamMap:
        itemList = taskParamMap['jobParameters'] + [taskParamMap['log']]
    else:
        itemList = taskParamMap['jobParameters']
    fileList = []
    includePatt = []
    excludePatt = []
    for tmpItem in itemList:
        if tmpItem['type'] == 'template' and 'dataset' in tmpItem and \
                ((tmpItem['dataset'] == datasetName or tmpItem['dataset'] == baseDatasetName) or \
                     ('expandedList' in tmpItem and \
                          (datasetName in tmpItem['expandedList'] or baseDatasetName in tmpItem['expandedList']))):
            if 'files' in tmpItem:
                fileList = tmpItem['files']
            if 'include' in tmpItem:
                includePatt = tmpItem['include'].split(',')
            if 'exclude' in tmpItem:
                excludePatt = tmpItem['exclude'].split(',')
    return fileList,includePatt,excludePatt



# append dataset
def appendDataset(taskParamMap,datasetSpec,fileList):
    # make item for dataset
    tmpItem = {}
    tmpItem['type']       = 'template'
    tmpItem['value']      = ''
    tmpItem['dataset']    = datasetSpec.datasetName
    tmpItem['param_type'] = datasetSpec.type
    if fileList != []:
        tmpItem['files']  = fileList
    # append
    if 'jobParameters' not in taskParamMap:
        taskParamMap['jobParameters'] = []
    taskParamMap['jobParameters'].append(tmpItem)
    return taskParamMap



# check if use random seed
def useRandomSeed(taskParamMap):
    for tmpItem in taskParamMap['jobParameters']:
        if 'value' in tmpItem:
            # get offset for random seed
            if tmpItem['type'] == 'template' and tmpItem['param_type'] == 'number':
                if '${RNDMSEED}' in tmpItem['value']:
                    return True
    return False
