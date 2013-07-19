import re
import json


# convert UTF-8 to ASCII in json dumps
def unicodeConvert(input):
    if isinstance(input,dict):
        retMap = {}
        for tmpKey,tmpVal in input.iteritems():
            retMap[unicodeConvert(tmpKey)] = unicodeConvert(tmpVal)
        return retMap
    elif isinstance(input,list):
        retList = []
        for tmpItem in input:
            retList.append(unicodeConvert(tmpItem))
        return retList
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    return input



# decode
def decodeJSON(inputStr):
    return json.loads(inputStr,object_hook=unicodeConvert)



# encode
def encodeJSON(inputMap):
    return json.dumps(inputMap)



# extract stream name
def extractStreamName(valStr):
    tmpMatch = re.search('\$\{([^\}]+)\}',valStr)
    if tmpMatch == None:
        return None
    return tmpMatch.group(1)



# extract output filename template and replace the value field
def extractReplaceOutFileTemplate(valStr,streamName):
    outFileTempl = valStr.split('=')[-1]
    valStr = valStr.replace(outFileTempl,'${{{0}}}'.format(streamName))
    return outFileTempl,valStr



# extract file list
def extractFileList(taskParamMap,datasetName):
    itemList = taskParamMap['jobParameters'] + [taskParamMap['log']]
    for tmpItem in itemList:
        if tmpItem['type'] == 'template' and tmpItem.has_key('dataset') and \
                tmpItem['dataset'] == datasetName:
            if tmpItem.has_key('files'):
                return tmpItem['files']
            else:
                return []
    return []



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
    if not taskParamMap.has_key('jobParameters'):
        taskParamMap['jobParameters'] = []
    taskParamMap['jobParameters'].append(tmpItem)
    return taskParamMap

