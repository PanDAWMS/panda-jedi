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
    outFileTempl = outFileTempl.replace("'","") 
    valStr = valStr.replace(outFileTempl,'${{{0}}}'.format(streamName))
    return outFileTempl,valStr



# extract file list
def extractFileList(taskParamMap,datasetName):
    itemList = taskParamMap['jobParameters'] + [taskParamMap['log']]
    fileList = []
    includePatt = []
    excludePatt = []
    for tmpItem in itemList:
        if tmpItem['type'] == 'template' and tmpItem.has_key('dataset') and \
                (tmpItem['dataset'] == datasetName or \
                     (tmpItem.has_key('expandedList') and datasetName in tmpItem['expandedList'])):
            if tmpItem.has_key('files'):
                fileList = tmpItem['files']
            if tmpItem.has_key('include'):
                includePatt = tmpItem['include'].split(',')
            if tmpItem.has_key('exclude'):
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
    if not taskParamMap.has_key('jobParameters'):
        taskParamMap['jobParameters'] = []
    taskParamMap['jobParameters'].append(tmpItem)
    return taskParamMap



# check if use random seed
def useRandomSeed(taskParamMap):
    for tmpItem in taskParamMap['jobParameters']:
        if tmpItem.has_key('value'):
            # get offset for random seed
            if tmpItem['type'] == 'template' and tmpItem['param_type'] == 'number':
                if '${RNDMSEED}' in tmpItem['value']:
                    return True
    return False

