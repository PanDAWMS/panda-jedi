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
def decodeJSON(input):
    return json.loads(input,object_hook=unicodeConvert)



# extract stream name
def extractStreamName(valStr):
    tmpMatch = re.search('\$\{([^\}]+)\}',valStr)
    if tmpMatch == None:
        return None
    return tmpMatch.group(1)



# extract output filename template and replace the value field
def extractReplaceOutFileTemplate(valStr,streamName):
    outFileTempl = valStr.split('=')[-1]
    valStr = valStr.replace(outFileTempl,'${{0}}'.format(streamName))
    return outFileTempl,valStr

