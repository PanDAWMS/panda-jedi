"""
dataset specification for JEDI

"""

import re
import math

from pandajedi.jediconfig import jedi_config


class JediDatasetSpec(object):
    # attributes
    _attributes = (
        'jediTaskID','datasetID','datasetName','containerName',
        'type','creationTime','modificationTime','vo','cloud',
        'site','masterID','provenanceID','status','state',
        'stateCheckTime','stateCheckExpiration','frozenTime',
        'nFiles','nFilesToBeUsed','nFilesUsed',
        'nFilesFinished','nFilesFailed',
        'nEvents','nEventsToBeUsed','nEventsUsed',
        'lockedBy','lockedTime','attributes','streamName',
        'storageToken','destination'
        )
    # attributes which have 0 by default
    _zeroAttrs = ()
    # attributes to force update
    _forceUpdateAttrs = ('lockedBy','lockedTime')
    # mapping between sequence and attr
    _seqAttrMap = {'datasetID':'{0}.JEDI_DATASETS_ID_SEQ.nextval'.format(jedi_config.db.schemaJEDI)}



    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            object.__setattr__(self,attr,None)
        # file list
        object.__setattr__(self,'Files',[])
        # map of changed attributes
        object.__setattr__(self,'_changedAttrs',{})



    # override __setattr__ to collecte the changed attributes
    def __setattr__(self,name,value):
        oldVal = getattr(self,name)
        object.__setattr__(self,name,value)
        newVal = getattr(self,name)
        # collect changed attributes
        if oldVal != newVal or name in self._forceUpdateAttrs:
            self._changedAttrs[name] = value



    # add File to files list
    def addFile(self,fileSpec):
        # append
        self.Files.append(fileSpec)


        
    # reset changed attribute list
    def resetChangedList(self):
        object.__setattr__(self,'_changedAttrs',{})



    # force update
    def forceUpdate(self,name):
        if name in self._attributes:
            self._changedAttrs[name] = getattr(self,name)
        

    
    # return map of values
    def valuesMap(self,useSeq=False,onlyChanged=False):
        ret = {}
        for attr in self._attributes:
            # use sequence
            if useSeq and self._seqAttrMap.has_key(attr):
                continue
            # only changed attributes
            if onlyChanged:
                if not self._changedAttrs.has_key(attr):
                    continue
            val = getattr(self,attr)
            if val == None:
                if attr in self._zeroAttrs:
                    val = 0
                else:
                    val = None
            ret[':%s' % attr] = val
        return ret



    # pack tuple into FileSpec
    def pack(self,values):
        for i in range(len(self._attributes)):
            attr= self._attributes[i]
            val = values[i]
            object.__setattr__(self,attr,val)



    # return column names for INSERT
    def columnNames(cls,prefix=None):
        ret = ""
        for attr in cls._attributes:
            if prefix != None:
                ret += '{0}.'.format(prefix)
            ret += '{0},'.format(attr)
        ret = ret[:-1]    
        return ret
    columnNames = classmethod(columnNames)



    # return expression of bind variables for INSERT
    def bindValuesExpression(cls,useSeq=True):
        ret = "VALUES("
        for attr in cls._attributes:
            if useSeq and cls._seqAttrMap.has_key(attr):
                ret += "%s," % cls._seqAttrMap[attr]
            else:
                ret += ":%s," % attr
        ret = ret[:-1]
        ret += ")"            
        return ret
    bindValuesExpression = classmethod(bindValuesExpression)


    
    # return an expression of bind variables for UPDATE to update only changed attributes
    def bindUpdateChangesExpression(self):
        ret = ""
        for attr in self._attributes:
            if self._changedAttrs.has_key(attr):
                ret += '%s=:%s,' % (attr,attr)
        ret  = ret[:-1]
        ret += ' '
        return ret



    # get the total size of files
    def getSize(self):
        totalSize = 0
        checkedList = []
        for tmpFileSpec in self.Files:
            if not tmpFileSpec.lfn in checkedList:
                totalSize += tmpFileSpec.fsize
                checkedList.append(tmpFileSpec.lfn)
        return totalSize    



    # return list of status to update contents
    def statusToUpdateContents(cls):
        return ['defined','toupdate']
    statusToUpdateContents = classmethod(statusToUpdateContents)



    # check if JEDI needs to keep track of file usage
    def toKeepTrack(self):
        if self.isNoSplit() and self.isRepeated():
            return False
        else:
            return True



    # check if it is not split
    def isNoSplit(self):
        if self.attributes != None and 'nosplit' in self.attributes:
            return True
        else:
            return False



    # check if it is repeatedly used
    def isRepeated(self):
        if self.attributes != None and 'repeat' in self.attributes:
            return True
        else:
            return False



    # check if it is a many-time dataset which is treated as long-standing at T2s
    def isManyTime(self):
        if self.attributes != None and 'manytime' in self.attributes:
            return True
        else:
            return False



    # check if it is a master dataset
    def isMaster(self):
        if self.masterID == None and self.type == 'input':
            return True
        else:
            return False



    # get the ratio to master
    def getRatioToMaster(self):
        try:
            tmpMatch = re.search('ratio=(\d+(\.\d+)*)',self.attributes)
            if tmpMatch != None:
                ratioStr = tmpMatch.group(1)
                try:
                    # integer
                    return int(ratioStr)
                except:
                    pass
                try:
                    # float
                    return float(ratioStr)
                except:
                    pass
        except:
            pass
        return 1



    # get N multiplied by ratio
    def getNumMultByRatio(self,num):
        # get ratio
        ratioVal = self.getRatioToMaster()
        # integer or float
        if isinstance(ratioVal,int):
            retVal = num * ratioVal
        else:
            retVal = float(num) * ratioVal
            retVal = int(math.ceil(retVal))
        return retVal
