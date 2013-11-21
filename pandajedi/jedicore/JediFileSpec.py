"""
file specification for JEDI

"""

from taskbuffer.FileSpec import FileSpec as JobFileSpec


class JediFileSpec(object):
    # attributes
    _attributes = ('jediTaskID','datasetID','fileID','creationDate','lastAttemptTime',
                   'lfn','GUID','type','status','fsize','checksum','scope',
                   'attemptNr','maxAttempt','nEvents','keepTrack',
                   'startEvent','endEvent','firstEvent','boundaryID')
    # attributes which have 0 by default
    _zeroAttrs = ('fsize','attemptNr')
    # mapping between sequence and attr
    _seqAttrMap = {'fileID':'ATLAS_PANDA.JEDI_DATASET_CONT_FILEID_SEQ.nextval'}


    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            if attr in self._zeroAttrs:
                object.__setattr__(self,attr,0)
            else:
                object.__setattr__(self,attr,None)
        # map of changed attributes
        object.__setattr__(self,'_changedAttrs',{})
        # locality
        object.__setattr__(self,'locality',{})
        # source name
        object.__setattr__(self,'sourceName',None)


    # override __setattr__ to collecte the changed attributes
    def __setattr__(self,name,value):
        oldVal = getattr(self,name)
        object.__setattr__(self,name,value)
        newVal = getattr(self,name)
        # collect changed attributes
        if oldVal != newVal:
            self._changedAttrs[name] = value


    # reset changed attribute list
    def resetChangedList(self):
        object.__setattr__(self,'_changedAttrs',{})
        
    
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
    def columnNames(cls):
        ret = ""
        for attr in cls._attributes:
            if ret != "":
                ret += ','
            ret += attr
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


    # convert to job's FileSpec
    def convertToJobFileSpec(self,datasetSpec,setType=None):
        jobFileSpec = JobFileSpec()
        jobFileSpec.fileID     = self.fileID
        jobFileSpec.datasetID  = datasetSpec.datasetID
        jobFileSpec.jediTaskID = datasetSpec.jediTaskID
        jobFileSpec.lfn        = self.lfn
        jobFileSpec.GUID       = self.GUID
        if setType == None:
            jobFileSpec.type   = self.type
        else:
            jobFileSpec.type   = setType
        jobFileSpec.scope      = self.scope
        jobFileSpec.fsize      = self.fsize
        jobFileSpec.checksum   = self.checksum
        jobFileSpec.attemptNr  = self.attemptNr
        # dataset attribute
        if datasetSpec != None:
            # dataset
            if not datasetSpec.containerName in [None,'']:
                jobFileSpec.dataset = datasetSpec.containerName
            else:
                jobFileSpec.dataset = datasetSpec.datasetName
            if self.type in datasetSpec.getInputTypes() or setType in datasetSpec.getInputTypes():
                # prodDBlock
                jobFileSpec.prodDBlock = datasetSpec.datasetName
                # storage token    
                if not datasetSpec.storageToken in ['',None]:
                    jobFileSpec.dispatchDBlockToken = datasetSpec.storageToken 
            else:
                # destinationDBlock
                jobFileSpec.destinationDBlock = datasetSpec.datasetName
                # storage token    
                if not datasetSpec.storageToken in ['',None]:
                    jobFileSpec.destinationDBlockToken = datasetSpec.storageToken 
        # return
        return jobFileSpec


    # convert from job's FileSpec
    def convertFromJobFileSpec(self,jobFileSpec):
        self.fileID     = jobFileSpec.fileID
        self.datasetID  = jobFileSpec.datasetID
        self.jediTaskID = jobFileSpec.jediTaskID
        self.lfn        = jobFileSpec.lfn
        self.GUID       = jobFileSpec.GUID
        self.type       = jobFileSpec.type
        self.scope      = jobFileSpec.scope
        self.fsize      = jobFileSpec.fsize
        self.checksum   = jobFileSpec.checksum
        self.attemptNr  = jobFileSpec.attemptNr
        # convert NULL to None
        for attr in self._attributes:
            val = getattr(self,attr)
            if val == 'NULL':
                object.__setattr__(self,attr,None)
        # return
        return


    # check if event-level splitting is used
    def doEventLevelSplit(self):
        if self.startEvent == None:
            return False
        return True
        

                       
