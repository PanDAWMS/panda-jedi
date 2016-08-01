"""
work queue specification

"""

import re
import types

class WorkQueue(object):
    # attributes
    _attributes = ('queue_id','queue_name','queue_type','VO','queue_share','queue_order',
                   'criteria','variables','partitionID','stretchable','status')
    # parameters for selection criteria
    _paramsForSelection = ('prodSourceLabel','workingGroup','processingType','coreCount',
                           'site','eventService','splitRule','campaign')
    
    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            setattr(self,attr,None)
        # sub queues    
        self.subQueues = []


    # string presentation
    def __str__(self):
        return str(self.queue_name)


    # dump
    def dump(self):
        dumpStr = 'id:%s order:%s name:%s share:%s ' % \
                  (self.queue_id,self.queue_order,self.queue_name,self.queue_share)
        if self.subQueues == []:
            # normal queue
            dumpStr += 'criteria:%s var:%s eval:%s' % \
                       (self.criteria,str(self.variables),self.evalString)
        else:
            # sub queues
            dumpStr += '\n'
            for subQueue in self.subQueues: 
                dumpStr += '        %s\n' % subQueue.dump()
            dumpStr = dumpStr[:-1]    
        return dumpStr


    # get IDs 
    def getIDs(self):
        if self.subQueues == []:
            return [self.queue_id]
        else:
            tmpIDs = []
            for subQueue in self.subQueues:
                tmpIDs.append(subQueue.queue_id)
            return tmpIDs
        
            
    # add sub queues
    def addSubQueue(self,subQueue):
        # disallow non-throttle in throttled partition
        if self.queue_share != None:
            if subQueue.queue_share == None:
                subQueue.queue_share = 0
        # set non-throttle in non-throttle partition
        if self.queue_share == None:
            if subQueue.queue_share != None:
                subQueue.queue_share = None
        # sort by queue_order
        idx = 0
        for tmpIdx,tmpQueue in enumerate(self.subQueues):
            if tmpQueue.queue_order < subQueue.queue_order:
                idx = tmpIdx + 1
        self.subQueues.insert(idx,subQueue)
            
        
    # pack tuple into obj
    def pack(self,values):
        for i,attr in enumerate(self._attributes):
            val = values[i]
            setattr(self,attr,val)
        # disallow negative share
        if self.queue_share != None and self.queue_share < 0:
            self.queue_share = 0
        # convert variables string to a map of bind-variables 
        tmpMap = {}        
        try:
            for item in self.variables.split(','):
                # look for key: value
                item = item.strip()
                items = item.split(':')
                if len(items) != 2:
                    continue
                # add
                tmpMap[':%s' % items[0]] = items[1]
        except:
            pass
        # assign map
        self.variables = tmpMap
        # make a python statement for eval
        if self.criteria in ['',None]:
            # catch all
            self.evalString = 'True'
        else:
            tmpEvalStr = self.criteria
            # replace IN/OR/AND to in/or/and
            tmpEvalStr = re.sub(' IN ', ' in ', tmpEvalStr,re.I)
            tmpEvalStr = re.sub(' OR ', ' or ', tmpEvalStr,re.I)
            tmpEvalStr = re.sub(' AND ',' and ',tmpEvalStr,re.I)
            # replace = to ==
            tmpEvalStr = tmpEvalStr.replace('=','==')
            # replace LIKE
            tmpEvalStr = re.sub('(?P<var>[^ \(]+)\s+LIKE\s+(?P<pat>[^ \(]+)',
                                "re.search(\g<pat>,\g<var>,re.I) != None",
                                tmpEvalStr,re.I)
            # NULL
            tmpEvalStr = re.sub(' IS NULL','==None',tmpEvalStr)
            tmpEvalStr = re.sub(' IS NOT NULL',"!=None",tmpEvalStr)
            # replace NOT to not
            tmpEvalStr = re.sub(' NOT ',' not ',tmpEvalStr,re.I)
            # fomat cases
            for tmpParam in self._paramsForSelection:
                tmpEvalStr = re.sub(tmpParam,tmpParam,tmpEvalStr,re.I)            
            # replace bind-variables
            for tmpKey,tmpVal in self.variables.iteritems():
                if '%' in tmpVal:
                    # wildcard
                    tmpVal = tmpVal.replace('%','.*')
                    tmpVal = "'^%s$'" % tmpVal
                else:
                    # normal variable
                    tmpVal = "'%s'" % tmpVal
                tmpEvalStr = tmpEvalStr.replace(tmpKey,tmpVal)
            # assign    
            self.evalString = tmpEvalStr


    # evaluate in python
    def evaluate(self,paramMap):
        # only active queues are evaluated
        if self.isActive():
            # normal queue
            if self.subQueues == []:
                # expand parameters to local namespace
                for tmpParamKey,tmpParamVal in paramMap.iteritems():
                    if isinstance(tmpParamVal,types.StringType):
                        # add quotes for string
                        exec '%s="%s"' % (tmpParamKey,tmpParamVal)
                    else:
                        exec '%s=%s' % (tmpParamKey,tmpParamVal)
                # add default parameters if missing
                for tmpParam in self._paramsForSelection:
                    if not paramMap.has_key(tmpParam):
                        exec '%s=None' % tmpParam
                # evaluate
                exec "retVar = " + self.evalString
                return self,retVar
            else:
                # loop over sub queues
                for tmpQueue in self.subQueues:
                    retQueue,retVar = tmpQueue.evaluate(paramMap)
                    if retVar == True:
                        return retQueue,retVar
        # return False
        return self,False


    # check if active
    def isActive(self):
        if self.status != 'inactive':
            return True
        return False


    # check if stretchable
    def isStretchable(self):
        if self.stretchable == 1:
            return True
        return False

    
    # check if there is workload
    def hasWorkload(self,queueList):
        # inactive
        if not self.isActive():
            return False
        # normal queue
        if self.subQueues == []:
            if self.queue_id in queueList:
                return True
            else:
                return False
        # partition
        for subQueue in self.subQueues:
            if subQueue.queue_id in queueList:
                return True
        # not found    
        return False


    # get normalized shares
    def getNormalizedShares(self,queueList,offset,normalization):
        # skip inactive
        if not self.isActive():
            return {}
        # normal queue
        if self.subQueues == []:
            # no workload
            if not self.queue_id in queueList:
                return {}
            # no throttle
            if self.queue_share == None:
                return {self.queue_id:None}
            else:
                # normal share
                normalizedShare = (float(self.queue_share) + offset) * normalization
                return {self.queue_id:normalizedShare}
        # partition
        queuesWithWork = []
        queuesWithWorkStretchable = []
        nQueuesNoWorkLoad = 0
        unUsedShare = 0
        totalShare = 0
        for subQueue in self.subQueues:
            # skip inactive
            if not subQueue.isActive():
                continue
            # check if it has workload
            if subQueue.queue_id in queueList:
                # total share
                if subQueue.queue_share != None:
                    totalShare += subQueue.queue_share
                # with workload
                queuesWithWork.append(subQueue)
                # stretchable
                if subQueue.isStretchable():
                    queuesWithWorkStretchable.append(subQueue)
            else:
                # skip no throttle
                if subQueue.queue_share == None:
                    continue
                # no workload
                nQueuesNoWorkLoad += 1
                unUsedShare += subQueue.queue_share
        # calcurate offset
        newOffset = 0
        nStretchable = len(queuesWithWorkStretchable)
        if nStretchable != 0:
            newOffset = float(unUsedShare) / float(nStretchable)
            totalShare += unUsedShare
        # calculate normalization factor
        if totalShare == 0 or self.queue_share == None:
            # use dummy factor
            newNormalization = 1.0
        else:
            newNormalization = float(normalization) * float(self.queue_share+offset) / float(totalShare)
        # get normalized shares for all sub queues
        retMap = {}
        for subQueue in self.subQueues:
            tmpOffset = 0
            if subQueue in queuesWithWorkStretchable:
                tmpOffset = newOffset
            tmpRet = subQueue.getNormalizedShares(queueList,tmpOffset,newNormalization)
            # add
            for tmpRetKey,tmpRetVal in tmpRet.iteritems():
                retMap[tmpRetKey] = tmpRetVal
        # return
        return retMap
    

    # return column names for INSERT
    def columnNames(cls):
        ret = ""
        for attr in cls._attributes:
            if ret != "":
                ret += ','
            ret += attr
        return ret
    columnNames = classmethod(columnNames)
    
