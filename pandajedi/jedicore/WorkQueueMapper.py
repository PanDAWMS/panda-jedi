"""
mapper to map task/job to a work queue

"""

from WorkQueue import WorkQueue

class WorkQueueMapper:

    # constructor
    def __init__(self):
        self.workQueueMap = {}
        self.workQueueGlobalMap = {}


    # get SQL query
    def getSqlQuery(self):
        sql = "SELECT %s FROM ATLAS_PANDA.JEDI_Work_Queue" % WorkQueue.columnNames()
        return sql

        
    # make map    
    def makeMap(self,records):
        subQueuesMap = {}
        # loop over all records
        for record in records:
            # pack
            workQueue = WorkQueue()
            workQueue.pack(record)
            # add to global map
            self.workQueueGlobalMap[workQueue.queue_id] = workQueue
            # skip inactive queues
            if not workQueue.isActive():
                continue
            # normal queue or sub queue
            if workQueue.partitionID in [None,-1]:
                targetMap = self.workQueueMap
            else:
                targetMap = subQueuesMap
            # add VO
            if not targetMap.has_key(workQueue.VO):
                targetMap[workQueue.VO] = {}
            # add type
            if not targetMap[workQueue.VO].has_key(workQueue.queue_type):
                targetMap[workQueue.VO][workQueue.queue_type] = []
            # add to list
            targetMap[workQueue.VO][workQueue.queue_type].append(workQueue)
        # add sub queues
        for VO,typeQueueMap in subQueuesMap.iteritems():
            for queue_type,subQueues in typeQueueMap.iteritems():
                for subQueue in subQueues:
                    try:
                        # look for partition
                        for normalQueue in self.workQueueMap[VO][queue_type]:
                            if normalQueue.queue_id == subQueue.partitionID:
                                normalQueue.addSubQueue(subQueue)
                                break
                    except:
                        pass
        # sort the queue list by order
        for VO,typeQueueMap in self.workQueueMap.iteritems():
            for queueType,queueMap in typeQueueMap.iteritems():
                # make ordered map
                orderedMap = {}
                for workQueue in queueMap:
                    if not orderedMap.has_key(workQueue.queue_order):
                        orderedMap[workQueue.queue_order] = []
                    # append
                    orderedMap[workQueue.queue_order].append(workQueue)
                # make orderd list
                orderList = orderedMap.keys()
                orderList.sort()
                newList = []
                for orderVal in orderList:
                    newList += orderedMap[orderVal]
                # set new list    
                self.workQueueMap[VO][queueType] = newList    
        # return
        return
    

    # dump
    def dump(self):
        dumpStr = 'WorkQueue mapping\n'
        for VO,typeQueueMap in self.workQueueMap.iteritems():
            dumpStr += '  VO=%s\n' % VO           
            for queueType,queueMap in typeQueueMap.iteritems():
                dumpStr += '    type=%s\n' % queueType
                for workQueue in queueMap:
                    dumpStr += '      %s\n' % workQueue.dump()
        # return
        return dumpStr


    # get queue with selection parameters
    def getQueueWithSelParams(self,vo,queueType,**paramMap):
        retStr = ''
        if not self.workQueueMap.has_key(vo):
            # check VO 
            retStr = 'queues for vo=%s are undefined' % vo
        elif not self.workQueueMap[vo].has_key(queueType):
            # check type
            retStr = 'queues for type=%s are undefined in vo=%s' % (queueType,vo)
        else:
            # check queues in order
            for workQueue in self.workQueueMap[vo][queueType]:
                # evaluate
                try:
                    retQueue,result = workQueue.evaluate(paramMap)
                    if result == True:
                        return retQueue,retStr
                except:
                    retStr += '%s,' % workQueue.queue_name
            retStr = retStr[:-1]
            if retStr != '':
                newRetStr = 'eval with VO=%s queuetype=%s ' % (vo,queueType)
                for tmpParamKey,tmpParamVal in paramMap.iteritems():
                    newRetStr += '%s=%s ' % (tmpParamKey,tmpParamVal)
                newRetStr += 'failed for %s' % retStr
                retStr = newRetStr
        # no matching
        return None,retStr


    # get queue with ID
    def getQueueWithID(self,queueID):
        if self.workQueueGlobalMap.has_key(queueID):
            return self.workQueueGlobalMap[queueID]
        # not found
        return None


    # get queue list with VO and type
    def getQueueListWithVoType(self,vo,queueType):
        if self.workQueueMap.has_key(vo) and self.workQueueMap[vo].has_key(queueType):
            return self.workQueueMap[vo][queueType]
        # not found
        return []


    # get shares
    def getShares(self,vo,queueType,queueList):
        # get queue list with VO and type
        queuesWithVoType = self.getQueueListWithVoType(vo,queueType)
        # get list of queues with workload
        sharesMap = {}
        queuesWithWork = []
        queuesWithWorkStretchable = []
        nQueuesNoWorkLoad = 0
        unUsedShare = 0
        for workQueue in self.getQueueListWithVoType(vo,queueType):
            # check if it has workload
            if workQueue.hasWorkload(queueList):
                queuesWithWork.append(workQueue)
                # stretchable
                if workQueue.isStretchable():
                    queuesWithWorkStretchable.append(workQueue)
            else:
                # no throttle
                if workQueue.queue_share == None:
                    continue
                # no workload
                nQueuesNoWorkLoad += 1
                unUsedShare += workQueue.queue_share
        # calcurate offset
        offset = 0
        nStretchable = len(queuesWithWorkStretchable)
        if nStretchable != 0:
            offset = float(unUsedShare) / float(nStretchable)
        # get normalized shares for all queues
        retMap = {}
        for workQueue in queuesWithWork:
            tmpOffset = 0
            if workQueue in queuesWithWorkStretchable:
                tmpOffset = offset
            tmpRet = workQueue.getNormalizedShares(queueList,tmpOffset,1.0)
            # add
            for tmpRetKey,tmpRetVal in tmpRet.iteritems():
                retMap[tmpRetKey] = tmpRetVal
        # return
        return retMap
        
