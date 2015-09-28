import copy
import random

import JediCoreUtils

from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])

# class for input
class InputChunk:
    
    def __str__(self):
        sb = []
        for key in self.__dict__:
            sb.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))
    
        return ', '.join(sb)
    
    def __repr__(self):
        return self.__str__() 

    # constructor
    def __init__(self,taskSpec,masterDataset=None,secondaryDatasetList=[], ramCount=0):
        # task spec
        self.taskSpec = taskSpec
        # the list of secondary datasets
        if secondaryDatasetList == None:
            self.secondaryDatasetList = []
        else:
            self.secondaryDatasetList = secondaryDatasetList
        # the list of site candidates
        self.siteCandidates = {}
        # the name of master index
        self.masterIndexName = None
        # dataset mapping including indexes of files/events
        self.datasetMap = {}
        # the master dataset
        self.masterDataset = None
        self.addMasterDS(masterDataset)
        # the list of secondary datasets
        self.secondaryDatasetList = []
        for secondaryDS in secondaryDatasetList:
            self.addSecondaryDS(secondaryDS)
        # read in a block   
        self.readBlock = None
        # merging
        self.isMerging = False
        # use scout
        self.useScoutFlag = None
        #memory requirements for the inputChunk
        self.ramCount = ramCount
        #flag to set if inputchunk is empty
        self.isEmpty = False



    # add master dataset
    def addMasterDS(self,masterDataset):
        if masterDataset != None:
            self.masterDataset = masterDataset
            self.masterIndexName = self.masterDataset.datasetID
            self.datasetMap[self.masterDataset.datasetID] = {'used':0,'datasetSpec':masterDataset}



    # add secondary dataset
    def addSecondaryDS(self,secondaryDataset):
        if not secondaryDataset in self.secondaryDatasetList:
            self.secondaryDatasetList.append(secondaryDataset)
            self.datasetMap[secondaryDataset.datasetID] = {'used':0,'datasetSpec':secondaryDataset}



    # return list of datasets
    def getDatasets(self,includePseudo=False):
        dataList = []
        if self.masterDataset != None:
            dataList.append(self.masterDataset)
        dataList += self.secondaryDatasetList
        # ignore pseudo datasets
        if not includePseudo:
            newDataList = []
            for datasetSpec in dataList:
                if not datasetSpec.isPseudo():
                    newDataList.append(datasetSpec)
            dataList = newDataList        
        return dataList



    # return dataset with datasetID
    def getDatasetWithID(self,datasetID):
        if self.datasetMap.has_key(datasetID):
            return self.datasetMap[datasetID]['datasetSpec'] 
        return None



    # return dataset with datasetName
    def getDatasetWithName(self,datasetName):
        for tmpDatasetID,tmpDatasetVal in self.datasetMap.iteritems():
            if tmpDatasetVal['datasetSpec'].datasetName == datasetName:
                return tmpDatasetVal['datasetSpec']
        return None



    # reset used counters
    def resetUsedCounters(self):
        for tmpKey,tmpVal in self.datasetMap.iteritems():
            tmpVal['used'] = 0



    # add site candidates
    def addSiteCandidate(self,siteCandidateSpec):
        self.siteCandidates[siteCandidateSpec.siteName] = siteCandidateSpec
        return



    # get one site candidate randomly
    def getOneSiteCandidate(self,nSubChunks=0,ngSites=None):
        retSiteCandidate = None
        if ngSites == None:
            ngSites = []
        # get total weight
        totalWeight = 0
        weightList  = []
        siteCandidateList = self.siteCandidates.values()
        newSiteCandidateList = []
        for siteCandidate in siteCandidateList:
            # remove NG sites
            if siteCandidate.siteName in ngSites:
                continue
            totalWeight += siteCandidate.weight
            newSiteCandidateList.append(siteCandidate)
        siteCandidateList = newSiteCandidateList
        # empty
        if siteCandidateList == []:
            return None
        # get random number
        rNumber = random.random() * totalWeight
        for siteCandidate in siteCandidateList:
            rNumber -= siteCandidate.weight
            if rNumber <= 0:
                retSiteCandidate = siteCandidate
                break
        # return something as a protection against precision of float
        if retSiteCandidate == None:
            retSiteCandidate = random.choice(siteCandidateList)
        # modify weight
        try:
            if retSiteCandidate.nQueuedJobs != None and retSiteCandidate.nAssignedJobs != None:
                oldNumQueued = retSiteCandidate.nQueuedJobs
                retSiteCandidate.nQueuedJobs += nSubChunks
                newNumQueued = retSiteCandidate.nQueuedJobs
                oldNumAssigned = retSiteCandidate.nAssignedJobs
                retSiteCandidate.nAssignedJobs += nSubChunks
                newNumAssigned = retSiteCandidate.nAssignedJobs
                siteCandidate.weight = siteCandidate.weight * float(oldNumQueued+1) / float(newNumQueued+1)
        except:
            pass
        return retSiteCandidate



    # get sites for parallel execution
    def getParallelSites(self,nSites,nSubChunks,usedSites):
        newSiteCandidate = self.getOneSiteCandidate(nSubChunks,usedSites)
        if newSiteCandidate != None:
            usedSites.append(newSiteCandidate.siteName)
            if nSites > len(usedSites):
                return self.getParallelSites(nSites,nSubChunks,usedSites)
        return ','.join(usedSites)



    # check if unused files/events remain
    def checkUnused(self):
        # master is undefined
        if self.masterIndexName == None:
            return False
        indexVal = self.datasetMap[self.masterIndexName]
        return indexVal['used'] < len(indexVal['datasetSpec'].Files)



    # get maximum size of atomic subchunk
    def getMaxAtomSize(self,effectiveSize=False,getNumEvents=False):
        # number of files per job if defined
        nFilesPerJob = self.taskSpec.getNumFilesPerJob()
        nEventsPerJob = None
        if nFilesPerJob == None:
            # number of events per job
            nEventsPerJob = self.taskSpec.getNumEventsPerJob()
            if nEventsPerJob == None:
                nFilesPerJob = 1
        # grouping with boundaryID
        useBoundary = self.taskSpec.useGroupWithBoundaryID()    
        # LB
        respectLB = self.taskSpec.respectLumiblock()
        maxAtomSize = 0    
        while True:
            # get one subchunk
            subChunk = self.getSubChunk(None,nFilesPerJob=nFilesPerJob,
                                        nEventsPerJob=nEventsPerJob,
                                        useBoundary=useBoundary,
                                        respectLB=respectLB)
            if subChunk == None:
                break
            # get size
            tmpAtomSize = 0
            for tmpDatasetSpec,tmpFileSpecList in subChunk:
                if (effectiveSize or getNumEvents) and not tmpDatasetSpec.isMaster():
                    continue
                for tmpFileSpec in tmpFileSpecList:
                    if effectiveSize:
                        tmpAtomSize += JediCoreUtils.getEffectiveFileSize(tmpFileSpec.fsize,tmpFileSpec.startEvent,
                                                                          tmpFileSpec.endEvent,tmpFileSpec.nEvents)
                    elif getNumEvents:
                        tmpAtomSize += tmpFileSpec.getEffectiveNumEvents()
                    else:
                        tmpAtomSize += tmpFileSpec.fsize
            if maxAtomSize < tmpAtomSize:
                maxAtomSize = tmpAtomSize
        # reset counters
        self.resetUsedCounters()
        # return
        return maxAtomSize



    # use scout
    def useScout(self):
        if self.masterDataset != None and self.useScoutFlag != None:
            return self.useScoutFlag
        if self.masterDataset != None and \
                self.masterDataset.nFiles > self.masterDataset.nFilesToBeUsed:
            return True
        return False



    # set use scout
    def setUseScout(self,useScoutFlag):
        self.useScoutFlag = useScoutFlag



    # get preassigned site
    def getPreassignedSite(self):
        if self.masterDataset != None:
            return self.masterDataset.site
        return None



    # get max output size
    def getOutSize(self,outSizeMap):
        values = outSizeMap.values()
        values.sort()
        try:
            return values[-1]
        except:
            return 0



    # get subchunk with a selection criteria
    def getSubChunk(self,siteName,maxNumFiles=None,maxSize=None,
                    sizeGradients=0,sizeIntercepts=0,
                    nFilesPerJob=None,multiplicand=1,
                    walltimeGradient=0,maxWalltime=0,
                    nEventsPerJob=None,useBoundary=None,
                    sizeGradientsPerInSize=None,
                    maxOutSize=None,
                    coreCount=1,
                    respectLB=False,
                    corePower=None,
                    tmpLog=None):
        # check if there are unused files/events
        if not self.checkUnused():
            return None
        # set default max number of files
        if maxNumFiles == None:
            # 20 files at most by default
            maxNumFiles = 20
        # set default max size    
        if maxSize == None and nFilesPerJob == None and nEventsPerJob == None:
            # 20 GB at most by default
            maxSize = 20 * 1024 * 1024 * 1024
        # set default output size 2G + 500MB (safety merging)
        minOutSize = 2500 * 1024 * 1024
        # set default max number of events
        maxNumEvents = None
        # ignore negative walltime gradient
        if walltimeGradient < 0:
            walltimeGradient = 0
        # overwrite parameters when nFiles/EventsPerJob is used
        if nFilesPerJob != None:
            maxNumFiles  = nFilesPerJob
            multiplicand = nFilesPerJob
        if nEventsPerJob != None:
            maxNumEvents = nEventsPerJob
        # split with boundayID
        splitWithBoundaryID = False    
        if useBoundary != None:
            splitWithBoundaryID = True
            if useBoundary['inSplit'] == 2:
                # unset max values to split only with boundaryID 
                maxNumFiles = None
                maxSize = None
                maxWalltime = 0
                maxNumEvents = None
                multiplicand = 1
        # get site when splitting per site
        if siteName != None:
            siteCandidate = self.siteCandidates[siteName]
        # start splitting
        inputNumFiles  = 0
        inputNumEvents = 0 
        fileSize       = 0
        firstLoop      = True
        firstMaster    = True
        inputFileMap   = {}
        expWalltime    = 0
        nextStartEvent = None
        boundaryID     = None
        newBoundaryID  = False
        nSecFilesMap   = {}
        numMaster      = 0
        outSizeMap     = {}
        lumiBlockNr    = None
        newLumiBlockNr = False
        while (maxNumFiles == None or inputNumFiles <= maxNumFiles) \
                and (maxSize == None or (maxSize != None and fileSize <= maxSize)) \
                and (maxWalltime <= 0 or expWalltime <= maxWalltime) \
                and (maxNumEvents == None or (maxNumEvents != None and inputNumEvents <= maxNumEvents)) \
                and (maxOutSize == None or self.getOutSize(outSizeMap) <= maxOutSize):
            # get one file (or one file group for MP) from master
            datasetUsage = self.datasetMap[self.masterDataset.datasetID]
            if not self.masterDataset.datasetID in outSizeMap:
                outSizeMap[self.masterDataset.datasetID] = 0
            boundaryIDs = set()
            for tmpFileSpec in self.masterDataset.Files[datasetUsage['used']:datasetUsage['used']+multiplicand]:
                # check start event to keep continuity
                if maxNumEvents != None and tmpFileSpec.startEvent != None:
                    if nextStartEvent != None and nextStartEvent != tmpFileSpec.startEvent:
                        break
                # check boundaryID
                if splitWithBoundaryID and boundaryID != None and boundaryID != tmpFileSpec.boundaryID \
                        and useBoundary['inSplit'] != 3:
                    newBoundaryID = True
                    break
                # check LB
                if respectLB and lumiBlockNr != None and lumiBlockNr != tmpFileSpec.lumiBlockNr:
                    newLumiBlockNr = True
                    break
                if not inputFileMap.has_key(self.masterDataset.datasetID):
                    inputFileMap[self.masterDataset.datasetID] = []
                inputFileMap[self.masterDataset.datasetID].append(tmpFileSpec)
                datasetUsage['used'] += 1
                numMaster += 1
                # get effective file size
                effectiveFsize = JediCoreUtils.getEffectiveFileSize(tmpFileSpec.fsize,tmpFileSpec.startEvent,
                                                                    tmpFileSpec.endEvent,tmpFileSpec.nEvents)
                # get num of events
                effectiveNumEvents = tmpFileSpec.getEffectiveNumEvents()
                # sum
                inputNumFiles += 1
                if self.taskSpec.outputScaleWithEvents():
                    fileSize += long(tmpFileSpec.fsize + sizeGradients * effectiveNumEvents)
                    outSizeMap[self.masterDataset.datasetID] += long(sizeGradients * effectiveNumEvents)
                else:
                    fileSize += long(tmpFileSpec.fsize + sizeGradients * effectiveFsize)
                    outSizeMap[self.masterDataset.datasetID] += long(sizeGradients * effectiveFsize)
                if sizeGradientsPerInSize != None:
                    fileSize += long(effectiveFsize * sizeGradientsPerInSize)
                    outSizeMap[self.masterDataset.datasetID] += long(effectiveFsize * sizeGradientsPerInSize)
                # sum offset only for the first master
                if firstMaster:
                    fileSize += sizeIntercepts
                # walltime
                if self.taskSpec.useHS06():
                    if firstMaster:
                        expWalltime += self.taskSpec.baseWalltime
                    tmpExpWalltime = walltimeGradient * effectiveNumEvents / float(coreCount)
                    if not corePower in [None,0]:
                        tmpExpWalltime /= corePower
                    tmpExpWalltime /= float(self.taskSpec.cpuEfficiency)/100.0
                    expWalltime += long(tmpExpWalltime)
                else:
                    expWalltime += long(walltimeGradient * effectiveFsize / float(coreCount))
                # the number of events
                if maxNumEvents != None and tmpFileSpec.startEvent != None and tmpFileSpec.endEvent != None:
                    inputNumEvents += (tmpFileSpec.endEvent - tmpFileSpec.startEvent + 1)
                    # set next start event
                    nextStartEvent = tmpFileSpec.endEvent + 1
                    if nextStartEvent == tmpFileSpec.nEvents:
                        nextStartEvent = 0
                # boundaryID
                if splitWithBoundaryID:
                    boundaryID = tmpFileSpec.boundaryID
                    if not boundaryID in boundaryIDs:
                        boundaryIDs.add(boundaryID)
                # LB
                if respectLB:
                    lumiBlockNr = tmpFileSpec.lumiBlockNr
                firstMaster = False
            # get files from secondaries
            for datasetSpec in self.secondaryDatasetList:
                if not datasetSpec.datasetID in outSizeMap:
                    outSizeMap[datasetSpec.datasetID] = 0
                if datasetSpec.isNoSplit():
                    # every job uses dataset without splitting
                    if firstLoop:
                        datasetUsage = self.datasetMap[datasetSpec.datasetID]
                        for tmpFileSpec in datasetSpec.Files:
                            if not inputFileMap.has_key(datasetSpec.datasetID):
                                inputFileMap[datasetSpec.datasetID] = []
                            inputFileMap[datasetSpec.datasetID].append(tmpFileSpec)
                            # sum
                            fileSize += tmpFileSpec.fsize
                            if sizeGradientsPerInSize != None:
                                fileSize += (tmpFileSpec.fsize * sizeGradientsPerInSize)
                                outSizeMap[datasetSpec.datasetID] += (tmpFileSpec.fsize * sizeGradientsPerInSize)
                            datasetUsage['used'] += 1
                else:
                    if not nSecFilesMap.has_key(datasetSpec.datasetID):
                        nSecFilesMap[datasetSpec.datasetID] = 0
                    # get number of files to be used for the secondary
                    nSecondary = datasetSpec.getNumFilesPerJob()
                    if nSecondary != None and firstLoop == False:
                        # read files only in the first bunch when number of files per job is specified
                        continue
                    if nSecondary == None:
                        nSecondary = datasetSpec.getNumMultByRatio(numMaster) - nSecFilesMap[datasetSpec.datasetID]
                        if splitWithBoundaryID and useBoundary['inSplit'] != 3:
                            # set large number to get all associated secondary files
                            nSecondary = 10000
                    datasetUsage = self.datasetMap[datasetSpec.datasetID]
                    # reset nUsed
                    if datasetSpec.isReusable() and datasetUsage['used']+nSecondary > len(datasetSpec.Files):
                        datasetUsage['used'] = 0
                    for tmpFileSpec in datasetSpec.Files[datasetUsage['used']:datasetUsage['used']+nSecondary]:
                        # check boundaryID
                        if splitWithBoundaryID and boundaryID != None and \
                                not (boundaryID == tmpFileSpec.boundaryID or tmpFileSpec.boundaryID in boundaryIDs):
                            break
                        if not inputFileMap.has_key(datasetSpec.datasetID):
                            inputFileMap[datasetSpec.datasetID] = []
                        inputFileMap[datasetSpec.datasetID].append(tmpFileSpec)
                        # sum
                        fileSize += tmpFileSpec.fsize
                        if sizeGradientsPerInSize != None:
                            fileSize += (tmpFileSpec.fsize * sizeGradientsPerInSize)
                            outSizeMap[datasetSpec.datasetID] += (tmpFileSpec.fsize * sizeGradientsPerInSize)
                        datasetUsage['used'] += 1
                        nSecFilesMap[datasetSpec.datasetID] += 1
            # unset first loop flag
            firstLoop = False
            # check if there are unused files/evets 
            if not self.checkUnused():
                break
            # break if nFilesPerJob is used
            if nFilesPerJob != None:
                break
            # boundayID is changed
            if newBoundaryID:
                break
            # LB is changed
            if newLumiBlockNr:
                break
            # check master in the next loop
            datasetUsage = self.datasetMap[self.masterDataset.datasetID]
            newInputNumFiles  = inputNumFiles
            newInputNumEvents = inputNumEvents
            newFileSize       = fileSize
            newExpWalltime    = expWalltime
            newNextStartEvent = nextStartEvent
            newNumMaster      = numMaster
            terminateFlag     = False
            newOutSizeMap     = copy.copy(outSizeMap)
            newBoundaryIDs    = set()
            if not self.masterDataset.datasetID in newOutSizeMap:
                newOutSizeMap[self.masterDataset.datasetID] = 0
            for tmpFileSpec in self.masterDataset.Files[datasetUsage['used']:datasetUsage['used']+multiplicand]:
                # check continuity of event
                if maxNumEvents != None and tmpFileSpec.startEvent != None and tmpFileSpec.endEvent != None:
                    newInputNumEvents += (tmpFileSpec.endEvent - tmpFileSpec.startEvent + 1)
                    # continuity of event is broken
                    if newNextStartEvent != None and newNextStartEvent != tmpFileSpec.startEvent:
                        # no files in the next loop
                        if newInputNumFiles == 0: 
                            terminateFlag = True
                        break
                    newNextStartEvent = tmpFileSpec.endEvent + 1
                # check boundary
                if splitWithBoundaryID and boundaryID != None and boundaryID != tmpFileSpec.boundaryID \
                        and useBoundary['inSplit'] != 3:
                    # no files in the next loop
                    if newInputNumFiles == 0:
                        terminateFlag = True
                    break
                # check LB
                if respectLB and lumiBlockNr != None and lumiBlockNr != tmpFileSpec.lumiBlockNr:
                    # no files in the next loop
                    if newInputNumFiles == 0:
                        terminateFlag = True
                    break
                # get effective file size
                effectiveFsize = JediCoreUtils.getEffectiveFileSize(tmpFileSpec.fsize,tmpFileSpec.startEvent,
                                                                    tmpFileSpec.endEvent,tmpFileSpec.nEvents)
                # get num of events
                effectiveNumEvents = tmpFileSpec.getEffectiveNumEvents()
                newInputNumFiles += 1
                newNumMaster += 1
                if self.taskSpec.outputScaleWithEvents():
                    newFileSize += long(tmpFileSpec.fsize + sizeGradients * effectiveNumEvents)
                    newOutSizeMap[self.masterDataset.datasetID] += long(sizeGradients * effectiveNumEvents)
                else:
                    newFileSize += long(tmpFileSpec.fsize + sizeGradients * effectiveFsize)
                    newOutSizeMap[self.masterDataset.datasetID] += long(sizeGradients * effectiveFsize)
                if sizeGradientsPerInSize != None:
                    newFileSize += long(effectiveFsize * sizeGradientsPerInSize)
                    newOutSizeMap[self.masterDataset.datasetID] += long(effectiveFsize * sizeGradientsPerInSize)
                if self.taskSpec.useHS06():
                    tmpExpWalltime = walltimeGradient * effectiveNumEvents / float(coreCount)
                    if not corePower in [None,0]:
                        tmpExpWalltime /= corePower
                    tmpExpWalltime /= float(self.taskSpec.cpuEfficiency)/100.0
                    newExpWalltime += long(tmpExpWalltime)
                else:
                    newExpWalltime += long(walltimeGradient * effectiveFsize / float(coreCount))
                # boundaryID
                if splitWithBoundaryID:
                    newBoundaryIDs.add(tmpFileSpec.boundaryID)
            # check secondaries
            for datasetSpec in self.secondaryDatasetList:
                if not datasetSpec.datasetID in newOutSizeMap:
                    newOutSizeMap[datasetSpec.datasetID] = 0
                if not datasetSpec.isNoSplit() and datasetSpec.getNumFilesPerJob() == None:
                    # check boundaryID
                    if splitWithBoundaryID and boundaryID != None and boundaryID != tmpFileSpec.boundaryID \
                            and useBoundary['inSplit'] != 3:
                        break
                    newNumSecondary = datasetSpec.getNumMultByRatio(newNumMaster) - nSecFilesMap[datasetSpec.datasetID]
                    datasetUsage = self.datasetMap[datasetSpec.datasetID]
                    for tmpFileSpec in datasetSpec.Files[datasetUsage['used']:datasetUsage['used']+nSecondary]:
                        # check boundaryID
                        if splitWithBoundaryID and boundaryID != None and boundaryID != tmpFileSpec.boundaryID \
                                and not tmpFileSpec.boundaryID in boundaryIDs and not tmpFileSpec.boundaryID in newBoundaryIDs:
                            break
                        newFileSize += tmpFileSpec.fsize
                        if sizeGradientsPerInSize != None:
                            newFileSize += (tmpFileSpec.fsize * sizeGradientsPerInSize)
                            newOutSizeMap[datasetSpec.datasetID] += (tmpFileSpec.fsize * sizeGradientsPerInSize)
            # termination            
            if terminateFlag:
                break
            # check
            newOutSize = self.getOutSize(newOutSizeMap)
            if (maxNumFiles != None and newInputNumFiles > maxNumFiles) \
                    or (maxSize != None and newFileSize > maxSize) \
                    or (maxSize != None and newOutSize < minOutSize and maxSize-minOutSize < newFileSize-newOutSize) \
                    or (maxWalltime > 0 and newExpWalltime > maxWalltime) \
                    or (maxNumEvents != None and newInputNumEvents > maxNumEvents) \
                    or (maxOutSize != None and self.getOutSize(newOutSizeMap) > maxOutSize):
                break
        # reset nUsed for repeated datasets
        for tmpDatasetID,datasetUsage in self.datasetMap.iteritems():
            tmpDatasetSpec = datasetUsage['datasetSpec']
            if tmpDatasetSpec.isRepeated():
                datasetUsage['used'] %= len(tmpDatasetSpec.Files)
        # make copy to return
        returnList = []
        for tmpDatasetID,inputFileList in inputFileMap.iteritems():
            tmpRetList = []
            for tmpFileSpec in inputFileList:
                # split par site or get atomic subchunk
                if siteName != None:
                    # make copy to individually set locality
                    newFileSpec = copy.copy(tmpFileSpec)
                    # set locality
                    newFileSpec.locality = siteCandidate.getFileLocality(tmpFileSpec)
                    if newFileSpec.locality == 'remote':
                        newFileSpec.sourceName = siteCandidate.remoteSource
                    # append
                    tmpRetList.append(newFileSpec)
                else:
                    # getting atomic subchunk
                    tmpRetList.append(tmpFileSpec)
            # add to return map    
            tmpDatasetSpec = self.getDatasetWithID(tmpDatasetID)    
            returnList.append((tmpDatasetSpec,tmpRetList))
        # return
        return returnList



    # check if master is mutable
    def isMutableMaster(self):
        if self.masterDataset != None and self.masterDataset.state == 'mutable':
            return True
        return False
