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
        # the list of site candidates for jumbo jobs
        self.siteCandidatesJumbo = {}
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
        # memory requirements for the inputChunk
        self.ramCount = ramCount
        # flag to set if inputchunk is empty
        self.isEmpty = False
        # flag to use jumbo jobs
        self.useJumbo = None



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



    # add site candidates
    def addSiteCandidateForJumbo(self,siteCandidateSpec):
        self.siteCandidatesJumbo[siteCandidateSpec.siteName] = siteCandidateSpec
        return



    # get one site candidate randomly
    def getOneSiteCandidate(self,nSubChunks=0,ngSites=None):
        retSiteCandidate = None
        if ngSites == None:
            ngSites = []
        ngSites = copy.copy(ngSites)
        # skip sites for distributed datasets
        for tmpDatasetSpec in self.getDatasets():
            if tmpDatasetSpec.isDistributed():
                datasetUsage = self.datasetMap[tmpDatasetSpec.datasetID]
                if len(tmpDatasetSpec.Files) > datasetUsage['used']:
                    tmpFileSpec = tmpDatasetSpec.Files[datasetUsage['used']]
                    for siteCandidate in self.siteCandidates.values():
                        # skip if the first file is unavalble at the site
                        if not siteCandidate.isAvailableFile(tmpFileSpec):
                            ngSites.append(siteCandidate.siteName)
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



    # get one site for jumbo jobs
    def getOneSiteCandidateForJumbo(self,ngSites):
        # get total weight
        totalWeight = 0
        weightList  = []
        siteCandidateList = self.siteCandidatesJumbo.values()
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
        return retSiteCandidate
        


    # check if unused files/events remain
    def checkUnused(self):
        # master is undefined
        if self.masterIndexName == None:
            return False
        indexVal = self.datasetMap[self.masterIndexName]
        return indexVal['used'] < len(indexVal['datasetSpec'].Files)



    # get master used index 
    def getMasterUsedIndex(self):
        # master is undefined
        if self.masterIndexName == None:
            return 0
        indexVal = self.datasetMap[self.masterIndexName]
        return indexVal['used']



    # check if secondary datasets use event ratios
    def useEventRatioForSec(self):
        for datasetSpec in self.secondaryDatasetList:
            if datasetSpec.getEventRatio() != None:
                return True
        return False



    # get maximum size of atomic subchunk
    def getMaxAtomSize(self,effectiveSize=False,getNumEvents=False):
        # number of files per job if defined
        if not self.isMerging:
            nFilesPerJob = self.taskSpec.getNumFilesPerJob()
        else:
            nFilesPerJob = self.taskSpec.getNumFilesPerMergeJob()
        nEventsPerJob = None
        if nFilesPerJob == None:
            # number of events per job
            if not self.isMerging:
                nEventsPerJob = self.taskSpec.getNumEventsPerJob()
            else:
                nEventsPerJob = self.taskSpec.getNumEventsPerMergeJob()
            if nEventsPerJob == None:
                nFilesPerJob = 1
        # grouping with boundaryID
        useBoundary = self.taskSpec.useGroupWithBoundaryID()    
        # LB
        respectLB = self.taskSpec.respectLumiblock()
        maxAtomSize = 0    
        while True:
            if not self.isMerging:
                maxNumFiles = self.taskSpec.getMaxNumFilesPerJob()
            else:
                maxNumFiles = self.taskSpec.getMaxNumFilesPerMergeJob()
            # get one subchunk
            subChunk = self.getSubChunk(None,nFilesPerJob=nFilesPerJob,
                                        nEventsPerJob=nEventsPerJob,
                                        useBoundary=useBoundary,
                                        respectLB=respectLB,
                                        maxNumFiles=maxNumFiles)
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
                    dynNumEvents=False,
                    maxNumEventRanges=None,
                    multiplicity=None,
                    splitByFields=None, 
                    tmpLog=None):
        # check if there are unused files/events
        if not self.checkUnused():
            return None
        # protection against unreasonable values
        if nFilesPerJob == 0:
            nFilesPerJob = None
        if nEventsPerJob == 0:
            nEventsPerJob = None
        # set default max number of files
        if maxNumFiles == None:
            # 20 files at most by default
            maxNumFiles = 20
        # set default max number of event ranges
        if maxNumEventRanges == None:
            maxNumEventRanges = 20
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
        if nFilesPerJob != None and not dynNumEvents:
            maxNumFiles  = nFilesPerJob
            if not respectLB:
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
        # use event ratios
        useEventRatio = self.useEventRatioForSec()
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
        eventJump      = False
        nSecFilesMap   = {}
        nSecEventsMap  = {}
        numMaster      = 0
        outSizeMap     = {}
        lumiBlockNr    = None
        newLumiBlockNr = False
        siteAvailable  = True
        inputFileSet   = set()
        fieldStr       = None
        while (maxNumFiles == None or (not dynNumEvents and inputNumFiles <= maxNumFiles) or \
                   (dynNumEvents and len(inputFileSet) <= maxNumFiles and inputNumFiles <= maxNumEventRanges)) \
                and (maxSize == None or (maxSize != None and fileSize <= maxSize)) \
                and (maxWalltime <= 0 or expWalltime <= maxWalltime) \
                and (maxNumEvents == None or (maxNumEvents != None and inputNumEvents <= maxNumEvents)) \
                and (maxOutSize == None or self.getOutSize(outSizeMap) <= maxOutSize):
            # get one file (or one file group for MP) from master
            datasetUsage = self.datasetMap[self.masterDataset.datasetID]
            if not self.masterDataset.datasetID in outSizeMap:
                outSizeMap[self.masterDataset.datasetID] = 0
            boundaryIDs = set()
            primaryHasEvents = False
            for tmpFileSpec in self.masterDataset.Files[datasetUsage['used']:datasetUsage['used']+multiplicand]:
                # check start event to keep continuity
                if (maxNumEvents != None or dynNumEvents) and tmpFileSpec.startEvent != None:
                    if nextStartEvent != None and nextStartEvent != tmpFileSpec.startEvent:
                        eventJump = True
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
                # check field
                if splitByFields != None:
                    tmpFieldStr = tmpFileSpec.extractFieldsStr(splitByFields)
                    if fieldStr == None:
                        fieldStr = tmpFieldStr
                    elif tmpFieldStr != fieldStr:
                        newBoundaryID = True
                        break
                # check for distributed datasets
                if self.masterDataset.isDistributed() and siteName != None and \
                        not siteCandidate.isAvailableFile(tmpFileSpec):
                    siteAvailable = False
                    break
                if not inputFileMap.has_key(self.masterDataset.datasetID):
                    inputFileMap[self.masterDataset.datasetID] = []
                inputFileMap[self.masterDataset.datasetID].append(tmpFileSpec)
                inputFileSet.add(tmpFileSpec.lfn)
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
                    fileSize += long(sizeGradients * effectiveNumEvents)
                    if not dynNumEvents or tmpFileSpec.lfn not in inputFileSet:
                        fileSize += long(tmpFileSpec.fsize)    
                    outSizeMap[self.masterDataset.datasetID] += long(sizeGradients * effectiveNumEvents)
                else:
                    fileSize += long(sizeGradients * effectiveFsize)
                    if not dynNumEvents or tmpFileSpec.lfn not in inputFileSet:
                        fileSize += long(tmpFileSpec.fsize)
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
                    if self.taskSpec.cpuEfficiency == 0:
                        tmpExpWalltime = 0
                    else:
                        tmpExpWalltime /= float(self.taskSpec.cpuEfficiency)/100.0
                    if multiplicity != None:
                        tmpExpWalltime /= float(multiplicity)
                    expWalltime += long(tmpExpWalltime)
                else:
                    tmpExpWalltime = walltimeGradient * effectiveFsize / float(coreCount)
                    if multiplicity != None:
                        tmpExpWalltime /= float(multiplicity)
                    expWalltime += long(tmpExpWalltime)
                # the number of events
                if (maxNumEvents != None or useEventRatio) and tmpFileSpec.startEvent != None and tmpFileSpec.endEvent != None:
                    primaryHasEvents = True
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
            firstSecondary = True
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
                        if (datasetSpec.getEventRatio() != None and inputNumEvents > 0) or (splitWithBoundaryID and useBoundary['inSplit'] != 3):
                            # set large number to get all associated secondary files
                            nSecondary = 10000
                    datasetUsage = self.datasetMap[datasetSpec.datasetID]
                    # reset nUsed
                    if datasetSpec.isReusable() and datasetUsage['used']+nSecondary > len(datasetSpec.Files):
                        datasetUsage['used'] = 0
                    for tmpFileSpec in datasetSpec.Files[datasetUsage['used']:datasetUsage['used']+nSecondary]:
                        # check boundaryID
                        if (splitWithBoundaryID or (useBoundary != None and useBoundary['inSplit'] == 3 and datasetSpec.getRatioToMaster() > 1)) \
                                and boundaryID != None and \
                                not (boundaryID == tmpFileSpec.boundaryID or tmpFileSpec.boundaryID in boundaryIDs):
                            break
                        # check for distributed datasets
                        if datasetSpec.isDistributed() and siteName != None and \
                                not siteCandidate.isAvailableFile(tmpFileSpec):
                            break
                        # check ratio
                        if not datasetSpec.datasetID in nSecEventsMap:
                            nSecEventsMap[datasetSpec.datasetID] = 0
                        if datasetSpec.getEventRatio() != None and inputNumEvents > 0:
                            if float(nSecEventsMap[datasetSpec.datasetID]) / float(inputNumEvents) >= datasetSpec.getEventRatio():
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
                        # the number of events
                        if firstSecondary and maxNumEvents != None and not primaryHasEvents:
                            if tmpFileSpec.startEvent != None and tmpFileSpec.endEvent != None:
                                inputNumEvents += (tmpFileSpec.endEvent - tmpFileSpec.startEvent + 1)
                            elif tmpFileSpec.nEvents != None:
                                inputNumEvents += tmpFileSpec.nEvents
                        if tmpFileSpec.nEvents != None:
                            nSecEventsMap[datasetSpec.datasetID] += tmpFileSpec.nEvents
                    # use only the first secondary
                    firstSecondary = False
            # unset first loop flag
            firstLoop = False
            # check if there are unused files/evets 
            if not self.checkUnused():
                break
            # break if nFilesPerJob is used as multiplicand
            if nFilesPerJob != None and not respectLB:
                break
            # boundayID is changed
            if newBoundaryID:
                break
            # LB is changed
            if newLumiBlockNr:
                break
            # event jump
            if eventJump:
                break
            # distributed files are unavailable
            if not siteAvailable:
                break
            primaryHasEvents = False
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
            newInputFileSet   = copy.copy(inputFileSet)
            if not self.masterDataset.datasetID in newOutSizeMap:
                newOutSizeMap[self.masterDataset.datasetID] = 0
            for tmpFileSpec in self.masterDataset.Files[datasetUsage['used']:datasetUsage['used']+multiplicand]:
                # check continuity of event
                if maxNumEvents != None and tmpFileSpec.startEvent != None and tmpFileSpec.endEvent != None:
                    primaryHasEvents = True
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
                # check field
                if splitByFields != None:
                    tmpFieldStr = tmpFileSpec.extractFieldsStr(splitByFields)
                    if tmpFieldStr != fieldStr:
                        # no files in the next loop
                        if newInputNumFiles == 0:
                            terminateFlag = True
                        break
                # check for distributed datasets
                if self.masterDataset.isDistributed() and siteName != None and \
                        not siteCandidate.isAvailableFile(tmpFileSpec):
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
                newInputFileSet.add(tmpFileSpec.lfn)
                if self.taskSpec.outputScaleWithEvents():
                    newFileSize += long(sizeGradients * effectiveNumEvents)
                    if not dynNumEvents or tmpFileSpec.lfn not in inputFileSet:
                        newFileSize += long(tmpFileSpec.fsize)
                    newOutSizeMap[self.masterDataset.datasetID] += long(sizeGradients * effectiveNumEvents)
                else:
                    newFileSize += long(sizeGradients * effectiveFsize)
                    if not dynNumEvents or tmpFileSpec.lfn not in inputFileSet:
                        newFileSize += long(tmpFileSpec.fsize)
                    newOutSizeMap[self.masterDataset.datasetID] += long(sizeGradients * effectiveFsize)
                if sizeGradientsPerInSize != None:
                    newFileSize += long(effectiveFsize * sizeGradientsPerInSize)
                    newOutSizeMap[self.masterDataset.datasetID] += long(effectiveFsize * sizeGradientsPerInSize)
                if self.taskSpec.useHS06():
                    tmpExpWalltime = walltimeGradient * effectiveNumEvents / float(coreCount)
                    if not corePower in [None,0]:
                        tmpExpWalltime /= corePower
                    if self.taskSpec.cpuEfficiency == 0:
                        tmpExpWalltime = 0
                    else:
                        tmpExpWalltime /= float(self.taskSpec.cpuEfficiency)/100.0
                    if multiplicity != None:
                        tmpExpWalltime /= float(multiplicity)
                    newExpWalltime += long(tmpExpWalltime)
                else:
                    tmpExpWalltime = walltimeGradient * effectiveFsize / float(coreCount)
                    if multiplicity != None:
                        tmpExpWalltime /= float(multiplicity)
                    newExpWalltime += long(tmpExpWalltime)
                # boundaryID
                if splitWithBoundaryID:
                    newBoundaryIDs.add(tmpFileSpec.boundaryID)
            # check secondaries
            firstSecondary = True
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
                        # the number of events
                        if firstSecondary and maxNumEvents != None and not primaryHasEvents:
                            if tmpFileSpec.startEvent != None and tmpFileSpec.endEvent != None:
                                newInputNumEvents += (tmpFileSpec.endEvent - tmpFileSpec.startEvent + 1)
                            elif tmpFileSpec.nEvents != None:
                                newInputNumEvents += tmpFileSpec.nEvents
                    firstSecondary = False
            # termination            
            if terminateFlag:
                break
            # check
            newOutSize = self.getOutSize(newOutSizeMap)
            if (maxNumFiles != None and ((not dynNumEvents and newInputNumFiles > maxNumFiles) \
                                             or (dynNumEvents and (len(newInputFileSet) > maxNumFiles or newInputNumFiles > maxNumEventRanges)))) \
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



    # figure out if output will go through express stream
    def isExpress(self):
        if self.taskSpec.processingType == 'urgent' or self.taskSpec.currentPriority > 1000:
            return True

        return False




    # skip unavailable files in distributed datasets
    def skipUnavailableFiles(self):
        # skip files if no candiate have them
        nSkip = 0
        for tmpDatasetSpec in self.getDatasets():
            if tmpDatasetSpec.isDistributed():
                datasetUsage = self.datasetMap[tmpDatasetSpec.datasetID]
                while len(tmpDatasetSpec.Files) > datasetUsage['used']:
                    tmpFileSpec = tmpDatasetSpec.Files[datasetUsage['used']]
                    isOK = False
                    for siteCandidate in self.siteCandidates.values():
                        if siteCandidate.isAvailableFile(tmpFileSpec):
                            isOK = True
                            break
                    if isOK:
                        break
                    # skip and check the next
                    datasetUsage['used'] += 1
                    nSkip += 1
        return nSkip



    # get max ramCount
    def getMaxRamCount(self):
        if self.isMerging:
            return max (self.taskSpec.mergeRamCount,self.ramCount)
        else:
            return max (self.taskSpec.ramCount,self.ramCount)


    # get site candidate
    def getSiteCandidate(self, name):
        if name in self.siteCandidates:
            return self.siteCandidates[name]
        return None
