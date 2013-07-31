import copy
import random


# class for input
class InputChunk:

    # constructor
    def __init__(self,taskSpec,masterDataset=None,secondaryDatasetList=[]):
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



    # reset used counters
    def resetUsedCounters(self):
        for tmpKey,tmpVal in self.datasetMap.iteritems():
            tmpVal['used'] = 0
            
        

    # add site candidates
    def addSiteCandidate(self,siteCandidateSpec):
        self.siteCandidates[siteCandidateSpec.siteName] = siteCandidateSpec
        return



    # get one site candidate randomly
    def getOneSiteCandidate(self):
        # get total weight
        totalWeight = 0
        weightList  = []
        siteCandidateList = self.siteCandidates.values()
        for siteCandidate in siteCandidateList:
            totalWeight += siteCandidate.weight
        # get random number
        rNumber = random.random() * totalWeight
        for siteCandidate in siteCandidateList:
            rNumber -= siteCandidate.weight
            if rNumber <= 0:
                return siteCandidate
        # return something as a protection against precision of float
        return random.choice(siteCandidateList)



    # check if unused files/events remain
    def checkUnused(self):
        # master is undefined
        if self.masterIndexName == None:
            return False
        indexVal = self.datasetMap[self.masterIndexName]
        return indexVal['used'] < len(indexVal['datasetSpec'].Files)



    # get maximum size of atomic subchunk
    def getMaxAtomSize(self):
        # number of files per job if defined
        nFilesPerJob = self.taskSpec.getNumFilesPerJob()
        if nFilesPerJob == None:
            nFilesPerJob = 1
        # grouping with boundaryID
        useBoundary = self.taskSpec.useGroupWithBoundaryID()    
        maxAtomSize = 0    
        while True:
            # get one subchunk
            subChunk = self.getSubChunk(None,nFilesPerJob=nFilesPerJob,
                                        useBoundary=useBoundary)
            if subChunk == None:
                break
            # get size
            tmpAtomSize = 0
            for tmpDatasetSpec,tmpFileSpecList in subChunk:
                for tmpFileSpec in tmpFileSpecList:
                    tmpAtomSize += tmpFileSpec.fsize
            if maxAtomSize < tmpAtomSize:
                maxAtomSize = tmpAtomSize
        # reset counters
        self.resetUsedCounters()
        # return
        return maxAtomSize
    
        

    # use scout
    def useScout(self):
        if self.masterDataset != None and \
                self.masterDataset.nFiles > self.masterDataset.nFilesToBeUsed:
            return True
        return False




    # get subchunk with a selection criteria
    def getSubChunk(self,siteName,maxNumFiles=None,maxSize=None,
                    sizeGradients=0,sizeIntercepts=0,
                    nFilesPerJob=None,multiplicand=1,
                    walltimeIntercepts=0,maxWalltime=0,
                    nEventsPerJob=None,useBoundary=None):
        # check if there are unused files/events
        if not self.checkUnused():
            return None
        # set default max number of files
        if maxNumFiles == None:
            # 20 files at most by default
            maxNumFiles = 20
        # set default max size    
        if maxSize == None:     
            # 20 GB at most by default
            maxSize = 20 * 1024 * 1024 * 1024
        # set default max number of events
        maxNumEvents = None
        # overwrite parameters when nFiles/EventsPerJob is used
        if nFilesPerJob != None:
            maxNumFiles = nFilesPerJob
        if nEventsPerJob != None:
            maxNumEvents = nEventsPerJob
        # split with boundayID
        splitWithBoundaryID = False    
        if useBoundary != None:
            splitWithBoundaryID = True
            if useBoundary['inSplit'] == False:
                # unset max values to split only with boundaryID 
                maxNumFiles = None
                maxSize = None
                maxWalltime = None
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
        while (maxNumFiles == None or inputNumFiles < maxNumFiles) \
                and (maxSize == None or (maxSize != None and fileSize < maxSize)) \
                and (maxWalltime <= 0 or expWalltime < maxWalltime) \
                and (maxNumEvents == None or (maxNumEvents != None and inputNumEvents < maxNumEvents)):
            # get one file (or one file group for MP) from master
            datasetUsage = self.datasetMap[self.masterDataset.datasetID]
            for tmpFileSpec in self.masterDataset.Files[datasetUsage['used']:datasetUsage['used']+multiplicand]:
                # check start event to keep continuity
                if maxNumEvents != None and tmpFileSpec.startEvent != None:
                    if nextStartEvent != None and nextStartEvent != tmpFileSpec.startEvent:
                        break
                # check boundaryID
                if splitWithBoundaryID and boundaryID != None and boundaryID != tmpFileSpec.boundaryID:
                    newBoundaryID = True
                    break
                if not inputFileMap.has_key(self.masterDataset.datasetID):
                    inputFileMap[self.masterDataset.datasetID] = []
                inputFileMap[self.masterDataset.datasetID].append(tmpFileSpec)
                datasetUsage['used'] += 1
                numMaster += 1
                # sum
                inputNumFiles += 1
                fileSize += (tmpFileSpec.fsize + sizeGradients)
                # sum offset only for the first master
                if firstMaster:
                    fileSize += sizeIntercepts
                firstMaster = False
                # walltime
                expWalltime += walltimeIntercepts
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
            # get files from secondaries 
            for datasetSpec in self.secondaryDatasetList:
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
                            datasetUsage['used'] += 1
                else:
                    if not nSecFilesMap.has_key(datasetSpec.datasetID):
                        nSecFilesMap[datasetSpec.datasetID] = 0
                    # get number of files to be used for the secondary
                    nSecondary = datasetSpec.getNumMultByRatio(numMaster) - nSecFilesMap[datasetSpec.datasetID]
                    datasetUsage = self.datasetMap[datasetSpec.datasetID]
                    for tmpFileSpec in datasetSpec.Files[datasetUsage['used']:datasetUsage['used']+nSecondary]:
                        # check boundaryID
                        if splitWithBoundaryID and boundaryID != None and boundaryID != tmpFileSpec.boundaryID:
                            break
                        if not inputFileMap.has_key(datasetSpec.datasetID):
                            inputFileMap[datasetSpec.datasetID] = []
                        inputFileMap[datasetSpec.datasetID].append(tmpFileSpec)
                        # sum
                        fileSize += tmpFileSpec.fsize
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
            # check master in the next loop
            datasetUsage = self.datasetMap[self.masterDataset.datasetID]
            newInputNumFiles  = inputNumFiles
            newInputNumEvents = inputNumEvents
            newFileSize       = fileSize
            newExpWalltime    = expWalltime
            newNextStartEvent = nextStartEvent
            newNumMaster      = numMaster
            terminateFlag     = False
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
                if splitWithBoundaryID and boundaryID != None and boundaryID != tmpFileSpec.boundaryID:
                    # no files in the next loop
                    if newInputNumFiles == 0:
                        terminateFlag = True
                    break
                newInputNumFiles += 1
                newNumMaster += 1
                newFileSize += (tmpFileSpec.fsize + sizeGradients)
                newExpWalltime += walltimeIntercepts
            # check secondaries
            for datasetSpec in self.secondaryDatasetList:
                if not datasetSpec.isNoSplit():
                    # check boundaryID
                    if splitWithBoundaryID and boundaryID != None and boundaryID != tmpFileSpec.boundaryID:
                        break
                    newNumSecondary = datasetSpec.getNumMultByRatio(newNumMaster) - nSecFilesMap[datasetSpec.datasetID]
                    datasetUsage = self.datasetMap[datasetSpec.datasetID]
                    for tmpFileSpec in datasetSpec.Files[datasetUsage['used']:datasetUsage['used']+nSecondary]:
                        # check boundaryID
                        if splitWithBoundaryID and boundaryID != None and boundaryID != tmpFileSpec.boundaryID:
                            break
                        newFileSize += tmpFileSpec.fsize
            # termination            
            if terminateFlag:
                break
            if newInputNumFiles >= maxNumFiles \
                    or (maxSize != None and newFileSize >= maxSize) \
                    or (maxWalltime > 0 and newExpWalltime >= maxWalltime) \
                    or (maxNumEvents != None and newInputNumEvents >= maxNumEvents):
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
