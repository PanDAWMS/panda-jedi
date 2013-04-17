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
    def getDatasets(self):
        dataList = []
        if self.masterDataset != None:
            dataList.append(self.masterDataset)
        dataList += self.secondaryDatasetList
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
        return random.choice(siteCandidate)



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
        maxAtomSize = 0    
        while True:
            # get one subchunk
            subChunk = self.getSubChunk(None,nFilesPerJob=nFilesPerJob)
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
    
        

    # get subchunk with a selection criteria
    def getSubChunk(self,siteName,maxNumFiles=None,maxSize=None,
                    sizeGradients=0,sizeIntercepts=0,
                    nFilesPerJob=None,multiplicand=1,
                    walltimeIntercepts=0,maxWalltime=0):
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
        # overwrite parameters when nFilesPerJob is used
        if nFilesPerJob != None:
            multiplicand = nFilesPerJob
        # get site when splitting per site
        if siteName != None:
            siteCandidate = self.siteCandidates[siteName]
        # start splitting
        inputNumFiles  = 0
        fileSize       = 0
        firstLoop      = True
        firstMaster    = True
        inputFileMap   = {}
        expWalltime    = 0
        while inputNumFiles < maxNumFiles and fileSize < maxSize and (maxWalltime <= 0 or expWalltime < maxWalltime):
            # get one file (or one file group for MP) from master
            datasetUsage = self.datasetMap[self.masterDataset.datasetID]
            for tmpFileSpec in self.masterDataset.Files[datasetUsage['used']:datasetUsage['used']+multiplicand]:
                if not inputFileMap.has_key(self.masterDataset.datasetID):
                    inputFileMap[self.masterDataset.datasetID] = []
                inputFileMap[self.masterDataset.datasetID].append(tmpFileSpec)
                datasetUsage['used'] += 1
                # sum
                inputNumFiles += 1
                fileSize += (tmpFileSpec.fsize + sizeGradients)
                # sum offset only for the first master
                if firstMaster:
                    fileSize += sizeIntercepts
                firstMaster = False
                # walltime
                expWalltime += walltimeIntercepts
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
                            inputNumFiles += 1
                            fileSize += tmpFileSpec.fsize
                            datasetUsage['used'] += 1
                else:
                    # FIXME for pileup etc
                    pass
            # unset first loop flag
            firstLoop = False
            # check if there are unused files/evets 
            if not self.checkUnused():
                break
            # break if nFilesPerJob is used
            if nFilesPerJob != None:
                break
            # check master
            datasetUsage = self.datasetMap[self.masterDataset.datasetID]
            newInputNumFiles = inputNumFiles
            newFileSize      = fileSize
            newExpWalltime   = expWalltime
            for tmpFileSpec in self.masterDataset.Files[datasetUsage['used']:datasetUsage['used']+multiplicand]:
                newInputNumFiles += 1
                newFileSize += (tmpFileSpec.fsize + sizeGradients)
                newExpWalltime += walltimeIntercepts
                if newInputNumFiles >= maxNumFiles or newFileSize >= maxSize \
                       or (maxWalltime > 0 and newExpWalltime >= maxWalltime) :
                    break
            # check secondaries
            for datasetSpec in self.secondaryDatasetList:
                if not datasetSpec.isNoSplit():
                    # FIXME for pileup etc
                    pass
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
