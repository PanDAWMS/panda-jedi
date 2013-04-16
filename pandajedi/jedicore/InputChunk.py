import copy
import random


# class for input
class InputChunk:

    # constructor
    def __init__(self,masterDataset=None,secondaryDatasetList=[]):
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



    # get subchunk with a selection criteria
    def getSubChunk(self,siteName,maxNumFiles=None,maxSize=None,
                    gradients=0,intercepts=0,multiplicand=1):
        # check if there are unused files/events
        if not self.checkUnused():
            return None
        # set default max number of files
        if maxNumFiles == None:
            # 50 files at most
            maxNumFiles = 50
        # set default max size    
        if maxSize == None:     
            # 50 GB at most
            maxSize = 50 * 1024 * 1024 * 1024
        # get site
        siteCandidate = self.siteCandidates[siteName]
        # start splitting
        inputNumFiles  = 0
        fileSize       = 0
        firstLoop      = True
        firstMaster    = True
        inputFileMap   = {}
        while inputNumFiles < maxNumFiles and fileSize < maxSize:
            # get one file (or one file group for MP) from master
            datasetUsage = self.datasetMap[self.masterDataset.datasetID]
            for tmpFileSpec in self.masterDataset.Files[datasetUsage['used']:datasetUsage['used']+multiplicand]:
                if not inputFileMap.has_key(self.masterDataset.datasetID):
                    inputFileMap[self.masterDataset.datasetID] = []
                inputFileMap[self.masterDataset.datasetID].append(tmpFileSpec)
                datasetUsage['used'] += 1
                # sum
                inputNumFiles += 1
                fileSize += (tmpFileSpec.fsize + gradients)
                # sum offset only for the first master
                if firstMaster:
                    fileSize += intercepts
                firstMaster = False    
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
            # check master
            datasetUsage = self.datasetMap[self.masterDataset.datasetID]
            newInputNumFiles = inputNumFiles
            newFileSize      = fileSize
            for tmpFileSpec in self.masterDataset.Files[datasetUsage['used']:datasetUsage['used']+multiplicand]:
                newInputNumFiles += 1
                newFileSize += (tmpFileSpec.fsize + gradients)
                if newInputNumFiles >= maxNumFiles or newFileSize >= maxSize:
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
                # make copy to individually set locality
                newFileSpec = copy.copy(tmpFileSpec)
                # set locality
                newFileSpec.locality = siteCandidate.getFileLocality(tmpFileSpec)
                # append
                tmpRetList.append(newFileSpec)
            # add to return map    
            tmpDatasetSpec = self.getDatasetWithID(tmpDatasetID)    
            returnList.append((tmpDatasetSpec,tmpRetList))
        # return
        return returnList
