import re


"""
task specification for JEDI

"""
class JediTaskSpec(object):
    # attributes
    attributes = (
        'jediTaskID','taskName','status','userName',
        'creationDate','modificationTime','startTime','endTime',
        'frozenTime','prodSourceLabel','workingGroup','vo','coreCount',
        'taskType','processingType','taskPriority','currentPriority',
        'architecture','transUses','transHome','transPath','lockedBy',
        'lockedTime','termCondition','splitRule','walltime','walltimeUnit',
        'outDiskCount','outDiskUnit','workDiskCount','workDiskUnit',
        'ramCount','ramUnit','ioIntensity','ioIntensityUnit',
        'workQueue_ID','progress','failureRate','errorDialog',
        'reqID','oldStatus','cloud','site','countryGroup','parent_tid',
        'eventService','ticketID','ticketSystemType','stateChangeTime',
        'superStatus','campaign','mergeRamCount','mergeRamUnit',
        'mergeWalltime','mergeWalltimeUnit','throttledTime','numThrottled',
        'mergeCoreCount','goal','assessmentTime','cpuTime','cpuTimeUnit',
        'cpuEfficiency','baseWalltime','nucleus','baseRamCount',
        'ttcRequested', 'ttcPredicted', 'ttcPredictionDate','rescueTime',
        'requestType', 'gshare', 'resource_type', 'useJumbo'
        )
    # attributes which have 0 by default
    _zeroAttrs = ()
    # attributes to force update
    _forceUpdateAttrs = ('lockedBy','lockedTime')
    # mapping between sequence and attr
    _seqAttrMap = {}
    # limit length
    _limitLength = {'errorDialog' : 255}
    # attribute length
    _attrLength = {'workingGroup': 32}
    # tokens for split rule
    splitRuleToken = {
        'allowEmptyInput'    : 'AE',
        'addNthFieldToLFN'   : 'AN',
        'allowPartialFinish' : 'AP',
        'altStageOut'        : 'AT',
        'ddmBackEnd'         : 'DE',
        'disableReassign'    : 'DI',
        'disableAutoRetry'   : 'DR',
        'dynamicNumEvents'   : 'DY',
        'nEsConsumers'       : 'EC',
        'nEventsPerWorker'   : 'ES',
        'failGoalUnreached'  : 'FG',
        'firstEvent'         : 'FT',
        'groupBoundaryID'    : 'GB',
        'instantiateTmplSite': 'IA',
        'inFilePosEvtNum'    : 'IF',
        'allowInputLAN'      : 'IL',
        'ignoreMissingInDS'  : 'IM',
        'ipConnectivity'     : 'IP',
        'instantiateTmpl'    : 'IT',
        'allowInputWAN'      : 'IW',
        'useLocalIO'         : 'LI',
        'limitedSites'       : 'LS',
        'loadXML'            : 'LX',
        'mergeEsOnOS'        : 'ME',
        'nMaxFilesPerJob'    : 'MF',
        'mergeOutput'        : 'MO',
        'noExecStrCnv'       : 'NC',
        'nEventsPerJob'      : 'NE',
        'nFilesPerJob'       : 'NF',
        'nGBPerJob'          : 'NG',
        'nJumboJobs'         : 'NJ',
        'nSitesPerJob'       : 'NS',
        'noWaitParent'       : 'NW',
        'pfnList'            : 'PL',
        'putLogToOS'         : 'PO',
        'runUntilClosed'     : 'RC',
        'registerDatasets'   : 'RD',
        'respectLB'          : 'RL',
        'reuseSecOnDemand'   : 'RO',
        'respectSplitRule'   : 'RR',
        'randomSeed'         : 'RS',
        'switchEStoNormal'   : 'SE',
        'stayOutputOnSite'   : 'SO',
        'scoutSuccessRate'   : 'SS',
        't1Weight'           : 'TW',
        'useBuild'           : 'UB',
        'useJobCloning'      : 'UC',
        'useRealNumEvents'   : 'UE',
        'useFileAsSourceLFN' : 'UF',
        'usePrePro'          : 'UP',
        'useScout'           : 'US',
        'useExhausted'       : 'UX',
        'writeInputToFile'   : 'WF',
        'waitInput'          : 'WI',
        'maxAttemptES'       : 'XA',
        'maxAttemptEsJob'    : 'XJ',
        'nEventsPerMergeJob'   : 'ZE',
        'nFilesPerMergeJob'    : 'ZF',
        'nGBPerMergeJob'       : 'ZG',
        'nMaxFilesPerMergeJob' : 'ZM',

        }
    # enum for preprocessing
    enum_toPreProcess = '1'
    enum_preProcessed = '2'
    enum_postPProcess = '3'
    # enum for limited sites
    enum_limitedSites = {'1' : 'inc',
                         '2' : 'exc',
                         '3' : 'incexc'}
    # enum for scout
    enum_noScout   = '1'
    enum_useScout  = '2'
    enum_postScout = '3'
    # enum for dataset registration
    enum_toRegisterDS   = '1'
    enum_registeredDS   = '2'
    # enum for IP connectivity
    enum_ipConnectivity = {'1' : 'full',
                           '2' : 'http'}
    # enum for alternative stage-out
    enum_altStageOut = {'1' : 'on',
                        '2' : 'off',
                        '3' : 'force'}
    # enum for local direct access
    enum_inputLAN = {'1' : 'use',
                     '2' : 'only'}
    # world cloud name
    worldCloudName = 'WORLD'

    # enum for useJumbo
    enum_useJumbo = {'waiting': 'W',
                     'running': 'R',
                     'disabled': 'D'}


    # constructor
    def __init__(self):
        # install attributes
        for attr in self.attributes:
            if attr in self._zeroAttrs:
                object.__setattr__(self,attr,0)
            else:
                object.__setattr__(self,attr,None)
        # map of changed attributes
        object.__setattr__(self,'_changedAttrs',{})
        # template to generate job parameters
        object.__setattr__(self,'jobParamsTemplate','')
        # associated datasets
        object.__setattr__(self,'datasetSpecList',[])


    # override __setattr__ to collect the changed attributes
    def __setattr__(self, name, value):
        oldVal = getattr(self,name)
        object.__setattr__(self,name,value)
        newVal = getattr(self,name)
        # collect changed attributes
        if oldVal != newVal or name in self._forceUpdateAttrs:
            self._changedAttrs[name] = value


    # copy old attributes
    def copyAttributes(self,oldTaskSpec):
        for attr in self.attributes + ('jobParamsTemplate',):
            if 'Time' in attr:
                continue
            if 'Date' in attr:
                continue
            if attr in ['progress','failureRate','errorDialog',
                        'status','oldStatus','lockedBy']:
                continue
            self.__setattr__(attr,getattr(oldTaskSpec,attr))
            
        
    # reset changed attribute list
    def resetChangedList(self):
        object.__setattr__(self,'_changedAttrs',{})


    # reset changed attribute
    def resetChangedAttr(self,name):
        try:
            del self._changedAttrs[name]
        except:
            pass


    # force update
    def forceUpdate(self,name):
        if name in self.attributes:
            self._changedAttrs[name] = getattr(self,name)
        
    
    # return map of values
    def valuesMap(self,useSeq=False,onlyChanged=False):
        ret = {}
        for attr in self.attributes:
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
            # truncate too long values
            if self._limitLength.has_key(attr):
                if val != None:
                    val = val[:self._limitLength[attr]]
            ret[':%s' % attr] = val
        return ret


    # pack tuple into TaskSpec
    def pack(self,values):
        for i in range(len(self.attributes)):
            attr= self.attributes[i]
            val = values[i]
            object.__setattr__(self,attr,val)


    # return column names for INSERT
    def columnNames(cls,prefix=None):
        ret = ""
        for attr in cls.attributes:
            if prefix != None:
                ret += '{0}.'.format(prefix)
            ret += '{0},'.format(attr)
        ret = ret[:-1]
        return ret
    columnNames = classmethod(columnNames)


    # return expression of bind variables for INSERT
    def bindValuesExpression(cls,useSeq=True):
        ret = "VALUES("
        for attr in cls.attributes:
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
        for attr in self.attributes:
            if self._changedAttrs.has_key(attr):
                ret += '%s=:%s,' % (attr,attr)
        ret  = ret[:-1]
        ret += ' '
        return ret



    # get the max size per job if defined
    def getMaxSizePerJob(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['nGBPerJob']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                nGBPerJob = int(tmpMatch.group(1)) * 1024 * 1024 * 1024
                return nGBPerJob
        return None    



    # get the max size per merge job if defined
    def getMaxSizePerMergeJob(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['nGBPerMergeJob']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                nGBPerJob = int(tmpMatch.group(1)) * 1024 * 1024 * 1024
                return nGBPerJob
        return None    



    # get the maxnumber of files per job if defined
    def getMaxNumFilesPerJob(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['nMaxFilesPerJob']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return None    



    # get the maxnumber of files per merge job if defined
    def getMaxNumFilesPerMergeJob(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['nMaxFilesPerMergeJob']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return 50



    # get the number of events per merge job if defined
    def getNumEventsPerMergeJob(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['nEventsPerMergeJob']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return None



    # check if using jumbo
    def usingJumboJobs(self):
        if self.useJumbo in [None, self.enum_useJumbo['disabled']]:
            return False
        return True



    # get the number of jumbo jobs if defined
    def getNumJumboJobs(self):
        if self.usingJumboJobs() and self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['nJumboJobs']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return None



    # get the number of sites per job
    def getNumSitesPerJob(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['nSitesPerJob']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return 1



    # get the number of files per job if defined
    def getNumFilesPerJob(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['nFilesPerJob']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return None    



    # get the number of files per merge job if defined
    def getNumFilesPerMergeJob(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['nFilesPerMergeJob']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return None    
        


    # get the number of events per job if defined
    def getNumEventsPerJob(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['nEventsPerJob']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return None    



    # get offset for random seed
    def getRndmSeedOffset(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['randomSeed']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return 0



    # get offset for first event
    def getFirstEventOffset(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['firstEvent']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return 0



    # grouping with boundaryID
    def useGroupWithBoundaryID(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['groupBoundaryID']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                gbID = int(tmpMatch.group(1))
                # 1 : input - can split,    output - free
                # 2 : input - can split,    output - mapped with provenanceID
                # 3 : input - cannot split, output - free
                # 4 : input - cannot split, output - mapped with provenanceID
                #
                # * rule for master
                # 1 : can split. one boundayID per sub chunk
                # 2 : cannot split. one boundayID per sub chunk
                # 3 : cannot split. multiple boundayIDs per sub chunk
                #
                # * rule for secodary
                # 1 : must have same boundayID. cannot split 
                #
                retMap = {}
                if gbID in [1,2]:
                    retMap['inSplit'] = 1
                else:
                    retMap['inSplit'] = 2
                if gbID in [1,3]:
                    retMap['outMap'] = False
                else:
                    retMap['outMap'] = True
                retMap['secSplit'] = None
                return retMap
        return None



    # use build 
    def useBuild(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['useBuild']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # use sjob cloning
    def useJobCloning(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['useJobCloning']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # get job cloning type
    def getJobCloningType(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['useJobCloning']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return tmpMatch.group(1)
        return ''



    # reuse secondary on demand
    def reuseSecOnDemand(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['reuseSecOnDemand']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # not wait for completion of parent 
    def noWaitParent(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['noWaitParent']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # check splitRule if not wait for completion of parent
    def noWaitParentSL(cls,splitRule):
        if splitRule != None:
            tmpMatch = re.search(cls.splitRuleToken['noWaitParent']+'=(\d+)',splitRule)
            if tmpMatch != None:
                return True
        return False
    noWaitParentSL = classmethod(noWaitParentSL)



    # use only limited sites
    def useLimitedSites(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['limitedSites']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # set limited sites
    def setLimitedSites(self,policy):
        tag = None
        for tmpIdx,tmpPolicy in self.enum_limitedSites.iteritems():
            if policy == tmpPolicy:
                tag = tmpIdx
                break
        # not found
        if tag == None:
            return
        # set
        if self.splitRule == None:
            # new
            self.splitRule = self.splitRuleToken['limitedSites']+'='+tag
        else:
            tmpMatch = re.search(self.splitRuleToken['limitedSites']+'=(\d+)',self.splitRule)
            if tmpMatch == None:
                # append
                self.splitRule += ','+self.splitRuleToken['limitedSites']+'='+tag
            else:
                # replace
                self.splitRule = re.sub(self.splitRuleToken['limitedSites']+'=(\d+)',
                                        self.splitRuleToken['limitedSites']+'='+tag,
                                        self.splitRule)



    # use local IO
    def useLocalIO(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['useLocalIO']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # use Event Service
    def useEventService(self,siteSpec=None):
        if self.eventService in [1,2]:
            # check site if ES is disabled
            if self.switchEStoNormal() and siteSpec != None and siteSpec.getJobSeed() in ['all']:
                return False
            return True
        return False



    # get the number of events per worker for Event Service
    def getNumEventsPerWorker(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['nEventsPerWorker']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return None    




    # get the number of event service consumers
    def getNumEventServiceConsumer(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['nEsConsumers']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return None    



    # disable automatic retry
    def disableAutoRetry(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['disableAutoRetry']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # disable reassign
    def disableReassign(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['disableReassign']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # allow empty input
    def allowEmptyInput(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['allowEmptyInput']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # use PFN list
    def useListPFN(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['pfnList']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # set preprocessing
    def setPrePro(self):
        if self.splitRule == None:
            # new
            self.splitRule = self.splitRuleToken['usePrePro']+'='+self.enum_toPreProcess
        else:
            # append
            self.splitRule += ','+self.splitRuleToken['usePrePro']+'='+self.enum_toPreProcess



    # use preprocessing
    def usePrePro(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['usePrePro']+'=(\d+)',self.splitRule)
            if tmpMatch != None and tmpMatch.group(1) == self.enum_toPreProcess:
                return True
        return False



    # set preprocessed
    def setPreProcessed(self):
        if self.splitRule == None:
            # new
            self.splitRule = self.splitRuleToken['usePrePro']+'='+self.enum_preProcessed
        else:
            tmpMatch = re.search(self.splitRuleToken['usePrePro']+'=(\d+)',self.splitRule)
            if tmpMatch == None:
                # append
                self.splitRule += ','+self.splitRuleToken['usePrePro']+'='+self.enum_preProcessed
            else:
                # replace
                self.splitRule = re.sub(self.splitRuleToken['usePrePro']+'=(\d+)',
                                        self.splitRuleToken['usePrePro']+'='+self.enum_preProcessed,
                                        self.splitRule)
        return



    # check preprocessed
    def checkPreProcessed(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['usePrePro']+'=(\d+)',self.splitRule)
            if tmpMatch != None and tmpMatch.group(1) == self.enum_preProcessed:
                return True
        return False



    # set post preprocess
    def setPostPreProcess(self):
        if self.splitRule == None:
            # new
            self.splitRule = self.splitRuleToken['usePrePro']+'='+self.enum_postPProcess
        else:
            tmpMatch = re.search(self.splitRuleToken['usePrePro']+'=(\d+)',self.splitRule)
            if tmpMatch == None:
                # append
                self.splitRule += ','+self.splitRuleToken['usePrePro']+'='+self.enum_postPProcess
            else:
                # replace
                self.splitRule = re.sub(self.splitRuleToken['usePrePro']+'=(\d+)',
                                        self.splitRuleToken['usePrePro']+'='+self.enum_postPProcess,
                                        self.splitRule)
        return



    # instantiate template datasets
    def instantiateTmpl(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['instantiateTmpl']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # instantiate template datasets at site
    def instantiateTmplSite(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['instantiateTmplSite']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # merge output
    def mergeOutput(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['mergeOutput']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # use random seed
    def useRandomSeed(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['randomSeed']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # get the size of workDisk in bytes
    def getWorkDiskSize(self):
        safetyMargin = 300 * 1024 * 1024
        tmpSize = self.workDiskCount
        if tmpSize == None:
            return 0
        if self.workDiskUnit == 'GB':
            tmpSize = tmpSize * 1024 * 1024 * 1024
        elif self.workDiskUnit == 'MB':
            tmpSize = tmpSize * 1024 * 1024
        elif self.workDiskUnit == 'kB':
            tmpSize = tmpSize * 1024
        tmpSize += safetyMargin
        return tmpSize



    # get the size of outDisk in bytes
    def getOutDiskSize(self):
        tmpSize = self.outDiskCount
        if tmpSize == None or tmpSize < 0:
            return 0
        if self.outDiskUnit != None:
            if self.outDiskUnit.startswith('GB'):
                tmpSize = tmpSize * 1024 * 1024 * 1024
            elif self.outDiskUnit.startswith('MB'):
                tmpSize = tmpSize * 1024 * 1024
            elif self.outDiskUnit.startswith('kB'):
                tmpSize = tmpSize * 1024
        return tmpSize



    # output scales with the number of events
    def outputScaleWithEvents(self):
        if self.outDiskUnit != None and self.outDiskUnit.endswith('PerEvent'):
            return True
        return False



    # return list of status to update contents
    def statusToUpdateContents(cls):
        return ['defined']
    statusToUpdateContents = classmethod(statusToUpdateContents)



    # set task status on hold
    def setOnHold(self):
        # change status
        if self.status in ['ready','running','merging','scouting','defined',
                           'topreprocess','preprocessing','registered',
                           'prepared','rerefine']:
            self.oldStatus = self.status
            self.status = 'pending'


    # return list of status to reject external changes
    def statusToRejectExtChange(cls):
        return ['finished','done','prepared','broken','tobroken','aborted','toabort','aborting','failed',
                'finishing','passed']
    statusToRejectExtChange = classmethod(statusToRejectExtChange)



    # return list of status for retry
    def statusToRetry(cls):
        return ['finished','failed','aborted','exhausted']
    statusToRetry = classmethod(statusToRetry)



    # return list of status for incexec
    def statusToIncexec(cls):
        return ['done'] + cls.statusToRetry()
    statusToIncexec = classmethod(statusToIncexec)



    # return list of status for reassign
    def statusToReassign(cls):
        return ['registered','defined','ready','running','scouting','scouted','pending',
                'assigning','exhausted']
    statusToReassign = classmethod(statusToReassign)



    # return list of status for Job Generator
    def statusForJobGenerator(cls):
        return ['ready','running','scouting','topreprocess','preprocessing']
    statusForJobGenerator = classmethod(statusForJobGenerator)



    # return list of status to not pause
    def statusNotToPause(cls):
        return ['finished','failed','done','aborted','broken','paused']
    statusNotToPause = classmethod(statusNotToPause)



    # return mapping of command and status
    def commandStatusMap(cls):
        return {'kill' : {'doing': 'aborting',
                          'done' : 'toabort'},
                'finish' : {'doing': 'finishing',
                            'done' : 'passed'},
                'retry' : {'doing': 'toretry',
                           'done' : 'ready'},
                'incexec' : {'doing': 'toincexec',
                             'done' : 'rerefine'},
                'reassign' : {'doing': 'toreassign',
                              'done' : 'reassigning'},
                'pause' : {'doing': 'paused',
                           'done' : 'dummy'},
                'resume' : {'doing': 'dummy',
                            'done' : 'dummy'},
                'avalanche' : {'doing': 'dummy',
                               'done' : 'dummy'},
                }
    commandStatusMap = classmethod(commandStatusMap)



    # set task status to active
    def setInActive(self):
        # change status
        if self.status == 'pending':
            self.status = self.oldStatus
            self.oldStatus = None
            self.errorDialog = None
        # reset error dialog    
        self.setErrDiag(None)
            
            
    # set error dialog
    def setErrDiag(self,diag,append=False):
        # set error dialog
        if append == True and self.errorDialog != None:
            self.errorDialog = "{0} {1}".format(self.errorDialog,diag)
        elif append == None:
            # keep old log
            if self.errorDialog == None:
                self.errorDialog = diag
        else:
            self.errorDialog = diag
        

        
    # use loadXML
    def useLoadXML(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['loadXML']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # make VOMS FQANs
    def makeFQANs(self):
        # no working group
        if self.workingGroup != None:
            fqan = '/{0}/{1}/Role=production'.format(self.vo,self.workingGroup)
        else:
            if self.vo != None: 
                fqan = '/{0}/Role=NULL'.format(self.vo)
            else:
                return []
        # return
        return [fqan]



    # set split rule
    def setSplitRule(self,ruleName,ruleValue):
        if self.splitRule == None:
            # new
            self.splitRule = self.splitRuleToken[ruleName]+'='+ruleValue
        else:
            tmpMatch = re.search(self.splitRuleToken[ruleName]+'=(\d+)',self.splitRule)
            if tmpMatch == None:
                # append
                self.splitRule += ','+self.splitRuleToken[ruleName]+'='+ruleValue
            else:
                # replace
                self.splitRule = re.sub(self.splitRuleToken[ruleName]+'=(\d+)',
                                        self.splitRuleToken[ruleName]+'='+ruleValue,
                                        self.splitRule)



    # remove split rule
    def removeSplitRule(self,ruleName):
        if self.splitRule != None:
            items = self.splitRule.split(',')
            newItems = []
            for item in items:
                # remove rile
                tmpRuleName = item.split('=')[0]
                if ruleName == tmpRuleName:
                    continue
                newItems.append(item)
            self.splitRule = ','.join(newItems)



    # set to use scout
    def setUseScout(self,useFlag):
        if useFlag:
            self.setSplitRule('useScout',self.enum_useScout)
        else:
            self.setSplitRule('useScout',self.enum_noScout)



    # set post scout
    def setPostScout(self):
        self.setSplitRule('useScout',self.enum_postScout)



    # use scout
    def useScout(self,splitRule=None):
        if splitRule is None:
            splitRule = self.splitRule
        if splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['useScout']+'=(\d+)',splitRule)
            if tmpMatch != None and tmpMatch.group(1) == self.enum_useScout:
                return True
        return False



    # use exhausted
    def useExhausted(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['useExhausted']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # use real number of events
    def useRealNumEvents(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['useRealNumEvents']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # use input LFN as source for output LFN
    def useFileAsSourceLFN(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['useFileAsSourceLFN']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # post scout
    def isPostScout(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['useScout']+'=(\d+)',self.splitRule)
            if tmpMatch != None and tmpMatch.group(1) == self.enum_postScout:
                return True
        return False

        

    # wait until input shows up
    def waitInput(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['waitInput']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # set DDM backend
    def setDdmBackEnd(self,backEnd):
        if self.splitRule == None:
            # new
            self.splitRule = self.splitRuleToken['ddmBackEnd']+'='+backEnd
        else:
            tmpMatch = re.search(self.splitRuleToken['ddmBackEnd']+'=([^,$]+)',self.splitRule)
            if tmpMatch == None:
                # append
                self.splitRule += ','+self.splitRuleToken['ddmBackEnd']+'='+backEnd
            else:
                # replace
                self.splitRule = re.sub(self.splitRuleToken['ddmBackEnd']+'=([^,$]+)',
                                        self.splitRuleToken['ddmBackEnd']+'='+backEnd,
                                        self.splitRule)



    # get DDM backend
    def getDdmBackEnd(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['ddmBackEnd']+'=([^,$]+)',self.splitRule)
            if tmpMatch != None:
                return tmpMatch.group(1)
        return None    



    # get field number to add middle name to LFN 
    def getFieldNumToLFN(self):
        try:
            if self.splitRule != None:
                tmpMatch = re.search(self.splitRuleToken['addNthFieldToLFN']+'=([,\d]+)',self.splitRule)
                if tmpMatch != None:
                    tmpList = tmpMatch.group(1).split(',')
                    try:
                        tmpList.remove('')
                    except:
                        pass
                    return map(int,tmpList)
        except:
            pass
        return None    



    # get required success rate for scout jobs
    def getScoutSuccessRate(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['scoutSuccessRate']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return None    



    # get T1 weight
    def getT1Weight(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['t1Weight']+'=(-*\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return 0



    # respect Lumiblock boundaries
    def respectLumiblock(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['respectLB']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # respect split rule
    def respectSplitRule(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['respectSplitRule']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # allow partial finish
    def allowPartialFinish(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['allowPartialFinish']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # check if datasets should be registered
    def toRegisterDatasets(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['registerDatasets']+'=(\d+)',self.splitRule)
            if tmpMatch != None and tmpMatch.group(1) == self.enum_toRegisterDS:
                return True
        return False



    # datasets were registered
    def registeredDatasets(self):
        self.setSplitRule('registerDatasets',self.enum_registeredDS)



    # set datasets to be registered
    def setToRegisterDatasets(self):
        self.setSplitRule('registerDatasets',self.enum_toRegisterDS)



    # get the max number of attempts for ES events
    def getMaxAttemptES(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['maxAttemptES']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return None    



    # get the max number of attempts for ES jobs
    def getMaxAttemptEsJob(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['maxAttemptEsJob']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return int(tmpMatch.group(1))
        return self.getMaxAttemptES()



    # check attribute length
    def checkAttrLength(self):
        for attrName,attrLength in self._attrLength.iteritems():
            attrVal = getattr(self,attrName)
            if attrVal == None:
                continue
            if len(attrVal) > attrLength:
                setattr(self,attrName,None)
                self.errorDialog = "{0} is too long (actual: {1}, maximum: {2})".format(attrName,len(attrVal),attrLength)
                return False
        return True



    # set IP connectivity
    def setIpConnectivity(self,value):
        if value in self.enum_ipConnectivity.values():
            for tmpKey,tmpVal in self.enum_ipConnectivity.iteritems():
                if value == tmpVal:
                    self.setSplitRule('ipConnectivity',tmpKey)
                    break



    # get IP connectivity
    def getIpConnectivity(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['ipConnectivity']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return self.enum_ipConnectivity[tmpMatch.group(1)]
        return None



    # use HS06 for walltime estimation
    def useHS06(self):
        return self.cpuTimeUnit == 'HS06sPerEvent'



    # RAM scales with nCores
    def ramPerCore(self):
        return self.ramUnit in ['MBPerCore','MBPerCoreFixed']



    # run until input is closed
    def runUntilClosed(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['runUntilClosed']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # stay output on site
    def stayOutputOnSite(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['stayOutputOnSite']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # fail when goal unreached
    def failGoalUnreached(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['failGoalUnreached']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # unset fail when goal unreached
    def unsetFailGoalUnreached(self):
        self.removeSplitRule(self.splitRuleToken['failGoalUnreached'])



    # switch ES to normal when jobs land at normal sites
    def switchEStoNormal(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['switchEStoNormal']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # use world cloud
    def useWorldCloud(self):
        return self.cloud == self.worldCloudName



    # dynamic number of events
    def dynamicNumEvents(self):
        if self.getNumEventsPerJob() is not None:
            return False
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['dynamicNumEvents']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # get min granularity for dynamic number of events
    def get_min_granularity(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['dynamicNumEvents']+'=(\d+)',self.splitRule)
            if tmpMatch is not None:
                return int(tmpMatch.group(1))
        return None



    # set alternative stage-out
    def setAltStageOut(self,value):
        if value in self.enum_altStageOut.values():
            for tmpKey,tmpVal in self.enum_altStageOut.iteritems():
                if value == tmpVal:
                    self.setSplitRule('altStageOut',tmpKey)
                    break



    # get alternative stage-out
    def getAltStageOut(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['altStageOut']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return self.enum_altStageOut[tmpMatch.group(1)]
        return None



    # allow WAN for input access 
    def allowInputWAN(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['allowInputWAN']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # set mode for input LAN access
    def setAllowInputLAN(self,value):
        if value in self.enum_inputLAN.values():
            for tmpKey,tmpVal in self.enum_inputLAN.iteritems():
                if value == tmpVal:
                    self.setSplitRule('allowInputLAN',tmpKey)
                    break



    # check if LAN is used for input access 
    def allowInputLAN(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['allowInputLAN']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return self.enum_inputLAN[tmpMatch.group(1)]
        return None



    # put log files to OS
    def putLogToOS(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['putLogToOS']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # merge ES on Object Store
    def mergeEsOnOS(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['mergeEsOnOS']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # write input to file
    def writeInputToFile(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['writeInputToFile']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # ignore missing input datasets
    def ignoreMissingInDS(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['ignoreMissingInDS']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # suppress execute string conversion
    def noExecStrCnv(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['noExecStrCnv']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False



    # in-file positional event number
    def inFilePosEvtNum(self):
        if self.splitRule != None:
            tmpMatch = re.search(self.splitRuleToken['inFilePosEvtNum']+'=(\d+)',self.splitRule)
            if tmpMatch != None:
                return True
        return False
