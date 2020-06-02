import re
import sys
import time
import datetime
import random

from six import iteritems

try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

from .PostProcessorBase import PostProcessorBase
from pandajedi.jedirefine import RefinerUtils
from pandaserver.taskbuffer import EventServiceUtils


# post processor for ATLAS production
class AtlasAnalPostProcessor (PostProcessorBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        PostProcessorBase.__init__(self,taskBufferIF,ddmIF)
        self.taskParamMap = None


    # main
    def doPostProcess(self,taskSpec,tmpLog):
        # freeze datasets
        try:
            # get DDM I/F
            ddmIF = self.ddmIF.getInterface(taskSpec.vo)
            # shuffle datasets
            random.shuffle(taskSpec.datasetSpecList)
            # loop over all datasets
            useLib = False
            nOkLib = 0
            lockUpdateTime = datetime.datetime.utcnow()
            for datasetSpec in taskSpec.datasetSpecList:
                # ignore template
                if datasetSpec.type.startswith('tmpl_'):
                    continue
                # only output, log or lib datasets
                if not datasetSpec.type.endswith('log') and not datasetSpec.type.endswith('output') \
                        and not datasetSpec.type == 'lib':
                    continue
                # only user group, or panda dataset
                if not datasetSpec.datasetName.startswith('user') and not datasetSpec.datasetName.startswith('panda') \
                        and not datasetSpec.datasetName.startswith('group'):
                    continue
                # check if already closed
                datasetAttrs = self.taskBufferIF.getDatasetAttributes_JEDI(datasetSpec.jediTaskID,datasetSpec.datasetID,['state'])
                if 'state' in datasetAttrs and datasetAttrs['state'] == 'closed':
                    tmpLog.info('skip freezing closed datasetID={0}:Name={1}'.format(datasetSpec.datasetID,datasetSpec.datasetName))
                    closedFlag = True
                else:
                    closedFlag = False
                # remove wrong files
                if not closedFlag and datasetSpec.type in ['output']:
                    # get successful files
                    okFiles = self.taskBufferIF.getSuccessfulFiles_JEDI(datasetSpec.jediTaskID,datasetSpec.datasetID)
                    if okFiles is None:
                        tmpLog.warning('failed to get successful files for {0}'.format(datasetSpec.datasetName))
                        return self.SC_FAILED
                    # get files in dataset
                    ddmFiles = ddmIF.getFilesInDataset(datasetSpec.datasetName,skipDuplicate=False)
                    tmpLog.debug('datasetID={0}:Name={1} has {2} files in DB, {3} files in DDM'.format(datasetSpec.datasetID,
                                                                                                      datasetSpec.datasetName,
                                                                                                      len(okFiles),len(ddmFiles)))
                    # check all files
                    toDelete = []
                    for tmpGUID,attMap in iteritems(ddmFiles):
                        if attMap['lfn'] not in okFiles:
                            did = {'scope':attMap['scope'], 'name':attMap['lfn']}
                            toDelete.append(did)
                            tmpLog.debug('delete {0} from {1}'.format(attMap['lfn'],datasetSpec.datasetName))
                    # delete
                    if toDelete != []:
                        ddmIF.deleteFilesFromDataset(datasetSpec.datasetName,toDelete)
                # freeze datasets
                if not closedFlag and not (datasetSpec.type.startswith('trn_') and datasetSpec.type not in ['trn_log']):
                    tmpLog.debug('freeze datasetID={0}:Name={1}'.format(datasetSpec.datasetID,datasetSpec.datasetName))
                    ddmIF.freezeDataset(datasetSpec.datasetName,ignoreUnknown=True)
                else:
                    if datasetSpec.type.startswith('trn_') and datasetSpec.type not in ['trn_log']:
                        tmpLog.debug('skip freezing transient datasetID={0}:Name={1}'.format(datasetSpec.datasetID,datasetSpec.datasetName))
                # update dataset
                datasetSpec.state = 'closed'
                datasetSpec.stateCheckTime = datetime.datetime.utcnow()
                # check if build step was succeeded
                if datasetSpec.type == 'lib':
                    useLib = True
                else:
                    nOkLib += 1
                # delete transient or empty datasets
                if not closedFlag:
                    emptyOnly = True
                    if datasetSpec.type.startswith('trn_') and datasetSpec.type not in ['trn_log']:
                        emptyOnly = False
                    retStr = ddmIF.deleteDataset(datasetSpec.datasetName,emptyOnly,ignoreUnknown=True)
                    tmpLog.debug(retStr)
                # extend lifetime
                if datasetSpec.type in ['output'] and datasetSpec.datasetName.startswith('user'):
                    tmpLog.debug('extend lifetime datasetID={0}:Name={1}'.format(datasetSpec.datasetID,datasetSpec.datasetName))
                    ddmIF.updateReplicationRules(datasetSpec.datasetName,{'type=.+':{'lifetime':14*24*60*60},
                                                                          '(SCRATCH|USER)DISK':{'lifetime':14*24*60*60}})
                # update dataset in DB
                self.taskBufferIF.updateDatasetAttributes_JEDI(datasetSpec.jediTaskID,datasetSpec.datasetID,
                                                               {'state':datasetSpec.state,
                                                                'stateCheckTime':datasetSpec.stateCheckTime})
                # update task lock
                if datetime.datetime.utcnow()-lockUpdateTime > datetime.timedelta(minutes=5):
                    lockUpdateTime = datetime.datetime.utcnow()
                    # update lock
                    self.taskBufferIF.updateTaskLock_JEDI(taskSpec.jediTaskID)
            # dialog
            if useLib and nOkLib == 0:
                taskSpec.setErrDiag('No build jobs succeeded',True)
        except Exception:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.warning('failed to freeze datasets with {0}:{1}'.format(errtype.__name__,errvalue))
        retVal = self.SC_SUCCEEDED
        try:
            self.doBasicPostProcess(taskSpec,tmpLog)
        except Exception:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doBasicPostProcess failed with {0}:{1}'.format(errtype.__name__,errvalue))
            retVal = self.SC_FATAL
        return retVal



    # final procedure
    def doFinalProcedure(self,taskSpec,tmpLog):
        # check email address
        toAdd = self.getEmail(taskSpec.userName,taskSpec.vo,tmpLog)
        # read task parameters
        try:
            taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(taskSpec.jediTaskID)
            self.taskParamMap = RefinerUtils.decodeJSON(taskParam)
        except Exception:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('task param conversion from json failed with {0}:{1}'.format(errtype.__name__,errvalue))
        if toAdd is None or \
                (self.taskParamMap is not None and 'noEmail' in self.taskParamMap and self.taskParamMap['noEmail'] is True):
            tmpLog.debug('email notification is suppressed')
        else:
            # send email notification
            fromAdd = self.senderAddress()
            msgBody = self.composeMessage(taskSpec,fromAdd,toAdd)
            self.sendMail(taskSpec.jediTaskID,fromAdd,toAdd,msgBody,3,False,tmpLog)
        return self.SC_SUCCEEDED



    # compose mail message
    def composeMessage(self,taskSpec,fromAdd,toAdd):
        # get full task parameters
        urlData = {}
        urlData['job'] = '*'
        urlData['jobsetID'] = taskSpec.reqID
        urlData['user'] = taskSpec.userName
        newUrlData = {}
        newUrlData['jobtype'] = 'analysis'
        newUrlData['jobsetID'] = taskSpec.reqID
        newUrlData['prodUserName'] = taskSpec.userName
        newUrlData['hours'] = 71
        # summary
        listInDS = []
        listOutDS = []
        listLogDS = []
        numTotal = 0
        numOK = 0
        numNG = 0
        numCancel = 0
        if not taskSpec.is_hpo_workflow():
            inputStr = 'Inputs'
            cancelledStr = 'Cancelled  '
            for datasetSpec in taskSpec.datasetSpecList:
                # dataset summary
                if datasetSpec.type == 'log':
                    if datasetSpec.containerName not in listLogDS:
                        listLogDS.append(datasetSpec.containerName)
                elif datasetSpec.type == 'input':
                    if datasetSpec.containerName not in listInDS:
                        listInDS.append(datasetSpec.containerName)
                elif datasetSpec.type == 'output':
                    if datasetSpec.containerName not in listOutDS:
                        listOutDS.append(datasetSpec.containerName)
                # process summary
                if datasetSpec.isMasterInput():
                    try:
                        numTotal += datasetSpec.nFiles
                        numOK    += datasetSpec.nFilesFinished
                        numNG    += datasetSpec.nFilesFailed
                    except Exception:
                        pass
        else:
            inputStr = 'Points'
            cancelledStr = 'Unprocessed'
            numTotal = taskSpec.get_total_num_jobs()
            event_stat = self.taskBufferIF.get_event_statistics(taskSpec.jediTaskID)
            if event_stat is not None:
                numOK = event_stat.get(EventServiceUtils.ST_finished, 0)
                numNG = event_stat.get(EventServiceUtils.ST_failed, 0)
        try:
            numCancel = numTotal - numOK - numNG
        except Exception:
            pass
        if numOK == numTotal:
            msgSucceeded = 'All Succeeded'
        else:
            msgSucceeded = 'Succeeded'
        listInDS.sort()
        listOutDS.sort()
        listLogDS.sort()
        dsSummary = ''
        for tmpDS in listInDS:
            dsSummary += 'In  : {0}\n'.format(tmpDS)
        for tmpDS in listOutDS:
            dsSummary += 'Out : {0}\n'.format(tmpDS)
        for tmpDS in listLogDS:
            dsSummary += 'Log : {0}\n'.format(tmpDS)
        dsSummary = dsSummary[:-1]
        # CLI param
        if 'cliParams' in self.taskParamMap:
            cliParams = self.taskParamMap['cliParams']
        else:
            cliParams = None
        # make message
        message = \
            """Subject: JEDI notification for TaskID:{jediTaskID} ({numOK}/{numTotal} {msgSucceeded})
From: {fromAdd}
To: {toAdd}

Summary of TaskID:{jediTaskID}

Created : {creationDate} (UTC)
Ended   : {endTime} (UTC)

Final Status : {status}

Total Number of {strInput}   : {numTotal}
             Succeeded   : {numOK}
             Failed      : {numNG}
             {strCancelled} : {numCancel}


Error Dialog : {errorDialog}

{dsSummary}

Parameters : {params}


PandaMonURL : http://bigpanda.cern.ch/task/{jediTaskID}/""".format(
            jediTaskID=taskSpec.jediTaskID,
            JobsetID=taskSpec.reqID,
            fromAdd=fromAdd,
            toAdd=toAdd,
            creationDate=taskSpec.creationDate,
            endTime=taskSpec.endTime,
            status=taskSpec.status,
            errorDialog=self.removeTags(taskSpec.errorDialog),
            params=cliParams,
            taskName=taskSpec.taskName,
            oldPandaMon=urlencode(urlData),
            newPandaMon=urlencode(newUrlData),
            numTotal=numTotal,
            numOK=numOK,
            numNG=numNG,
            numCancel=numCancel,
            dsSummary=dsSummary,
            msgSucceeded=msgSucceeded,
            strInput=inputStr,
            strCancelled=cancelledStr,
            )

        # tailer
        message += \
"""


Report Panda problems of any sort to

  the eGroup for help request
    hn-atlas-dist-analysis-help@cern.ch

  the JIRA portal for software bug
    https://its.cern.ch/jira/browse/ATLASPANDA
"""
        # return
        return message



    # get email
    def getEmail(self,userName,vo,tmpLog):
        # return to suppress mail
        retSupp = None
        # get DN
        tmpLog.debug("getting email for {0}".format(userName))
        # get email from MetaDB
        mailAddrInDB,dn,dbUptime = self.taskBufferIF.getEmailAddr(userName,withDN=True)
        tmpLog.debug("email from MetaDB : {0}".format(mailAddrInDB))
        # email mortification is suppressed
        notSendMail = False
        if mailAddrInDB is not None and mailAddrInDB.startswith('notsend'):
            notSendMail = True
        # DN is unavilable
        if dn in ['',None]:
            tmpLog.debug("DN is empty")
            notSendMail = True
        else:
            # avoid too frequently lookup
            if dbUptime is not None and datetime.datetime.utcnow()-dbUptime < datetime.timedelta(hours=1):
                tmpLog.debug("no lookup")
                if notSendMail or mailAddrInDB in [None,'']:
                    return retSupp
                else:
                    return mailAddrInDB.split(':')[-1]
            else:
                # get email from DQ2
                tmpLog.debug("getting email using dq2Info.finger({0})".format(dn))
                nTry = 3
                for iDDMTry in range(nTry):
                    try:
                        userInfo = self.ddmIF.getInterface(vo).finger(dn)
                        mailAddr = userInfo['email']
                        tmpLog.debug("email from DQ2 : {0}".format(mailAddr))
                        if mailAddr is None:
                            mailAddr = ''
                        # make email field to update DB
                        mailAddrToDB = ''
                        if notSendMail:
                            mailAddrToDB += 'notsend:'
                        mailAddrToDB += mailAddr
                        # update database
                        tmpLog.debug("update email to {0}".format(mailAddrToDB))
                        self.taskBufferIF.setEmailAddr(userName,mailAddrToDB)
                        # return
                        if notSendMail or mailAddr == '':
                            return retSupp
                        return mailAddr
                    except Exception:
                        if iDDMTry+1 < nTry:
                            tmpLog.debug("sleep for retry {0}/{1}".format(iDDMTry,nTry))
                            time.sleep(10)
                        else:
                            errType,errValue = sys.exc_info()[:2]
                            tmpLog.error("{0}:{1}".format(errType,errValue))
        # not send email
        return retSupp



    # remove tags
    def removeTags(self,tmpStr):
        try:
            if tmpStr is not None:
                tmpStr = re.sub('>[^<]+<','><',tmpStr)
                tmpStr = re.sub('<[^<]+>','',tmpStr)
        except Exception:
            pass
        return tmpStr
