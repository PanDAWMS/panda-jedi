import re
import sys
import time
import urllib
import datetime

from pandajedi.jedicore import Interaction
from PostProcessorBase import PostProcessorBase
from pandajedi.jedirefine import RefinerUtils


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
            # loop over all datasets
            useLib = False
            nOkLib = 0
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
                # freeze datasets
                tmpLog.info('freeze datasetID={0}:Name={1}'.format(datasetSpec.datasetID,datasetSpec.datasetName))
                ddmIF.freezeDataset(datasetSpec.datasetName,ignoreUnknown=True)
                # update dataset
                datasetSpec.state = 'closed'
                datasetSpec.stateCheckTime = datetime.datetime.utcnow()
                # check if build step was succeeded
                if datasetSpec.type == 'lib':
                    useLib = True
                else:
                    nOkLib += 1
                # delete transient or empty datasets
                emptyOnly = True
                if datasetSpec.type.startswith('trn_'):
                    emptyOnly = False
                retStr = ddmIF.deleteDataset(datasetSpec.datasetName,emptyOnly,ignoreUnknown=True)
                tmpLog.info(retStr)
            # dialog
            if useLib and nOkLib == 0:
                taskSpec.setErrDiag('No build jobs succeeded',True)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.warning('failed to freeze datasets with {0}:{1}'.format(errtype.__name__,errvalue))
        retVal = self.SC_SUCCEEDED
        try:
            self.doBasicPostProcess(taskSpec,tmpLog)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doBasicPostProcess failed with {0}:{1}'.format(errtype.__name__,errvalue))
            retVal = self.SC_FAILED
        return retVal



    # final procedure
    def doFinalProcedure(self,taskSpec,tmpLog):
        # check email address
        toAdd = self.getEmail(taskSpec.userName,taskSpec.vo,tmpLog)
        # read task parameters
        try:
            taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(taskSpec.jediTaskID)
            self.taskParamMap = RefinerUtils.decodeJSON(taskParam)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('task param conversion from json failed with {0}:{1}'.format(errtype.__name__,errvalue))
        if toAdd == None or \
                (self.taskParamMap != None and self.taskParamMap.has_key('noEmail') and self.taskParamMap['noEmail'] == True):
            tmpLog.info('email notification is suppressed')
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
        # make message
        message = \
            """Subject: JEDI notification for JobsetID:{JobsetID} (jediTaskID:{jediTaskID})
From: {fromAdd}
To: {toAdd}

Summary of JobsetID:{JobsetID} (jediTaskID:{jediTaskID})

Created : {creationDate} (UTC)
Ended   : {endTime} (UTC)

Final Status : {status}

Error Dialog : {errorDialog}

Parameters : {params}


PandaMonURL : http://panda.cern.ch/server/pandamon/query?{oldPandaMon}

TaskMonitorURL : https://dashb-atlas-task.cern.ch/templates/task-analysis/#task={taskName}

NewPandaMonURL : https://pandamon.cern.ch/jobinfo?{newPandaMon}

JediMonURL : http://pandamon.cern.ch/jedi/taskinfo?task={jediTaskID}""".format(\
            jediTaskID=taskSpec.jediTaskID,
            JobsetID=taskSpec.reqID,
            fromAdd=fromAdd,
            toAdd=toAdd,
            creationDate=taskSpec.creationDate,
            endTime=taskSpec.endTime,
            status=taskSpec.status,
            errorDialog=taskSpec.errorDialog,
            params=self.taskParamMap['cliParams'],
            taskName=taskSpec.taskName,
            oldPandaMon=urllib.urlencode(urlData),
            newPandaMon=urllib.urlencode(newUrlData),
            )
                    
        # tailer            
        message += \
"""


Report Panda problems of any sort to

  the eGroup for help request
    hn-atlas-dist-analysis-help@cern.ch

  the Savannah for software bug
    https://savannah.cern.ch/projects/panda/
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
        if mailAddrInDB != None and mailAddrInDB.startswith('notsend'):
            notSendMail = True
        # DN is unavilable
        if dn in ['',None]:
            tmpLog.debug("DN is empty")
            notSendMail = True
        else:
            # avoid too frequently lookup
            if dbUptime != None and datetime.datetime.utcnow()-dbUptime < datetime.timedelta(hours=1):
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
                        if mailAddr == None:
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
                    except:
                        if iDDMTry+1 < nTry:
                            tmpLog.debug("sleep for retry {0}/{1}".format(iDDMTry,nTry))
                            time.sleep(10)
                        else:
                            errType,errValue = sys.exc_info()[:2]
                            tmpLog.error("{0}:{1}".format(errType,errValue))
        # not send email
        return retSupp
