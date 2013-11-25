import re
import sys
import time
import datetime

from pandajedi.jedicore import Interaction
from PostProcessorBase import PostProcessorBase



# post processor for ATLAS production
class AtlasAnalPostProcessor (PostProcessorBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        PostProcessorBase.__init__(self,taskBufferIF,ddmIF)


    # main
    def doPostProcess(self,taskSpec,tmpLog):
        # freeze datasets
        try:
            # get DDM I/F
            ddmIF = self.ddmIF.getInterface(taskSpec.vo)
            # loop over all datasets
            for datasetSpec in taskSpec.datasetSpecList:
                # ignore template
                if datasetSpec.type.startswith('tmpl_'):
                    continue
                # only output or log datasets
                if not datasetSpec.type.endswith('log') and not datasetSpec.type.endswith('output'):
                    continue
                # only user or panda dataset
                if not datasetSpec.datasetName.startswith('user') and not datasetSpec.datasetName.startswith('panda'):
                    continue
                # freeze datasets
                tmpLog.info('freeze datasetID={0}:Name={1}'.format(datasetSpec.datasetID,datasetSpec.datasetName))
                ddmIF.freezeDataset(datasetSpec.datasetName)
                # update dataset
                datasetSpec.state = 'closed'
                datasetSpec.stateCheckTime = datetime.datetime.utcnow()
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('failed to freeze datasets with {0}:{1}'.format(errtype.__name__,errvalue))
        retVal = self.SC_SUCCEEDED
        try:
            self.doBasicPostProcess(taskSpec,tmpLog)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doBasicPostProcess failed with {0}:{1}'.format(errtype.__name__,errvalue))
            retVal = self.SC_FAILED
        return retVal



    # final procedure
    def doFinalProcedre(self,taskSpec,tmpLog):
        # check email address
        toAdd = self.getEmail(taskSpec.userName,taskSpec.vo,tmpLog)
        if toAdd == None:
            tmpLog.info('email notification is suppressed')
        else:
            # send email notification
            fromAdd = self.senderAddress()
            msgBody = self.composeMessage(taskSpec,fromAdd,toAdd)
            self.sendMail(taskSpec.jediTaskID,fromAdd,toAdd,msgBody,3,False,tmpLog)
        return self.SC_SUCCEEDED
            
    

    # compose mail message
    def composeMessage(self,taskSpec,fromAdd,toAdd):
        message = \
            """Subject: JEDI notification for jediTaskID : {jediTaskID}
From: {fromAdd}
To: {toAdd}

Summary of jediTaskID : {jediTaskID}

Created : {creationDate} (UTC)
Ended   : {endTime} (UTC)

Final Status : {status}


Parameters : {params}


PandaMonURL : http://pandamon.cern.ch/jedi/taskinfo?task={jediTaskID}""".format(\
            jediTaskID=taskSpec.jediTaskID,
            fromAdd=fromAdd,
            toAdd=toAdd,
            creationDate=taskSpec.creationDate,
            endTime=taskSpec.endTime,
            status=taskSpec.status,
            params=None,
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
        mailAddr,dn = self.taskBufferIF.getEmailAddr(userName,withDN=True)
        if mailAddr == 'notsend':
            tmpLog.debug("email from MetaDB : {0}".format(mailAddr))
            return retSupp
        if dn in ['',None]:
            tmpLog.debug("DN is empty")
            return retSupp
        # get email from DQ2
        realDN = re.sub('/CN=limited proxy','',dn)
        realDN = re.sub('(/CN=proxy)+','',realDN)
        tmpLog.debug("getting email using dq2Info.finger({0})".format(realDN))
        nTry = 3
        for iDDMTry in range(nTry):
            try:
                userInfo = self.ddmIF.getInterface(vo).finger(realDN)
                mailAddr = userInfo['email']
                tmpLog.debug("email from DQ2 : {0}".format(mailAddr))
                return mailAddr
            except:
                if iDDMTry+1 < nTry:
                    tmpLog.debug("sleep for retry {0}/{1}".format(iDDMTry,nTry))
                    time.sleep(10)
                else:
                    errType,errValue = sys.exc_info()[:2]
                    tmpLog.error("{0}:{1}".format(errType,errValue))
        return retSupp
