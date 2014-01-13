import re
import sys
import uuid
import copy
import time
import types
import smtplib
import datetime

from pandajedi.jedicore import Interaction
from pandaserver.config import panda_config

# port for SMTP server
smtpPortList = [25,587]


# wrapper to patch smtplib.stderr to send debug info to logger 
class StderrLogger(object):
    def __init__(self,tmpLog):
        self.tmpLog = tmpLog
    def write(self,message):
        message = message.strip()
        if message != '':
            self.tmpLog.debug(message)



# base class for post process
class PostProcessorBase (object):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.siteMapper = taskBufferIF.getSiteMapper()



    # basic post procedure
    def doBasicPostProcess(self,taskSpec,tmpLog):
        # loop over all datasets
        nFiles = 0
        nFilesFinished = 0
        for datasetSpec in taskSpec.datasetSpecList:
            # update dataset
            if datasetSpec.type in ['output','log','lib']:
                datasetSpec.status = 'done'
                self.taskBufferIF.updateDataset_JEDI(datasetSpec,{'datasetID':datasetSpec.datasetID,
                                                                  'jediTaskID':datasetSpec.jediTaskID})
            # count nFiles
            if datasetSpec.isMaster():
                nFiles += datasetSpec.nFiles
                nFilesFinished += datasetSpec.nFilesFinished
        # update task status
        taskSpec.lockedBy = None
        if taskSpec.status == 'tobroken':
            taskSpec.status = 'broken'
        elif nFiles == nFilesFinished:
            taskSpec.status = 'finished'
        elif nFilesFinished == 0:
            taskSpec.status = 'failed'
            # set dialog for preprocess
            if taskSpec.usePrePro() and not taskSpec.checkPreProcessed():
                taskSpec.setErrDiag('Preprocessing step failed',True)
        else:
            taskSpec.status = 'partial'
        # end time
        taskSpec.endTime = datetime.datetime.utcnow()
        # update task
        self.taskBufferIF.updateTask_JEDI(taskSpec,{'jediTaskID':taskSpec.jediTaskID})    
        tmpLog.info('doBasicPostProcess set taskStatus={0}'.format(taskSpec.status))
        return



    # final procedure
    def doFinalProcedure(self,taskSpec,tmpLog):
        return self.SC_SUCCEEDED

    

    # send mail
    def sendMail(self,jediTaskID,fromAdd,toAdd,msgBody,nTry,fileBackUp,tmpLog):
        tmpLog.debug("sending notification to {0}\n{1}".format(toAdd,msgBody))
        for iTry in range(nTry):
            try:
                org_smtpstderr = smtplib.stderr
                smtplib.stderr = StderrLogger(tmpLog)
                smtpPort = smtpPortList[iTry % len(smtpPortList)]
                server = smtplib.SMTP(panda_config.emailSMTPsrv,smtpPort)
                server.set_debuglevel(1)
                server.ehlo()
                server.starttls()
                out = server.sendmail(fromAdd,toAdd,msgBody)
                tmpLog.debug(str(out))
                server.quit()
                break
            except:
                errType,errValue = sys.exc_info()[:2]
                if iTry+1 < nTry:
                    # sleep for retry
                    tmpLog.debug("sleep {0} due to {1}:{2}".format(iTry,errType,errValue))
                    time.sleep(30)
                else:
                    tmpLog.error("failed to send notification with {0}:{1}".format(errType,errValue))
                    if fileBackUp:
                        # write to file which is processed in add.py
                        mailFile = '{0}/jmail_{1}_{2}' % (panda_config.logdir,jediTaskID,commands.getoutput('uuidgen'))
                        oMail = open(mailFile,"w")
                        oMail.write(str(jediTaskID)+'\n'+toAdd+'\n'+msgBody)
                        oMail.close()
                break
        try:
            smtplib.stderr = org_smtpstderr
        except:
            pass



    # return email sender
    def senderAddress(self):
        return panda_config.emailSender



    
Interaction.installSC(PostProcessorBase)
