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
        self.msgType = 'postprocessor'
        self.refresh()


    # refresh 
    def refresh(self):
        self.siteMapper = self.taskBufferIF.getSiteMapper()



    # basic post procedure
    def doBasicPostProcess(self,taskSpec,tmpLog):
        # update task status
        taskSpec.lockedBy = None
        taskSpec.status = self.getFinalTaskStatus(taskSpec)
        if taskSpec.status == 'failed':
            # set dialog for preprocess
            if taskSpec.usePrePro() and not taskSpec.checkPreProcessed():
                taskSpec.setErrDiag('Preprocessing step failed',True)
        tmpLog.sendMsg('set task.status={0}'.format(taskSpec.status),self.msgType)
        # update dataset
        for datasetSpec in taskSpec.datasetSpecList:
            if taskSpec.status in ['failed','broken','aborted']:
                datasetSpec.status = 'failed'
            else:
                # set dataset status
                if datasetSpec.type in ['output','log','lib']:
                    # normal output datasets
                    if datasetSpec.nFiles > datasetSpec.nFilesFinished:
                        datasetSpec.status = 'finished'
                    else:
                        datasetSpec.status = 'done'
                elif datasetSpec.type.startswith('trn_') or datasetSpec.type.startswith('tmpl_'):
                    # set done for template or transient datasets
                    datasetSpec.status = 'done'
                else:
                    # not for input
                    continue
            # set nFiles
            if datasetSpec.type in ['output','log','lib']:
                datasetSpec.nFiles = datasetSpec.nFilesFinished
            self.taskBufferIF.updateDataset_JEDI(datasetSpec,{'datasetID':datasetSpec.datasetID,
                                                              'jediTaskID':datasetSpec.jediTaskID})
        # end time
        taskSpec.endTime = datetime.datetime.utcnow()
        # update task
        self.taskBufferIF.updateTask_JEDI(taskSpec,{'jediTaskID':taskSpec.jediTaskID},
                                          updateDEFT=True)    
        # kill child tasks
        if taskSpec.status in ['failed','broken','aborted']:
            self.taskBufferIF.killChildTasks_JEDI(taskSpec.jediTaskID,taskSpec.status)
        tmpLog.info('doBasicPostProcess done with taskStatus={0}'.format(taskSpec.status))
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



    # get final task status
    def getFinalTaskStatus(self,taskSpec):
        # count nFiles
        nFiles = 0
        nFilesFinished = 0
        for datasetSpec in taskSpec.datasetSpecList:
            if datasetSpec.isMasterInput():
                nFiles += datasetSpec.nFiles
                nFilesFinished += datasetSpec.nFilesFinished
        if taskSpec.status == 'tobroken':
            status = 'broken'
        elif taskSpec.status == 'toabort':
            status = 'aborted'
        elif nFiles == nFilesFinished:
            status = 'done'
        elif nFilesFinished == 0:
            status = 'failed'
        else:
            status = 'finished'
        return status



    # pre-check
    def doPreCheck(self,taskSpec,tmpLog):
        # send task to exhausted
        if taskSpec.useExhausted() and not taskSpec.status in ['passed'] \
                and self.getFinalTaskStatus(taskSpec) in ['finished']:
            taskSpec.status = 'exhausted'
            taskSpec.lockedBy = None
            taskSpec.lockedTime = None
            # update task
            tmpLog.info('set task.status={0}'.format(taskSpec.status))
            self.taskBufferIF.updateTask_JEDI(taskSpec,{'jediTaskID':taskSpec.jediTaskID})
            return True
        return False



    
Interaction.installSC(PostProcessorBase)
