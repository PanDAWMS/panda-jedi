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
        tmpMsg = 'set task_status={0}'.format(taskSpec.status)
        tmpLog.info(tmpMsg)
        tmpLog.sendMsg('set task_status={0}'.format(taskSpec.status),self.msgType)
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
        # kill or kick child tasks
        if taskSpec.status in ['failed','broken','aborted']:
            self.taskBufferIF.killChildTasks_JEDI(taskSpec.jediTaskID,taskSpec.status)
        else:
            self.taskBufferIF.kickChildTasks_JEDI(taskSpec.jediTaskID)
        tmpLog.debug('doBasicPostProcess done with taskStatus={0}'.format(taskSpec.status))
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



    # get task completeness
    def getTaskCompleteness(self,taskSpec):
        nFiles = 0
        nFilesFinished = 0
        totalInputEvents = 0
        totalOkEvents = 0
        for datasetSpec in taskSpec.datasetSpecList:
            if datasetSpec.isMasterInput():
                nFiles += datasetSpec.nFiles
                nFilesFinished += datasetSpec.nFilesFinished
                try:
                    totalInputEvents += datasetSpec.nEvents
                except:
                    pass
                try:
                    totalOkEvents += datasetSpec.nEventsUsed
                except:
                    pass
        # completeness
        if totalInputEvents != 0:
            taskCompleteness = float(totalOkEvents)/float(totalInputEvents)*1000.0
        elif nFiles != 0:
            taskCompleteness = float(nFilesFinished)/float(nFiles)*1000.0
        else:
            taskCompleteness = 0
        return nFiles,nFilesFinished,totalInputEvents,totalOkEvents,taskCompleteness



    # get final task status
    def getFinalTaskStatus(self,taskSpec,checkParent=True,checkGoal=False):
        # count nFiles and nEvents
        nFiles,nFilesFinished,totalInputEvents,totalOkEvents,taskCompleteness = self.getTaskCompleteness(taskSpec)
        # set new task status
        if taskSpec.status == 'tobroken':
            status = 'broken'
        elif taskSpec.status == 'toabort':
            status = 'aborted'
        elif taskSpec.status == 'paused':
            status = 'paused'
        elif nFiles == nFilesFinished and nFiles > 0:
            # check parent status
            if checkParent and not taskSpec.parent_tid in [None,taskSpec.jediTaskID] and \
                    self.taskBufferIF.getTaskStatus_JEDI(taskSpec.parent_tid) != 'done':
                status = 'finished'
            else:
                status = 'done'
        elif nFilesFinished == 0:
            status = 'failed'
        else:
            status = 'finished'
        # task goal
        if taskSpec.goal == None:
            taskGoal = 1000
        else:
            taskGoal = taskSpec.goal
        # fail if goal is not reached
        if taskSpec.failGoalUnreached() and status == 'finished' and \
                (not taskSpec.useExhausted() or (taskSpec.useExhausted() and taskSpec.status in ['passed'])):
            if taskCompleteness < taskGoal:
                status = 'failed'
        # check goal only
        if checkGoal:
            # no goal
            if taskSpec.goal != None and taskCompleteness >= taskGoal:
                return True
            return False
        # return status
        return status



    # pre-check
    def doPreCheck(self,taskSpec,tmpLog):
        # send task to exhausted
        if taskSpec.useExhausted() and not taskSpec.status in ['passed'] \
                and self.getFinalTaskStatus(taskSpec) in ['finished'] \
                and not self.getFinalTaskStatus(taskSpec,checkParent=False) in ['done'] \
                and not self.getFinalTaskStatus(taskSpec,checkGoal=True):
            taskSpec.status = 'exhausted'
            taskSpec.lockedBy = None
            taskSpec.lockedTime = None
            # update task
            tmpLog.info('set task_status={0}'.format(taskSpec.status))
            self.taskBufferIF.updateTask_JEDI(taskSpec,{'jediTaskID':taskSpec.jediTaskID},
                                              updateDEFT=True)
            # kick child tasks
            self.taskBufferIF.kickChildTasks_JEDI(taskSpec.jediTaskID)
            return True
        return False



    
Interaction.installSC(PostProcessorBase)
