import os
import sys
import time
import socket
import datetime

from pandajedi.jedicore import Interaction
from pandajedi.jedicore import JediCoreUtils
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.FactoryBase import FactoryBase
from JediKnight import JediKnight
from pandajedi.jediconfig import jedi_config


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# worker class for watchdog 
class WatchDog (JediKnight,FactoryBase):

    # constructor
    def __init__(self,commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.pid = '{0}-{1}-dog'.format(socket.getfqdn().split('.')[0],os.getpid())
        JediKnight.__init__(self,commuChannel,taskBufferIF,ddmIF,logger)
        FactoryBase.__init__(self,self.vos,self.prodSourceLabels,logger,
                             jedi_config.watchdog.modConfig)
        

    # main
    def start(self):
        # start base classes
        JediKnight.start(self)
        FactoryBase.initializeMods(self,self.taskBufferIF,self.ddmIF)
        # go into main loop
        while True:
            startTime = datetime.datetime.utcnow()
            try:
                # get logger
                tmpLog = MsgWrapper(logger)
                tmpLog.info('start')
                # loop over all vos
                for vo in self.vos:
                    # loop over all sourceLabels
                    for prodSourceLabel in self.prodSourceLabels:
                        # rescue picked files
                        tmpLog.info('rescue tasks with picked files for vo={0} label={1}'.format(vo,prodSourceLabel)) 
                        tmpRet = self.taskBufferIF.rescuePickedFiles_JEDI(vo,prodSourceLabel,
                                                                          jedi_config.watchdog.waitForPicked)
                        if tmpRet == None:
                            # failed
                            tmpLog.error('failed to rescue')
                        else:
                            tmpLog.info('rescued {0} tasks'.format(tmpRet))

                        # reactivate pending tasks
                        tmpLog.info('reactivate pending tasks for vo={0} label={1}'.format(vo,prodSourceLabel)) 
                        timeoutForPending = None
                        if hasattr(jedi_config.watchdog,'timeoutForPendingVoLabel'): 
                            timeoutForPending = JediCoreUtils.getConfigParam(jedi_config.watchdog.timeoutForPendingVoLabel,vo,prodSourceLabel)
                        if timeoutForPending == None:
                            timeoutForPending = jedi_config.watchdog.timeoutForPending
                        timeoutForPending = int(timeoutForPending)    
                        tmpRet = self.taskBufferIF.reactivatePendingTasks_JEDI(vo,prodSourceLabel,
                                                                               jedi_config.watchdog.waitForPending,
                                                                               timeoutForPending)
                        if tmpRet == None:
                            # failed
                            tmpLog.error('failed to reactivate')
                        else:
                            tmpLog.info('reactivated {0} tasks'.format(tmpRet))
                        # unlock tasks
                        tmpLog.info('unlock tasks for vo={0} label={1}'.format(vo,prodSourceLabel)) 
                        tmpRet = self.taskBufferIF.unlockTasks_JEDI(vo,prodSourceLabel,
                                                                    jedi_config.watchdog.waitForLocked)
                        if tmpRet == None:
                            # failed
                            tmpLog.error('failed to unlock')
                        else:
                            tmpLog.info('unlock {0} tasks'.format(tmpRet))
                        # restart contents update
                        tmpLog.info('restart contents update for vo={0} label={1}'.format(vo,prodSourceLabel)) 
                        tmpRet = self.taskBufferIF.restartTasksForContentsUpdate_JEDI(vo,prodSourceLabel)
                        if tmpRet == None:
                            # failed
                            tmpLog.error('failed to restart')
                        else:
                            tmpLog.info('restarted {0} tasks'.format(tmpRet))
                        # kick exhausted tasks
                        tmpLog.info('kick exhausted tasks for vo={0} label={1}'.format(vo,prodSourceLabel)) 
                        tmpRet = self.taskBufferIF.kickExhaustedTasks_JEDI(vo,prodSourceLabel,
                                                                           jedi_config.watchdog.waitForExhausted)
                        if tmpRet == None:
                            # failed
                            tmpLog.error('failed to kick')
                        else:
                            tmpLog.info('kicked {0} tasks'.format(tmpRet))
                        # finish tasks when goal is reached
                        tmpLog.info('finish achieved tasks for vo={0} label={1}'.format(vo,prodSourceLabel)) 
                        tmpRet = self.taskBufferIF.getAchievedTasks_JEDI(vo,prodSourceLabel,
                                                                         jedi_config.watchdog.waitForAchieved)
                        if tmpRet == None:
                            # failed
                            tmpLog.error('failed to finish')
                        else:
                            for jediTaskID in tmpRet:
                                self.taskBufferIF.sendCommandTaskPanda(jediTaskID,'JEDI. Goal reached',True,'finish',comQualifier='soft')
                            tmpLog.info('finished {0} tasks'.format(tmpRet))
                        # vo/prodSourceLabel specific action
                        impl = self.getImpl(vo,prodSourceLabel)
                        if impl != None:
                            tmpLog.info('special action for vo={0} label={1} with {2}'.format(vo,prodSourceLabel,impl.__class__.__name__))
                            tmpStat = impl.doAction()
                            if tmpStat !=  Interaction.SC_SUCCEEDED:
                                tmpLog.error('failed to run special acction for vo={0} label={1}'.format(vo,prodSourceLabel))
                            else:
                                tmpLog.info('done for vo={0} label={1}'.format(vo,prodSourceLabel))
                tmpLog.info('done')
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed in {0}.start() with {1} {2}'.format(self.__class__.__name__,errtype.__name__,errvalue))
            # sleep if needed
            loopCycle = jedi_config.watchdog.loopCycle
            timeDelta = datetime.datetime.utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep()



########## launch 
                
def launcher(commuChannel,taskBufferIF,ddmIF,vos=None,prodSourceLabels=None):
    p = WatchDog(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels)
    p.start()
