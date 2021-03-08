import os
import sys
import socket

from pandajedi.jedicore import Interaction
from pandajedi.jedicore import JediCoreUtils
from pandajedi.jediconfig import jedi_config

from .WatchDogBase import WatchDogBase


# base class for typical watchdog (for production and analysis, etc.)
class TypicalWatchDogBase(WatchDogBase):

    # pre-action
    def pre_action(self, tmpLog, vo, prodSourceLabel, pid, *args, **kwargs):
        # rescue picked files
        tmpLog.info('rescue tasks with picked files for vo={0} label={1}'.format(vo,prodSourceLabel))
        tmpRet = self.taskBufferIF.rescuePickedFiles_JEDI(vo,prodSourceLabel,
                                                          jedi_config.watchdog.waitForPicked)
        if tmpRet is None:
            # failed
            tmpLog.error('failed to rescue')
        else:
            tmpLog.info('rescued {0} tasks'.format(tmpRet))

        # reactivate pending tasks
        tmpLog.info('reactivate pending tasks for vo={0} label={1}'.format(vo, prodSourceLabel))
        timeoutForPending = self.taskBufferIF.getConfigValue('watchdog', 'PENDING_TIMEOUT_{}'.format(prodSourceLabel),
                                                             'jedi', vo)
        if timeoutForPending is None:
            if hasattr(jedi_config.watchdog,'timeoutForPendingVoLabel'):
                timeoutForPending = JediCoreUtils.getConfigParam(jedi_config.watchdog.timeoutForPendingVoLabel,vo,prodSourceLabel)
            if timeoutForPending is None:
                timeoutForPending = jedi_config.watchdog.timeoutForPending
            timeoutForPending = int(timeoutForPending) * 24
        tmpRet = self.taskBufferIF.reactivatePendingTasks_JEDI(vo,prodSourceLabel,
                                                               jedi_config.watchdog.waitForPending,
                                                               timeoutForPending)
        if tmpRet is None:
            # failed
            tmpLog.error('failed to reactivate')
        else:
            tmpLog.info('reactivated {0} tasks'.format(tmpRet))
        # unlock tasks
        tmpLog.info('unlock tasks for vo={0} label={1} host={2} pgid={3}'.format(vo,prodSourceLabel,
                                                                                 socket.getfqdn().split('.')[0],
                                                                                 os.getpgrp()))
        tmpRet = self.taskBufferIF.unlockTasks_JEDI(vo,prodSourceLabel,10,
                                                    socket.getfqdn().split('.')[0],
                                                    os.getpgrp())
        if tmpRet is None:
            # failed
            tmpLog.error('failed to unlock')
        else:
            tmpLog.info('unlock {0} tasks'.format(tmpRet))
        # unlock tasks
        tmpLog.info('unlock tasks for vo={0} label={1}'.format(vo,prodSourceLabel))
        tmpRet = self.taskBufferIF.unlockTasks_JEDI(vo,prodSourceLabel,
                                                    jedi_config.watchdog.waitForLocked)
        if tmpRet is None:
            # failed
            tmpLog.error('failed to unlock')
        else:
            tmpLog.info('unlock {0} tasks'.format(tmpRet))
        # restart contents update
        tmpLog.info('restart contents update for vo={0} label={1}'.format(vo,prodSourceLabel))
        tmpRet = self.taskBufferIF.restartTasksForContentsUpdate_JEDI(vo,prodSourceLabel)
        if tmpRet is None:
            # failed
            tmpLog.error('failed to restart')
        else:
            tmpLog.info('restarted {0} tasks'.format(tmpRet))
        # kick exhausted tasks
        tmpLog.info('kick exhausted tasks for vo={0} label={1}'.format(vo,prodSourceLabel))
        tmpRet = self.taskBufferIF.kickExhaustedTasks_JEDI(vo,prodSourceLabel,
                                                           jedi_config.watchdog.waitForExhausted)
        if tmpRet is None:
            # failed
            tmpLog.error('failed to kick')
        else:
            tmpLog.info('kicked {0} tasks'.format(tmpRet))
        # finish tasks when goal is reached
        tmpLog.info('finish achieved tasks for vo={0} label={1}'.format(vo,prodSourceLabel))
        tmpRet = self.taskBufferIF.getAchievedTasks_JEDI(vo,prodSourceLabel,
                                                         jedi_config.watchdog.waitForAchieved)
        if tmpRet is None:
            # failed
            tmpLog.error('failed to finish')
        else:
            for jediTaskID in tmpRet:
                self.taskBufferIF.sendCommandTaskPanda(jediTaskID,'JEDI. Goal reached',True,'finish',comQualifier='soft')
            tmpLog.info('finished {0} tasks'.format(tmpRet))
        # rescue unlocked tasks with picked files
        tmpLog.info('rescue unlocked tasks with picked files for vo={0} label={1}'.format(vo,prodSourceLabel))
        tmpRet = self.taskBufferIF.rescueUnLockedTasksWithPicked_JEDI(vo,prodSourceLabel,60,pid)
        if tmpRet is None:
            # failed
            tmpLog.error('failed to rescue unlocked tasks')
        else:
            tmpLog.info('rescue unlocked {0} tasks'.format(tmpRet))


Interaction.installSC(TypicalWatchDogBase)
