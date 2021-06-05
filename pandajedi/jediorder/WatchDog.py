import os
import sys
import time
import socket
import datetime

from pandajedi.jedicore import Interaction
from pandajedi.jedicore import JediCoreUtils
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.FactoryBase import FactoryBase
from .JediKnight import JediKnight
from pandajedi.jediconfig import jedi_config


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# worker class for watchdog
class WatchDog(JediKnight,FactoryBase):

    # constructor
    def __init__(self, commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels, subStr, period):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.subStr = subStr
        self.period = period
        self.pid = '{0}-{1}-dog'.format(socket.getfqdn().split('.')[0],os.getpid())
        JediKnight.__init__(self, commuChannel, taskBufferIF, ddmIF, logger)
        FactoryBase.__init__(self, self.vos, self.prodSourceLabels, logger,
                             jedi_config.watchdog.modConfig)

    # main
    def start(self):
        # start base classes
        JediKnight.start(self)
        FactoryBase.initializeMods(self, self.taskBufferIF, self.ddmIF)
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
                        # vo/prodSourceLabel specific action
                        impl = self.getImpl(vo, prodSourceLabel, subType=self.subStr)
                        if impl is not None:
                            plugin_name = impl.__class__.__name__
                            tmpLog.info('pre-action for vo={} label={} cls={}'.format(vo, prodSourceLabel, plugin_name))
                            impl.pre_action(tmpLog, vo, prodSourceLabel, self.pid)
                            tmpLog.info('do action for vo={} label={} cls={}'.format(vo, prodSourceLabel, plugin_name))
                            tmpStat = impl.doAction()
                            if tmpStat != Interaction.SC_SUCCEEDED:
                                tmpLog.error(
                                    'failed to run special action for vo={} label={} cls={}'.format(vo,
                                                                                                    prodSourceLabel,
                                                                                                    plugin_name))
                            else:
                                tmpLog.info(
                                    'done for vo={} label={} cls={}'.format(vo, prodSourceLabel, plugin_name))
                tmpLog.info('done')
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed in {0}.start() with {1} {2}'.format(self.__class__.__name__,errtype.__name__,errvalue))
            # sleep if needed
            loopCycle = jedi_config.watchdog.loopCycle if self.period is None else self.period
            timeDelta = datetime.datetime.utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep(max_val=loopCycle)



########## launch

def launcher(commuChannel, taskBufferIF, ddmIF, vos=None, prodSourceLabels=None, subStr=None, period=None):
    p = WatchDog(commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels, subStr, period)
    p.start()
