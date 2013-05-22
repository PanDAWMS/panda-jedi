import os
import sys
import time
import socket
import datetime

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from JediKnight import JediKnight
from pandajedi.jediconfig import jedi_config


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# worker class for watchdog 
class WatchDog (JediKnight):

    # constructor
    def __init__(self,commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.pid = '{0}-{1}-dog'.format(socket.getfqdn().split('.')[0],os.getpid())
        JediKnight.__init__(self,commuChannel,taskBufferIF,ddmIF,logger)


    # main
    def start(self):
        # start base classes
        JediKnight.start(self)
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
                            tmpLog.error('failed')
                        else:
                            tmpLog.info('rescued {0} tasks'.format(tmpRet))
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
        


########## launch 
                
def launcher(commuChannel,taskBufferIF,ddmIF,vo=None,prodSourceLabel=None):
    p = WatchDog(commuChannel,taskBufferIF,ddmIF,vo,prodSourceLabel)
    p.start()
