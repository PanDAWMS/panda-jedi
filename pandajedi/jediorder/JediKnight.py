import sys

from pandajedi.jedicore import Interaction

class JediKnight (Interaction.CommandReceiveInterface):
    # constructor
    def __init__(self,commuChannel,taskBufferIF,ddmIF,logger):
        Interaction.CommandReceiveInterface.__init__(self,commuChannel)
        self.taskBufferIF = taskBufferIF
        self.ddmIF        = ddmIF
        self.logger       = logger 


    # start communication channel in a thread
    def start(self):
        import threading
        thr = threading.Thread(target=self.startImpl)
        thr.start()
        

    # implementation of start()
    def startImpl(self):
        try:
            Interaction.CommandReceiveInterface.start(self)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            self.logger.error('crashed in JediKnight.startImpl() with %s %s' % (errtype.__name__,errvalue))

            
# install SCs
Interaction.installSC(JediKnight)
