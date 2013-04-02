# the master class of JEDI which runs the main process

from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface


class JEDI_Master:
    # constrictor
    def __init__(self):
        pass


    # main loop
    def start(self):
        # setup DDM I/F
        ddmIF = DDMInterface()        
        ddmIF.setupInterface()
        # setup TaskBuffer I/F
        taskBufferIF = JediTaskBufferInterface()
        taskBufferIF.setupInterface()
