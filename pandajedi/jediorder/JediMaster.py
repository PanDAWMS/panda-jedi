# the master class of JEDI which runs the main process

from  pandajedi.jediddm import DDMInterface

class JEDI_Master:
    # constrictor
    def __init__(self):
        pass


    # main loop
    def start(self):
        # setup DDM I/F
        ddmIF = DDMInterface.DDMInterface()        
        ddmIF.setupInterface()
