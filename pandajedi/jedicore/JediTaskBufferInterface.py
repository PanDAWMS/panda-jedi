from pandajedi.jedicore import Interaction

# interface to JediTaskBuffer
class JediTaskBufferInterface:
    
    # constructor
    def __init__(self):
        self.interface = None


    # setup interface
    def setupInterface(self):
        vo = 'any'
        maxSize = 1
        moduleName = 'JediTaskBuffer'
        className  = 'JediTaskBuffer'
        self.interface = Interaction.CommandSendInterface(vo,maxSize,
                                                          moduleName,
                                                          className)
        self.interface.initialize()
        

    # get interface with VO
    def getInterface(self):
        return self.interface


if __name__ == '__main__':
    def dummyClient(dif,stime):
        print "client test"
        import time
        for i in range(3):
            time.sleep(i*stime)
            try:
                dif.getInterface().testIF()
            except:
                print "exp"
        print 'client done'

    dif = JediTaskBufferInterface()
    dif.setupInterface()
    print "master test"
    atlasIF = dif.getInterface()
    atlasIF.testIF()
    print "master done"
    import multiprocessing
    pList = []
    for i in range(5):
        p = multiprocessing.Process(target=dummyClient,
                                    args=(dif,i))
        pList.append(p)
        p.start()
