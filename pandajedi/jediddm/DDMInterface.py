from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction
    

# interface to DDM
class DDMInterface:
    
    # constructor
    def __init__(self):
        self.interfaceMap = {}


    # setup interface
    def setupInterface(self):
        # parse config
        for configStr in jedi_config.ddmifconfig.split(','):
            configStr = configStr.strip()
            items = configStr.split(':')
            # check format
            try:
                vo = items[0]
                maxSize = int(items[1])
                moduleName = items[2]
                className  = items[3]
            except:
                # TODO add config error message
                continue
            # add VO interface
            voIF = Interaction.CommandSendInterface(vo,maxSize,moduleName,className)
            voIF.initialize()
            self.interfaceMap[vo] = voIF


    # get interface with VO
    def getInterface(self,vo):
        if self.interfaceMap.has_key(vo):
            return self.interfaceMap[vo]
        # not found
        return None


    
if __name__ == '__main__':
    def dummyClient(dif):
        print "client test"
        dif.getInterface('atlas').test()
        print 'client done'

    dif = DDMInterface()
    dif.setupInterface()
    print "master test"
    atlasIF = dif.getInterface('atlas')
    atlasIF.test()
    print "master done"
    import multiprocessing
    p = multiprocessing.Process(target=dummyClient,
                                args=(dif,))
    p.start()
    p.join()
