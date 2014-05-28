from pandajedi.jedicore.Interaction import CommandReceiveInterface

# base class to interact with DDM
class DDMClientBase(CommandReceiveInterface):

    # constructor
    def __init__(self,con):
        CommandReceiveInterface.__init__(self,con)



    # list dataset/container
    def listDatasets(self,datasetName,ignorePandaDS=True):
        return self.SC_SUCCEEDED,[datasetName]
