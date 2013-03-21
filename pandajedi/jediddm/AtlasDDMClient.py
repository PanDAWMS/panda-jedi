from DDMClientBase import DDMClientBase

class AtlasDDMClient(DDMClientBase):

    # constructor
    def __init__(self,con):
        # initialize base class
        DDMClientBase.__init__(self,con)


    # method
    def test(self):
        print "aaaa"
        return self.SC_SUCCEEDED
