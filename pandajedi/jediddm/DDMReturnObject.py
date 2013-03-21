import sys


# status code
class StatusCode(object):
    def __init__(self,value):
        self.value = value

    def __str__(self):
        return "%s" % self.value

    # comparator
    def __eq__(self,other):
        return self.value == other.value

    def __ne__(self,other):
        return self.value != other.value


    
# mapping to accessors   
statusCodeMap = {'SC_SUCCEEDED': StatusCode(0),
                 'SC_FAILED'   : StatusCode(1),
                 'SC_FATAL'    : StatusCode(2),
                 }


# object class for return
class DDMReturnObject(object):

    # constructor
    def __init__(self):
        self.statusCode  = None
        self.errorValue  = None
        self.returnValue = None



# install the list of status codes to a class
def installSC(cls):
    for sc,val in statusCodeMap.iteritems():
        setattr(cls,sc,val)


# install SCs in this module
installSC(sys.modules[ __name__ ])

        
