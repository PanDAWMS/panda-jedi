import sys
import types

from pandajedi.jedicore.Interaction import CommandReceiveInterface

# base class to interact with DDM
class DDMClientBase(CommandReceiveInterface):

    # constructor
    def __init__(self,con):
        CommandReceiveInterface.__init__(self,con)
