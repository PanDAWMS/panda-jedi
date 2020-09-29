
from pandacommon.pandamsgbkr.msg_processor import SimpleMsgProcPluginBase

from pandajedi.jedicore.JediTaskBuffer import JediTaskBuffer

# Base simple message processing plugin
class BaseMsgProcPlugin(SimpleMsgProcPluginBase):

    def initialize(self):
        """
        initialize plugin instance, run once before loop in thread
        """
        # set up JEDI TaskBuffer interface
        self.tbIF= JediTaskBuffer(None)

    def process(self, msg_obj):
        """
        process the message
        Get msg_obj from the incoming MQ (if any; otherwise msg_obj is None)
        Returned value will be sent to the outgoing MQ (if any)
        """
        pass
