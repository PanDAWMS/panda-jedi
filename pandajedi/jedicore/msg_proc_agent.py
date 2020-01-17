
from pandacommon.pandamsgbkr.msg_processor import MsgProcAgentBase


# Main message processing agent
class MsgProcAgent(MsgProcAgentBase):

    def __init__(self, config_file, **kwargs):
        MsgProcAgentBase.__init__(self, config_file, **kwargs)
