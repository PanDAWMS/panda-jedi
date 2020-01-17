from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin


# Tape carousel message processing plugin
class TapeCarouselMsgProcPlugin(BaseMsgProcPlugin):

    def process(self, msg_obj):
        # TODO
        # update dataset table DB
        # update file status
        self.tbIF.updateAboutIddsMsgTapeCarousel_JEDI()
        pass
