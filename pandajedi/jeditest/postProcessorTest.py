from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

from pandajedi.jediddm.DDMInterface import DDMInterface

ddmIF = DDMInterface()
ddmIF.setupInterface()

import multiprocessing

from pandajedi.jediorder import PostProcessor

parent_conn, child_conn = multiprocessing.Pipe()

postProcessor = multiprocessing.Process(target=PostProcessor.launcher,
                                        args=(child_conn,tbIF,ddmIF))
postProcessor.start()
