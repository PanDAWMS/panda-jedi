from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

from pandajedi.jediddm.DDMInterface import DDMInterface

ddmIF = DDMInterface()
ddmIF.setupInterface()

import multiprocessing

from pandajedi.jediorder import TaskBroker

parent_conn, child_conn = multiprocessing.Pipe()

taskBroker = multiprocessing.Process(target=TaskBroker.launcher,
                                     args=(child_conn,tbIF,ddmIF,
                                           'atlas','managed'))
taskBroker.start()
