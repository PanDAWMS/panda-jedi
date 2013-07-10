from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

from pandajedi.jediddm.DDMInterface import DDMInterface

ddmIF = DDMInterface()
ddmIF.setupInterface()

import multiprocessing

from pandajedi.jediorder import TaskRefiner

parent_conn, child_conn = multiprocessing.Pipe()

taskRefiner = multiprocessing.Process(target=TaskRefiner.launcher,
                                      args=(child_conn,tbIF,ddmIF,
                                            'atlas','test'))
taskRefiner.start()
