from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

from pandajedi.jediddm.DDMInterface import DDMInterface

ddmIF = DDMInterface()
ddmIF.setupInterface()

import multiprocessing

from pandajedi.jediorder import TaskCommando

parent_conn, child_conn = multiprocessing.Pipe()

taskCommando = multiprocessing.Process(target=TaskCommando.launcher,
                                       args=(child_conn,tbIF,ddmIF,
                                             'atlas','test'))
taskCommando.start()
