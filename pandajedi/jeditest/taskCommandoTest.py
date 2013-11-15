from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

from pandajedi.jediddm.DDMInterface import DDMInterface

ddmIF = DDMInterface()
ddmIF.setupInterface()

import multiprocessing

from pandajedi.jediorder import TaskCommando

parent_conn, child_conn = multiprocessing.Pipe()

try:
    testVO = sys.argv[1]
except:
    testVO = 'any'

try:
    testTaskType = sys.argv[2]
except:
    testTaskType = 'any'

taskCommando = multiprocessing.Process(target=TaskCommando.launcher,
                                       args=(child_conn,tbIF,ddmIF,
                                             testVO,testTaskType))
taskCommando.start()
