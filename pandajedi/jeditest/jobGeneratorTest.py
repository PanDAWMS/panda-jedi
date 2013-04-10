from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

from pandajedi.jediddm.DDMInterface import DDMInterface

ddmIF = DDMInterface()
ddmIF.setupInterface()

import multiprocessing

from pandajedi.jediorder import JobGenerator

parent_conn, child_conn = multiprocessing.Pipe()

gen = multiprocessing.Process(target=JobGenerator.launcher,
                              args=(child_conn,tbIF,ddmIF,'atlas','managed'))
gen.daemon = True
gen.start()
