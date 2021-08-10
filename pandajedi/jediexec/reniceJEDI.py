import subprocess
from pandajedi.jediconfig import jedi_config

proc = subprocess.Popen("ps -l x -U {0}".format(jedi_config.master.uname),
                        shell=True,
                        stdout=subprocess.PIPE,
                        )
stdoutList = str(proc.communicate()[0]).split('\n')
for line in stdoutList:
    try:
        items = line.split()
        if len(items)< 6:
            continue
        pid = items[3]
        nice = int(items[7])
        if 'JediMaster.py' in line and nice>0:
            reniceProc = subprocess.Popen("renice 0 {0}".format(pid),
                                          shell=True,
                                          stdout=subprocess.PIPE,
                                          )
    except Exception:
        pass
