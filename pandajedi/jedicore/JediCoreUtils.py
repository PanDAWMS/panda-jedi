import os


# get effective file size
def getEffectiveFileSize(fsize,startEvent,endEvent,nEvents):
    inMB = 1024 * 1024
    if fsize in [None,0]:
        # use dummy size for pseudo input
        effectiveFsize = inMB
    elif nEvents != None and startEvent != None and endEvent != None:
        # take event range into account 
        effectiveFsize = long(float(fsize)*float(endEvent-startEvent+1)/float(nEvents))
    else:
        effectiveFsize = fsize
    # use dummy size if input is too small
    if effectiveFsize == 0:
        effectiveFsize = inMB
    # in MB
    effectiveFsize = float(effectiveFsize) / inMB 
    # return
    return effectiveFsize



# get effective number of events                                                                                                                          
def getEffectiveNumEvents(startEvent,endEvent,nEvents):
    if endEvent != None and startEvent != None:
        evtCounts = endEvent-startEvent+1
        if evtCounts > 0:
            return evtCounts
        return 1
    if nEvents != None and nEvents > 0:
        return nEvents
    return 1



# get memory usage
def getMemoryUsage():
    try:
        t = open('/proc/{0}/status'.format(os.getpid()))
        v = t.read()
        t.close()
        value = 0
        for line in v.split('\n'):
            if line.startswith('VmRSS'):
                items = line.split()
                value = int(items[1])
                if items[2] in ['kB','KB']:
                    value /= 1024
                elif items[2] in ['mB','MB']:
                    pass
                break
        return value
    except:
        return None



# check process
def checkProcess(pid):
    return os.path.exists('/proc/{0}/status'.format(pid))



# offset for walltime
wallTimeOffset = 10*60

# add offset to walltime
def addOffsetToWalltime(oldWalltime):
    if oldWalltime > 0:
        # add offset of 10min
        oldWalltime += wallTimeOffset
    return oldWalltime


# reduce offset from walltime
def reduceOffsetFromWalltime(oldWalltime):
    if oldWalltime > 0:
        # add offset of 10min
        oldWalltime -= wallTimeOffset
        if oldWalltime < 0:
            oldWalltime = 0
    return oldWalltime



# get config param for vo and prodSourceLabel
def getConfigParam(configStr,vo,sourceLabel):
    try:
        for tmpConfigStr in configStr.split(','):
            items = configStr.split(':')
            vos          = items[0].split('|')
            sourceLabels = items[1].split('|')
            if not vo in ['','any'] and \
                    not vo in vos and \
                    not None in vos and \
                    not 'any' in vos and \
                    not '' in vos:
                continue
            if not sourceLabel in ['','any'] and \
                    not sourceLabel in sourceLabels and \
                    not None in sourceLabels and \
                    not 'any' in sourceLabels and \
                    not '' in sourceLabels:
                continue
            return ','.join(items[2:])
    except:
        pass
    return None
