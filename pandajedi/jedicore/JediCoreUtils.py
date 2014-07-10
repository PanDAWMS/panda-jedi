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
