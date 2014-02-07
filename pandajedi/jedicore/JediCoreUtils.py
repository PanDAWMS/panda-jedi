# get effective file size
def getEffectiveFileSize(fsize,startEvent,endEvent,nEvents):
    if fsize in [None,0]:
        # use dummy size for pseudo input
        effectiveFsize = 1024 * 1024
    elif nEvents != None and startEvent != None and endEvent != None:
        # take event range into account 
        effectiveFsize = long(float(fsize)*float(endEvent-startEvent+1)/float(nEvents))
    else:
        effectiveFsize = fsize
    # in MB
    effectiveFsize = float(effectiveFsize) / 1024.0 / 1024.0
    # return
    return effectiveFsize
