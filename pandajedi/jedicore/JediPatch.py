import socket
import errno
import time

import multiprocessing.connection as MP


# set timeout to socket
def SocketClient(address):
    '''
    Return a connection object connected to the socket given by `address`
    '''
    family = MP.address_type(address)
    s = socket.socket( getattr(socket, family) )
    defTimeOut = socket.getdefaulttimeout()
    s.settimeout(30)
    t = MP._init_timeout()

    while 1:
        try:
            s.connect(address)
        except socket.error as e:
            if e.args[0] != errno.ECONNREFUSED or MP._check_timeout(t):
                MP.debug('failed to connect to address %s', address)
                raise
            time.sleep(0.01)
        else:
            break
    else:
        raise
    s.settimeout(defTimeOut)

    fd = MP.duplicate(s.fileno())
    conn = MP._multiprocessing.Connection(fd)
    s.close()
    return conn


#MP.SocketClient = SocketClient
