import socket
import select
import errno
from select import POLLIN, POLLOUT, POLLERR, POLLHUP, POLLNVAL
from ccstruct import *
from collections import deque
# define channel types
CH_TYPE_PIPE  = 0
CH_TYPE_TCP   = 1
CH_TYPE_UDP_S = 2  # send-only UDP
CH_TYPE_UDP_R = 3  # recv-only UDP
# define channel state
CH_STATE_NOTCONN = 0
CH_STATE_PRECONN = 1     # to handle EINPROGRESS after non-blocking connect()
CH_STATE_CONNECT = 2     # TCP connected
CH_STATE_LISTEN = 3      # listening
CH_STATE_CLOSE = 4       # Channel will be closed
# define channel eventmask
CH_READ  = POLLIN
CH_WRITE = POLLOUT
CH_ERROR = POLLERR


class Buffer:
    """ Message buffer
        Each buffer contains the serialized wait-to-send/wait-to-receive ccfd packet
    """
    def __init__(self, data=None):
        if not data:
            self.data = bytearray()   #message itself
            self.length = 0       # length of message to receive
        else:
            self.data = data
            self.length = len(data)
        self.pos = 0       # Position of read/write

class MsgQueue:
    def __init__(self):
        self.messages = deque()

    def enqueue(self, buf):
        self.messages.append(buf)

    def dequeue(self):
        return self.messages.popleft()

    def size(self):
        return len(self.messages)

    def is_empty(self):
        return (not self.size())

    def first(self):
        """ Return the very first message in the queue
        """
        if self.is_empty():
            return None
        else:
            return self.messages[0]

    def last(self):
        """ Return most recent message being worked on
        """
        if self.is_empty():
            return None
        else:
            return self.messages[self.size()-1]


class Channel:
    """ Each point-to-point communication is performed via a channel
    """
    def __init__(self, handle=-1, chan_t=CH_TYPE_TCP, remote=None):
        self.chid   = -1
        self.handle = handle    # handle of the channel (fd)
        self.chan_t = chan_t    # channel type
        self.remote = remote    # remote address tuple (ip, port)
        self.state = CH_STATE_NOTCONN
        self.sendq = MsgQueue()    # send queue (first-in-first-out)
        self.recvq = MsgQueue()    # recv queue (first-in-first-out)
        self.eventmask = 0      # events on the handle

    def set_channel_id(self, id):
        self.chid = id

    def send(self, data):
        self.sendq.enqueue(Buffer(data))
        return

    def receive(self):
        if not self.recvq.is_empty() and \
                self.recvq.first().length == self.recvq.first().pos:
            return self.recvq.dequeue().data
        else:
            self.eventmask &= CH_READ
            return None

    def doread(self):
        """ read from the handle and store messages to recvq
        """
        if self.recvq.is_empty():
            self.recvq.enqueue(Buffer())

        # Case 1) Read from pipe
        if self.chan_t == CH_TYPE_PIPE:
            data = self.handle.recv()
            self.recvq.last().data += data
            self.recvq.last().length = len(data)
            self.recvq.last().pos += len(data)
            self.eventmask |= CH_READ
            return

        # Case 2) Try to read from socket
        # Start with header
        if self.recvq.last().length == 0:
            self.recvq.last().length = hdrlen()
            self.recvq.last().pos = 0
        # A complete message is here, set ready to process.
        if self.recvq.last().length == self.recvq.last().pos:
            self.eventmask |= CH_READ
            return

        try:
            data, addr = self.handle.recvfrom(self.recvq.last().length-self.recvq.last().pos,
                                                socket.MSG_DONTWAIT)
        except Exception as details:
            print("Channel.doread() cannot recvfrom(). Error: %s" % (details, ))
            self.eventmask = CH_ERROR
            return
        if len(data) == 0:
            print("Channel.doread() didn't read anything, something is wrong")
            self.eventmask = CH_ERROR
            return
        self.recvq.last().data += data
        self.recvq.last().pos += len(data)

        # If a complete header is received, determine how big the message is
        if (self.recvq.last().length == hdrlen() and
            self.recvq.last().length == self.recvq.last().pos):
            hdr = CCHeader()
            hdr.parse(self.recvq.last().data)
            # Message has a body
            if hdr.length != 0:
                self.recvq.last().length += hdr.length

        # If a complete message is received, mark the channel as readable
        if self.recvq.last().length == self.recvq.last().pos:
            print("[Channel] recvq of channel %s is readable" % (self.chid))
            self.eventmask |= CH_READ

        return

    def dowrite(self):
        """ write messages in send queue to socket
        """
        if self.sendq.is_empty():
            return
        offset = self.sendq.first().pos
        if self.chan_t == CH_TYPE_UDP_S:
            cc = self.handle.sendto(self.sendq.first().data[offset:], self.remote)
        else:
            cc = self.handle.send(self.sendq.first().data[offset:])  # FIXME: am I non-blocking?

        if cc < 0:
            self.eventmask = CH_ERROR
            return

        self.sendq.first().pos += cc;
        # remove message if sent was complete
        if self.sendq.first().pos == self.sendq.first().length:
            self.sendq.dequeue()
        return


def open_pipe_channel(chans, s):
    chid = find_a_free_channel(chans)
    ch = Channel(s, CH_TYPE_PIPE)
    ch.state = CH_STATE_CONNECT
    ch.set_channel_id(chid)
    chans[chid] = ch
    return chid

def open_listen_channel(chans, port):
    chid = find_a_free_channel(chans)
    # Setup TCP socket for receiving requests
    s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0)
    s.bind(('', port))  # bind (PORT+1), specifically for sharing
    s.listen(10)
    ch = Channel(s, CH_TYPE_TCP)
    ch.state = CH_STATE_LISTEN
    ch.set_channel_id(chid)
    chans[chid] = ch
    return chid

def open_inconn_channel(chans, s):
    chid = find_a_free_channel(chans)
    s.setblocking(0)  # Set it non-blocking
    ch = Channel(s, CH_TYPE_TCP, s.getpeername())
    ch.state = CH_STATE_CONNECT
    ch.set_channel_id(chid)
    chans[chid] = ch
    return chid

def open_outconn_channel(chans, addr, port):
    """ Open a channel for outgoing connection
        Need to handle non-blocking connect()
    """
    chid = find_a_free_channel(chans)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setblocking(0)
    err = s.connect_ex((addr, port))
    ch = Channel(s, CH_TYPE_TCP, remote=(addr, port))
    if not err:
        # Connected successfully
        ch.state = CH_STATE_CONNECT
        ch.set_channel_id(chid)
        chans[chid] = ch
        return chid
    elif err == errno.EINPROGRESS:
        # Connection in progress
        ch.state = CH_STATE_PRECONN
        ch.set_channel_id(chid)
        chans[chid] = ch
        return chid
    else:
        # other errors
        print("Cannot open outgoing channel to (%s, %d): %s" % (addr, port, errno.errorcode[err]))
        return -1

def open_data_channel(chans, addr, port):
    chid = find_a_free_channel(chans)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setblocking(0)
    ch = Channel(s, CH_TYPE_UDP_S, (addr, port))
    ch.state = CH_STATE_CONNECT
    ch.set_channel_id(chid)
    chans[chid] = ch
    return chid

def close_channel(chans, chid):
    # properly close connections
    print("[Channel] Closing channel %d" % (chid, ))
    try:
        chans[chid].handle.close()
    except Exception as details:
        print("Error: close_channel: %s" % (details, ))
    del chans[chid]

def find_a_free_channel(chans):
    """ Find a free channel, return the id
    """
    i = 0
    while True:
        if i not in chans.keys():
            break
        i += 1
    return i

def poll_channels(chans):
    """ Poll channels in the list
    """
    nready = 0
    poller = select.poll()
    fd_array = {}  # map fd to channel index
    # Register channel fd's to poll
    for i in list(chans):
        ch = chans[i]
        if (ch.state == CH_STATE_CLOSE and ch.sendq.is_empty()) \
                or ch.eventmask == CH_ERROR:
            # print("Channel %d is in error, skip it" % (i,))
            close_channel(chans, i)
            continue
        ch.eventmask = 0  # Reset eventmask before each round of polling
        if ch.chan_t == CH_TYPE_UDP_S:
            poller.register(ch.handle.fileno(), POLLOUT)
            #print("Registered channel %d for POLLOUT" % (i, ))
            fd_array[ch.handle.fileno()] = i
        if ch.chan_t == CH_TYPE_UDP_R:
            poller.register(ch.handle.fileno(), POLLIN)
            #print("Registered channel %d for POLLIN" % (i, ))
            fd_array[ch.handle.fileno()] = i
        if ch.chan_t == CH_TYPE_TCP:
            if ch.state == CH_STATE_PRECONN:
                poller.register(ch.handle.fileno(), POLLOUT)
                # print("Registered channel %d for POLLOUT" % (i, ))
                fd_array[ch.handle.fileno()] = i
            else:
                poller.register(ch.handle.fileno(), POLLIN | POLLOUT)
                # print("Registered channel %d for POLLIN and POLLOUT" % (i, ))
                fd_array[ch.handle.fileno()] = i
        if ch.chan_t == CH_TYPE_PIPE:
            poller.register(ch.handle.fileno(), POLLIN)
            # print("Registered channel %d for POLLIN" % (i, ))
            fd_array[ch.handle.fileno()] = i

    for fd, event in poller.poll(1):
        i = fd_array[fd]
        if event & (POLLHUP | POLLERR | POLLNVAL):
            chans[i].eventmask = CH_ERROR
            continue
        if chans[i].state == CH_STATE_PRECONN:
            # A INPROGRESS connection is already connected
            if event & POLLOUT:
                chans[i].state = CH_STATE_CONNECT
                chans[i].eventmask |= CH_WRITE
        else:
            if event & POLLIN:
                if chans[i].state == CH_STATE_LISTEN:
                    print('[Channel] New connection arrive on channel %d' % (i, ))
                    chans[i].eventmask |= CH_READ
                else:
                    if chans[i].chan_t != CH_TYPE_UDP_S and chans[i].chan_t != CH_TYPE_PIPE:
                        print('[Channel] POLLIN on channel %d' % (i, ))
                    chans[i].doread()  # try our best to receive
            if not chans[i].sendq.is_empty() \
                    and (event & POLLOUT):
                if chans[i].chan_t != CH_TYPE_UDP_S:
                    print('[Channel] POLLOUT on channel %d' % (i, ))
                chans[i].dowrite() # try out best to send



