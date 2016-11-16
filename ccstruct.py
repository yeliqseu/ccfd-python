import os
import math
import struct
import pickle
from datetime import datetime
from pysnc import *
BUFSIZE = 4096       # TCP receive buffer size
DATASIZE = 26214400  # Maximum datasize of each segment
PORT = 7653
UDP_START = 7655
HB_INTVAL = 1  # Heartbeat every HB_INTVAL seconds
# Message types
MSG =  {'INVALID'           : -1,
        'OK'                : 0,
        'HEARTBEAT'         : 1,
        'HEARTBEAT_ACK'     : 2,
        'HEARTBEAT_NOPEER'  : 3,     # Peer not in my record
        # Client to server
        'CHK_FILE'          : 11,    # Check file meta
        'REQ_SEG'           : 12,    # Request segment
        'REQ_SEG_ACK'       : 120,    # Request segment
        'REQ_START'         : 13,    # Request to start segment transmission
        'REQ_STOP'          : 14,    # Request to stop segment trans.
        'REQ_STOP_ACK'      : 140,    # Request to stop segment trans.
        'CHK_PEERS'         : 15,    # Ask for peers' information
        'OFR_HELP'          : 16,    # Offer to help
        'EXIT'              : 19,    # Leaving the transmission
        'EXIT_ACK'          : 190,    # Server acknowledge client's leave
        # Server to client
        'FILEMETA'          : 21,
        'SESSIONMETA'       : 22,
        'PEERINFO'          : 23,
        # Client to client
        'ASK_COOP'          : 31,    # Ask to send to me
        'ASK_COOP_ACK'      : 310,   # Ask_coop ackownledged
        'STOP_COOP'         : 32,    # Ask to stop sending to me
        'EXIT_COOP'         : 33,    # Let the other side know that I'll stop sending
        'EXIT_COOP_ACK'     : 330,   # EXIT_COOP acknowledged
        'EXIT_COOP_NOPEER'  : 331,   # EXIT_COOP not acknowledged, peer not in my record
        # Inter-process
        'NEW_SESSION'       : 41,    # New session info to cooperation process
        'END_SESSION'       : 42,    # Ending session info to cooperation process
        'COOP_PKT'          : 43,    # SNC packet to cooperation process
        'EXIT_PROC'         : 44,    # Exit cooperation process
        # Error message
        'ERR_NOFILE'        : 90,
        'ERR_MAXFILE'       : 91,
        'ERR_MAXCONN'       : 92,
        'ERR_NOBEAT'        : 93,
        'ERR_PNOSEG'        : 94,   # Peer error: requested segment not available
        'ERR_DUPRECV'       : 95,   # Peer is already in receiving list
        'ERR_PNOTFOUND'     : 96,   # Peer not in record
        # Data
        'DATA'              : 99}

iMSG = {v: k for k, v in MSG.items()}

def hdrlen():
    """ CCFD header contains two integers, its length is fixed
    """
    return len(struct.pack('ii', 1, 1))

class CCHeader:
    """ CCFD's protocol header
        The header contains two int-length members. It must always be packed into a
        bytearray before being sent out via TCP connections. This is required because
        paritial read/write may happen with non-blocking sockets. SO header size must be
        constant..
    """
    def __init__(self, mtype=-1):
        self.mtype = mtype    # int type
        self.length = 0       # int type (0 if packet has no body)

    def __str__(self):
        str = 'Packet Type: %d, length: %d' % (self.mtype, self.length)
        return str

    def packed(self):
        return struct.pack('ii', self.mtype, self.length)

    def parse(self, data):
        if len(data) != hdrlen():
            print("Error: I can't parse an invalid header!")
            self.mtype = -1
            self.length = 0
            return
        hdr = struct.unpack('ii', data)
        self.mtype = hdr[0]
        self.length = hdr[1]


class CCPacket:
    def __init__(self, header=None, body=None):
        if not header:
            self.header = CCHeader()
        else:
            self.header = header
        self.body = body

    def packed(self):
        if self.body is None:
            return self.header.packed()
        elif self.header.mtype == MSG['DATA']:
            # Don't pickle data packets because snc_packet is already serialized
            self.header.length = len(self.body)
            return (self.header.packed()+self.body)
        else:
            d = pickle.dumps(self.body)
            self.header.length = len(d)  # mark message length
            return (self.header.packed()+d)

    def parse(self, data):
        if len(data) < hdrlen():
            print("Error: len(data)<hdrlen(), cannot parse.")
            return
        self.header.parse(data[0:hdrlen()])
        # Verify if packet size is correct
        if len(data) != self.header.length + hdrlen():
            print("Error: data length seems not match with header!")
        if self.header.length == 0:
            return
        elif self.header.mtype == MSG['DATA']:
            self.body = data[hdrlen():]
        else:
            self.body = pickle.loads(data[hdrlen():])


class MetaInfo:
    """ Metainfo of a file and session

    Each session is responsible for transmitting a segment of a file. Session
    is identified by the following information:
        - session ID
        - segment id
        - snc parameter of the session
        - filename
        - filesize
        - segment size
        - number of segments

    Uses:
    a) sessionid=-1, segmentid=-1, used as metainfo of a file.
    b) sessionid=-1, segmentid>=0, used when clients request for a segment
    c) sessionid>=0, segmentid>=0, metainfo of the session

    """
    def __init__(self, filename, segmentid=-1, sessionid=-1):
        self.filename = filename
        self.segmentid = segmentid
        self.sessionid = sessionid
        if not os.path.isfile(filename):
            return
        statinfo = os.stat(self.filename)
        self.filesize = statinfo.st_size
        self.numofseg = int(math.ceil(float(self.filesize)/DATASIZE))
        if segmentid == self.numofseg - 1:
            self.segsize = self.filesize - DATASIZE * (self.numofseg - 1)
        else:
            self.segsize = DATASIZE
        self.sp = None

    def set_snc_params(self, sp):
        self.sp = sp

    def __str__(self):
        str = '\n\t<--------------- Meta Info --------------->\n'
        str += '\t  File Name: %s\n' % (self.filename)
        str += '\t  Segment ID: %d\n' % (self.segmentid)
        str += '\t  Session ID: %d\n' % (self.sessionid)
        if self.filesize is not None:
            str += '\t  File Size: %d\n' % (self.filesize)
        if self.segsize is not None:
            str += '\t  Segment Size: %d\n' % (self.segsize)
        if self.numofseg is not None:
            str += '\t  Number of Segments: %d\n' % (self.numofseg)
        if self.sp is not None:
            str += '\t [------------ SNC Parameter ------------]\n'
            str += '\t  Datasize: %d, pcrate: %.3f\n' % (self.sp.datasize,
                                                      self.sp.pcrate)
            str += '\t  size_b: %d, size_g: %d, size_p: %d\n' % (self.sp.size_b,
                                                              self.sp.size_g,
                                                              self.sp.size_p)
            str += '\t  code type: %d, bpc: %d, bnc: %d, sys: %s\n\t  seed: %s\n' % (self.sp.type,
                                                                                     self.sp.bpc,
                                                                                     self.sp.bnc,
                                                                                     self.sp.sys,
                                                                                     self.sp.seed)
        return str


class HostInfo:
    def __init__(self, ip, sessionid=-1, lastBeat=None):
        self.ip = ip                       # ip address of the host
        self.sessionid = sessionid         # sessionid the host belongs to
        if not lastBeat:
            self.lastBeat = datetime.now() # last heartbeat time

    def set_heartbeat(self, lastBeat=None):
        """ Set last heartbeat time of a remote host
        """
        self.lastBeat = lastBeat
        if not lastBeat:
            self.lastBeat = datetime.now()


# define peer's status
PRESEND = 1         # already asked for its help but not confirmed yet
SEND    = 2         # sending packets to me
PRESTOP = 3         # already asked to stop sending to me, but not confirmed yet
STOP    = 4         # stopped to send to me
RECV    = 5         # receive packets from me
PREEXIT = 6         # already notified my exit (to send), but not confirmed yet
EXIT    = 7         # peer is notified, and has exited to receive from me
class PeerInfo(HostInfo):
    def __init__(self, ip, sessionid=-1, lastBeat=None, chfd_c=-1, chfd_d=-1, state=None):
        super(PeerInfo, self).__init__(ip, sessionid, lastBeat)
        self.chfd_c = chfd_c        # control channel id with the peer
        self.chfd_d = chfd_d        # data channel id of the peer (if it's a receiving peer)
        self.state = state
