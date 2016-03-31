import os
import math
from datetime import datetime
from pysnc import *
BUFSIZE = 4096       # TCP receive buffer size
DATASIZE = 26214400  # Maximum datasize of each segment
PORT = 7653
UDP_START = 7655
HB_INTVAL = 10  # Heartbeat every 10 seconds
# Message types
MSG =  {'OK'          : 0,
        'HEARTBEAT'   : 1,
        # Client to server
        'CHK_FILE'    : 11,    # Check file meta
        'REQ_SEG'     : 12,    # Request segment
        'REQ_START'   : 13,    # Request to start segment transmission
        'REQ_STOP'    : 14,    # Request to stop segment trans.
        'CHK_PEERS'   : 15,    # Ask for peers' information
        'OFR_HELP'    : 16,    # Offer to help
        'EXIT'        : 19,    # Leaving the transmission
        # Server to client
        'FILEMETA'    : 21,
        'SESSIONMETA' : 22,
        'PEERINFO'    : 23,
        # Client to client
        'ASK_COOP'    : 31,    # Ask to send to me
        'STOP_COOP'   : 32,    # Ask to stop sending to me
        'EXIT_COOP'   : 33,    # Let the other side know that I'll stop sending
        # Inter-process
        'NEW_SESSION' : 41,    # New session info to cooperation process
        'END_SESSION' : 42,    # Ending session info to cooperation process
        'COOP_PKT'    : 43,    # SNC packet to cooperation process
        'EXIT_PROC'   : 44,    # Exit cooperation process
        # Error message
        'ERR_NOFILE'  : 90,
        'ERR_MAXFILE' : 91,
        'ERR_MAXCONN' : 92,
        'ERR_NOBEAT'  : 93,
        # Data
        'DATA'        : 99}


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
            str += '\t  code type: %d, bpc: %d, bnc: %d, sys: %s\n\t  seed: %s\n' % (self.sp.type, self.sp.bpc, self.sp.bnc, self.sp.sys, self.sp.seed)
        return str


class HostInfo:
    def __init__(self, ip, sessionid=-1, lastBeat=None):
        self.ip = ip                       # ip address of the host
        self.sessionid = sessionid         # sessionid the host belongs to
        if not lastBeat:
            self.lastBeat = datetime.now() # last heartbeat time

    def set_heartbeat(self, lastBeat):
        """ Set last heartbeat time of a remote host
        """
        self.lastBeat = lastBeat


class Packet:
    """ Packet format in ccFileD
        mtype   - type of the message delivered in the packet
        payload - payload
    """
    def __init__(self, mtype=-1, payload=None):
        self.mtype = mtype
        self.payload = payload

    def __str__(self):
        str = 'Packet Type: %d' % (self.mtype)
        return str

    def set_type(self, mtype):
        self.mtype = mtype

    def set_payload(self, payload):
        self.payload = payload
