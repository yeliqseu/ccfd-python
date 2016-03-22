import os
import math
from datetime import datetime
from pysnc import *
DATASIZE = 5120000  # Maximum datasize of each segment
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
        # Server to client
        'FILEMETA'    : 21,
        'SESSIONMETA' : 22,
        'PEERINFO'    : 23,
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
        statinfo = os.stat(self.filename)
        self.filesize = statinfo.st_size
        self.numofseg = int(math.ceil(float(self.filesize)/DATASIZE))
        self.segmentid = segmentid
        if segmentid == self.numofseg - 1:
            self.segsize = self.filesize - DATASIZE * (self.numofseg - 1)
        else:
            self.segsize = DATASIZE
        self.sessionid = sessionid
        self.sp = None

    def set_snc_params(self, sp):
        self.sp = sp

    def __str__(self):
        str = '<--\n'
        str += ' File Name: %s\n' % (self.filename)
        str += ' Segment ID: %d\n' % (self.segmentid)
        str += ' Session ID: %d\n' % (self.sessionid)
        if self.filesize is not None:
            str += ' File Size: %d\n' % (self.filesize)
        if self.segsize is not None:
            str += ' Segment Size: %d\n' % (self.segsize)
        if self.numofseg is not None:
            str += ' Number of Segments: %d\n' % (self.numofseg)
        if self.sp is not None:
            str += ' [------------ SNC Parameter ------------]\n'
            str += ' Datasize: %d, pcrate: %.3f\n' % (self.sp.datasize,
                                                      self.sp.pcrate)
            str += ' size_b: %d, size_g: %d, size_p: %d\n' % (self.sp.size_b,
                                                              self.sp.size_g,
                                                              self.sp.size_p)
            str += ' code type: %d, bpc: %d, bnc: %d, sys: %s, seed: %s\n' % (self.sp.type,
                                                           self.sp.bpc,
                                                           self.sp.bnc,
                                                           self.sp.sys,
                                                           self.sp.seed)
        str += '-->\n'
        return str


class HostInfo:
    def __init__(self, ip, lastBeat=None):
        self.ip = ip
        if not lastBeat:
            self.lastBeat = datetime.now()

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
