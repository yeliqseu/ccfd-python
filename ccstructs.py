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
        self.segmentid = segmentid
        self.sessionid = sessionid
        self.filesize = None
        self.sp = None
        self.segsize = None
        self.numofseg = None

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
            str += ' code type: %d, bpc: %d, bnc: %d\n' % (self.sp.type,
                                                           self.sp.bpc,
                                                           self.sp.bnc)
        str += '-->\n'
        return str

    def fill_info(self):
        """
        (Re)fill detailed file information
        """
        statinfo = os.stat(self.filename)
        self.filesize = statinfo.st_size
        if self.filesize > DATASIZE:
            self.numofseg = math.ceil(self.filesize/DATASIZE)
            if self.segmentid == self.numofseg - 1:
                # Last segment's size may < DATASIZE
                currseg = self.filesize - DATASIZE * (self.numofseg - 1)
            else:
                currseg = DATASIZE
            print("Segment ID: %d, segment size: %d" %
                  (self.segmentid, currseg))
            self.sp = snc_parameter(currseg, 0.01, 32, 64, 1280,
                                    BAND_SNC, 0, 0)
            self.segsize = currseg
        else:
            # Only one segment
            self.numofseg = 1
            self.sp = snc_parameter(self.filesize, 0.01, 32, 64, 1280,
                                    BAND_SNC, 0, 0)
            self.segsize = self.filesize


class Session:
    """ Session details
    """
    def __init__(self, meta):
        self.meta = meta      # Metainfo of the session
        self.sc = snc.snc_create_enc_context(None, meta.sp)
        self.fdp = None       # fd of pipe of the parent side
        self.fdc = None       # fd of pipe of the child side
        self.datasock = None  # UDP socket for data transmission
        self.clients = []     # List of clients in serving in the session
        self.lastClean = datetime.now()
        self.lastIdle = datetime.now()

    def load_file(self):
        """ Load file content into snc context of the session
            This only happens in the session process (i.e., child).
        """
        offset = self.meta.segmentid * DATASIZE
        filename = self.meta.filename.encode('UTF-8')
        snc.snc_load_file_to_context(c_char_p(filename), offset, self.sc)

    def add_client(self, cli):
        """ Add a client to client list
            cli - client
        """
        if self.clients.count(cli) is 0:
            self.clients.append(cli)

    def has_client(self, ip):
        """ Check an ip is in client list
        """
        for cli in self.clients:
            if cli.ip == ip:
                return cli
        return None

    def remove_client(self, ip):
        """ Remove the client of the given ip from client list
        """
        cli = self.has_client(ip)
        if cli is not None:
            self.clients.remove(cli)


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
