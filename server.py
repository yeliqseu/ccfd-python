import os
import sys
import socket
import select
import pickle
import logging
import copy
import multiprocessing as mp
from ccstruct import *

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s: [%(asctime)s] %(message)s')

HOST = ''

CLIENT_EXPIRE = 30  # Client is expired if no heartbeat for long
SESSION_EXPIRE = 60  # Session expires when idle for long time (in seconds)

"""
Server maintains a number of sessions, each of which corresponds to a child
process serving one segment of a file. Sessions are maintained in a dict:
{('filename', segmentid) : (Session, Process)}

"""
sessions = {}


class Session:
    """ Session details
    """
    def __init__(self, meta):
        self.meta = meta      # Metainfo of the session
        self.sc = snc.snc_create_enc_context(None, byref(self.meta.sp))
        self.fdp = None       # fd of pipe of the parent side
        self.fdc = None       # fd of pipe of the child side
        self.datasock = None  # UDP socket for data transmission
        self.clients = []     # List of clients (HostInfo objects) in serving in the session
        self.cooplist = []   # List of clients available for cooperative transmission
        self.lastClean = datetime.now()
        self.lastIdle = datetime.now()

    def main(self):
        """
        Main loop of the child process of each session.
        This routine never exit
        """
        self.pid = os.getpid()
        self.fdp.close()  # Close fdp on child side
        if self.datasock is None:
            # Create session's data socket and load file
            self.datasock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.load_file()
            logging.info("Child process finished loading file")
        port = UDP_START + self.meta.sessionid  # Port used by the session
        poller = select.poll()  # poll fdc and datasock
        poller.register(self.fdc.fileno(), select.POLLIN)
        poller.register(self.datasock.fileno(), select.POLLOUT)
        pkt_p = snc.snc_alloc_empty_packet(snc.snc_get_parameters(self.sc))
        while True:
            for fd, event in poller.poll():
                if fd == self.fdc.fileno() and event is select.POLLIN:
                    pkt, ip = self.fdc.recv()
                    logging.info("Session [%d] received msg <%s> from %s." %
                                    (self.meta.sessionid, iMSG[pkt.header.mtype], ip))
                    if pkt.header.mtype == MSG['REQ_SEG']:
                        self.add_client(HostInfo(ip, self.meta.sessionid))
                        self.fdc.send(CCPacket(CCHeader(MSG['OK'])))
                    elif pkt.header.mtype == MSG['HEARTBEAT']:
                        self.client_heartbeat(ip)
                        self.fdc.send(CCPacket(CCHeader(MSG['OK'])))
                    elif pkt.header.mtype == MSG['REQ_STOP'] or pkt.header.mtype == MSG['EXIT']:
                        self.remove_client(ip)
                        self.fdc.send(CCPacket(CCHeader(MSG['OK'])))

                if fd == self.datasock.fileno() and event is select.POLLOUT:
                    # writable datasock, send data packets to clients
                    for cli in self.clients:
                        snc.snc_generate_packet_im(self.sc, pkt_p)
                        pktstr = pkt_p.contents.serialize(self.meta.sp.size_g,
                                                          self.meta.sp.size_p,
                                                          self.meta.sp.bnc)
                        try:
                            # Construct data packet with serialized snc_packet
                            self.datasock.sendto(CCPacket(CCHeader(MSG['DATA']), pktstr).packed(), (cli.ip, port))
                        except:
                            logging.warning("Caught exception in session %s."
                                                % (self.meta.sessionid,))
                        self.lastIdle = datetime.now()  # Refresh idle time
            self.housekeeping()

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

    def add_client_coop(self, cli):
        """ Add a client to cooperation list
        """
        if self.cooplist.count(cli) is 0:
            self.cooplist.append(cli)

    def remove_client_coop(self, ip):
        """ Remove a client from cooperation list
        """
        for cli in self.cooplist:
            if cli.ip == ip:
                self.cooplist.remove(cli)
                logging.info("Removed %s from cooplist of segment %d"
                                % (ip, self.meta.segmentid))
        return

    def client_heartbeat(self, ip):
        """ Update heartbeat of clients
        """
        for cli in self.clients:
            if cli.ip == ip:
                cli.set_heartbeat()
        for cli in self.cooplist:
            if cli.ip == ip:
                cli.set_heartbeat()

    def housekeeping(self):
        """ Housekeeping of a session
            - Remove clients that didn't heartbeat for long
            - Exit if the session has been idle (i.e., no clients) for long

            Fixme:
              May not be efficient because now() will be called too often.
        """
        now = datetime.now()
        if (now - self.lastClean).seconds > CLIENT_EXPIRE:
            # Clean up no heartbeat clients
            logging.info("Session [%d] do housekeeping..." % (self.meta.sessionid))
            for cli in copy.deepcopy(self.clients):
                if (now - cli.lastBeat).seconds > CLIENT_EXPIRE:
                    logging.warning("Remove client %s because no heartbeat"
                                    % (cli.ip,))
                    self.remove_client(cli.ip)
            self.lastClean = now
        if (now - self.lastIdle).seconds > SESSION_EXPIRE:
            logging.info("Session [%d] exiting..." % (self.meta.sessionid))
            self.datasock.close()
            sys.exit(0)


def send_file_meta(conn, filename):
    """
    Send meta information of a given filename
    """
    if not os.path.isfile(filename):
        pkt = CCPacket(CCHeader(MSG['ERR_NOFILE']))
        conn.send(pkt.packed())
    else:
        meta = MetaInfo(filename)
        pkt = CCPacket(CCHeader(MSG['FILEMETA']), meta)
        conn.send(pkt.packed())

def send_peers_info(conn, session, ip):
    """
    For a given client IP of a session, select and send
    a list of peers that he can connect
    """
    peers = [cli.ip for cli in session.cooplist if cli.ip != ip]
    if not peers:
        peers = []
    pkt = CCPacket(CCHeader(MSG['PEERINFO']), peers)
    conn.send(pkt.packed())

def find_session(sessions, segmeta):
    """
    Find existing session with segment metainfo.
    If exists, return the session;
    Otherwise, return None
    """
    if (segmeta.filename, segmeta.segmentid) in sessions.keys():
        return sessions[(segmeta.filename, segmeta.segmentid)][0]
    else:
        return None

def create_new_session(sessions, segmeta):
    """
    Create a new session with the given segment metainfo.
    A child process is forked dedicating to the session.
    """
    # Find an available session id
    new_sid = 0
    while new_sid in [s[0].meta.sessionid for s in sessions.values()]:
        new_sid += 1
    # Create meta and fill in information of the file
    meta = MetaInfo(segmeta.filename, segmeta.segmentid, new_sid)
    sp = snc_parameters(meta.segsize, 0.01, 16, 64, 1280, BAND_SNC, 1, 1, 0, -1)
    meta.set_snc_params(sp)
    # Fork a child process and build pipe between parent and child
    session = Session(meta)
    (fdp, fdc) = mp.Pipe()
    session.fdp = fdp
    session.fdc = fdc
    logging.info("New session created, ID: %d " % (new_sid,))
    print(session.meta)
    # Fork a process to serve the clients of the session
    child = mp.Process(target=session.main)
    child.start()
    session.fdc.close()  # Close parent's fdc
    sessions[(segmeta.filename, segmeta.segmentid)] = (session, child)
    return session

def call_session_process(session, msg):
    """
    Call the session process, which is a child process of the main process
    """
    # print("Send packet to session process")
    session.fdp.send(msg)
    try:
        reply = session.fdp.recv()
        # print("Receive reply from session process")
        if reply.header.mtype == MSG['OK']:
            return 0
        else:
            return -1
    except EOFError:
        return -1


def housekeeping():
    """ Housekeeping tasks of the parent process
        - Clean exited session from sessions list
    """
    # logging.info("Main process doing housekeeping...")
    # print(sessions)
    exited = []
    for k in sessions.keys():
        if not sessions[k][1].is_alive():
            logging.info("Session [%d] of %s (segment %d) is expired."
                            % (sessions[k][0].meta.sessionid, k[0], k[1]))
            sessions[k][1].join()
            exited.append(k)
    for k in exited:
        del sessions[k]


def handle_ctrl_packet(conn, pkt):
    """
    Server's main routine for processing client messages
    """
    global sessions
    ip, port = conn.getpeername()
    logging.info("Server receives msg <%s> from %s " %
                    (iMSG[pkt.header.mtype], ip))
    session = find_session(sessions, pkt.body)
    if pkt.header.mtype == MSG['CHK_FILE']:
        send_file_meta(conn, pkt.body.filename)

    elif pkt.header.mtype == MSG['REQ_SEG']:
        if session is None:
            session = create_new_session(sessions, pkt.body)
        ret = call_session_process(session, (pkt, ip))
        if ret is 0:
            pkt = CCPacket(CCHeader(MSG['SESSIONMETA']), session.meta)
            try:
                conn.send(pkt.packed())  # Send session's data
                # Add to client list of the session
                logging.info("Add %s into client list of segment %d"
                                % (ip, session.meta.segmentid))
                session.add_client(HostInfo(ip, session.meta.sessionid))
                session.add_client_coop(HostInfo(ip, session.meta.sessionid))
            except Exception as detail:
                logging.warning("Caught exception receiving from %s for segment %d: %s."
                                    % (ip, session.meta.segmentid, detail))
        else:
            logging.info("Call_cession_process return -1")
    
    elif pkt.header.mtype == MSG['HEARTBEAT']:
        if session is None:
            # No such session, error notice
            pass
        else:
            ret = call_session_process(session, (pkt, ip))
            if ret is 0:
                conn.send(pkt.packed())  # Echo back the heartbeat
            else:
                # Session no reply, error notice
                pass
            session.client_heartbeat(addr[0])
    
    elif pkt.header.mtype == MSG['REQ_STOP']:
        # remove client from session
        if session is None:
            # No such session, error notice to client
            pass
        else:
            ret = call_session_process(session, (pkt, ip))
            if ret is 0:
                session.remove_client(ip)
                conn.send(CCPacket(CCHeader(MSG['REQ_STOP_ACK']), session.meta).packed())
            else:
                # Child no reply, error notice to client
                pass
    
    elif pkt.header.mtype == MSG['CHK_PEERS']:
        if session is None:
            pass
        else:
            send_peers_info(conn, session, ip)
        # conn.close()
    
    elif pkt.header.mtype == MSG['EXIT']:
        if session is None:
            return
        ret = call_session_process(session, (pkt, ip))
        if ret is 0:
            session.remove_client(ip)
            # Remove the client from cooplist of all sessions
            for v in sessions.values():
                v[0].remove_client_coop(ip)
            conn.send(CCPacket(CCHeader(MSG['EXIT_ACK'])).packed())
        else:
            pass
        conn.close()


if __name__ == "__main__":
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5.0)
    s.bind((HOST, PORT))
    s.listen(1)
    poller = select.poll()  # poll listen socket and client connections
    poller.register(s.fileno(), select.POLLIN)
    sockets = {s.fileno() : s}
    while True:
        housekeeping()
        for fd, event in poller.poll(1):
            # print("fd %d triggered event %s" % (fd, event))
            if event & (select.POLLHUP | select.POLLERR | select.POLLNVAL):
                logging.warning("Unregister broken fd: %d" % (fd, ))
                poller.unregister(fd)
                del sockets[fd]
            elif fd == s.fileno() and event is select.POLLIN:
                newconn, addr = s.accept()
                # newconn.setblocking(False)
                sockets[newconn.fileno()] = newconn  # save the socket
                poller.register(newconn.fileno(), select.POLLIN)
                logging.info('Accepted connection from: %s on fd %d '
                                % (addr[0], newconn.fileno()))
            elif event is select.POLLIN:
                conn = sockets[fd]  # get socket of the file descriptor
                try:
                    data = conn.recv(BUFSIZE)
                    ccpkt = CCPacket()
                    ccpkt.parse(data)
                    handle_ctrl_packet(conn, ccpkt)
                except Exception as detail:
                    logging.warning("Cannot receive from fd %d. Error: %s"
                                        % (fd, detail))
                    poller.unregister(fd)
                    del sockets[fd]
                    continue
