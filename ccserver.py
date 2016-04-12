import os
import sys
import socket
import select
import pickle
import logging
import copy
import multiprocessing as mp
from ccstructs import *

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s: [%(asctime)s] %(message)s')

HOST = ''

CLIENT_EXPIRE = 30  # Client is expired if no heartbeat for long
SESSION_EXPIRE = 60  # Session expires when idle for long time (in seconds)

"""
Server maintains a number of sessions, each of which corresponds to a child
process serving some segment of a file. Sessions are maintained in a dict:
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
            logging.info("Child process finish loading file")
        port = UDP_START + self.meta.sessionid  # Port used by the session
        poller = select.poll()  # poll fdc and datasock
        poller.register(self.fdc.fileno(), select.POLLIN)
        poller.register(self.datasock.fileno(), select.POLLOUT)
        pkt_p = snc.snc_alloc_empty_packet(snc.snc_get_parameters(self.sc))
        while True:
            for fd, event in poller.poll():
                if fd == self.fdc.fileno() and event is select.POLLIN:
                    pkt, ip = self.fdc.recv()
                    logging.info("Session [%d] receives msg <%s> from %s " %
                                    (self.meta.sessionid, iMSG[pkt.mtype], ip))
                    if pkt.mtype == MSG['REQ_SEG']:
                        self.add_client(HostInfo(ip, self.meta.sessionid))
                        self.fdc.send(Packet(MSG['OK']))
                    elif pkt.mtype == MSG['HEARTBEAT']:
                        self.client_heartbeat(ip)
                        self.fdc.send(Packet(MSG['OK']))
                    elif pkt.mtype == MSG['REQ_STOP'] or pkt.mtype == MSG['EXIT']:
                        self.remove_client(ip)
                        self.fdc.send(Packet(MSG['OK']))
                        
                if fd == self.datasock.fileno() and event is select.POLLOUT:
                    # writable datasock, send data packets to clients
                    for cli in self.clients:
                        snc.snc_generate_packet_im(self.sc, pkt_p)
                        pktstr = pkt_p.contents.serialize(self.meta.sp.size_g,
                                                          self.meta.sp.size_p,
                                                          self.meta.sp.bnc)
                        try:
                            self.datasock.sendto(pktstr, (cli.ip, port))
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
            for cli in copy.deepcopy(self.clients):
                if (now - cli.lastBeat).seconds > CLIENT_EXPIRE:
                    logging.info("Remove client %s because no heartbeat"
                                    % (cli.ip,))
                    self.remove_client(cli.ip)
            self.lastClean = now
        if (now - self.lastIdle).seconds > SESSION_EXPIRE:
            self.datasock.close()
            sys.exit(0)


def send_file_meta(conn, filename):
    """
    Send meta information of a given filename
    """
    if not os.path.isfile(filename):
        pkt = Packet(MSG['ERR_NOFILE'])
        conn.send(pickle.dumps(pkt))
    else:
        meta = MetaInfo(filename)
        pkt = Packet(MSG['FILEMETA'], meta)
        conn.send(pickle.dumps(pkt))

def send_peers_info(conn, session, ip):
    """
    For a given client IP of a session, select and send
    a list of peers that he can connect
    """
    peers = [cli.ip for cli in session.cooplist if cli.ip != ip]
    pkt = Packet(MSG['PEERINFO'], peers)
    conn.send(pickle.dumps(pkt))

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
        if reply.mtype == MSG['OK']:
            return 0
        else:
            return -1
    except EOFError:
        return -1


def housekeeping():
    """ Housekeeping tasks of the parent process
        - Clean exited session from sessions list
    """
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
    Server main
    """
    global sessions
    # print("Message type: ", pkt.header.op)
    session = find_session(sessions, pkt.payload)
    ip, port = conn.getpeername()
    if pkt.mtype == MSG['CHK_FILE']:
        send_file_meta(conn, pkt.payload.filename)
    if pkt.mtype == MSG['REQ_SEG']:
        if session is None:
            session = create_new_session(sessions, pkt.payload)
        ret = call_session_process(session, (pkt, ip))
        if ret is 0:
            pkt = Packet(MSG['SESSIONMETA'], session.meta)
            conn.send(pickle.dumps(pkt))  # Send session's data
            try:
                pkt = pickle.loads(conn.recv(BUFSIZE))  # Get client's reply
                if pkt.mtype == MSG['OK']:
                    # Add to client list of the session
                    logging.info("Add %s into client list of segment %d" 
                                    % (ip, session.meta.segmentid))
                    session.add_client(HostInfo(ip, session.meta.sessionid))
                    session.add_client_coop(HostInfo(ip, session.meta.sessionid))
            except Exception as detail:
                logging.warning("Caught exception receiving from %s for segment %d."
                        % (ip, session.meta.segmentid))
        else:
            logging.info("Call_cession_process return -1")
    if pkt.mtype == MSG['HEARTBEAT']:
        if session is None:
            # No such session, error notice
            pass
        else:
            ret = call_session_process(session, (pkt, ip))
            if ret is 0:
                conn.send(pickle.dumps(pkt))  # Echo back the heartbeat
            else:
                # Session no reply, error notice
                pass
            session.client_heartbeat(addr[0])
    if pkt.mtype == MSG['REQ_STOP']:
        # remove client from session
        if session is None:
            # No such session, error notice to client
            pass
        else:
            ret = call_session_process(session, (pkt, ip))
            if ret is 0:
                session.remove_client(ip)
                conn.send(pickle.dumps(Packet(MSG['OK'])))
            else:
                # Child no reply, error notice to client
                pass
    if pkt.mtype == MSG['CHK_PEERS']:
        if session is None:
            pass
        else:
            send_peers_info(conn, session, ip)
    if pkt.mtype == MSG['EXIT']:
        if session is None:
            return
        ret = call_session_process(session, (pkt, ip))
        if ret is 0:
            session.remove_client(ip)
            # Remove the client from cooplist of all sessions
            for v in sessions.values():
                v[0].remove_client_coop(ip)
            conn.send(pickle.dumps(Packet(MSG['OK'])))
        else:
            pass


if __name__ == "__main__":
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5.0)
    s.bind((HOST, PORT))
    s.listen(1)
    while True:
        housekeeping()
        try:
            conn, addr = s.accept()
            # handle control connections
            ip = addr[0]
            # print('Connection from: ', ip)
            data = conn.recv(BUFSIZE)
            ccpkt = pickle.loads(data)
            handle_ctrl_packet(conn, ccpkt)
            conn.close()
        except socket.timeout:
            continue
