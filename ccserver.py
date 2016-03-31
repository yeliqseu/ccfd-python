import os
import sys
import socket
import select
import pickle
import multiprocessing as mp
from ccstructs import *
HOST = ''

CLIENT_EXPIRE = 60  # Client is expired if no heartbeat for long
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
                print("Removed %s from cooplist of segment %d"
                        % (ip, self.meta.segmentid))
        return


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
    sp = snc_parameters(meta.segsize, 0.01, 32, 128, 1280, BAND_SNC, 1, 0, 0, -1)
    meta.set_snc_params(sp)
    # Fork a child process and build pipe between parent and child
    session = Session(meta)
    (fdp, fdc) = mp.Pipe()
    session.fdp = fdp
    session.fdc = fdc
    print("New session created, ID: ", new_sid)
    print(session.meta)
    # Fork a process to serve the clients of the session
    child = mp.Process(target=session_main, args=(session, ))
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
            print("Session [", sessions[k][0].meta.sessionid,
                  "] of ", k, " is expired.")
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
            pkt = pickle.loads(conn.recv(BUFSIZE))  # Get client's reply
            if pkt.mtype == MSG['OK']:
                # Add to client list of the session
                print("Add ", ip, "into client list")
                session.add_client(HostInfo(ip, session.meta.sessionid))
                session.add_client_coop(HostInfo(ip, session.meta.sessionid))
        else:
            print("Call_cession_process return -1")
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
            # FIXME: update heartbeat time in parent process as well
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

def handle_ctrl_packet_child(session, pkt):
    """
    Child process handle control packet passed from parent process
    """
    pass


def session_main(session):
    """
    Main loop of the child process of each session.
    This routine never exit
    """
    session.pid = os.getpid()
    session.fdp.close()  # Close fdp on child side
    if session.datasock is None:
        # Create session's data socket and load file
        session.datasock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        session.load_file()
        print("Child process finish loading file")
    port = UDP_START + session.meta.sessionid  # Port used by the session
    poller = select.poll()  # poll fdc and datasock
    poller.register(session.fdc.fileno(), select.POLLIN)
    poller.register(session.datasock.fileno(), select.POLLOUT)
    pkt_p = snc.snc_alloc_empty_packet(snc.snc_get_parameters(session.sc))
    while True:
        for fd, event in poller.poll():
            if fd == session.fdc.fileno() and event is select.POLLIN:
                pkt, ip = session.fdc.recv()
                print("Session [%d] receives msg of type <%d> from %s " %
                      (session.meta.sessionid, pkt.mtype, ip))
                if pkt.mtype == MSG['REQ_SEG']:
                    session.add_client(HostInfo(ip, session.meta.sessionid))
                    session.fdc.send(Packet(MSG['OK']))
                elif pkt.mtype == MSG['HEARTBEAT']:
                    session.fdc.send(Packet(MSG['OK']))
                elif pkt.mtype == MSG['REQ_STOP'] or pkt.mtype == MSG['EXIT']:
                    session.remove_client(ip)
                    session.fdc.send(Packet(MSG['OK']))
                    
            if fd == session.datasock.fileno() and event is select.POLLOUT:
                # writable datasock, send data packets to clients
                for cli in session.clients:
                    snc.snc_generate_packet_im(session.sc, pkt_p)
                    pktstr = pkt_p.contents.serialize(session.meta.sp.size_g,
                                                      session.meta.sp.size_p)
                    try:
                        session.datasock.sendto(pktstr, (cli.ip, port))
                    except:
                        print("Caught exception in session ID: ",
                              session.meta.sessionid)
                    session.lastIdle = datetime.now()  # Refresh idle time
        session_housekeeping(session)


def session_housekeeping(session):
    """ Housekeeping of a session
        - Remove clients that didn't heartbeat for long
        - Exit if the session has been idle (i.e., no clients) for long

        Fixme:
          May not be efficient because now() will be called too often.
    """
    now = datetime.now()
    if (now - session.lastClean).seconds > CLIENT_EXPIRE:
        # Clean up no heartbeat clients
        for cli in session.clients:
            if (now - cli.lastBeat).seconds > CLIENT_EXPIRE:
                print("Remove client ", cli.ip, "because no heartbeat")
                session.remove_client(cli.ip)
        session.lastClean = now
    if (now - session.lastIdle).seconds > SESSION_EXPIRE:
        session.datasock.close()
        sys.exit(0)


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
            bindata = conn.recv(BUFSIZE)
            ccpkt = pickle.loads(bindata)
            handle_ctrl_packet(conn, ccpkt)
            conn.close()
        except socket.timeout:
            continue
