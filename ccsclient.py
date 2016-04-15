import sys
import socket
import pickle
import select
import logging
from datetime import datetime
from ccstructs import *
import multiprocessing as mp
import copy

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s: [%(asctime)s] %(message)s')
#HOST = '127.0.0.1'
HOST = '172.30.40.4'
#HOST = '192.168.1.110'
HK_INTVAL = 15  # Housekeeping interval


class Cooperation:
    """ Cooperation session
        A cooperation session is responsible for sharing all segments
        of a file that the current node has decoded and is decoding. The decoder process is 
        responsible for decoding one segment at a time. The cooperation process needs to track 
        which segment is currently in decoding and ask peers to send packets to the decoder.
        For other segments (which decoder has recovered), cooperation process continues
        sending packets to requesting peers, until it is told to stop (by peers or parent).

        filemeta  - metainfo of the file sharing with peers
        fdp       - parent side of the pipe; will be closed in child process
        fdc       - child side of the pipe for receiving msg from parent (decoder process)
        pcl       - socket for accepting peer control connections
        datasock  - data socket sending recoded packets
        currsess  - session meta of the segment currently decoding by the decoder
        sendpeers - peers sending packets belonging to current session to the decoder
                    ['ip1', 'ip2', ...]
        sncbuf    - buffer of segments for recoding
                    {'segmentid': [sp, buf_p, pkt_p, new, done]}
        recvpeers - connected peers receiving recoded packets from me
                    {'segmentid': [hostinfo, ...]}
                    hostinfo contains peer's ip and its session id. The latter
                    indicates what UDP port data packets should be sent to.

        There are two TCP connections established between each pair of peers. One is for 
        sending control messages and the other is for receiving. This design is to avoid
        messing up replies. For example, a node may send a control message and expect an
        OK from its peer. At the same time, the peer might be sending a HEARTBEAT to the
        node. If use only one connection between the peers, the replies may be messed up.

        connip    - accepted TCP connections from peers (i.e., in-bound)
                    {'ip1': conn1, 'ip2': conn2, ...}
        connfd    - fd-connection dict of the accepted connections from peers
                    {'fd1': conn1, 'fd2': conn2, ...}
        outconn   - out-bound connections of the current node for sending 
                    control messages to peers.
        
        lastClean - Last time housekeeping was done
                    Cooperation process periodically checks list of receiving
                    peers and removes peers that didn't have heartbeat for a 
                    long time.
    """
    def __init__(self, filemeta):
        self.filemeta = filemeta
        self.poller = None    # poller of fds
        self.fdp = None       # fd of pipe of the parent side
        self.fdc = None       # fd of pipe of the child side
        self.pcl = None       # TCP socket for peer ctrl
        self.datasock = None  # UDP socket for data transmission
        self.currsess = None  # Meta info of current session
        self.sendpeers = []   # Peers cooperating on current session
        self.sncbuf = {}      # Each segment has a snc buffer
        self.recvpeers = {}   # Each segment has a list of receiving peers
        self.connip = {}      # Established in-bound peer TCP connections
        self.connfd = {}      # fd-connection mapping of in-bound connections
        self.outconn = {}     # Established out-bound TCP connections
        self.lastClean = datetime.now()

    def main(self):
        """ Main function of the cooperation process
        """
        self.fdp.close() # Close fdp on child side
        # Setup TCP socket for receiving requests
        self.pcl = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.pcl.settimeout(5.0)
        self.pcl.bind(('', PORT+1))  # bind (PORT+1), specifically for sharing
        self.pcl.listen(10)
        # Setup UDP socket for sending recoded packets
        self.datasock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Poll fdc, pcl, datasock, and established peer connections
        # fdc      - messages from the parent (e.g., snc packets)
        # pcl      - requests from new (or re-establishing) peers
        # datasock - sending snc recoded packets to peers
        self.poller = select.poll()  # poll fdc and datasock
        self.poller.register(self.fdc.fileno(), select.POLLIN)
        self.poller.register(self.pcl.fileno(), select.POLLIN)
        self.poller.register(self.datasock.fileno(), select.POLLOUT)
        logging.info("Start main loop of cooperation...")
        while True:
            for fd, event in self.poller.poll(1):
                if event & (select.POLLHUP | select.POLLERR | select.POLLNVAL):
                    # Remove broken file descriptors. (e.g. disconntected peers)
                    # print("Unregister broken fd: %d" % (fd, ))
                    if fd in self.connfd.keys():
                        self.connip = {k: v for k, v in self.connip.items() if v != self.connfd[fd]}
                        del self.connfd[fd]
                    self.poller.unregister(fd)
                elif fd == self.fdc.fileno() and event is select.POLLIN:
                    """ message from parent (the decoder process)
                         - session/snc_parameters
                         - duplicated snc packets to buffer
                         - exit sharing
                    """
                    pkt = self.fdc.recv()
                    if pkt.mtype == MSG['NEW_SESSION']:
                        sinfo = pkt.payload
                        logging.info("Create SNC buffer for segment %d" 
                                        % (sinfo.segmentid,))
                        self.create_buffer(sinfo.segmentid, sinfo.sp)
                        logging.info("SNC buffer for segment %d created successfully" 
                                        % (sinfo.segmentid,))
                        if self.currsess:
                            logging.warning("A session is currently marked as running.")
                        self.currsess = sinfo  # set new current session
                        # Check available peers from server
                        peers = self.check_curr_session_peers()
                        for peerip in peers:
                            self.request_help_curr_session(peerip)
                    if pkt.mtype == MSG['END_SESSION']:
                        sinfo = pkt.payload
                        logging.info("End session %d (segment %d)"
                                        % (sinfo.sessionid, sinfo.segmentid))
                        if self.currsess:
                            self.stop_recv_curr_session_all()
                        self.sncbuf[self.currsess.segmentid][4] = True
                        self.currsess = None
                    if pkt.mtype == MSG['EXIT_PROC']:
                        logging.info("Clean up cooperation process before exiting...")
                        for segid in self.recvpeers.keys():
                            for peer in copy.deepcopy(self.recvpeers[segid]):
                                # I'm exiting. Notify receiving peers.
                                self.notify_exit_sending(peer.ip, segid)
                            snc.snc_free_buffer(self.sncbuf[segid][1])
                            snc.snc_free_packet(self.sncbuf[segid][2])
                        logging.info("Cooperation process exiting...")
                        exit(0)
                    if pkt.mtype == MSG['COOP_PKT']:
                        pkt_p = snc.snc_alloc_empty_packet(byref(self.currsess.sp))
                        pkt_p.contents.deserialize(pkt.payload, 
                                                   self.currsess.sp.size_g,
                                                   self.currsess.sp.size_p,
                                                   self.currsess.sp.bnc)
                        snc.snc_buffer_packet(self.sncbuf[self.currsess.segmentid][1], pkt_p)
                        self.sncbuf[self.currsess.segmentid][3] += 1
                    if pkt.mtype == MSG['HEARTBEAT']:
                        # send heartbeat to peers on the sending list
                        self.heartbeat_sending_peers()
                elif fd == self.pcl.fileno() and event is select.POLLIN:
                    """ peer to peer control new connections
                    """
                    conn, addr = self.pcl.accept()
                    peerip, port = conn.getpeername()
                    # Check if there was outdated connection with this peer
                    if peerip in self.connip.keys():
                        del self.connip[peerip]
                    self.connip[peerip] = conn
                    self.connfd[conn.fileno()] = conn
                    self.poller.register(conn.fileno(), select.POLLIN)
                    logging.info("Accepted connection from %s, fd %d" 
                                    % (peerip, conn.fileno()))
                elif event is select.POLLIN:
                    """ Messages from established peer connections
                         - ask for share
                         - stop share
                         - exiting
                         - heartbeat
                    """
                    logging.info("Message available on fd %d" % (fd, ))
                    conn = self.connfd[fd]
                    try:
                        peerip, port = conn.getpeername()
                        pkt = pickle.loads(conn.recv(BUFSIZE))
                        logging.info("Poll() loop receives message %s from %s"
                                        % (iMSG[pkt.mtype], peerip))
                    except Exception as detail:
                        logging.warning("Poll() cannot receive message from fd %d: %s" 
                                            % (fd, detail))
                    if pkt.mtype == MSG['ASK_COOP']:
                        sinfo = pkt.payload
                        logging.info("Request from %s to send segment %d."
                                        % (peerip, sinfo.segmentid))
                        if self.add_receiving_peer(peerip,
                                                   sinfo.segmentid, 
                                                   sinfo.sessionid) == 0:
                            conn.send(pickle.dumps(Packet(MSG['OK'])))
                        else:
                            conn.send(pickle.dumps(Packet(MSG['ERR_PNOSEG'])))
                        # Send request if it is the same segment as we need
                        if (self.currsess is not None 
                            and sinfo.segmentid == self.currsess.segmentid 
                            and peerip not in self.sendpeers):
                            self.request_help_curr_session(peerip)
                    if pkt.mtype == MSG['STOP_COOP']:
                        sinfo = pkt.payload
                        logging.info("Request from %s to stop sending segment %d."
                                        % (peerip, sinfo.segmentid))
                        if self.remove_receiving_peer(peerip, sinfo.segmentid) == 0:
                            conn.send(pickle.dumps(Packet(MSG['OK'])))
                        else:
                            conn.send(pickle.dumps(Packet(MSG['ERR_PNOTFOUND'])))
                    if pkt.mtype == MSG['EXIT_COOP']:
                        segmentid = pkt.payload
                        logging.info("Notification from %s about exiting cooperation."
                                        % (peerip, ))
                        if self.remove_sending_peer(peerip, segmentid) == 0:
                            conn.send(pickle.dumps(Packet(MSG['OK'])))
                        else:
                            conn.send(pickle.dumps(Packet(MSG['ERR_PNOTFOUND'])))
                        # Shutdown the connection with this peer
                        self.poller.unregister(fd)
                        conn.close()
                        del self.connip[peerip]
                        del self.connfd[fd]
                        if peerip in self.outconn.keys():
                            self.outconn[peerip].close()
                            del self.outconn[peerip]
                    if pkt.mtype == MSG['HEARTBEAT']:
                        # Update heartbeat of the peer on receiving list
                        segmentid = pkt.payload.segmentid
                        logging.info("Heartbeat from %s about receiving sgement %d."
                                        % (peerip, segmentid))
                        if self.update_peer_heartbeat(peerip, segmentid) == 0:
                            conn.send(pickle.dumps(Packet(MSG['OK'])))
                        else:
                            conn.send(pickle.dumps(Packet(MSG['ERR_PNOTFOUND'])))
                elif fd == self.datasock.fileno() and event is select.POLLOUT:
                    """ send recoded packets
                    """
                    for sid in self.recvpeers.keys():
                        if self.sncbuf[sid][3] < 100 and not self.sncbuf[sid][4]:
                            # not sending cooperative packets if the buffer is not updated much
                            continue
                        for peer in self.recvpeers[sid]:
                            snc.snc_recode_packet_im(self.sncbuf[sid][1],
                                                     self.sncbuf[sid][2],
                                                     MLPI_SCHED)
                            pktstr = self.sncbuf[sid][2][0].serialize(self.sncbuf[sid][0].size_g,
                                                                      self.sncbuf[sid][0].size_p,
                                                                      self.sncbuf[sid][0].bnc)
                            try:
                                self.datasock.sendto(pktstr, (peer.ip, UDP_START+peer.sessionid))
                            except:
                                logging.warning("Caught exception sending to %s for segment %d."
                                                    % (peer.ip, sid))
                        self.sncbuf[sid][3] = 0 # reset new buffered counter
            self.housekeeping()  # Housekeeping to clean inactive peers

    def create_buffer(self, segmentid, sp):
        """ Create snc buffer for a segment and a given snc_parameters
              pkt - pointer to the packet sending buffer
        """
        try:
            pkt = snc.snc_alloc_empty_packet(byref(sp))
            self.sncbuf[segmentid] = [sp, snc.snc_create_buffer(byref(sp), 128), pkt, 0, False]
            if self.sncbuf[segmentid][1] is None:
                logging.error("Create buffer failed.")
        except Exception as detail:
            logging.error("Create buffer failed: %s" % (detail, ))
        return

    def check_curr_session_peers(self):
        """ Check out peer list of current session from server
        """
        # Check peers list of the session
        pkt = Packet(MSG['CHK_PEERS'], self.currsess)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        try:
            s.connect((HOST, PORT))
            s.send(pickle.dumps(pkt))
            reply = pickle.loads(s.recv(BUFSIZE))
            peers = reply.payload
            logging.info(("Available peers according to server: ["
                           +', '.join(['%s']*len(peers))+"]") % tuple(peers))
        except Exception as detail:
            logging.warning("Cannot connect to server %s " % (detail, ))
            peers = []
        s.close()
        return peers

    def establish_connection(self, peerip):
        """ Establish out-bound connections to other peers. The connections 
            are for sending control messages and get replies. On the other 
            hand, "incoming" connections are those accepted in poller().
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        try:
            s.connect((peerip, PORT+1))
            self.outconn[peerip] = s
            return 0
        except Exception as detail:
            logging.warning("Cannot connect to peer %s: %s" % (peerip, detail))
            s.close()
            return -1

    def request_help_curr_session(self, peerip):
        """ Request a peer to help send packets of my current segment
        """
        if not peerip or peerip in self.sendpeers:
            return
        if peerip not in self.outconn.keys():
            if self.establish_connection(peerip) == -1:
                logging.warning("Cannot connect to %s, skipping it." % (peerip,))
                return
        conn = self.outconn[peerip]
        pkt = Packet(MSG['ASK_COOP'], self.currsess)
        logging.info("Request %s to send segment %d to me." 
                        % (peerip, self.currsess.segmentid))
        try:
            conn.send(pickle.dumps(pkt))
            ret = pickle.loads(conn.recv(BUFSIZE))
            logging.info("Peer %s returns message %s " % (peerip, iMSG[ret.mtype]))
            if ret.mtype == MSG['OK']:
                logging.info("Peer %s acknowledged my request of sending segment %d."
                                % (peerip, self.currsess.segmentid))
                self.add_sending_peer(peerip)
                logging.info("Done. Add %s to sending list of segment %d." 
                                % (peerip, self.currsess.segmentid))
                logging.info(("Current sending list: ["
                                +', '.join(['%s']*len(self.sendpeers))+"]") 
                                % tuple(self.sendpeers))
            elif ret.mtype == MSG['ERR_PNOSEG']:
                logging.warning("Peer %s does not have segment %d."
                                    % (peerip, self.currsess.segmentid))
        except Exception as detail:
            logging.warning("Cannot connect to peer %s: %s" % (peerip, detail))
            conn.close()
            del self.outconn[peerip]
    
    def stop_recv_curr_session(self, peerip):
        """ Notify a peer to stop sending packets for current session
        """
        if peerip not in self.outconn.keys():
            if self.establish_connection(peerip) == -1:
                logging.warning("Cannot connect to %s, skipping it." % (peerip,))
                self.sendpeers.remove(peerip)
                logging.info("Done. Remove %s from sending list of segment %d."
                                % (peerip, self.currsess.segmentid))
                return
        conn = self.outconn[peerip]
        pkt = Packet(MSG['STOP_COOP'], self.currsess)
        logging.info("Request %s to stop sending segment %d to me."
                        % (peerip, self.currsess.segmentid))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(5.0)
        try:
            conn.send(pickle.dumps(pkt))
            ret = pickle.loads(conn.recv(BUFSIZE))
            logging.info("Peer %s returns message %s" % (peerip, iMSG[ret.mtype]))
            if ret.mtype == MSG['OK']:
                logging.info("Peer %s acknowledged my request of stopping segment %d."
                                % (peerip, self.currsess.segmentid))
            else:
                logging.warning("Peer %s doesn't have me on recv-list of segment %d."
                                % (peerip, self.currsess.segmentid))
        except Exception as detail:
            logging.warning("Cannot connect to peer %s %s" % (peerip, detail))
            conn.close()
            del self.outconn[peerip]
        # Aways remove the peer from sending list.
        self.sendpeers.remove(peerip)
        logging.info("Done. Remove %s from sending list of segment %d."
                        % (peerip, self.currsess.segmentid))
    
    def stop_recv_curr_session_all(self):
        """ Stop peers sending me packets of current segment
        """
        for peerip in list(self.sendpeers):
            # Note: make a copy of sendpeers to allow looping
            self.stop_recv_curr_session(peerip)

    def notify_exit_sending(self, peerip, segmentid):
        """ Notify remote peer that I'll stop sending a segment
        """
        if peerip not in self.outconn.keys():
            if self.establish_connection(peerip) == -1:
                logging.warning("Cannot connect to %s, skipping it." % (peerip,))
                self.remove_receiving_peer(peerip, segmentid)
                return
        conn = self.outconn[peerip]
        pkt = Packet(MSG['EXIT_COOP'], segmentid)
        logging.info("Notify %s about my exit of sending segment %d."
                        % (peerip, segmentid))
        try:
            conn.send(pickle.dumps(pkt))
            ret = pickle.loads(conn.recv(BUFSIZE))
            logging.info("Peer %s returns message %s" % (peerip, iMSG[ret.mtype]))
            if ret.mtype == MSG['OK']:
                logging.info("Peer %s acknowledged my exit of sending segment %d."
                                % (peerip, segmentid))
            else:
                logging.warning("Peer %s doesn't have me on send-list of segment %d."
                                    % (peerip, segmentid))
        except Exception as detail:
            logging.warning("Cannot connect to peer %s: %s" % (peerip, detail))
            conn.close()
            del self.outconn[peerip]
        self.remove_receiving_peer(peerip, segmentid)
        return

    def add_receiving_peer(self, peerip, segmentid, sessionid):
        """ Add peer to cooperation list of (receiving) a segment
            Return 
                 0  - Add successfully
                -1  - Request segment not available
        """
        if segmentid not in self.sncbuf:
            # segment not seen
            logging.info("Requested segment %d by %s is not available"
                            % (segmentid, peerip))
            return -1
        if segmentid not in self.recvpeers.keys():
            # first peer in list; initialize
            self.recvpeers[segmentid] = []
        if peerip in [p.ip for p in self.recvpeers[segmentid]]:
            logging.warning("%s is already in receiving list of segment %d."
                                % (peerip, segmentid))
        else:
            peer = HostInfo(peerip, sessionid)
            self.recvpeers[segmentid].append(peer)
            logging.info("Done. Add %s to receiving list of segment %d."
                            % (peerip, segmentid))
            peerips = [p.ip for p in self.recvpeers[segmentid]]
            logging.info(("Current receiving list of the segment ["
                           +', '.join(['%s']*len(peerips))+"]") % tuple(peerips))
        return 0

    def remove_receiving_peer(self, peerip, segmentid):
        """ Remove a peer from recv-list of a segment. The peer
            won't receive packets from me anymore.
            Return
                 0 - successful
                -1 - peer not in receiving list of the segment
        """
        if segmentid not in self.recvpeers:
            # no peer list for the segment
            logging.warning("No peers in receiving list of segment %d"
                                % (segmentid, ))
            return -1
        found = False
        for peer in self.recvpeers[segmentid]:
            if peer.ip == peerip:
                found = True
                self.recvpeers[segmentid].remove(peer)
                logging.info("Done. Remove %s from receiving list of segment %s" 
                                % (peerip, segmentid))
                return 0
        if not found:
            logging.warning("%s is not found in receiving list of segment %d."
                                % (peerip, segmentid))
            return -1

    def add_sending_peer(self, peerip):
        """ Add a peer to the send list of current session
            TO EXTEND: A host may receiving multiple sessions at a time.
            In this case, there would be multiple sending lists being
            maintained at a time.
        """
        if peerip and peerip not in self.sendpeers:
            self.sendpeers.append(peerip)
            return 0
        else:
            return -1

    def remove_sending_peer(self, peerip, segmentid):
        """ Remove a sending peer (from sendpeers)
        """
        if self.currsess.segmentid == segmentid \
            and peerip in self.sendpeers:
            self.sendpeers.remove(peerip)
            logging.info("Done. Remove %s from sending list of segment %s." 
                            % (peerip, segmentid))
            return 0
        else:
            logging.warning("%s is not in the receiving list of segment %d."
                                % (peerip, segmentid))
            return -1

    def heartbeat_sending_peers(self):
        """ Send heartbeat signal to peers sending packets to me
        """
        pkt = Packet(MSG['HEARTBEAT'], self.currsess)
        for peerip in self.sendpeers:
            if peerip not in self.outconn.keys():
                if self.establish_connection(peerip) == -1:
                    logging.warning("Cannot connect to %s, skipping it." % (peerip,))
                    return
            conn = self.outconn[peerip]
            logging.info("Heartbeat to %s to continue receiving segment %d."
                            % (peerip, self.currsess.segmentid))
            try:
                conn.send(pickle.dumps(pkt))
                ret = pickle.loads(conn.recv(BUFSIZE))
                logging.info("Peer %s returns message %s" % (peerip, iMSG[ret.mtype]))
                if ret.mtype == MSG['OK']:
                    logging.info("Peer %s acknowledged my hearbeat about segment %d."
                                    % (peerip, self.currsess.segmentid))
            except Exception as detail:
                logging.warning("Cannot connect to peer %s: %s" % (peerip, detail))
                # FIXME: If no reply from the peer, shoudl remove the peer from
                # the sending list of the segment.
                conn.close()
                del self.outconn[peerip]

    def update_peer_heartbeat(self, peerip, segmentid):
        if segmentid not in self.recvpeers:
            # no peer list for the segment
            logging.warning("No peers in receiving list of segment %d"
                                % (segmentid, ))
            return -1
        found = False
        for i, peer in enumerate(self.recvpeers[segmentid]):
            if peer.ip == peerip:
                found = True
                peer.set_heartbeat()
                self.recvpeers[segmentid][i] = peer
                logging.info("Done. Update heartbeat of %s in receiving list of segment %s" 
                                % (peerip, segmentid))
                peerips = [p.ip for p in self.recvpeers[segmentid]]
                logging.info(("Current receiving list of the segment ["
                               +', '.join(['%s']*len(peerips))+"]") % tuple(peerips))
                return 0
        if not found:
            logging.warning("%s is not found in receiving list of segment %d."
                                % (peerip, segmentid))
            return -1

    def housekeeping(self):
        """ Housekeeping of the cooperation process
            1, Remove peers from receving lists if there were no heartbeats for long time
        """
        now = datetime.now()
        if (now - self.lastClean).seconds > HK_INTVAL:
            logging.info("Coopeartion process do housekeeping...")
            # Clean up no heartbeat clients
            for segid in self.recvpeers.keys():
                for peer in copy.deepcopy(self.recvpeers[segid]):
                    if (now - peer.lastBeat).seconds > HK_INTVAL:
                        logging.info("Remove peer %s from segment %d because no heartbeat" 
                                        % (peer.ip, segid))
                        self.remove_receiving_peer(peer.ip, segid)
            self.lastClean = now

if len(sys.argv) == 1:
    filename = '/tmp/reffile.dat'
    # For test time
    storename = '/tmp/hello.tar.bz2'
else:
    filename = sys.argv[1]
    storename = sys.argv[1]+".copy"

if os.path.isfile(storename):
    os.remove(storename)
# Check file meta info
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(5.0)
s.connect((HOST, PORT))
pkt = Packet(MSG['CHK_FILE'], MetaInfo(filename))
s.send(pickle.dumps(pkt))
reply = pickle.loads(s.recv(BUFSIZE))
if reply.mtype == MSG['FILEMETA']:
    filemeta = reply.payload  # File meta information
    print(filemeta)
else:
    logging.info("Server returns message %s" % (iMSG[reply.mtype],))
    s.close()
    exit(0)
lastBeat = datetime.now()
# Set up sharing process
coop = Cooperation(filemeta)  # Pass the established server connection
(fdp, fdc) = mp.Pipe()
coop.fdp = fdp
coop.fdc = fdc
child = mp.Process(target=coop.main)
child.start()
coop.fdc.close()  # Close fdc on parent side
for segid in range(filemeta.numofseg):
    # Request file segment by segment
    # Get session information about the segment
    filemeta.segmentid = segid
    pkt = Packet(MSG['REQ_SEG'], filemeta)
    try:
        s.send(pickle.dumps(pkt))
        reply = pickle.loads(s.recv(BUFSIZE))
        sessioninfo = reply.payload  # Session info
        print(sessioninfo)
        # s.send(pickle.dumps(Packet(MSG['OK'], sessioninfo)))
    except Exception as detail:
        logging.warning("Cannot connect to server", detail)
        s.close()
        sys.exit(0)
    # Pass snc_parameters to sharing process, so that it creates
    # SNC packets buffer for the in-serving segment
    pkt = Packet(MSG['NEW_SESSION'], sessioninfo)
    coop.fdp.send(pkt)
    # Start receiving UDP data packets and decode the segment
    # Create decoder based on snc parameters
    decoder = snc.snc_create_decoder(byref(sessioninfo.sp), CBD_DECODER)
    sp_ptr = snc.snc_get_parameters(snc.snc_get_enc_context(decoder))
    udp_port = UDP_START + sessioninfo.sessionid
    ds = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ds.settimeout(5.0)
    ds.bind(('', udp_port))
    sncpkt_p = snc.snc_alloc_empty_packet(sp_ptr)  # recv packet buffer
    count_s = 0  # pkts from server
    count_p = 0  # pkts from peers
    while not snc.snc_decoder_finished(decoder):
        if (datetime.now() - lastBeat).seconds > HB_INTVAL:
            pkt = Packet(MSG['HEARTBEAT'], sessioninfo)
            try:
                s.send(pickle.dumps(pkt))
                reply = pickle.loads(s.recv(BUFSIZE))
                lastBeat = datetime.now()
            except Exception as detail:
                logging.warning("Cannot connect to server", detail)
                s.close()
                # break
            # Notify coop process to send heartbeat to peers
            coop.fdp.send(pkt)
        # Receive data packets
        try:
            (data, addr) = ds.recvfrom(1500)
        except socket.timeout.bnc:
            continue
        if addr[0] != HOST:
            count_p += 1
        else:
            count_s += 1
            # print("An SNC packet from peer ", addr[0])
            pkt = Packet(MSG['COOP_PKT'], copy.copy(data))
            coop.fdp.send(pkt)  # Forward a copy to coop process
        sncpkt_p.contents.deserialize(data, sp_ptr[0].size_g, sp_ptr[0].size_p, sp_ptr[0].bnc)
        snc.snc_process_packet(decoder, sncpkt_p)

    if snc.snc_decoder_finished(decoder):
        snc.snc_recover_to_file(c_char_p(storename.encode('utf-8')),
                                snc.snc_get_enc_context(decoder))
        logging.info("Finish segment %d" % (sessioninfo.segmentid,))
        # Request to stop the segment
        pkt = Packet(MSG['REQ_STOP'], sessioninfo)
        try:
            s.send(pickle.dumps(pkt))
            reply = pickle.loads(s.recv(BUFSIZE))
        except Exception as detail:
            logging.warning("Cannot connect to server", detail)
            s.close()
    pkt = Packet(MSG['END_SESSION'], sessioninfo)
    coop.fdp.send(pkt)
    snc.print_code_summary(snc.snc_get_enc_context(decoder), 
                           snc.snc_decode_overhead(decoder),
                           snc.snc_decode_cost(decoder))
    logging.info("Received from server: %d | Received from peers: %d"
            % (count_s, count_p))
    snc.snc_free_decoder(decoder)
    snc.snc_free_packet(sncpkt_p)
logging.info("Finished")
pkt = Packet(MSG['EXIT'], sessioninfo)
try:
    s.send(pickle.dumps(pkt))
    reply = pickle.loads(s.recv(BUFSIZE))
    if reply.mtype  == MSG['OK']:
        logging.info("Server has acknowledged my exit.")
except Exception as detail:
    logging.warning("Cannot connect to server", detail)
s.close()
coop.fdp.send(Packet(MSG['EXIT_PROC']))
child.join()
