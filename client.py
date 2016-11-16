import sys
import socket
import pickle
import select
import logging
from datetime import datetime
from ccstructs import *
import multiprocessing as mp
import copy
import getopt

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s: [%(asctime)s] %(message)s')
HK_INTVAL = 15  # Housekeeping interval
TIMEOUT = 1.0     # TCP connect request timeout


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
        sendpeers - peers sending packets belonging to current session to me
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

        inconnip    - accepted TCP connections from peers (i.e., in-bound)
                      {'ip1': conn1, 'ip2': conn2, ...}
        inconnfd    - fd-connection dict of the accepted connections from peers
                      {'fd1': conn1, 'fd2': conn2, ...}
        outconn     - out-bound connections of the current node for sending
                      control messages to peers.
        availpeers  - available peers that we may connect to to help current segment.
                      ['ip1', 'ip2', ...]
        lastClean   - Last time housekeeping was performed. Cooperation process periodically
                      checks list of receiving peers and removes peers that didn't heartbeat
                      for a long time.
    """
    def __init__(self, serverhost, filemeta):
        self.server = serverhost
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
        self.inconnip = {}    # Established in-bound peer TCP connections
        self.inconnfd = {}    # fd-connection mapping of in-bound connections
        self.outconn = {}     # Established out-bound TCP connections
        self.availpeers = []  # Available peers that we may connect to
        self.lastClean = datetime.now()

    def main(self):
        """ Main function of the cooperation process
        """
        self.fdp.close() # Close fdp on child side
        # Setup TCP socket for receiving requests
        self.pcl = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.pcl.settimeout(TIMEOUT)
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
        logging.info("[COOP] Start main loop of cooperation...")
        while True:
            for fd, event in self.poller.poll(1):
                if event & (select.POLLHUP | select.POLLERR | select.POLLNVAL):
                    # Remove broken file descriptors. (e.g. disconntected peers)
                    # print("Unregister broken fd: %d" % (fd, ))
                    if fd in self.inconnfd.keys():
                        self.inconnip = {k: v for k, v in self.inconnip.items() if v != self.inconnfd[fd]}
                        del self.inconnfd[fd]
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
                        logging.info("[COOP] Create SNC buffer for segment %d"
                                        % (sinfo.segmentid,))
                        self.create_buffer(sinfo.segmentid, sinfo.sp)
                        logging.info("[COOP] SNC buffer for segment %d created successfully"
                                        % (sinfo.segmentid,))
                        if self.currsess:
                            logging.warning("[COOP] A session is currently marked as running.")
                        self.currsess = sinfo  # set new current session
                        # Check available peers from server
                        peers = self.check_available_peers()
                        if peers == []:
                            logging.info("[COOP] No peers available to cooperate for now.")
                            continue
                        # Add available peers into list
                        for peerip in peers:
                            if peerip not in self.sendpeers and peerip not in self.availpeers:
                                logging.info("[COOP] Add %s to available peer list." % (peerip, ))
                                self.availpeers.append(peerip)
                    if pkt.mtype == MSG['END_SESSION']:
                        sinfo = pkt.payload
                        logging.info("[COOP] End session %d (segment %d)"
                                        % (sinfo.sessionid, sinfo.segmentid))
                        if self.currsess:
                            self.stop_recv_curr_session_all()
                        self.sncbuf[self.currsess.segmentid][4] = True
                        self.currsess = None
                    if pkt.mtype == MSG['EXIT_PROC']:
                        logging.info("[COOP] Clean up cooperation process before exiting...")
                        for segid in self.recvpeers.keys():
                            for peer in copy.deepcopy(self.recvpeers[segid]):
                                # I'm exiting. Notify receiving peers.
                                self.notify_exit_sending(peer.ip, segid)
                            snc.snc_free_buffer(self.sncbuf[segid][1])
                            snc.snc_free_packet(self.sncbuf[segid][2])
                        logging.info("[COOP] Cooperation process exiting...")
                        exit(0)
                    if pkt.mtype == MSG['COOP_PKT']:
                        logging.debug("[COOP] Get a coded packet from parent.")
                        try:
                            pkt_p = snc.snc_alloc_empty_packet(byref(self.currsess.sp))
                            logging.debug("[COOP] Finished snc_alloc_empty_packet.")
                            pkt_p.contents.deserialize(pkt.payload,
                                                    self.currsess.sp.size_g,
                                                    self.currsess.sp.size_p,
                                                    self.currsess.sp.bnc)
                            logging.debug("[COOP] Finished deserialize.")
                            snc.snc_buffer_packet(self.sncbuf[self.currsess.segmentid][1], pkt_p)
                            logging.debug("[COOP] Finished snc_buffer_packet.")
                            self.sncbuf[self.currsess.segmentid][3] += 1
                        except Exception as detail:
                            logging.info("[COOP] Something wrong when buffering the packet", detail)
                        logging.debug("[COOP] Finished buffering the coded packet from parent.")
                    if pkt.mtype == MSG['HEARTBEAT']:
                        # send heartbeat to peers on the sending list
                        #FIXME: better to move it to where POLLOUT is handled
                        self.heartbeat_sending_peers()
                elif fd == self.pcl.fileno() and event is select.POLLIN:
                    """ new control connection from a peer node
                    """
                    conn, addr = self.pcl.accept()
                    peerip, port = conn.getpeername()
                    # Check if there was outdated connection with this peer
                    if peerip in self.inconnip.keys():
                        del self.inconnip[peerip]
                        logging.info("[COOP] Deleted the old connection as we get a new connection from %s." % (peerip, ))
                    self.inconnip[peerip] = conn
                    self.inconnfd[conn.fileno()] = conn
                    self.poller.register(conn.fileno(), select.POLLIN)
                    logging.info("[COOP] Accepted the new connection from %s on fd %d."
                                    % (peerip, conn.fileno()))
                elif event is select.POLLIN:
                    """ Messages from established peer connections
                         - ask for share
                         - stop share
                         - exiting
                         - heartbeat
                    """
                    conn = self.inconnfd[fd]
                    try:
                        peerip, port = conn.getpeername()
                        logging.info("[COOP] Message from %s is available on %d." % (peerip, fd))
                        pkt = pickle.loads(conn.recv(BUFSIZE))
                    except Exception as detail:
                        logging.warning("[COOP] Message on fd %d not obtained. Error: %s"
                                            % (fd, detail))
                    if pkt.mtype == MSG['ASK_COOP']:
                        sinfo = pkt.payload
                        logging.info("[COOP] Get ASK_COOP [%s] from %s for segment %d."
                                        % (pkt.stamp, peerip, sinfo.segmentid))
                        if self.add_receiving_peer(peerip,
                                                   sinfo.segmentid,
                                                   sinfo.sessionid) == 0:
                            try:
                                conn.send(pickle.dumps(Packet(MSG['OK'])))
                            except Exception as detail:
                                logging.warning("[COOP] Cannot send OK to %s. Error: %s" % (peerip, detail))
                        else:
                            conn.send(pickle.dumps(Packet(MSG['ERR_PNOSEG'])))
                        # If the peer is downloading the same segment as me, mark it as
                        # an available peer for cooperation
                        if (self.currsess is not None
                            and sinfo.segmentid == self.currsess.segmentid
                            and peerip not in self.sendpeers):
                            logging.info("[COOP] Add %s to available peer list." % (peerip, ))
                            self.availpeers.append(peerip)
                    if pkt.mtype == MSG['STOP_COOP']:
                        sinfo = pkt.payload
                        logging.info("[COOP] Get STOP_COOP [%s] from %s for segment %d."
                                        % (pkt.stamp, peerip, sinfo.segmentid))
                        if self.remove_receiving_peer(peerip, sinfo.segmentid) == 0:
                            conn.send(pickle.dumps(Packet(MSG['OK'])))
                        else:
                            conn.send(pickle.dumps(Packet(MSG['ERR_PNOTFOUND'])))
                        #FIXME: we might need to shutdown the established in-bound connection
                        #       because the connections are per session (i.e., segment).
                        self.poller.unregister(fd)
                        conn.close()
                        del self.inconnip[peerip]
                        del self.inconnfd[fd]
                    if pkt.mtype == MSG['EXIT_COOP']:
                        segmentid = pkt.payload
                        logging.info("[COOP] Get EXIT_COOP [%s] from %s for segment %d."
                                        % (pkt.stamp, peerip, segmentid))
                        if self.remove_sending_peer(peerip, segmentid) == 0:
                            conn.send(pickle.dumps(Packet(MSG['OK'])))
                        else:
                            conn.send(pickle.dumps(Packet(MSG['ERR_PNOTFOUND'])))
                        # Shutdown the in-bound connection of the peer
                        self.poller.unregister(fd)
                        conn.close()
                        del self.inconnip[peerip]
                        del self.inconnfd[fd]
                        # Shutdown the out-bound connection to the peer
                        if peerip in self.outconn.keys():
                            self.outconn[peerip].close()
                            del self.outconn[peerip]
                    if pkt.mtype == MSG['HEARTBEAT']:
                        # Update heartbeat of the peer on receiving list
                        segmentid = pkt.payload.segmentid
                        logging.info("[COOP] Get HEARTBEAT [%s] from %s for segment %d."
                                        % (pkt.stamp, peerip, segmentid))
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
                                logging.warning("[COOP] Caught exception sending to %s for segment %d."
                                                    % (peer.ip, sid))
                        self.sncbuf[sid][3] = 0 # reset new buffered counter
            # Try to connect to available peers to cooperate the current segment
            if len(self.availpeers) != 0:
                self.connect_to_available_peers()
            # Housekeeping to clean inactive peers
            self.housekeeping()

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

    def check_available_peers(self):
        """ Check out peer list of current session from server
        """
        # Check peers list of the session
        pkt = Packet(MSG['CHK_PEERS'], self.currsess)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TIMEOUT)
        try:
            s.connect((self.server, PORT))
            s.send(pickle.dumps(pkt))
            reply = pickle.loads(s.recv(BUFSIZE))
            peers = reply.payload
            logging.info(("[COOP] Available peers according to server: ["
                           +', '.join(['%s']*len(peers))+"]") % tuple(peers))
        except Exception as detail:
            logging.warning("[COOP] Cannot connect to the server. Error: %s " % (detail, ))
            peers = []
        s.close()
        return peers

    def establish_connection(self, peerip):
        """ Establish out-bound connections to other peers. The connections
            are only used for sending control messages.

            On the peer side, the connections are accepted and handled in poller().
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TIMEOUT)
        try:
            s.connect((peerip, PORT+1))
        except Exception as detail:
            logging.warning("[COOP] Cannot connect to peer %s. Error: %s" % (peerip, detail))
            s.close()
            return -1
        self.outconn[peerip] = s
        logging.info("[COOP] Established out-bound connection to %s." % (peerip, ))
        return 0

    def connect_to_available_peers(self):
        """ Connect to available peers. Request their help on current segment.
            It will establish a connection to the peer's pcl socket and store
            the established connection in outconn[]. If the connection wasn't
            successful, the peer is removed from available
        """
        logging.info("[COOP] Connect to available peers...")
        # print(self.availpeers)
        for peerip in copy.deepcopy(self.availpeers):
            logging.info("[COOP] Connecting to peer %s..." % (peerip, ))
            if self.establish_connection(peerip) == -1:
                logging.warning("[COOP] Skip requesting peer %s for help; remove it." % (peerip,))
                self.availpeers.remove(peerip)
                continue
            conn = self.outconn[peerip] # the established connection
            pkt = Packet(MSG['ASK_COOP'], self.currsess)
            logging.info("[COOP] Sending ASK_COOP [%s] to %s for segment %d."
                            % (pkt.stamp, peerip, self.currsess.segmentid))
            try:
                conn.send(pickle.dumps(pkt))
                ret = pickle.loads(conn.recv(BUFSIZE))
                logging.info("[COOP] Peer %s returns message %s " % (peerip, iMSG[ret.mtype]))
                if ret.mtype == MSG['OK']:
                    logging.info("[COOP] Peer %s acknowledged my request of sending segment %d."
                                    % (peerip, self.currsess.segmentid))
                    self.add_sending_peer(peerip)
                    logging.info("[COOP] Added %s to sending list of segment %d."
                                    % (peerip, self.currsess.segmentid))
                    logging.info(("[COOP] Current sending list: ["
                                    +', '.join(['%s']*len(self.sendpeers))+"]")
                                    % tuple(self.sendpeers))
                elif ret.mtype == MSG['ERR_PNOSEG']:
                    logging.warning("[COOP] Peer %s does not have segment %d."
                                        % (peerip, self.currsess.segmentid))
            except Exception as detail:
                logging.warning("[COOP] Cannot send to %s. Error: %s" % (peerip, detail))
                conn.close()
                del self.outconn[peerip]
            self.availpeers.remove(peerip)

    def stop_recv_curr_session(self, peerip):
        """ Notify a peer to stop sending packets for current session
        """
        if peerip not in self.outconn.keys():
            if self.establish_connection(peerip) == -1:
                logging.warning("[COOP] Skip notifying peer %s to stop sending." % (peerip,))
                self.sendpeers.remove(peerip)
                logging.info("[COOP] Removed %s from sending list of segment %d."
                                % (peerip, self.currsess.segmentid))
                return
        conn = self.outconn[peerip]
        pkt = Packet(MSG['STOP_COOP'], self.currsess)
        logging.info("[COOP] Sending STOP_COOP [%s] to %s for segment %d."
                        % (pkt.stamp, peerip, self.currsess.segmentid))
        try:
            conn.send(pickle.dumps(pkt))
            ret = pickle.loads(conn.recv(BUFSIZE))
            logging.info("[COOP] Peer %s returns message %s [%s]"
                            % (peerip, iMSG[ret.mtype], ret.stamp))
            if ret.mtype == MSG['OK']:
                logging.info("[COOP] Peer %s acknowledged my request of stopping segment %d."
                                % (peerip, self.currsess.segmentid))
            else:
                logging.warning("[COOP] Peer %s doesn't have me on recv-list of segment %d."
                                % (peerip, self.currsess.segmentid))
        except Exception as detail:
            logging.warning("[COOP] Cannot send STOP_COOP to %s. Error: %s" % (peerip, detail))
            conn.close()
            del self.outconn[peerip]
        # Aways remove the peer from sending list.
        self.sendpeers.remove(peerip)
        logging.info("[COOP] Removed %s from sending list of segment %d."
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
                logging.warning("[COOP] Skip notifying peer %s of my exit." % (peerip,))
                self.remove_receiving_peer(peerip, segmentid)
                return
        conn = self.outconn[peerip]
        pkt = Packet(MSG['EXIT_COOP'], segmentid)
        logging.info("[COOP] Send EXIT_COOP [%s] to %s for segment %d."
                        % (pkt.stamp, peerip, segmentid))
        try:
            conn.send(pickle.dumps(pkt))
            ret = pickle.loads(conn.recv(BUFSIZE))
            logging.info("[COOP] Peer %s returns message %s" % (peerip, iMSG[ret.mtype]))
            if ret.mtype == MSG['OK']:
                logging.info("[COOP] Peer %s acknowledged my exit of sending segment %d."
                                % (peerip, segmentid))
            else:
                logging.warning("[COOP] Peer %s doesn't have me on send-list of segment %d."
                                    % (peerip, segmentid))
        except Exception as detail:
            logging.warning("[COOP] Cannot connect to peer %s. Error: %s" % (peerip, detail))
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
            logging.warning("[COOP] Requested segment %d by %s is not available"
                            % (segmentid, peerip))
            return -1
        if segmentid not in self.recvpeers.keys():
            # first peer in list; initialize
            self.recvpeers[segmentid] = []
        if peerip in [p.ip for p in self.recvpeers[segmentid]]:
            logging.warning("[COOP] %s is already in receiving list of segment %d."
                                % (peerip, segmentid))
        else:
            peer = HostInfo(peerip, sessionid)
            self.recvpeers[segmentid].append(peer)
            logging.info("[COOP] Added %s to receiving list of segment %d."
                            % (peerip, segmentid))
            peerips = [p.ip for p in self.recvpeers[segmentid]]
            logging.info(("[COOP] Current receiving list of the segment ["
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
            logging.warning("[COOP] No peers in receiving list of segment %d"
                                % (segmentid, ))
            return -1
        found = False
        for peer in self.recvpeers[segmentid]:
            if peer.ip == peerip:
                found = True
                self.recvpeers[segmentid].remove(peer)
                logging.info("[COOP] Removed %s from receiving list of segment %s"
                                % (peerip, segmentid))
                return 0
        if not found:
            logging.warning("[COOP] %s is not found in receiving list of segment %d."
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
                    logging.warning("[COOP] Skip sending heartbeat to %s." % (peerip,))
                    return
            conn = self.outconn[peerip]
            logging.info("[COOP] Sending HEARTBEAT [%s] to %s for segment %d."
                            % (pkt.stamp, peerip, self.currsess.segmentid))
            try:
                conn.send(pickle.dumps(pkt))
                ret = pickle.loads(conn.recv(BUFSIZE))
                logging.info("[COOP] Peer %s returns message %s" % (peerip, iMSG[ret.mtype]))
                if ret.mtype == MSG['OK']:
                    logging.info("[COOP] Peer %s acknowledged my hearbeat about segment %d."
                                    % (peerip, self.currsess.segmentid))
            except Exception as detail:
                logging.warning("[COOP] Cannot send to %s. Error: %s" % (peerip, detail))
                # FIXME: If no reply from the peer, shoudl remove the peer from
                # the sending list of the segment.
                conn.close()
                del self.outconn[peerip]

    def update_peer_heartbeat(self, peerip, segmentid):
        if segmentid not in self.recvpeers:
            # no peer list for the segment
            logging.warning("[COOP] No peers in receiving list of segment %d"
                                % (segmentid, ))
            return -1
        found = False
        for i, peer in enumerate(self.recvpeers[segmentid]):
            if peer.ip == peerip:
                found = True
                peer.set_heartbeat()
                self.recvpeers[segmentid][i] = peer
                logging.info("[COOP] Updated heartbeat of %s in receiving list of segment %s"
                                % (peerip, segmentid))
                peerips = [p.ip for p in self.recvpeers[segmentid]]
                logging.info(("[COOP] Current receiving list of the segment ["
                               +', '.join(['%s']*len(peerips))+"]") % tuple(peerips))
                return 0
        if not found:
            logging.warning("[COOP] %s is not found in receiving list of segment %d."
                                % (peerip, segmentid))
            return -1

    def housekeeping(self):
        """ Housekeeping of the cooperation process
            1, Remove peers from receving lists if there were no heartbeats for long time
        """
        now = datetime.now()
        if (now - self.lastClean).seconds > HK_INTVAL:
            logging.info("[COOP] Coopeartion process do housekeeping...")
            # Clean up no heartbeat clients
            for segid in self.recvpeers.keys():
                for peer in copy.deepcopy(self.recvpeers[segid]):
                    if (now - peer.lastBeat).seconds > HK_INTVAL:
                        logging.info("[COOP] Remove peer %s from segment %d because no heartbeat"
                                        % (peer.ip, segid))
                        self.remove_receiving_peer(peer.ip, segid)
            self.lastClean = now


if __name__ == "__main__":
    serverhost = ''
    filepath = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:],"hs:f:",["server=","filepath="])
    except getopt.GetoptError:
        print('ccsclient.py -s <server ip> -f <filepath>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('ccsclient.py -s <server ip> -f <filepath>')
            sys.exit()
        elif opt in ("-s", "--server"):
            serverhost = arg
        elif opt in ("-f", "--filepath"):
            filepath = arg
    if serverhost == '' or filepath == '':
        print('ccsclient.py -s <server ip> -f <filepath>')
        sys.exit(2)

    # For test time
    storename = '/tmp/hello.tar.bz2'  # where to save downloaded file
    if os.path.isfile(storename):
        os.remove(storename)
    # Check file meta info
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(TIMEOUT)
    s.connect((serverhost, PORT))
    pkt = Packet(MSG['CHK_FILE'], MetaInfo(filepath))
    s.send(pickle.dumps(pkt))
    reply = pickle.loads(s.recv(BUFSIZE))
    if reply.mtype == MSG['FILEMETA']:
        filemeta = reply.payload  # File meta information
        print(filemeta)
    else:
        logging.info("[MAIN] Server returns message %s" % (iMSG[reply.mtype],))
        s.close()
        exit(0)
    lastBeat = datetime.now()
    # Set up sharing process
    coop = Cooperation(serverhost, filemeta)  # Pass the established server connection
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
            logging.warning("[MAIN] Cannot connect to server. Error: %s" % (detail, ))
            s.close()
            sys.exit(0)
        # Pass snc_parameters to sharing process, so that it creates
        # SNC packets buffer for the in-serving segment
        pkt = Packet(MSG['NEW_SESSION'], sessioninfo)
        try:
            coop.fdp.send(pkt)
        except Exception as detail:
            logging.warning("[MAIN] Cannot send NEW_SESSION to coop process. Error: %s" % (detail, ))
        # Start receiving UDP data packets and decode the segment
        # Create decoder based on snc parameters
        decoder = snc.snc_create_decoder(byref(sessioninfo.sp), CBD_DECODER)
        sp_ptr = snc.snc_get_parameters(snc.snc_get_enc_context(decoder))
        udp_port = UDP_START + sessioninfo.sessionid
        ds = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ds.settimeout(TIMEOUT)
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
                    logging.warning("[MAIN] Cannot connect to server. Error: %s", (detail, ))
                    s.close()
                    # break
                # Notify coop process to send heartbeat to peers
                try:
                    coop.fdp.send(pkt)
                except Exception as detail:
                    logging.warning("[MAIN] Cannot notify coop process to heartbeat peers")
            # Receive data packets
            try:
                (data, addr) = ds.recvfrom(1500)
            except socket.timeout:
                continue
            if addr[0] != serverhost:
                count_p += 1
            else:
                count_s += 1
                # print("An SNC packet from peer ", addr[0])
                pkt = Packet(MSG['COOP_PKT'], copy.copy(data))
                try:
                    coop.fdp.send(pkt)  # Forward a copy to coop process
                except Exception as detail:
                    logging.warning("[MAIN] Cannot forward packet to coop process")
                    if not child.is_alive():
                        logging.info("[MAIN] Coop process is dead, exiting...")
                        exit(1)
                    else:
                        logging.info("[MAIN] Coop process is still alive, but not reachable")
            sncpkt_p.contents.deserialize(data, sp_ptr[0].size_g, sp_ptr[0].size_p, sp_ptr[0].bnc)
            snc.snc_process_packet(decoder, sncpkt_p)

        if snc.snc_decoder_finished(decoder):
            snc.snc_recover_to_file(c_char_p(storename.encode('utf-8')),
                                    snc.snc_get_enc_context(decoder))
            logging.info("[MAIN] Finish segment %d" % (sessioninfo.segmentid,))
            # Request to stop the segment
            pkt = Packet(MSG['REQ_STOP'], sessioninfo)
            try:
                s.send(pickle.dumps(pkt))
                reply = pickle.loads(s.recv(BUFSIZE))
            except Exception as detail:
                logging.warning("[MAIN] Cannot connect to server. Error: %s", (detail, ))
                s.close()
        pkt = Packet(MSG['END_SESSION'], sessioninfo)
        try:
            coop.fdp.send(pkt)
        except Exception as detail:
            logging.warning("[MAIN] Cannot send END_SESSION to coop process")
        snc.print_code_summary(snc.snc_get_enc_context(decoder),
                               snc.snc_decode_overhead(decoder),
                               snc.snc_decode_cost(decoder))
        logging.info("[MAIN] Received from server: %d | Received from peers: %d"
                % (count_s, count_p))
        snc.snc_free_decoder(decoder)
        snc.snc_free_packet(sncpkt_p)
    logging.info("[MAIN] Finished")
    pkt = Packet(MSG['EXIT'], sessioninfo)
    try:
        s.send(pickle.dumps(pkt))
        reply = pickle.loads(s.recv(BUFSIZE))
        if reply.mtype  == MSG['OK']:
            logging.info("[MAIN] Server has acknowledged my exit.")
    except Exception as detail:
        logging.warning("[MAIN] Cannot connect to server. Error: %s", (detail, ))
    s.close()
    try:
        coop.fdp.send(Packet(MSG['EXIT_PROC']))
    except Exception as detail:
        logging.warning("[MAIN] Cannot send EXIT_PROC to coop process. Error: %s", (detail, ))
    child.join()
