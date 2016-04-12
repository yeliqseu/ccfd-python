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
        which segment is currently being decoded and ask peers to send packets to the decoder.
        For other segments (which decoder has recovered), cooperation process continues
        sending packets to requesting peers, until it is told to stop (by peers or parent).

        filemeta  - metainfo of the file sharing with peers
        fdp       - parent side of the pipe; will be closed in child process
        fdc       - child side of the pipe
        pcl       - socket for peer control
        datasock  - data socket sending recoded packets
        currsess  - session meta of the segment currently being received in decoder
        recvpeers - peers sending packets belonging to current session to the decoder
                    ['ip1', 'ip2', ...]
        sncbuf    - buffer of segments for recoding
                    {'segmentid': [sp, buf_p, pkt_p, new, done]}
        peers     - connected peers sharing segments
                    {'segmentid': [hostinfo, ...]}
                    hostinfo contains peer's ip and its session id. The latter
                    indicates what UDP port data packets should be sent to.
    """
    def __init__(self, filemeta):
        self.filemeta = filemeta
        self.fdp = None       # fd of pipe of the parent side
        self.fdc = None       # fd of pipe of the child side
        self.pcl = None       # TCP socket for peer ctrl
        self.datasock = None  # UDP socket for data transmission
        self.currsess = None  # Meta info of current session
        self.recvpeers = []   # Peers cooperating on current session
        self.sncbuf = {}      # Each segment has a snc buffer
        self.peers  = {}      # Each segment has a list of peers
        self.lastClean = datetime.now()

    def main(self):
        """ Main function of the cooperation process
            A sharing process is responsible for transmission
            cooperation with other peers requesting the same
            file.
        """
        # Close fdp on child side
        self.fdp.close()
        # Setup TCP socket for receiving share requests
        self.pcl = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.pcl.settimeout(5.0)
        self.pcl.bind(('', PORT+1))  # bind (PORT+1), specifically for sharing
        self.pcl.listen(10)
        # Setup UDP socket for sending recoded packets
        self.datasock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Poll s, fdc, datasock
        # s        - messages from peers
        # fdc      - messages from parent (e.g., snc packets)
        # datasock - sending snc recoded packets to peers
        poller = select.poll()  # poll fdc and datasock
        poller.register(self.pcl.fileno(), select.POLLIN)
        poller.register(self.fdc.fileno(), select.POLLIN)
        poller.register(self.datasock.fileno(), select.POLLOUT)
        logging.info("Start main loop of cooperation...")
        while True:
            for fd, event in poller.poll():
                if fd == self.pcl.fileno() and event is select.POLLIN:
                    """ peer to peer control
                         - ask for share
                         - stop share
                         - exiting
                         - heartbeat
                    """
                    conn, addr = self.pcl.accept()
                    pkt = pickle.loads(conn.recv(BUFSIZE))
                    # logging.info('Peer connection of type %d from %s' % (pkt.mtype, addr[0]))
                    if pkt.mtype == MSG['ASK_COOP']:
                        sinfo = pkt.payload
                        logging.info("Request from %s to send segment %d."
                                        % (addr[0], sinfo.segmentid))
                        if self.add_receiving_peer(addr[0],
                                                   sinfo.segmentid, 
                                                   sinfo.sessionid) == 0:
                            conn.send(pickle.dumps(Packet(MSG['OK'])))
                        else:
                            conn.send(pickle.dumps(Packet(MSG['ERR_PNOSEG'])))
                        # Send request if it is the same segment as we need
                        if self.currsess and sinfo.segmentid == self.currsess.segmentid:
                            self.request_help_curr_session(addr[0])
                    if pkt.mtype == MSG['STOP_COOP']:
                        sinfo = pkt.payload
                        logging.info("Request from %s to stop sending segment %d."
                                        % (addr[0], sinfo.segmentid))
                        if self.remove_receiving_peer(addr[0], sinfo.segmentid) == 0:
                            conn.send(pickle.dumps(Packet(MSG['OK'])))
                        else:
                            conn.send(pickle.dumps(Packet(MSG['ERR_PNOTFOUND'])))
                    if pkt.mtype == MSG['EXIT_COOP']:
                        segmentid = pkt.payload
                        logging.info("Notification from %s about exiting cooperation."
                                        % (addr[0], ))
                        if self.remove_sending_peer(addr[0], segmentid) == 0:
                            conn.send(pickle.dumps(Packet(MSG['OK'])))
                        else:
                            conn.send(pickle.dumps(Packet(MSG['ERR_PNOTFOUND'])))
                    if pkt.mtype == MSG['HEARTBEAT']:
                        # Update heartbeat of the peer on receiving list
                        segmentid = pkt.payload.segmentid
                        logging.info("Heartbeat from %s about receiving sgement %d."
                                        % (addr[0], segmentid))
                        if self.update_peer_heartbeat(addr[0], segmentid) == 0:
                            conn.send(pickle.dumps(Packet(MSG['OK'])))
                        else:
                            conn.send(pickle.dumps(Packet(MSG['ERR_PNOTFOUND'])))
                    conn.close()
                if fd == self.fdc.fileno() and event is select.POLLIN:
                    """ message from parent
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
                        peers = self.check_curr_session_peers()
                        for peerip in peers:
                            self.request_help_curr_session(peerip)
                    if pkt.mtype == MSG['END_SESSION']:
                        sinfo = pkt.payload
                        logging.info("End session %d (segment %d)" % (sinfo.sessionid, sinfo.segmentid))
                        if self.currsess:
                            self.stop_recv_curr_session_all()
                        self.sncbuf[self.currsess.segmentid][4] = True
                        self.currsess = None
                    if pkt.mtype == MSG['EXIT_PROC']:
                        logging.info("Clean up cooperation process before exiting...")
                        for segid in self.peers.keys():
                            for peer in copy.deepcopy(self.peers[segid]):
                                # I'm exiting. Please all receiving peers stop receiving from me
                                self.notify_exit_sending(peer.ip, segid)
                            snc.snc_free_buffer(self.sncbuf[segid][1])
                            snc.snc_free_packet(self.sncbuf[segid][2])
                        logging.info("Cooperation process exiting...")
                        exit(0)
                    if pkt.mtype == MSG['COOP_PKT']:
                        pkt_p = snc.snc_alloc_empty_packet(byref(self.currsess.sp))  # recv packet buffer
                        pkt_p.contents.deserialize(pkt.payload, 
                                                   self.currsess.sp.size_g,
                                                   self.currsess.sp.size_p,
                                                   self.currsess.sp.bnc)
                        snc.snc_buffer_packet(self.sncbuf[self.currsess.segmentid][1], pkt_p)
                        self.sncbuf[self.currsess.segmentid][3] += 1
                    if pkt.mtype == MSG['HEARTBEAT']:
                        # send heartbeat to peers on sending list
                        self.heartbeat_sending_peers()
                if fd == self.datasock.fileno() and event is select.POLLOUT:
                    """ send recoded packets
                    """
                    for sid in self.peers.keys():
                        if self.sncbuf[sid][3] < 100 and not self.sncbuf[sid][4]:
                            # not sending cooperative packets if the buffer is not updated much
                            continue
                        for peer in self.peers[sid]:
                            # print("Send a recoded packet from segment %s to %s at port %s" 
                            #        % (sid, peer.ip, peer.sessionid+UDP_START))
                            # print("Transmission opportunity to peers is available")
                            snc.snc_recode_packet_im(self.sncbuf[sid][1], self.sncbuf[sid][2], MLPI_SCHED)
                            pktstr = self.sncbuf[sid][2].contents.serialize(self.sncbuf[sid][0].size_g,
                                                                            self.sncbuf[sid][0].size_p,
                                                                            self.sncbuf[sid][0].bnc)
                            try:
                                self.datasock.sendto(pktstr, (peer.ip, UDP_START+peer.sessionid))
                            except:
                                logging.warning("Caught exception sending to %s for segment %d."
                                        % (peer.ip, sid))
                        self.sncbuf[sid][3] = 0 # reset new buffered counter
            # Do housekeeping
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
            logging.info(("Available peers according to server: ["+', '.join(['%s']*len(peers))+"]") % tuple(peers))
            s.close()
        except Exception as detail:
            logging.warning("Cannot connect to server %s " % (detail, ))
            peers = []
            s.close()
        return peers

    def request_help_curr_session(self, peerip):
        """ Request a peer to send packets for a segment specified in sessioninfo
        """
        if not peerip or peerip in self.recvpeers:
            return
        pkt = Packet(MSG['ASK_COOP'], self.currsess)
        logging.info("Request %s to send segment %d to me." 
                        % (peerip, self.currsess.segmentid))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        try:
            s.connect((peerip, PORT+1))
            s.send(pickle.dumps(pkt))
            if pickle.loads(s.recv(BUFSIZE)).mtype == MSG['OK']:
                logging.info("Peer %s acknowledged my request of sending segment %d."
                                % (peerip, self.currsess.segmentid))
                self.add_sending_peer(peerip)
                logging.info("Done. Add %s to sending list of segment %d." 
                                % (peerip, self.currsess.segmentid))
                logging.info(("Current sending list: ["+', '.join(['%s']*len(self.recvpeers))+"]") 
                                % tuple(self.recvpeers))
            else:
                logging.warning("Peer %s does not have segment %d."
                                % (peerip, self.currsess.segmentid))
            s.close()
        except Exception as detail:
            logging.warning("Cannot connect to peer %s: %s" % (peerip, detail))
            s.close()
    
    def stop_recv_curr_session(self, peerip):
        """ Notify a peer to stop sending packets for current session
        """
        pkt = Packet(MSG['STOP_COOP'], self.currsess)
        logging.info("Request %s to stop sending segment %d to me."
                        % (peerip, self.currsess.segmentid))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        try:
            s.connect((peerip, PORT+1))
            s.send(pickle.dumps(pkt))
            if pickle.loads(s.recv(BUFSIZE)).mtype == MSG['OK']:
                logging.info("Peer %s acknowledged my request of stopping segment %d."
                                % (peerip, self.currsess.segmentid))
            else:
                logging.warning("Peer %s doesn't have me on his receiving-list of segment %d."
                                % (peerip, self.currsess.segmentid))
            s.close()
        except Exception as detail:
            logging.warning("Cannot connect to peer %s %s" % (peerip, detail))
            s.close()
        # Remove peers from sending list no matter peers acknowledged or 
        # connection failed
        self.recvpeers.remove(peerip)
        logging.info("Done. Remove %s from sending list of segment %d."
                        % (peerip, self.currsess.segmentid))
    
    def stop_recv_curr_session_all(self):
        """ Notify all peer to stop sending packets for current segment
        """
        for peerip in list(self.recvpeers):
            # Note: make a copy of recvpeers to allow looping
            self.stop_recv_curr_session(peerip)

    def notify_exit_sending(self, peerip, segmentid):
        """ Notify remote peer that I'll stop sending a segment
        """
        pkt = Packet(MSG['EXIT_COOP'], segmentid)
        logging.info("Notify %s about my exit of sending segment %d."
                        % (peerip, segmentid))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        try:
            s.connect((peerip, PORT+1))
            s.send(pickle.dumps(pkt))
            if pickle.loads(s.recv(BUFSIZE)).mtype == MSG['OK']:
                logging.info("Peer %s acknowledged my exit of sending segment %d."
                                % (peerip, segmentid))
            else:
                logging.warning("Peer %s doesn't have me on his sending-list of segment %d."
                                % (peerip, segmentid))
            s.close()
        except Exception as detail:
            logging.warning("Cannot connect to peer %s: %s" % (peerip, detail))
            s.close()
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
            logging.info("%s: requested segment %d by %s is not available"
                            % (__name__, segmentid, peerip))
            return -1
        if segmentid not in self.peers.keys():
            # first peer in list; initialize
            self.peers[segmentid] = []
        if peerip in [p.ip for p in self.peers[segmentid]]:
            logging.warning("%s is already in receiving list of segment %d."
                            % (peerip, segmentid))
        else:
            peer = HostInfo(peerip, sessionid)
            self.peers[segmentid].append(peer)
            logging.info("Done. Add %s to receiving list of segment %d."
                            % (peerip, segmentid))
        return 0

    def remove_receiving_peer(self, peerip, segmentid):
        """ Remove a peer from cooperation list of a segment. The peer
            won't receive packets from me anymore.
            Return
                 0 - successful
                -1 - peer not in receiving list of the segment
        """
        if segmentid not in self.peers:
            # no peer list for the segment
            logging.warning("No peers in receiving list of segment %d"
                                % (segmentid, ))
            return -1
        found = False
        for peer in self.peers[segmentid]:
            if peer.ip == peerip:
                found = True
                self.peers[segmentid].remove(peer)
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
        if peerip and peerip not in self.recvpeers:
            self.recvpeers.append(peerip)
            return 0
        else:
            return -1

    def remove_sending_peer(self, peerip, segmentid):
        """ Remove a sending peer (from recvpeers)
        """
        if self.currsess.segmentid == segmentid \
            and peerip in self.recvpeers:
            self.recvpeers.remove(peerip)
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
        for peerip in self.recvpeers:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            logging.info("Heartbeat to %s to continue receiving segment %d."
                            % (peerip, self.currsess.segmentid))
            s.settimeout(5.0)
            try:
                s.connect((peerip, PORT+1))
                s.send(pickle.dumps(pkt))
                if pickle.loads(s.recv(BUFSIZE)).mtype == MSG['OK']:
                    logging.info("Peer %s acknowledged my hearbeat about segment %d."
                                    % (peerip, self.currsess.segmentid))
            except Exception as detail:
                logging.warning("Cannot connect to peer %s: %s" % (peerip, detail))
                # FIXME: If no reply from the peer, shoudl remove the peer from
                # the sending list of the segment.
            s.close()

    def update_peer_heartbeat(self, peerip, segmentid):
        if segmentid not in self.peers:
            # no peer list for the segment
            logging.warning("No peers in receiving list of segment %d"
                                % (segmentid, ))
            return -1
        found = False
        for i, peer in enumerate(self.peers[segmentid]):
            if peer.ip == peerip:
                found = True
                peer.set_heartbeat()
                self.peers[segmentid][i] = peer
                logging.info("Done. Update heartbeat of %s in receiving list of segment %s" 
                                % (peerip, segmentid))
                peerips = [p.ip for p in self.peers[segmentid]]
                logging.info(("Current receiving list of segment ["+', '.join(['%s']*len(peerips))+"]") % tuple(peerips))
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
            for segid in self.peers.keys():
                for peer in copy.deepcopy(self.peers[segid]):
                    if (now - peer.lastBeat).seconds > HK_INTVAL:
                        logging.info("Remove peer %s from segment %d because no heartbeat" 
                                        % (peer.ip, segid))
                        self.remove_receiving_peer(peer.ip, segid)
            self.lastClean = now


filename = '/tmp/reffile.dat'
# For test time
storename = '/tmp/hello.tar.bz2'
if os.path.isfile(storename):
    os.remove(storename)
# Check file meta info
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
pkt = Packet(MSG['CHK_FILE'], MetaInfo(filename))
s.send(pickle.dumps(pkt))
reply = pickle.loads(s.recv(BUFSIZE))
s.close()
filemeta = reply.payload  # File meta information
print(filemeta)
lastBeat = datetime.now()
# Set up sharing process
coop = Cooperation(filemeta)
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
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5.0)
    try:
        s.connect((HOST, PORT))
        s.send(pickle.dumps(pkt))
        reply = pickle.loads(s.recv(BUFSIZE))
        sessioninfo = reply.payload  # Session info
        print(sessioninfo)
        s.send(pickle.dumps(Packet(MSG['OK'])))
        s.close()
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
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5.0)
            try:
                s.connect((HOST, PORT))
                s.send(pickle.dumps(pkt))
                reply = pickle.loads(s.recv(BUFSIZE))
                s.close()
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
        snc.snc_recover_to_file(c_char_p(b'/tmp/hello.tar.bz2'),
                                snc.snc_get_enc_context(decoder))
        logging.info("Finish segment %d" % (sessioninfo.segmentid,))
        # Request to stop the segment
        pkt = Packet(MSG['REQ_STOP'], sessioninfo)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        try:
            s.connect((HOST, PORT))
            s.send(pickle.dumps(pkt))
            reply = pickle.loads(s.recv(BUFSIZE))
            s.close()
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
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
pkt = Packet(MSG['EXIT'], sessioninfo)
s.send(pickle.dumps(pkt))
reply = pickle.loads(s.recv(BUFSIZE))
if reply.mtype  == MSG['OK']:
    logging.info("Server has acknowledged my exit.")
s.close()
coop.fdp.send(Packet(MSG['EXIT_PROC']))
child.join()
