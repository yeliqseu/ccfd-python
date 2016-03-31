import sys
import socket
import pickle
import select
from datetime import datetime
from ccstructs import *
import multiprocessing as mp
import copy
#HOST = '127.0.0.1'
HOST = '172.30.40.4'
#HOST = '192.168.1.110'

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

    def create_buffer(self, segmentid, sp):
        """ Create snc buffer for a segment and a given snc_parameters
              pkt - pointer to the packet sending buffer
        """
        pkt = snc.snc_alloc_empty_packet(byref(sp))
        self.sncbuf[segmentid] = [sp, snc.snc_create_buffer(byref(sp), 128), pkt, 0, False]
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
            print("Available peers according to server: ", peers)
            s.close()
        except Exception as detail:
            print("Cannot connect to peer", peerip, detail)
            peers = []
            s.close()
        return peers

    def request_help_curr_session(self, peerip):
        """ Request a peer to send packets for a segment specified in sessioninfo
        """
        if peerip and peerip not in self.recvpeers:
            pkt = Packet(MSG['ASK_COOP'], self.currsess)
            print("Request %s to send segment %d to me." 
                    % (peerip, self.currsess.segmentid))
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5.0)
            try:
                s.connect((peerip, PORT+1))
                s.send(pickle.dumps(pkt))
                if pickle.loads(s.recv(BUFSIZE)).mtype == MSG['OK']:
                    print("Peer %s acknowledged my request of sending segment %d."
                            % (peerip, self.currsess.segmentid))
                    self.add_sending_peer(peerip)
                    print("Done. Add %s to sending list of segment %d." 
                            % (peerip, self.currsess.segmentid))
                    print("Current sending list: ", self.recvpeers)
                s.close()
            except Exception as detail:
                print("Cannot connect to peer", peerip, detail)
                s.close()
    
    def stop_recv_curr_session(self, peerip):
        """ Notify a peer to stop sending packets for current session
        """
        pkt = Packet(MSG['STOP_COOP'], self.currsess)
        print("Request %s to stop sending segment %d to me."
                % (peerip, self.currsess.segmentid))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        try:
            s.connect((peerip, PORT+1))
            s.send(pickle.dumps(pkt))
            if pickle.loads(s.recv(BUFSIZE)).mtype == MSG['OK']:
                print("Peer %s acknowledged my request of stopping segment %d."
                        % (peerip, self.currsess.segmentid))
                self.recvpeers.remove(peerip)
                print("Done. Remove %s from sending list of segment %d."
                        % (peerip, self.currsess.segmentid))
            s.close()
        except Exception as detail:
            print("Cannot connect to peer ", peerip, detail)
            s.close()
    
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
        print("Notify %s about my exit of sending segment %d."
                % (peerip, segmentid))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        try:
            s.connect((peerip, PORT+1))
            s.send(pickle.dumps(pkt))
            if pickle.loads(s.recv(BUFSIZE)).mtype == MSG['OK']:
                print("Peer %s acknowledged my exit of sending segment %d."
                        % (peerip, segmentid))
            s.close()
        except Exception as detail:
            print("Cannot connect to peer", peerip, detail)
            s.close()
        self.remove_receiving_peer(peerip, segmentid)
        return

    def add_receiving_peer(self, peerip, segmentid, sessionid):
        """ Add peer to cooperation list of (receiving) a segment
        """
        if segmentid not in self.sncbuf:
            # segment not seen
            print("%s: requested segment %d by %s is not available"
                   % (__name__, segmentid, peerip))
        if segmentid not in self.peers.keys():
            # first peer in list; initialize
            self.peers[segmentid] = []
        if peerip in [p.ip for p in self.peers[segmentid]]:
            print("Warning: %s is already in receiving list of segment %d."
                    % (peerip, segmentid))
        else:
            peer = HostInfo(peerip, sessionid)
            self.peers[segmentid].append(peer)
            print("Done. Add %s to receiving list of segment %d."
                    % (peerip, segmentid))
        return

    def remove_receiving_peer(self, peerip, segmentid):
        """ Remove a peer from cooperation list of a segment. The peer
            won't receive packets from me anymore.
        """
        if segmentid not in self.peers:
            # no peer list for the segment
            print("No peers in receiving list of segment %d"
                    % (segmentid, ))
            return
        found = False
        for peer in self.peers[segmentid]:
            if peer.ip == peerip:
                found = True
                self.peers[segmentid].remove(peer)
                print("Done. Remove %s from receiving list of segment %s" 
                        % (peerip, segmentid))
        if not found:
            print("Warning: %s is not found in receiving list of segment %d."
                    % (peerip, segmentid))
        return

    def add_sending_peer(self, peerip):
        """ Add a peer to the send list of current session
            TO EXTEND: A host may receiving multiple sessions at a time.
            In this case, there would be multiple sending lists being
            maintained at a time.
        """
        if peerip and peerip not in self.recvpeers:
            self.recvpeers.append(peerip)
        return

    def remove_sending_peer(self, peerip, segmentid):
        """ Remove a sending peer (from recvpeers)
        """
        if self.currsess.segmentid == segmentid \
            and peerip in self.recvpeers:
            self.recvpeers.remove(peerip)
            print("Done. Remove %s from sending list of segment %s." 
                    % (peerip, segmentid))
        else:
            print("Warning: %s is not in the receiving list of segment %d."
                    % (peerip, segmentid))



def cooperation_main(coop):
    """ Main function of the cooperation process
        A sharing process is responsible for transmission
        cooperation with other peers requesting the same
        file.
    """
    # Close fdp on child side
    coop.fdp.close()
    # Setup TCP socket for receiving share requests
    coop.pcl = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    coop.pcl.settimeout(5.0)
    coop.pcl.bind(('', PORT+1))  # bind (PORT+1), specifically for sharing
    coop.pcl.listen(10)
    # Setup UDP socket for sending recoded packets
    coop.datasock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Poll s, fdc, datasock
    # s        - receive requests from peers
    # fdc      - receive message from parent (e.g., snc packets)
    # datasock - sending snc recoded packets to peers
    poller = select.poll()  # poll fdc and datasock
    poller.register(coop.pcl.fileno(), select.POLLIN)
    poller.register(coop.fdc.fileno(), select.POLLIN)
    poller.register(coop.datasock.fileno(), select.POLLOUT)
    print("Start main loop of cooperation...")
    while True:
        for fd, event in poller.poll():
            if fd == coop.pcl.fileno() and event is select.POLLIN:
                """ peer to peer control
                     - ask for share
                     - stop share
                     - exiting
                     - heartbeat
                """
                conn, addr = coop.pcl.accept()
                pkt = pickle.loads(conn.recv(BUFSIZE))
                print('Peer connection of type %d from %s' % (pkt.mtype, addr[0]))
                if pkt.mtype == MSG['ASK_COOP']:
                    sinfo = pkt.payload
                    print("Request from %s to send segment %d."
                            % (addr[0], sinfo.segmentid))
                    coop.add_receiving_peer(addr[0], sinfo.segmentid, sinfo.sessionid)
                    conn.send(pickle.dumps(Packet(MSG['OK'])))
                    # Send request if it is the same segment as we need
                    if coop.currsess and sinfo.segmentid == coop.currsess.segmentid:
                        coop.request_help_curr_session(addr[0])
                if pkt.mtype == MSG['STOP_COOP']:
                    sinfo = pkt.payload
                    print("Request from %s to stop sending segment %d."
                            % (addr[0], sinfo.segmentid))
                    coop.remove_receiving_peer(addr[0], sinfo.segmentid)
                    conn.send(pickle.dumps(Packet(MSG['OK'])))
                if pkt.mtype == MSG['EXIT_COOP']:
                    segmentid = pkt.payload
                    print("Notification from %s about exiting cooperation."
                            % (addr[0], ))
                    coop.remove_sending_peer(addr[0], segmentid)
                    conn.send(pickle.dumps(Packet(MSG['OK'])))
                conn.close()
            if fd == coop.fdc.fileno() and event is select.POLLIN:
                """ message from parent
                     - session/snc_parameters
                     - duplicated snc packets to buffer
                     - exit sharing
                """
                pkt = coop.fdc.recv()
                if pkt.mtype == MSG['NEW_SESSION']:
                    sinfo = pkt.payload
                    print("Create SNC buffer for segment ", sinfo.segmentid)
                    coop.create_buffer(sinfo.segmentid, sinfo.sp)
                    if coop.currsess:
                        print("Warning: a session is currently marked as running.")
                    coop.currsess = sinfo  # set new current session
                    peers = coop.check_curr_session_peers()
                    for peerip in peers:
                        coop.request_help_curr_session(peerip)
                if pkt.mtype == MSG['END_SESSION']:
                    sinfo = pkt.payload
                    print("End session %d (segment %d)" % (sinfo.sessionid, sinfo.segmentid))
                    if coop.currsess:
                        coop.stop_recv_curr_session_all()
                    coop.sncbuf[coop.currsess.segmentid][4] = True
                    coop.currsess = None
                if pkt.mtype == MSG['EXIT_PROC']:
                    print("Clean up cooperation process before exiting...")
                    for segid in coop.peers.keys():
                        for peer in copy.deepcopy(coop.peers[segid]):
                            # I'm exiting. Please all receiving peers stop receiving from me
                            coop.notify_exit_sending(peer.ip, segid)
                        snc.snc_free_buffer(coop.sncbuf[segid][1])
                        snc.snc_free_packet(coop.sncbuf[segid][2])
                    print("Cooperation process exiting...")
                    exit(0)
                if pkt.mtype == MSG['COOP_PKT']:
                    pkt_p = snc.snc_alloc_empty_packet(byref(coop.currsess.sp))  # recv packet buffer
                    pkt_p.contents.deserialize(pkt.payload, 
                                               coop.currsess.sp.size_g,
                                               coop.currsess.sp.size_p)
                    snc.snc_buffer_packet(coop.sncbuf[coop.currsess.segmentid][1], pkt_p)
                    coop.sncbuf[coop.currsess.segmentid][3] += 1
                    # print("Buffered a packet for segment %d (gid=%d)" % (coop.currsess.segmentid, pkt_p[0].gid))
            if fd == coop.datasock.fileno() and event is select.POLLOUT:
                """ send recoded packets
                """
                for sid in coop.peers.keys():
                    if coop.sncbuf[sid][3] < 100 and not coop.sncbuf[sid][4]:
                        # not sending cooperative packets if the buffer is not updated much
                        continue
                    for peer in coop.peers[sid]:
                        # print("Send a recoded packet from segment %s to %s at port %s" 
                        #        % (sid, peer.ip, peer.sessionid+UDP_START))
                        # print("Transmission opportunity to peers is available")
                        snc.snc_recode_packet_im(coop.sncbuf[sid][1], coop.sncbuf[sid][2], MLPI_SCHED)
                        pktstr = coop.sncbuf[sid][2].contents.serialize(coop.sncbuf[sid][0].size_g,
                                                                        coop.sncbuf[sid][0].size_p)
                        try:
                            coop.datasock.sendto(pktstr, (peer.ip, UDP_START+peer.sessionid))
                        except:
                            print("Caught exception sending to %s for segment %d."
                                    % (peer.ip, sid))
                    coop.sncbuf[sid][3] = 0 # reset new buffered counter


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
child = mp.Process(target=cooperation_main, args=(coop, ))
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
        print("Cannot connect to server", detail)
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
                print("Cannot connect to server", detail)
                s.close()
                break
        try:
            (data, addr) = ds.recvfrom(1500)
        except socket.timeout:
            continue
        if addr[0] != HOST:
            count_p += 1
        else:
            count_s += 1
            # print("An SNC packet from peer ", addr[0])
            pkt = Packet(MSG['COOP_PKT'], copy.copy(data))
            coop.fdp.send(pkt)  # Forward a copy to coop process
        sncpkt_p.contents.deserialize(data, sp_ptr[0].size_g, sp_ptr[0].size_p)
        snc.snc_process_packet(decoder, sncpkt_p)

    if snc.snc_decoder_finished(decoder):
        snc.snc_recover_to_file(c_char_p(b'/tmp/hello.tar.bz2'),
                                snc.snc_get_enc_context(decoder))
        print("Finish segment", sessioninfo.segmentid)
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
            print("Cannot connect to server", detail)
            s.close()
    pkt = Packet(MSG['END_SESSION'], sessioninfo)
    coop.fdp.send(pkt)
    snc.print_code_summary(snc.snc_get_enc_context(decoder), 
                           snc.snc_code_overhead(decoder),
                           snc.snc_decode_cost(decoder))
    print("Received from server: %d | Received from peers: %d"
            % (count_s, count_p))
    snc.snc_free_decoder(decoder)
    snc.snc_free_packet(sncpkt_p)
print("Finished")
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
pkt = Packet(MSG['EXIT'], sessioninfo)
s.send(pickle.dumps(pkt))
reply = pickle.loads(s.recv(BUFSIZE))
if reply.mtype  == MSG['OK']:
    print("Server has acknowledged my exit.")
s.close()
coop.fdp.send(Packet(MSG['EXIT_PROC']))
child.join()
