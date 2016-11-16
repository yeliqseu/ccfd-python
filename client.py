import sys
import socket
import pickle
import select
import logging
from datetime import datetime
from ccstruct import *
import multiprocessing as mp
import copy
import getopt
from channel import *

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
        self.fdp = None       # fd of pipe of the parent side
        self.fdc = None       # fd of pipe of the child side
        self.ppchid = None    # Pipe channel with parent process
        self.pcchid = None    # Peer control channel
        self.svchid = None    # Server channel id
        self.currsess = None  # Meta info of current session
        self.sendpeers = {}   # Peers cooperating on current session
                              # {'ip1': chanfd1, 'ip2': chanfd2}
        self.recvpeers = {}   # Each segment has a list of receiving peers
                              # {'segmentid': [ [hostinfo1, chfd1_c, chfd1_d],
                              #                 [hostinfo2, chfd2_c, chfd2_d],
                              #                 [...]
                              #  'segmentid2': []
                              # }
        self.sncbuf = {}      # Each segment has a snc buffer
        self.channels = {}    # Established channels (TCP, UDP, pipe) with other entities
                              # I should have a channel with each sending peer (outconn), and two
                              # channels with each receiving peer: 1) inconn for TCP control messages,
                              # and 2) CH_TYEP_UDP_S for sending snc coded packets
                              # Data structure: {ch_id: Channel()}
        self.availpeers = []  # Available peers that we may connect to
        self.toexit = False   # Received EXIT notice from parent, will exit after cleaning every thing
        self.lastClean = datetime.now()

    def main(self):
        """ Main function of the cooperation process
        """
        self.fdp.close() # Close fdp on child side
        # Establish pipe channel with parent process
        self.ppchid = open_pipe_channel(self.channels, self.fdc)
        # Setup peer control channel
        self.pcchid = open_listen_channel(self.channels, PORT+1)

        logging.info("[COOP] Start main loop of cooperation...")
        while True:
            poll_channels(self.channels)

            # Accept connections on pcl
            pcl_ch = self.channels[self.pcchid]
            if pcl_ch.eventmask & CH_READ:
                self.accept_connection(pcl_ch.handle)

            # perform control IO on all channels
            self.client_IO(self.channels)

            # perform recoding and send data packets if it's time
            self.recode_and_send()

            # do housekeeping, clean up outdated/failed stuff
            self.housekeeping()

    def accept_connection(self, listen_s):
        conn, addr = listen_s.accept()
        peerip, port = conn.getpeername()
        chid = open_inconn_channel(self.channels, conn)
        logging.info("[COOP] Accepted the new connection from %s, channel id: %d."
                        % (peerip, chid))

    def client_IO(self, chans):
        # receive from parent pipe
        pipe_ch = self.channels[self.ppchid]
        if (pipe_ch.eventmask & CH_READ):
            # read message
            logging.debug("[COOP] Pipe channel is readable.")
            while True:
                msg = pipe_ch.receive()
                # logging.info("[COOP] Message received on pipe channel: %s" % (msg,))
                if not msg:
                    break
                pkt = CCPacket()
                pkt.parse(msg)
                if pkt.header.mtype != MSG['DATA']:
                    logging.debug("[COOP] Receive %s from parent process" % (iMSG[pkt.header.mtype],))
                self.process_parent(pkt)

        # receive from server port (peerinfo only so far)
        if self.svchid:
            serv_ch = self.channels[self.svchid]
            if (serv_ch.eventmask & CH_READ):
                logging.debug("[COOP] Server channel is readable.")
                while True:
                    msg = serv_ch.receive()
                    if not msg:
                        break
                    pkt = CCPacket()
                    pkt.parse(msg)
                    logging.info("[COOP] Receive %s from server host" % (iMSG[pkt.header.mtype],))
                    if pkt.header.mtype == MSG['PEERINFO']:
                        peerlist = pkt.body
                        logging.info('Available peers: %s' % (peerlist, ))
                        self.connect_to_available_peers(peerlist)
                serv_ch.state = CH_STATE_CLOSE  # Mark the channel as to-be-closed
                self.svchid = None

        # Process other channels which are from peer nodes
        # We should iterate over peers instead of channels, because channel list
        # may change during iteration
        # Process
        for chid in list(self.channels):
            ch = self.channels[chid]
            if (ch.eventmask & CH_READ):
                while True:
                    msg = ch.receive()
                    # logging.info("[COOP] Message received on pipe channel: %s" % (msg,))
                    if not msg:
                        break
                    pkt = CCPacket()
                    pkt.parse(msg)
                    # logging.info("[COOP] Receive %s from %s" % (iMSG[pkt.header.mtype], ch.remote[0]))
                    self.process_peer(chid, pkt)
        return

    def process_parent(self, pkt):
        if pkt.header.mtype == MSG['DATA']:
            logging.debug("[COOP] Get a coded packet from parent.")
            try:
                pkt_p = snc.snc_alloc_empty_packet(byref(self.currsess.sp))
                logging.debug("[COOP] Finished snc_alloc_empty_packet.")
                pkt_p.contents.deserialize(pkt.body,
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

        elif pkt.header.mtype == MSG['NEW_SESSION']:
            sinfo = pkt.body
            logging.info("[COOP] Create SNC buffer for segment %d"
                            % (sinfo.segmentid,))
            self.create_buffer(sinfo.segmentid, sinfo.sp)
            logging.info("[COOP] SNC buffer for segment %d created successfully"
                            % (sinfo.segmentid,))
            if self.currsess:
                logging.warning("[COOP] A session is currently marked as running.")
            self.currsess = sinfo  # set new current session
            # Establish a connection to server to check available peers
            self.svchid = open_outconn_channel(self.channels, self.server, PORT)
            logging.info("[COOP] Opened channel to server host, channel id: %d" % (self.svchid, ))
            reply = CCPacket(CCHeader(MSG['CHK_PEERS']), self.currsess)
            self.channels[self.svchid].send(reply.packed())
            logging.info("[COOP] Enqueued CHK_PEERS to server channel (chid: %d)." % (self.svchid, ))

        elif pkt.header.mtype == MSG['END_SESSION']:
            sinfo = pkt.body
            logging.info("[COOP] End session %d (segment %d)"
                            % (sinfo.sessionid, sinfo.segmentid))
            if self.currsess:
                self.stop_recv_curr_session_all()
            self.sncbuf[self.currsess.segmentid][4] = True
            # clean up sending list for the segment
            logging.info("[COOP] Clean up sending peer list for segment %d" % (self.currsess.segmentid, ))
            self.sendpeers = {}
            self.currsess = None

        elif pkt.header.mtype == MSG['EXIT_PROC']:
            logging.info("[COOP] Clean up cooperation process before exiting...")
            logging.info("[COOP] Time to exit. Check sendpeers and recvpeers")
            logging.info("[COOP] sendpeers: %s" % (self.sendpeers,))
            logging.info("[COOP] recvpeers: %s" % (self.recvpeers,))
            for segid in self.recvpeers.keys():
                for st in copy.deepcopy(self.recvpeers[segid]):
                    # Note: each segmentid corresponds to a list of [hostinfo, chid_ctrl, chid_data]
                    # I'm exiting. Notify receiving peers.
                    peerip = st[0].ip
                    self.notify_exit_sending(peerip, segid)
            self.toexit = True

        elif pkt.header.mtype == MSG['HEARTBEAT']:
            # send heartbeat to peers on the sending list
            self.heartbeat_sending_peers()

    def process_peer(self, chid, pkt):
        ch = self.channels[chid]
        peerip = ch.remote[0]

        if pkt.header.mtype == MSG['ASK_COOP']:
            sinfo = pkt.body
            logging.info("[COOP] Get ASK_COOP [] from %s on channel (chid: %d) for segment %d."
                            % (peerip, chid, sinfo.segmentid))
            # ``Establish'' UDP data channel to the host
            chid_d = self.add_receiving_peer(peerip, sinfo.segmentid, sinfo.sessionid, chid)
            if chid_d >= 0:
                reply = CCPacket(CCHeader(MSG['ASK_COOP_ACK']), sinfo)
                logging.info("[COOP] Enqueue ASK_COOP_ACK onto channel (chid: %d) to %s for segment %d."
                                        % (chid, peerip, sinfo.segmentid))
                logging.info("[COOP] Data channel to %s for segment %d is : %d"
                                        % (peerip, sinfo.segmentid, chid_d))
                ch.send(reply.packed())
            else:
                reply = CCPacket(CCHeader(MSG['ERR_PNOSEG']), sinfo)
                ch.send(reply.packed())
                logging.warning("[COOP] Enqueue ERR_PNOSEG onto channel (chid: %d) to %s for segment %d."
                                        % (chid, peerip, sinfo.segmentid))
                self.channels[chid].state = CH_STATE_CLOSE
            # If the peer is downloading the same segment as me, ask for his help too
            if (self.currsess is not None
                and sinfo.segmentid == self.currsess.segmentid
                and peerip not in self.sendpeers):
                self.connect_to_available_peers([peerip])
            elif (self.currsess is not None
                and sinfo.segmentid == self.currsess.segmentid
                and peerip in self.sendpeers):
                logging.info("[COOP] Peer %s is already a sendpeer of segment %d, pass." %
                                (peerip, self.currsess.segmentid))

        elif pkt.header.mtype == MSG['ASK_COOP_ACK']:
            sinfo = pkt.body
            peerip = ch.remote[0]
            logging.info("[COOP] Get ASK_COOP_ACK [] from %s on channel (chid: %d) for segment %d."
                            % (peerip, chid, sinfo.segmentid))
            if (self.currsess is not None and sinfo.segmentid == self.currsess.segmentid):
                self.add_sending_peer(peerip, chid)

        elif pkt.header.mtype == MSG['STOP_COOP']:
            sinfo = pkt.body
            logging.info("[COOP] Get STOP_COOP [] from %s on channel (chid: %d)  for segment %d."
                            % ( peerip, chid, sinfo.segmentid))
            # Just remove peer from recvpeer list, doesn't need to ack
            if self.remove_receiving_peer(peerip, sinfo.segmentid) == 0:
                pass
            else:
                pass
            # shutdown the channel
            self.channels[chid].state = CH_STATE_CLOSE

        elif pkt.header.mtype == MSG['EXIT_COOP']:
            segmentid = pkt.body
            logging.info("[COOP] Get EXIT_COOP [] from %s on channel (chid: %d) for segment %d."
                            % ( peerip, chid, segmentid))
            if self.remove_sending_peer(peerip, segmentid) == 0:
                reply = CCPacket(CCHeader(MSG['EXIT_COOP_ACK']), segmentid)
            else:
                reply = CCPacket(CCHeader(MSG['EXIT_COOP_NOPEER']), segmentid)
            ch.send(reply.packed())
            logging.info("[COOP] Enqueue %s onto channel (chid: %d) to %s for segment %d."
                                    % (iMSG[reply.header.mtype], chid, peerip, segmentid))
            self.channels[chid].state = CH_STATE_CLOSE

        elif pkt.header.mtype == MSG['EXIT_COOP_ACK'] or\
                pkt.header.mtype == MSG['EXIT_COOP_NOPEER']:
            segmentid = pkt.body
            logging.info("[COOP] Get %s [] from %s on channel (chid: %d) for segment %d."
                            % ( iMSG[pkt.header.mtype], peerip, chid, segmentid))
            self.remove_receiving_peer(peerip, segmentid)
            self.channels[chid].state = CH_STATE_CLOSE

        elif pkt.header.mtype == MSG['HEARTBEAT']:
            # Update heartbeat of the peer on receiving list
            sinfo = pkt.body
            segmentid = pkt.body.segmentid
            logging.info("[COOP] Get HEARTBEAT [] from %s on channel (chid: %d) for segment %d."
                            % ( peerip, chid, segmentid))
            if self.update_peer_heartbeat(peerip, segmentid) == 0:
                reply = CCPacket(CCHeader(MSG['HEARTBEAT_ACK']), sinfo)
                ch.send(reply.packed())
                logging.info("[COOP] Enqueue %s onto channel (chid: %d) to %s for segment %d."
                                        % (iMSG[reply.header.mtype], chid, peerip, segmentid))
            else:
                reply = CCPacket(CCHeader(MSG['ERR_PNOTFOUND']), sinfo)
                ch.send(reply.packed())
                logging.info("[COOP] Enqueue %s onto channel (chid: %d) to %s for segment %d."
                                        % (iMSG[reply.header.mtype], chid, peerip, segmentid))

        elif pkt.header.mtype == MSG['HEARTBEAT_ACK']:
            sinfo = pkt.body
            segmentid = pkt.body.segmentid
            logging.info("[COOP] Get HEARTBEAT_ACK [] from %s on channel (chid: %d) for segment %d."
                            % ( peerip, chid, segmentid))

        elif pkt.header.mtype == MSG['ERR_PNOTFOUND']:
            sinfo = pkt.body
            logging.info("[COOP] Get ERR_PNOTFOUND [] from %s on channel (chid: %d) for segment %d."
                            % ( peerip, chid, segmentid))
            logging.info("[COOP] Let's remove peer %s from sending list." % (peerip, ))
            self.remove_sending_peer(peerip, sinfo.segmentid)
            self.channels[chid].state = CH_STATE_CLOSE

    def recode_and_send(self):
        # go through recv peers
        # recv peers have two chanels: TCP (for control) and UDP (for data)
        for segid in self.recvpeers:
            # not sending cooperative packets if the buffer is not updated much
            if self.sncbuf[segid][3] < 100 and not self.sncbuf[segid][4]:
                logging.debug("[COOP] Not time to recode and send for segment %d" % (segid, ))
                continue
            for entity in self.recvpeers[segid]:
                ch = self.channels[entity[2]]  # data channel
                snc.snc_recode_packet_im(self.sncbuf[segid][1], self.sncbuf[segid][2], MLPI_SCHED)
                pktstr = self.sncbuf[segid][2][0].serialize(self.sncbuf[segid][0].size_g,
                                                            self.sncbuf[segid][0].size_p,
                                                            self.sncbuf[segid][0].bnc)
                ch.send(CCPacket(CCHeader(MSG['DATA']), pktstr).packed())
                logging.debug("[COOP] Enqueued DATA onto channel (chid: %d) to %s for segment %d"
                            % (ch.chid, entity[0].ip, segid))
            self.sncbuf[segid][3] = 0 # reset new buffered counter
        return

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

    def connect_to_available_peers(self, availpeers):
        """ Connect to available peers. Request their help on current segment.
            It will establish a connection to the peer's pcl socket and store
            the established connection in outconn[]. If the connection wasn't
            successful, the peer is removed from available
        """
        logging.info("[COOP] Connect to available peers...")
        if len(availpeers) == 0:
            logging.info("[COOP] Empty available peer list.")
            return
        # print(self.availpeers)
        for peerip in copy.deepcopy(availpeers):
            logging.info("[COOP] Initiate outconn channel to peer %s..." % (peerip, ))
            chid = open_outconn_channel(self.channels, peerip, PORT+1)
            pkt = CCPacket(CCHeader(MSG['ASK_COOP']), self.currsess)
            logging.info("[COOP] Enqueued ASK_COOP [] onto channel (chid: %d) to %s for segment %d."
                            % (chid, peerip, self.currsess.segmentid))
            self.channels[chid].send(pkt.packed())
            availpeers.remove(peerip)

    def stop_recv_curr_session(self, peerip):
        """ Notify a peer to stop sending packets for current session
        """
        chid = self.sendpeers[peerip]
        ch = self.channels[chid]
        pkt = CCPacket(CCHeader(MSG['STOP_COOP']), self.currsess)
        logging.info("[COOP] Enqueue STOP_COOP onto channel (chid: %d) to %s for segment %d."
                                % (chid, peerip, self.currsess.segmentid))
        ch.send(pkt.packed())
        ch.state = CH_STATE_CLOSE

    def stop_recv_curr_session_all(self):
        """ Stop peers sending me packets of current segment
        """
        for peerip in list(self.sendpeers):
            # Note: make a copy of sendpeers to allow looping
            self.stop_recv_curr_session(peerip)

    def notify_exit_sending(self, peerip, segmentid):
        """ Notify remote peer that I'll stop sending a segment
        """
        # Find receiving peer's inconn control channel
        chid = -1
        for entity in self.recvpeers[segmentid]:
            if entity[0].ip == peerip:
                chid = entity[1]
                break
        if chid < 0:
            logging.warning("[COOP] Cannot find %s in notify_exiting_sending()" % (peerip,))
            return
        ch = self.channels[chid]
        pkt = CCPacket(CCHeader(MSG['EXIT_COOP']), segmentid)
        ch.send(pkt.packed())
        logging.info("[COOP] Enqueue EXIT_COOP [] onto channel (chid: %d) to %s for segment %d."
                        % (chid, peerip, segmentid))
        return

    def add_receiving_peer(self, peerip, segmentid, sessionid, chid_c):
        """ Add peer to cooperation list of (receiving) a segment
            chid_c - control channel id
            Return
                >=0  - Add successfully, return data channel id
                -1   - Request segment not available
        """
        if segmentid not in self.sncbuf:
            # segment not seen
            logging.warning("[COOP] Requested segment %d by %s is not available"
                            % (segmentid, peerip))
            return -1
        if segmentid not in self.recvpeers.keys():
            # first peer for the segment, initialize peer list
            self.recvpeers[segmentid] = []
        if peerip in [p[0].ip for p in self.recvpeers[segmentid]]:
            logging.warning("[COOP] %s is already in receiving list of segment %d."
                                % (peerip, segmentid))
            # find out the peer and return its data channel id
            for entity in self.recvpeers[segmentid]:
                if entity[0].ip == peerip:
                    # FIXME: Let's close previous ctrl channel, and replace with the new one
                    # self.channels[entity[1]].state = CH_STATE_CLOSE
                    # entity[1] = chid_c
                    return entity[2]
        else:
            peer = HostInfo(peerip, sessionid)
            chid_d = open_data_channel(self.channels, peerip, UDP_START+sessionid)
            self.recvpeers[segmentid].append([peer, chid_c, chid_d])
            logging.info("[COOP] Added %s to receiving list of segment %d."
                            % (peerip, segmentid))
            peerips = [p[0].ip for p in self.recvpeers[segmentid]]
            logging.info(("[COOP] Current receiving list of the segment ["
                           +', '.join(['%s']*len(peerips))+"]") % tuple(peerips))
            return chid_d

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
        for entity in self.recvpeers[segmentid]:
            if entity[0].ip == peerip:
                found = True
                chid_c = entity[1]
                self.channels[chid_c].state = CH_STATE_CLOSE
                chid_d = entity[2]
                self.channels[chid_d].state = CH_STATE_CLOSE
                self.recvpeers[segmentid].remove(entity)
                logging.info("[COOP] Removed %s from receiving list of segment %s"
                                % (peerip, segmentid))
                break
        if found:
            if len(self.recvpeers[segmentid]) == 0:
                logging.info("[COOP] recv peer list of segment %d is now empty." % (segmentid, ))
                del self.recvpeers[segmentid]
            return 0
        else:
            logging.warning("[COOP] %s is not found in receiving list of segment %d."
                                % (peerip, segmentid))
            return -1

    def add_sending_peer(self, peerip, chid):
        """ Add a peer to the send list of current session
            TO EXTEND: A host may receiving multiple sessions at a time.
            In this case, there would be multiple sending lists being
            maintained at a time.
        """
        if peerip and peerip not in self.sendpeers:
            self.sendpeers[peerip] = chid
            return 0
        else:
            logging.warning("[COOP] %s is already in sendpeers for segment %d, previous ctrl channel id: %d" % (peerip, 
                                                     self.currsess.segmentid, 
                                                     self.sendpeers[peerip]))
            # logging.info("[COOP] replace ctrl channel to %s with new id %d" % (peerip, chid))
            # self.channels[self.sendpeers[peerip]].state = CH_STATE_CLOSE
            # self.sendpeers[peerip] = chid
            return -1

    def remove_sending_peer(self, peerip, segmentid):
        """ Remove a sending peer (from sendpeers)
        """
        if self.currsess.segmentid == segmentid \
            and peerip in self.sendpeers:
            del self.sendpeers[peerip]
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
        pkt = CCPacket(CCHeader(MSG['HEARTBEAT']), self.currsess)
        for peerip in self.sendpeers:
            chid = self.sendpeers[peerip]
            ch = self.channels[chid]
            ch.send(pkt.packed())
            logging.info("[COOP] Enqueued HEARTBEAT [] onto channel (chid: %d) to %s for segment %d."
                            % ( chid, peerip, self.currsess.segmentid))

    def update_peer_heartbeat(self, peerip, segmentid):
        if segmentid not in self.recvpeers:
            # no peer list for the segment
            logging.warning("[COOP] No peers in receiving list of segment %d"
                                % (segmentid, ))
            return -1
        found = False
        for entity in self.recvpeers[segmentid]:
            if entity[0].ip == peerip:
                found = True
                entity[0].set_heartbeat()
                return 0
        """
        for i, entity in enumerate(self.recvpeers[segmentid]):
            if entity[0].ip == peerip:
                found = True
                entity[0].set_heartbeat()
                self.recvpeers[segmentid][i] = entity[0]
                logging.info("[COOP] Updated heartbeat of %s in receiving list of segment %s"
                                % (peerip, segmentid))
                # peerips = [p[0].ip for p in self.recvpeers[segmentid]]
                # logging.info(("[COOP] Current receiving list of the segment ["
                #               +', '.join(['%s']*len(peerips))+"]") % tuple(peerips))
                return 0
        """
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
            for segid in list(self.recvpeers):
                for entity in copy.deepcopy(self.recvpeers[segid]):
                    peer = entity[0]
                    if (now - peer.lastBeat).seconds > HK_INTVAL:
                        logging.info("[COOP] Remove peer %s from segment %d because no heartbeat"
                                        % (peer.ip, segid))
                        self.remove_receiving_peer(peer.ip, segid)
            self.lastClean = now
        # Cooperation exit if parent has told to do so and everything is cleaned
        if self.toexit:
            if not self.sendpeers and not self.recvpeers:
                # Let's free snc buffers
                for segid in list(self.sncbuf):
                    logging.info("[COOP] Freeing SNC buffer for segment %d" % (segid, ))
                    snc.snc_free_buffer(self.sncbuf[segid][1])
                    snc.snc_free_packet(self.sncbuf[segid][2])
                logging.info("[COOP] Nothing left to do. Exit...")
                exit(0)


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
    pkt = CCPacket(CCHeader(MSG['CHK_FILE']), MetaInfo(filepath))
    s.send(pkt.packed())
    reply = CCPacket()
    reply.parse(s.recv(BUFSIZE))
    if reply.header.mtype == MSG['FILEMETA']:
        filemeta = reply.body  # File meta information
        print(filemeta)
    else:
        logging.info("[MAIN] Server returns message %s" % (iMSG[reply.header.mtype],))
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
        pkt = CCPacket(CCHeader(MSG['REQ_SEG']), filemeta)
        try:
            s.send(pkt.packed())
            reply = CCPacket()
            reply.parse(s.recv(BUFSIZE))
            sessioninfo = reply.body  # Session info
            print(sessioninfo)
            # s.send(pickle.dumps(Packet(MSG['OK'], sessioninfo)))
        except Exception as detail:
            logging.warning("[MAIN] Cannot connect to server. Error: %s" % (detail, ))
            s.close()
            sys.exit(0)
        # Pass snc_parameters to sharing process, so that it creates
        # SNC packets buffer for the in-serving segment
        pkt = CCPacket(CCHeader(MSG['NEW_SESSION']), sessioninfo)
        logging.info("[MAIN] Sending NEW_SESSION to coop process")
        try:
            coop.fdp.send(pkt.packed())
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
                pkt = CCPacket(CCHeader(MSG['HEARTBEAT']), sessioninfo)
                try:
                    s.send(pkt.packed())
                    reply = CCPacket()
                    reply.parse(s.recv(BUFSIZE))
                    lastBeat = datetime.now()
                except Exception as detail:
                    logging.warning("[MAIN] Cannot connect to server. Error: %s", (detail, ))
                    s.close()
                    # break
                # Notify coop process to send heartbeat to peers
                try:
                    coop.fdp.send(pkt.packed())
                except Exception as detail:
                    logging.warning("[MAIN] Cannot notify coop process to heartbeat peers")
            # Receive data packets
            try:
                (data, addr) = ds.recvfrom(1500)
            except socket.timeout:
                continue
            if addr[0] != serverhost:
                count_p += 1
                logging.debug("[MAIN] An SNC packet from %s for segment %d " % (addr[0], segid))
            else:
                count_s += 1
            pkt = CCPacket()
            pkt.parse(copy.copy(data))
            try:
                coop.fdp.send(pkt.packed())  # Forward a copy to coop process
            except Exception as detail:
                logging.warning("[MAIN] Cannot forward packet to coop process")
                if not child.is_alive():
                    logging.info("[MAIN] Coop process is dead, exiting...")
                    exit(1)
                else:
                    logging.info("[MAIN] Coop process is still alive, but not reachable")
            sncpkt_p.contents.deserialize(pkt.body, sp_ptr[0].size_g, sp_ptr[0].size_p, sp_ptr[0].bnc)
            snc.snc_process_packet(decoder, sncpkt_p)

        if snc.snc_decoder_finished(decoder):
            snc.snc_recover_to_file(c_char_p(storename.encode('utf-8')),
                                    snc.snc_get_enc_context(decoder))
            logging.info("[MAIN] Finish segment %d" % (sessioninfo.segmentid,))
            # Request to stop the segment
            pkt = CCPacket(CCHeader(MSG['REQ_STOP']), sessioninfo)
            try:
                s.send(pkt.packed())
                reply = CCPacket()
                reply.parse(s.recv(BUFSIZE))
            except Exception as detail:
                logging.warning("[MAIN] Cannot connect to server. Error: %s", (detail, ))
                s.close()
        pkt = CCPacket(CCHeader(MSG['END_SESSION']), sessioninfo)
        try:
            coop.fdp.send(pkt.packed())
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
    pkt = CCPacket(CCHeader(MSG['EXIT']), sessioninfo)
    try:
        s.send(pkt.packed())
        reply = CCPacket()
        reply.parse(s.recv(BUFSIZE))
        if reply.header.mtype  == MSG['EXIT_ACK']:
            logging.info("[MAIN] Server has acknowledged my exit.")
    except Exception as detail:
        logging.warning("[MAIN] Cannot connect to server. Error: %s", (detail, ))
    s.close()
    try:
        coop.fdp.send(CCPacket(CCHeader(MSG['EXIT_PROC'])).packed())
    except Exception as detail:
        logging.warning("[MAIN] Cannot send EXIT_PROC to coop process. Error: %s", (detail, ))
    child.join()
