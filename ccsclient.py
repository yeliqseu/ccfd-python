import sys
import socket
import pickle
from datetime import datetime
from ccstructs import *
#HOST = '127.0.0.1'
HOST = '192.168.1.110'
PORT = 7653
UDP_START = 7655
HB_INTVAL = 10  # Heartbeat every 10 seconds

filename = '../ccFileD/Big Data course.zip'
# filename = '../ccFileD/Pro Git.pdf'
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
pkt = Packet(MSG['CHK_FILE'], MetaInfo(filename))
s.send(pickle.dumps(pkt))
reply = pickle.loads(s.recv(4096))
s.close()
filemeta = reply.payload  # File meta information
print(filemeta)
lastBeat = datetime.now()
for segid in range(filemeta.numofseg):
    # Request file segment by segment
    filemeta.segmentid = segid
    pkt = Packet(MSG['REQ_SEG'], filemeta)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5.0)
    try:
        s.connect((HOST, PORT))
        s.send(pickle.dumps(pkt))
        reply = pickle.loads(s.recv(4096))
        ssmeta = reply.payload  # Session metainfo
        print(ssmeta)
        s.send(pickle.dumps(Packet(MSG['OK'])))
        s.close()
    except socket.timeout:
        print("Cannot connect to server")
        s.close()
        sys.exit(0)
    # Start receiving UDP data packets and decode the segment
    decoder = snc.snc_create_decoder(ssmeta.sp, CBD_DECODER)
    sncmeta_p = snc.snc_get_metainfo(snc.snc_get_enc_context(decoder))
    udp_port = UDP_START + ssmeta.sessionid
    ds = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ds.settimeout(5.0)
    ds.bind(('', udp_port))
    while not snc.snc_decoder_finished(decoder):
        if (datetime.now() - lastBeat).seconds > HB_INTVAL:
            pkt = Packet(MSG['HEARTBEAT'], ssmeta)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5.0)
            try:
                s.connect((HOST, PORT))
                s.send(pickle.dumps(pkt))
                reply = pickle.loads(s.recv(4096))
                s.close()
                lastBeat = datetime.now()
            except socket.timeout:
                print("Connecting to server timeout.")
                s.close()
                break
        try:
            (data, addr) = ds.recvfrom(1500)
        except socket.timeout:
            continue
        # ccpkt = pickle.loads(data)
        sncpkt_p = snc.snc_alloc_empty_packet(sncmeta_p)
        # ccpkt.payload.parse_dataload(sncpkt_p, ssmeta.sp)
        sncpkt_p.contents.deserialize(data, ssmeta.sp.size_g, ssmeta.sp.size_p)
        snc.snc_process_packet(decoder, sncpkt_p)

    if snc.snc_decoder_finished(decoder):
        snc.snc_recover_to_file(c_char_p(b'hello.tar.bz2'),
                                snc.snc_get_enc_context(decoder))
        print("Finish segment", ssmeta.segmentid)
        # Request to stop the segment
        pkt = Packet(MSG['REQ_STOP'], ssmeta)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        try:
            s.connect((HOST, PORT))
            s.send(pickle.dumps(pkt))
            reply = pickle.loads(s.recv(4096))
            print(reply)
            s.close()
        except socket.timeout:
            print("Connecting to server timeout")
            s.close()
    snc.snc_free_decoder(decoder)
print("Finished")
