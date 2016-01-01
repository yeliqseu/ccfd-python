from pysnc import *
import math
import os
import sys
import struct


def print_usage():
    print("Usage: ./ProgramName c_type d_type filename")
    print("        c_type   - code type, available options: RAND BAND")
    print("        d_type   - decoder type, available options: GG OA BD CBD")
    print("        filename - filename to be encoded and decoded.")


"""
def serialize(pkt_p, size_g, size_p):
    pktstr = bytearray()
    pktstr += struct.pack('i', pkt_p.contents.gid)
    pktstr += cast(pkt_p.contents.coes, POINTER(c_ubyte * size_g))[0]
    pktstr += cast(pkt_p.contents.syms, POINTER(c_ubyte * size_p))[0]
    return pktstr


def deserialize(pkt_p, pktstr, size_g, size_p):
    pkt_p.contents.gid = c_int.from_buffer_copy(pktstr)
    coes = (c_ubyte * size_g).from_buffer_copy(pktstr, sizeof(c_int))
    memmove(pkt_p.contents.coes, coes, size_g)
    syms = (c_ubyte * size_p).from_buffer_copy(pktstr, sizeof(c_int)+size_g)
    memmove(pkt_p.contents.syms, syms, size_p)
"""

if len(sys.argv) != 4:
    print_usage()
    sys.exit(1)

c_type = 0
if sys.argv[1] == "RAND":
    c_type = RAND_SNC
elif sys.argv[1] == "BAND":
    c_type = BAND_SNC
else:
    print_usage()
    sys.exit(1)

d_type = 0
if sys.argv[2] == "GG":
    d_type = GG_DECODER
elif sys.argv[2] == "OA":
    d_type = OA_DECODER
elif sys.argv[2] == "BD":
    d_type = BD_DECODER
elif sys.argv[2] == "CBD":
    d_type = CBD_DECODER
else:
    print_usage()
    sys.exit(1)

filename = sys.argv[3]
if not os.path.isfile(filename):
    print("Filename is invalid.")
    sys.exit(1)

filename = filename.encode('UTF-8') # byte string literal for compatibility in Python 3.x
statinfo = os.stat(filename)
filesize = statinfo.st_size
datasize = 5120000

chunks = math.ceil(filesize / datasize)
offset = 0
remaining = filesize
while chunks > 0:
    print ("%d segments remain " % (chunks))
    if remaining > datasize:
        toEncode = datasize
    else:
        toEncode = remaining
    sp = snc_parameter(toEncode, 0.01, 32, 64, 1280, c_type, 0, 0)
    sc = snc.snc_create_enc_context(None, sp)
    snc.snc_load_file_to_context(c_char_p(filename),
                                 offset, sc)  # Load file to snc_context
    decoder = snc.snc_create_decoder(sp, d_type)  # Create decoder
    while not snc.snc_decoder_finished(decoder):
        pkt_p = snc.snc_generate_packet(sc)
        # Emulate (de)-serialization
        """
        pktstr = serialize(pkt_p, sp.size_g, sp.size_p)
        snc.snc_free_packet(pkt_p)
        pkt_p2 = snc.snc_alloc_empty_packet(snc.snc_get_metainfo(sc))
        deserialize(pkt_p2, pktstr, sp.size_g, sp.size_p)
        """
        pktstr = pkt_p.contents.serialize(sp.size_g, sp.size_p)
        snc.snc_free_packet(pkt_p)
        pkt_p2 = snc.snc_alloc_empty_packet(snc.snc_get_metainfo(sc))
        pkt_p2.contents.deserialize(pktstr, sp.size_g, sp.size_p)

        snc.snc_process_packet(decoder, pkt_p2)

    copyname = filename + b'.dec.copy'
    snc.snc_recover_to_file(c_char_p(copyname),
                            snc.snc_get_enc_context(decoder))
    snc.snc_free_decoder(decoder)
    snc.snc_free_enc_context(sc)
    offset += toEncode
    remaining -= toEncode
    chunks -= 1

