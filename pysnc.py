# This file wraps APIs from libspasenc.so in Python
from __future__ import division
from math import ceil
# from ctypes import *
from ctypes import cdll, c_int, c_ubyte, c_double, c_long, c_longlong, c_char_p, POINTER, sizeof, byref, cast, memmove, Structure
# code types
RAND_SNC = 0
BAND_SNC = 1
RLNC = 9

# decoder types
GG_DECODER = 0
OA_DECODER = 1
BD_DECODER = 2
CBD_DECODER = 3

# scheduling algorithm of recoder
TRIV_SCHED = 0
RAND_SCHED = 1
MLPI_SCHED = 2


class snc_context(Structure):
    pass


class snc_packet(Structure):
    _fields_ = [("gid",  c_int),
                ("coes", POINTER(c_ubyte)),
                ("syms", POINTER(c_ubyte))]

    def serialize(self, size_g, size_p, bnc):
        """ Serialize an SNC packet to a binary byte string
        """
        pktstr = bytearray()
        pktstr += c_int(self.gid)
        if bnc == 1:
            ce_len = int(ceil(size_g/8))  # Python 2/3 compatibility
        else:
            ce_len = size_g
        pktstr += cast(self.coes, POINTER(c_ubyte * ce_len))[0]
        pktstr += cast(self.syms, POINTER(c_ubyte * size_p))[0]
        return pktstr

    def deserialize(self, pktstr, size_g, size_p, bnc):
        """ Deserialize a byte stream and fill in an existing snc_packet
        instance
        """
        self.gid = c_int.from_buffer_copy(pktstr)
        if bnc == 1:
            ce_len = int(ceil(size_g/8))  # Python 2/3 compatibility
        else:
            ce_len = size_g
        coes = (c_ubyte * ce_len).from_buffer_copy(pktstr, sizeof(c_int))
        memmove(self.coes, coes, ce_len)
        syms = (c_ubyte * size_p).from_buffer_copy(pktstr, sizeof(c_int)+ce_len)
        memmove(self.syms, syms, size_p)


class snc_parameters(Structure):
    _fields_ = [("datasize", c_long),
                ("pcrate", c_double),
                ("size_b", c_int),
                ("size_g", c_int),
                ("size_p", c_int),
                ("type",   c_int),
                ("bpc",    c_int),
                ("bnc",    c_int),
                ("sys",    c_int),
                ("seed",   c_int)]


class snc_decoder(Structure):
    pass


class snc_buffer(Structure):
    pass


snc = cdll.LoadLibrary("libsparsenc.so")

##########################
# Wrap encoder functions #
##########################
snc.snc_create_enc_context.argtypes = [POINTER(c_ubyte), POINTER(snc_parameters)]
snc.snc_create_enc_context.restype = POINTER(snc_context)

snc.snc_get_parameters.argtypes = [POINTER(snc_context)]
snc.snc_get_parameters.restype = POINTER(snc_parameters)

snc.snc_load_file_to_context.argtypes = [c_char_p, c_long, POINTER(snc_context)]
snc.snc_load_file_to_context.restype = c_int

snc.snc_free_enc_context.argtypes = [POINTER(snc_context)]
snc.snc_free_enc_context.restype = None

snc.snc_recover_data.argtypes = [POINTER(snc_context)]
snc.snc_recover_data.restype = POINTER(c_ubyte)

snc.snc_free_recovered.argtypes = [POINTER(c_ubyte)]
snc.snc_free_recovered.restype = None

snc.snc_recover_to_file.argtypes = [c_char_p, POINTER(snc_context)]
snc.snc_recover_to_file.restype = c_long

snc.snc_alloc_empty_packet.argtypes = [POINTER(snc_parameters)]
snc.snc_alloc_empty_packet.restype = POINTER(snc_packet)

snc.snc_generate_packet.argtypes = [POINTER(snc_context)]
snc.snc_generate_packet.restype = POINTER(snc_packet)

snc.snc_generate_packet_im.argtypes = [POINTER(snc_context), POINTER(snc_packet)]
snc.snc_generate_packet_im.restype = c_int

snc.snc_free_packet.argtypes = [POINTER(snc_packet)]
snc.snc_free_packet.restype = None

snc.print_code_summary.argtypes = [POINTER(snc_context), c_double, c_double]
snc.print_code_summary.restype = None

##########################
# Wrap decoder functions #
##########################
snc.snc_create_decoder.argtypes = [POINTER(snc_parameters), c_int]
snc.snc_create_decoder.restype = POINTER(snc_decoder)

snc.snc_get_enc_context.argtypes = [POINTER(snc_decoder)]
snc.snc_get_enc_context.restype = POINTER(snc_context)

snc.snc_process_packet.argtypes = [POINTER(snc_decoder), POINTER(snc_packet)]
snc.snc_process_packet.restype = None

snc.snc_decoder_finished.argtypes = [POINTER(snc_decoder)]
snc.snc_decoder_finished.restype = c_int

snc.snc_decode_overhead.argtypes = [POINTER(snc_decoder)]
snc.snc_decode_overhead.restype = c_double

snc.snc_decode_cost.argtypes = [POINTER(snc_decoder)]
snc.snc_decode_cost.restype = c_double

snc.snc_free_decoder.argtypes = [POINTER(snc_decoder)]
snc.snc_free_decoder.restype = None

snc.snc_save_decoder_context.argtypes = [POINTER(snc_decoder), c_char_p]
snc.snc_save_decoder_context.restype = c_long

snc.snc_restore_decoder.argtypes = [c_char_p]
snc.snc_restore_decoder.restype = POINTER(snc_decoder)

#################################
# Wrap recoder/buffer functions #
#################################
snc.snc_create_buffer.argtypes = [POINTER(snc_parameters), c_int]
snc.snc_create_buffer.restype = POINTER(snc_buffer)

snc.snc_buffer_packet.argtypes = [POINTER(snc_buffer), POINTER(snc_packet)]
snc.snc_buffer_packet.restype = None

snc.snc_recode_packet.argtypes = [POINTER(snc_buffer), c_int]
snc.snc_recode_packet.restype = POINTER(snc_packet)

snc.snc_recode_packet_im.argtypes = [POINTER(snc_buffer), POINTER(snc_packet), c_int]
snc.snc_recode_packet_im.restype = c_int

snc.snc_free_buffer.argtypes = [POINTER(snc_buffer)]
snc.snc_free_buffer.restype = None
