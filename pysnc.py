# This file wraps APIs from libspasenc.so in Python
from ctypes import *
# code types
RAND_SNC = 0
BAND_SNC = 1

# decoder types
GG_DECODER = 0
OA_DECODER = 1
BD_DECODER = 2
CBD_DECODER = 3


class snc_context(Structure):
    pass


class snc_packet(Structure):
    _fields_ = [("gid",  c_int),
                ("coes", POINTER(c_ubyte)),
                ("syms", POINTER(c_ubyte))]


class snc_parameter(Structure):
    _fields_ = [("datasize", c_long),
                ("pcrate", c_double),
                ("size_b", c_int),
                ("size_g", c_int),
                ("size_p", c_int),
                ("type",   c_int),
                ("bpc",    c_int),
                ("bnc",    c_int)]


class snc_metainfo(Structure):
    _fields_ = [("datasize", c_long),
                ("pcrate", c_double),
                ("size_b", c_int),
                ("size_g", c_int),
                ("size_p", c_int),
                ("type",   c_int),
                ("bpc",    c_int),
                ("bnc",    c_int),
                ("snum",   c_int),
                ("cnum",   c_int),
                ("gnum",   c_int)]


class snc_decoder(Structure):
    pass


class snc_buffer(Structure):
    pass


snc = cdll.LoadLibrary("libsparsenc.so")

##########################
# Wrap encoder functions #
##########################
snc.snc_create_enc_context.argtypes = [c_char_p, snc_parameter]
snc.snc_create_enc_context.restype = POINTER(snc_context)

snc.snc_get_metainfo.argtypes = [POINTER(snc_context)]
snc.snc_get_metainfo.restype = POINTER(snc_metainfo)

snc.snc_load_file_to_context.argtypes = [c_char_p, c_long, POINTER(snc_context)]
snc.snc_load_file_to_context.restype = c_int

snc.snc_free_enc_context.argtypes = [POINTER(snc_context)]
snc.snc_free_enc_context.restype = None

snc.snc_recover_data.argtypes = [POINTER(snc_context)]
snc.snc_recover_data.restype = POINTER(c_ubyte)

snc.snc_recover_to_file.argtypes = [c_char_p, POINTER(snc_context)]
snc.snc_recover_to_file.restype = c_long

snc.snc_alloc_empty_packet.argtypes = [POINTER(snc_metainfo)]
snc.snc_alloc_empty_packet.restype = POINTER(snc_packet)

snc.snc_generate_packet.argtypes = [POINTER(snc_context)]
snc.snc_generate_packet.restype = POINTER(snc_packet)

snc.snc_generate_packet_im.argtypes = [POINTER(snc_context), POINTER(snc_packet)]
snc.snc_generate_packet_im.restype = c_int

snc.snc_free_packet.argtypes = [POINTER(snc_packet)]
snc.snc_free_packet.restype = None

snc.print_code_summary.argtypes = [POINTER(snc_context), c_int, c_longlong]
snc.print_code_summary.restype = None

##########################
# Wrap decoder functions #
##########################
snc.snc_create_decoder.argtypes = [snc_parameter, c_int]
snc.snc_create_decoder.restype = POINTER(snc_decoder)

snc.snc_get_enc_context.argtypes = [POINTER(snc_decoder)]
snc.snc_get_enc_context.restype = POINTER(snc_context)

snc.snc_process_packet.argtypes = [POINTER(snc_decoder), POINTER(snc_packet)]
snc.snc_process_packet.restype = None

snc.snc_decoder_finished.argtypes = [POINTER(snc_decoder)]
snc.snc_decoder_finished.restype = c_int

snc.snc_code_overhead.argtypes = [POINTER(snc_decoder)]
snc.snc_code_overhead.restype = c_int

snc.snc_decode_cost.argtypes = [POINTER(snc_decoder)]
snc.snc_decode_cost.restype = c_longlong

snc.snc_free_decoder.argtypes = [POINTER(snc_decoder)]
snc.snc_free_decoder.restype = None

snc.snc_save_decoder_context.argtypes = [POINTER(snc_decoder), c_char_p]
snc.snc_save_decoder_context.restype = c_long

snc.snc_restore_decoder.argtypes = [c_char_p]
snc.snc_restore_decoder.restype = POINTER(snc_decoder)

#################################
# Wrap recoder/buffer functions #
#################################
snc.snc_create_buffer.argtypes = [POINTER(snc_metainfo), c_int]
snc.snc_create_buffer.restype = POINTER(snc_buffer)

snc.snc_buffer_packet.argtypes = [POINTER(snc_buffer), POINTER(snc_packet)]
snc.snc_buffer_packet.restype = None

snc.snc_recode_packet.argtypes = [POINTER(snc_buffer), c_int]
snc.snc_recode_packet.restype = POINTER(snc_packet)

snc.snc_free_buffer.argtypes = [POINTER(snc_buffer)]
snc.snc_free_buffer.restype = None
