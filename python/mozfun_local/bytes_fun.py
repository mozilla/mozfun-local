import sys
from mozfun_local.mozfun_local_rust import (
    bytes_bit_pos_to_byte_pos as _bytes_bit_pos_to_byte_pos,
)
from numba import njit


def bytes_bit_pos_to_byte_pos(bit_pos: int) -> int:
    """Given a bit position, get the byte that bit appears in. 1-indexed (to match substr), and accepts negative values

    Args:
        bits: bit position
    Returns:
        the byte the the bit appears in
    """
    return _bytes_bit_pos_to_byte_pos(bit_pos)


def bytes_extract_bits(b: bytes, begin: int, length: int) -> bytes:
    """PROVIDED WITHOUT WARRANTY

    BQ ENDIANESS IS NOT REPLICABLE ON M1 Macs SO THIS FUNCTION DOES NOT
    PRODUCE EQUIVALENT RESULTS TO BQ"""
    b_len = len(b)

    the_bits = int.from_bytes(b, sys.byteorder)

    result = _shift_and_zero_right(the_bits, b_len, begin, length)

    bit_results = result.to_bytes(8, sys.byteorder)

    clip_at = bytes_bit_pos_to_byte_pos(length)

    return bit_results[:clip_at]


@njit
def _shift_and_zero_right(b, len_b, begin, length):

    if begin >= 0:
        shift_by = max([begin - 1, 0])
    else:
        shift_by = len_b * 8 + begin

    the_bytes = b << shift_by

    right_shift_by = max([8 * len_b - length, 0])

    shifted_right = _bytes_zero_right(the_bytes, right_shift_by)

    return shifted_right


def bytes_zero_right(b: bytes, length: int) -> bytes:
    """Zero bits on the right of byte

    Args:
        b (bytes): the bytes
        length (int): the start of where the bits will be zeroed

    """
    u8_length = len(b)
    give_me_the_bits = int.from_bytes(b, sys.byteorder)
    result = _bytes_zero_right(give_me_the_bits, length)
    return result.to_bytes(u8_length, sys.byteorder)


@njit
def _bytes_zero_right(the_bytes, length):
    result = the_bytes >> length << length
    return result
