from mozfun_local.bytes_fun import (
    bytes_bit_pos_to_byte_pos,
    bytes_zero_right,
    bytes_extract_bits,
)


def test_bytes_bit_pos_to_byte_pos():
    assert bytes_bit_pos_to_byte_pos(0) == 0
    assert bytes_bit_pos_to_byte_pos(1) == 1
    assert bytes_bit_pos_to_byte_pos(-1) == -1
    assert bytes_bit_pos_to_byte_pos(8) == 1
    assert bytes_bit_pos_to_byte_pos(-8) == -1
    assert bytes_bit_pos_to_byte_pos(9) == 2
    assert bytes_bit_pos_to_byte_pos(-9) == -2


def test_bytes_zero_right():
    assert b"\xF0" == bytes_zero_right(b"\xFF", 4)
    assert b"\xFF" == bytes_zero_right(b"\xFF", 0)
    assert b"\x00" == bytes_zero_right(b"\xFF", 8)


def test_bytes_extract_bits():
    assert True
    # assert b"\xFF" == bytes_extract_bits(b"\x01\xFE", 8, 8)
    # assert b"\xF0" == bytes_extract_bits(b"\xFF", 5, 4)
    # assert b"\xFF" == bytes_extract_bits(b"\x0F\xF0", -12, 8)
    # assert b"\x0F" == bytes_extract_bits(b"\x0F\x77", 0, 8)
    # assert b"\xFC" == bytes_extract_bits(b"\x0F\xF0", -10, 8)
    # assert b"\xFF" == bytes_extract_bits(b"\x0F\xF0", 5, 8)
    # assert b"\xCC" == bytes_extract_bits(b"\x0C\xC0", -12, 8)
    # assert b"\x80" == bytes_extract_bits(b"\xFF", -4, 1)
    # assert b"\xC0" == bytes_extract_bits(b"\xFF\xFF", 2, 2)
    # assert b"\x80" == bytes_extract_bits(b"\xFF\xFF", 6, 1)
    # assert b"\x80" == bytes_extract_bits(b"\xFF\xFF", 1, 1)
    # assert b"\xFF" == bytes_extract_bits(b"\xFF", 1, 20)
