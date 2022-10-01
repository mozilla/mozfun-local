use math::round::ceil;
use pyo3::prelude::*;

#[allow(dead_code)]
#[pyfunction]
pub fn bytes_bit_pos_to_byte_pos(bit_pos: i64) -> PyResult<i64> {
    let sign = bit_pos.signum();

    let ceiling = ceil(bit_pos.abs() as f64 / 8.0, 0) as i64;

    Ok(sign * ceiling)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_bit_pos_to_bytes_pos() {
        assert_eq!(bytes_bit_pos_to_byte_pos(0).unwrap(), 0);
        assert_eq!(bytes_bit_pos_to_byte_pos(1).unwrap(), 1);
        assert_eq!(bytes_bit_pos_to_byte_pos(-1).unwrap(), -1);
        assert_eq!(bytes_bit_pos_to_byte_pos(8).unwrap(), 1);
        assert_eq!(bytes_bit_pos_to_byte_pos(-8).unwrap(), -1);
        assert_eq!(bytes_bit_pos_to_byte_pos(9).unwrap(), 2);
        assert_eq!(bytes_bit_pos_to_byte_pos(-9).unwrap(), -2);
    }
}
