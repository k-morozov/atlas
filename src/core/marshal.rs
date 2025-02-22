use std::ptr;

use crate::errors::Result;

pub fn write_u32(dst: &mut [u8], src: u32) -> Result<usize> {
    let src = src.to_le_bytes();

    unsafe {
        ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), size_of::<u32>());
    }

    Ok(size_of::<u32>() as usize)
}

pub fn write_data(dst: &mut [u8], src: &[u8], bytes: usize) -> Result<usize> {
    unsafe {
        ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), bytes);
    }

    Ok(bytes)
}
