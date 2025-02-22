use std::ptr;

use crate::errors::Result;

pub fn write_u32(dst: &mut [u8], src: u32) -> Result<usize> {
    let src = src.to_le_bytes();

    unsafe {
        ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), size_of::<u32>());
    }

    Ok(size_of::<u32>() as usize)
}

pub fn read_u32(src: &[u8]) -> Result<u32> {
    let mut dst = [0u8; 4];

    unsafe {
        ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), size_of::<u32>());
    }

    Ok(u32::from_le_bytes(dst))
}

pub fn write_data(dst: &mut [u8], src: &[u8], bytes: usize) -> Result<usize> {
    unsafe {
        ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), bytes);
    }

    Ok(bytes)
}
