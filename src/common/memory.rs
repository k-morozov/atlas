use std::alloc;

pub fn alloc_aligned(size: usize, align: usize) -> Vec<u8> {
    let layout = alloc::Layout::from_size_align(size, align).expect("layout without problems");
    let ptr = unsafe { alloc::alloc_zeroed(layout) };

    if ptr.is_null() {
        alloc::handle_alloc_error(layout);
    }

    unsafe {
        let data = std::slice::from_raw_parts_mut(ptr, size);
        Vec::from_raw_parts(data.as_mut_ptr(), size, size)
    }
}
