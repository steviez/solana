use {
    crate::{cuda_runtime::PinnedVec, packet::PacketBatch},
    enum_iterator::Sequence,
    solana_sdk::packet::{Meta, Packet, PACKET_DATA_SIZE},
    //rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator},
    std::{
        cmp, fmt,
        marker::PhantomData,
        mem::{size_of, ManuallyDrop},
        ops::Range,
        slice,
        slice::SliceIndex,
    },
};

/// Supported packet buffer sizes for TxPacketBatch
#[derive(Sequence)]
pub enum TxPacketSize {
    Single,
    Double,
}

impl From<TxPacketSize> for usize {
    fn from(value: TxPacketSize) -> usize {
        match value {
            TxPacketSize::Single => PACKET_DATA_SIZE,
            TxPacketSize::Double => PACKET_DATA_SIZE * 2,
        }
    }
}

/// A structure to hold a batch of packets in a contiguous chunk of memory.
/// The structure support variable sized packet buffer from batch to batch;
/// however, the packets must all be the same size within one batch.
pub struct TxPacketBatch {
    /// Shared buffer for all packets in batch
    // TODO: change to u64 so we get guaranteed same alignment as Packet (8)
    data: PinnedVec<u8>,
    /// Current capacity of batch (packets)
    capacity: usize,
    /// Current length of batch (packets)
    len: usize,
    /// Length of batch packet's payload buffer (bytes)
    buffer_len: usize,
    /// Offset to ensure Meta references from buffer are properly aligned
    alignment_offset: usize,
}

/// A container to store variable length packets. The length of the packets
/// can vary instance to instance; however, the size of all packets in one
/// batch is the same.
impl TxPacketBatch {
    /// Constructs a new, empty TxPacketBatch with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        // TODO: Promote to arg, or default to starting with smallest?
        let buffer_len = TxPacketSize::Single;
        let buffer_len = buffer_len.into();

        let data_len = Self::calc_data_len(capacity, buffer_len);
        let mut data = PinnedVec::<u8>::with_capacity(data_len);
        unsafe {
            // We know data has data_len capacity, so set_len() with data_len
            // is also safe. data is intentionally a raw buffer and it is up to
            // the caller to do any initialization on contents of data.
            data.set_len(data_len);
        }
        let alignment_offset = Self::calc_alignment_offset(&data);

        let batch = Self {
            data,
            capacity,
            len: 0,
            buffer_len,
            alignment_offset,
        };
        debug!("{:?}", batch);
        batch
    }

    /// Constructs a new TxPacketBatch with the specified size. The packet
    /// buffers themselves will be contain uninitialized data; however,
    /// the Meta's will be in a known (default) state.
    pub fn with_size(size: usize) -> Self {
        let mut batch = Self::with_capacity(size);
        batch.len = size;

        for index in 0..size {
            *batch.meta_mut(index) = Meta::default();
        }

        batch
    }

    /// Consumes a PacketBatch and turns it into a TxPacketBatch with the
    /// same length, capacity and contents without performing a copy.
    pub fn from_packet_batch(old_batch: PacketBatch) -> Self {
        // This implementation adapted from an example provided in the
        // std::mem::transmute documentation
        // https://doc.rust-lang.org/std/mem/fn.transmute.html
        let mut old_batch = ManuallyDrop::new(old_batch);

        // TODO: Retain the PinnedVec instead of pulling the Vec out.
        //       We lose the recycler by doing this conversion.
        let (ptr, len, capacity) = (
            old_batch.as_mut_ptr(),
            old_batch.len(),
            old_batch.capacity(),
        );

        let data = unsafe {
            PinnedVec::from_vec(Vec::from_raw_parts(
                ptr as *mut u8,
                len * size_of::<Packet>(),
                capacity * size_of::<Packet>(),
            ))
        };

        Self {
            data,
            capacity,
            len,
            buffer_len: TxPacketSize::Single.into(),
            alignment_offset: 0,
        }
    }

    pub fn resize(&mut self, new_len: usize) {
        if new_len <= self.len {
            self.len = new_len;
            return;
        }

        // TODO: Think we can skip updating metas; only need to update len
        if new_len <= self.capacity {
            let old_len = self.len;
            for index in old_len..new_len {
                *self.meta_mut(index) = Meta::default();
            }
            self.len = new_len;
            return;
        }

        // We've reached this point if growing the number of packets. Doing
        // this will incur a re-allocation on the PinnedVec.
        //
        // This new buffer could come back with a different alignment than the
        // original buffer. If that happens, then all of existing elements
        // will be out of alignment, and we need to copy all of the old packets
        // over a couple bytes.
        //
        // NOTE: Above concern goes away with switch to PinnedVec<u64>
        //
        // This would mean a second, immediate copy:
        // - Copy #1 done automatically on realloc to put old contents in new
        // - Copy #2 to if we need to shift everything several bytes to align
        //
        // TODO
        // a) Figure out if we even need to support resize
        // b) Probably reallocate and do copy ourselves to avoid a potential
        //    wasted copy if the alignment differs
        panic!("TxPacketBatch::resize() that grows capacity not supported");
    }

    /// Shortens the batch, keeping the first `new_len` packets. If `new_len`
    /// is greater than the current batch length, this has no effect.
    pub fn truncate(&mut self, new_len: usize) {
        self.len = cmp::min(self.len, new_len);
    }

    /// Grow the packet buffer for each packet in the batch.
    ///
    /// Warning: Because packets are laid out to have the meta following the
    /// buffer, growing the buffer will require two copies per packet since
    /// the existing buffer and meta will be copied to non-consecutive
    /// locations in the buffer. Due to this, this method suffers a performance
    /// hit that scales with the size of tha batch.
    pub fn grow(&mut self) {
        // TODO: If promoted to arg, expose the enum
        let new_buffer_len = TxPacketSize::Double;
        let new_buffer_len = new_buffer_len.into();
        if self.buffer_len == new_buffer_len {
            return;
        }

        let new_data_len = Self::calc_data_len(self.capacity, new_buffer_len);
        self.data.reserve(new_data_len);
        unsafe {
            // We know data has data_len capacity, so set_len() with data_len
            // is also safe. data is intentionally a raw buffer and it is up to
            // the caller to do any initialization on contents of data.
            self.data.set_len(new_data_len);
        }
        let new_alignment_offset = Self::calc_alignment_offset(&self.data);

        // Any existing packets will need to re-laid out.
        if !self.is_empty() {
            for index in 0..self.len {
                // The new space in the buffer is at the end; since we're
                // doing the grow in-place, we need to start copying data from
                // the end first to avoid overwriting other packets.
                let index = self.len - index - 1;

                // Copy the existing meta into the new range
                let old_meta_range =
                    Self::meta_byte_range(self.alignment_offset, index, self.buffer_len);
                let new_meta_range =
                    Self::meta_byte_range(new_alignment_offset, index, new_buffer_len);
                self.data.copy_within(old_meta_range, new_meta_range.start);

                // TODO: Maybe adjust to only copy meta.size bytes instead of entire payload
                // Copy the existing buffer into the new range
                let old_buffer_range =
                    Self::buffer_byte_range(self.alignment_offset, index, self.buffer_len);
                let new_buffer_range =
                    Self::buffer_byte_range(new_alignment_offset, index, new_buffer_len);
                self.data
                    .copy_within(old_buffer_range, new_buffer_range.start);
            }
        }

        self.buffer_len = new_buffer_len;
        self.alignment_offset = new_alignment_offset;
    }

    /// Returns an iterator over the packets in the batch.
    pub fn iter(&self) -> TxPacketBatchIter<'_> {
        TxPacketBatchIter::new(self)
    }

    /// Returns a mutable iterator over the packets in the batch.
    pub fn iter_mut(&mut self) -> TxPacketBatchIterMut<'_> {
        TxPacketBatchIterMut::new(self)
    }

    #[inline]
    /// Access a reference to the Meta for packet at specified index.
    pub fn meta(&self, packet_index: usize) -> &Meta {
        assert!(
            packet_index < self.len,
            "packet_index = {}, batch_len = {}",
            packet_index,
            self.len
        );

        let range = Self::meta_byte_range(self.alignment_offset, packet_index, self.buffer_len);
        let meta_slice: &[u8] = &self.data[range];
        unsafe {
            let meta = meta_slice.as_ptr().cast::<Meta>();
            meta.as_ref().unwrap()
        }
    }

    #[inline]
    /// Access a mutable reference to the Meta for packet at specified index.
    pub fn meta_mut(&mut self, packet_index: usize) -> &mut Meta {
        assert!(
            packet_index < self.len,
            "packet_index = {}, batch_len = {}",
            packet_index,
            self.len
        );

        let range = Self::meta_byte_range(self.alignment_offset, packet_index, self.buffer_len);
        let meta_slice: &mut [u8] = &mut self.data[range];
        unsafe {
            let meta = meta_slice.as_mut_ptr().cast::<Meta>();
            meta.as_mut().unwrap()
        }
    }

    #[inline]
    /// Access a slice to the buffer for the packet at specified index.
    pub fn buffer(&self, packet_index: usize) -> &[u8] {
        assert!(
            packet_index < self.len,
            "packet_index = {}, batch_len = {}",
            packet_index,
            self.len
        );

        let range = Self::buffer_byte_range(self.alignment_offset, packet_index, self.buffer_len);
        &self.data[range]
    }

    #[inline]
    /// Access a mutable slice to the buffer for the packet at specified index.
    pub fn buffer_mut(&mut self, packet_index: usize) -> &mut [u8] {
        assert!(
            packet_index < self.len,
            "packet_index = {}, batch_len = {}",
            packet_index,
            self.len
        );

        let range = Self::buffer_byte_range(self.alignment_offset, packet_index, self.buffer_len);
        &mut self.data[range]
    }

    #[inline]
    /// Access a slice to the buffer for the packet at specified index.
    pub fn data(&self, packet_index: usize) -> &[u8] {
        assert!(
            packet_index < self.len,
            "packet_index = {}, batch_len = {}",
            packet_index,
            self.len
        );

        let size = self.meta(packet_index).size;
        &self.buffer(packet_index)[..size]
    }

    #[inline]
    /// Returns a pointer to the beginning of the data buffer.
    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.data.as_ptr().add(self.alignment_offset) }
    }

    #[inline]
    /// Returns a pointer to the beginning of `index` packet.
    pub fn as_ptr_at_index(&self, index: usize) -> *const u8 {
        unsafe { self.as_ptr().add(index * self.buffer_len) }
    }

    #[inline]
    /// Returns a mutable pointer to the beginning of the data buffer.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        unsafe { self.data.as_mut_ptr().add(self.alignment_offset) }
    }

    #[inline]
    /// Returns a mutable pointer to the beginning of `index` packet.
    pub fn as_mut_ptr_at_index(&mut self, index: usize) -> *mut u8 {
        unsafe { self.as_mut_ptr().add(index * self.buffer_len) }
    }

    #[inline]
    /// Returns the number of packets the batch can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    /// Returns true if the batch contains no packets.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    /// Returns the number of packets in the batch.
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    /// Returns the packet buffer length of the packets in the batch.
    pub fn buffer_len(&self) -> usize {
        self.buffer_len
    }

    #[inline]
    /// Calculate the length of a buffer necessary to fit `len` packets, with
    /// each packets having `size` payload buffers.
    fn calc_data_len(num_packets: usize, payload_size: usize) -> usize {
        // Allocate an extra std::mem::align_of::<Meta>() bytes to provide an
        // offset for alignment if necessary.
        (num_packets * (payload_size + std::mem::size_of::<Meta>())) + std::mem::align_of::<Meta>()
    }

    #[inline]
    fn calc_alignment_offset(data: &PinnedVec<u8>) -> usize {
        data.as_ptr().align_offset(std::mem::align_of::<Meta>())
    }

    #[inline]
    /// Return the byte index range for the Meta for the packet at specified index.
    fn meta_byte_range(offset: usize, packet_index: usize, buffer_len: usize) -> Range<usize> {
        let meta_size = std::mem::size_of::<Meta>();
        let start_index = offset + (packet_index * (buffer_len + meta_size)) + buffer_len;
        start_index..start_index + meta_size
    }

    #[inline]
    /// Return the byte index range for the buffer for the packet at specified index.
    fn buffer_byte_range(offset: usize, packet_index: usize, buffer_len: usize) -> Range<usize> {
        let meta_size = std::mem::size_of::<Meta>();
        let start_index = offset + (packet_index * (buffer_len + meta_size));
        start_index..start_index + buffer_len
    }
}

unsafe impl Send for TxPacketBatch {}
unsafe impl Sync for TxPacketBatch {}

impl fmt::Debug for TxPacketBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxPacketBatch")
            .field("data.len()", &self.data.len())
            .field("capacity", &self.capacity)
            .field("len", &self.len)
            .field("buffer_len", &self.buffer_len)
            .field("alignment_offset", &self.alignment_offset)
            .finish()
    }
}

/// An immutable view of an individual packet inside a TxPacketBatch
pub struct TxPacketView<'a> {
    // TODO: use NonNull like std::core::slice::Iter ?
    ptr: *const u8,
    buf_len: usize,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl TxPacketView<'_> {
    pub fn new(ptr: *const u8, buf_len: usize) -> Self {
        Self {
            ptr,
            buf_len,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn meta(&self) -> &Meta {
        unsafe { self.ptr.add(self.buf_len).cast::<Meta>().as_ref().unwrap() }
    }

    #[inline]
    pub fn buffer(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr, self.buf_len) }
    }

    #[inline]
    pub fn data<I>(&self, index: I) -> Option<&<I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        let meta = self.meta();
        if meta.discard() {
            None
        } else {
            self.buffer().get(..meta.size)?.get(index)
        }
    }
}

/// An iterator that provides TxPacketView's over an entire batch
pub struct TxPacketBatchIter<'a> {
    idx: usize,
    idx_rev: usize,
    batch: &'a TxPacketBatch,
}

impl<'a> TxPacketBatchIter<'a> {
    pub fn new(batch: &'a TxPacketBatch) -> Self {
        Self {
            idx: 0,
            idx_rev: batch.len(),
            batch,
        }
    }
}

impl<'a> Iterator for TxPacketBatchIter<'a> {
    type Item = TxPacketView<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.idx_rev {
            None
        } else {
            self.idx += 1;
            Some(Self::Item::new(
                self.batch.as_ptr_at_index(self.idx - 1),
                self.batch.buffer_len(),
            ))
        }
    }
}

impl<'a> DoubleEndedIterator for TxPacketBatchIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.idx_rev <= self.idx {
            None
        } else {
            self.idx_rev -= 1;
            Some(Self::Item::new(
                self.batch.as_ptr_at_index(self.idx_rev),
                self.batch.buffer_len(),
            ))
        }
    }
}

/// A mutable view of an individual packet inside a TxPacketBatch
pub struct TxPacketViewMut<'a> {
    // TODO: use NonNull like std::core::slice::Iter ?
    ptr: *mut u8,
    buf_len: usize,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl TxPacketViewMut<'_> {
    pub fn new(ptr: *mut u8, buf_len: usize) -> Self {
        Self {
            ptr,
            buf_len,
            _marker: PhantomData,
        }
    }

    pub fn meta(&self) -> &Meta {
        unsafe { self.ptr.add(self.buf_len).cast::<Meta>().as_ref().unwrap() }
    }

    pub fn meta_mut(&self) -> &mut Meta {
        unsafe { self.ptr.add(self.buf_len).cast::<Meta>().as_mut().unwrap() }
    }

    #[inline]
    pub fn buffer(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr, self.buf_len) }
    }

    pub fn buffer_mut(&self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr, self.buf_len) }
    }

    #[inline]
    pub fn data<I>(&self, index: I) -> Option<&<I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        let meta = self.meta();
        if meta.discard() {
            None
        } else {
            self.buffer().get(..meta.size)?.get(index)
        }
    }
}

/// An iterator that provides TxPacketViewMut's over an entire batch
pub struct TxPacketBatchIterMut<'a> {
    idx: usize,
    idx_rev: usize,
    batch: &'a mut TxPacketBatch,
}

impl<'a> TxPacketBatchIterMut<'a> {
    pub fn new(batch: &'a mut TxPacketBatch) -> Self {
        Self {
            idx: 0,
            idx_rev: batch.len(),
            batch,
        }
    }
}

impl<'a> Iterator for TxPacketBatchIterMut<'a> {
    type Item = TxPacketViewMut<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.batch.len() {
            None
        } else {
            self.idx += 1;
            Some(Self::Item::new(
                self.batch.as_mut_ptr_at_index(self.idx - 1),
                self.batch.buffer_len(),
            ))
        }
    }
}

impl<'a> DoubleEndedIterator for TxPacketBatchIterMut<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.idx_rev <= self.idx {
            None
        } else {
            self.idx_rev -= 1;
            Some(Self::Item::new(
                self.batch.as_mut_ptr_at_index(self.idx_rev),
                self.batch.buffer_len(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        rand::{thread_rng, Rng},
        solana_sdk::native_token::LAMPORTS_PER_SOL,
        std::{
            iter::repeat_with,
            net::{IpAddr, Ipv4Addr, SocketAddr},
        },
    };

    fn build_test_buffers(batch_len: usize, max_buffer_len: usize) -> Vec<Vec<u8>> {
        let mut rng = thread_rng();
        repeat_with(|| {
            let buffer_len = rng.gen_range(0, max_buffer_len);
            repeat_with(|| rng.gen_range(u8::MIN, u8::MAX))
                .take(buffer_len)
                .collect()
        })
        .take(batch_len)
        .collect()
    }

    #[test]
    fn test_tx_packet_batch_sizes() {
        // Ensure all supported packet sizes allow for Meta's to fall at
        // offset for proper alignment
        for packet_size in enum_iterator::all::<TxPacketSize>() {
            let packet_size: usize = packet_size.into();
            assert_eq!(packet_size % std::mem::align_of::<Meta>(), 0);
            // Packet contains a Meta and u8 array so the below assertion
            // being true should follow as a result of the above.
            assert_eq!(packet_size % std::mem::align_of::<Packet>(), 0);
        }
    }

    #[test]
    fn test_tx_packet_batch_empty() {
        solana_logger::setup();
        let batch_capacity = 10;
        let batch = TxPacketBatch::with_capacity(batch_capacity);

        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
        assert_eq!(batch.capacity(), batch_capacity);
    }

    #[test]
    fn test_tx_packet_batch_meta_initialization() {
        solana_logger::setup();
        let batch_size = 10;
        let batch = TxPacketBatch::with_size(batch_size);

        let default_meta = Meta::default();
        for idx in 0..batch_size {
            assert_eq!(batch.meta(idx), &default_meta);
        }
    }

    #[test]
    fn test_tx_packet_batch_meta_reference() {
        solana_logger::setup();
        let batch_size = 10;
        let mut batch = TxPacketBatch::with_size(batch_size);

        // Adjust some values in the Meta
        for idx in 0..batch_size {
            batch.meta_mut(idx).size = 1 + idx;
            batch.meta_mut(idx).sender_stake = 2 + idx as u64;
        }

        // Ensure the values can be read back
        for idx in 0..batch_size {
            assert_eq!(batch.meta(idx).size, 1 + idx);
            assert_eq!(batch.meta(idx).sender_stake, 2 + idx as u64);
        }
    }

    #[test]
    fn test_tx_packet_batch_grow() {
        solana_logger::setup();
        let batch_size = 10;
        let mut batch = TxPacketBatch::with_size(batch_size);

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 16, 32, 64)), 128);
        let test_buffers = build_test_buffers(batch_size, TxPacketSize::Single.into());

        // Adjust some values in the Meta and fill the buffer
        for (idx, buffer) in test_buffers.iter().enumerate() {
            batch.meta_mut(idx).set_socket_addr(&addr);
            batch.meta_mut(idx).sender_stake = LAMPORTS_PER_SOL * idx as u64;
            batch.buffer_mut(idx)[0..buffer.len()].copy_from_slice(&buffer);
            batch.meta_mut(idx).size = buffer.len();
        }

        // Grow the batch's buffers
        assert_eq!(batch.buffer_len(), usize::from(TxPacketSize::Single));
        batch.grow();
        assert_eq!(batch.buffer_len(), usize::from(TxPacketSize::Double));

        // Ensure the values can be read back
        for (idx, buffer) in test_buffers.iter().enumerate() {
            assert_eq!(batch.meta(idx).socket_addr(), addr);
            assert_eq!(batch.meta(idx).sender_stake, LAMPORTS_PER_SOL * idx as u64);
            assert_eq!(batch.meta(idx).size, buffer.len());
            assert_eq!(batch.data(idx), buffer);
        }
    }
}
