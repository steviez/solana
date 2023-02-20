//! The `packet` module defines data structures and methods to pull data from the network.
pub use solana_sdk::packet::{
    GenericPacket, Meta, Packet, PacketFlags, PACKET_DATA_1X_SIZE, PACKET_DATA_2X_SIZE,
    PACKET_DATA_3X_SIZE, PACKET_DATA_SIZE,
};
use {
    crate::{cuda_runtime::PinnedVec, recycler::Recycler},
    bincode::config::Options,
    rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator},
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    std::{
        io::Read,
        net::SocketAddr,
        ops::{Index, IndexMut},
        slice::{Iter, IterMut, SliceIndex},
    },
};

pub const NUM_PACKETS: usize = 1024 * 8;

pub const PACKETS_PER_BATCH: usize = 64;
pub const NUM_RCVMMSGS: usize = 64;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GenericPacketBatch<const N: usize> {
    packets: PinnedVec<GenericPacket<N>>,
}
pub type GenericPacketBatchRecycler<const N: usize> = Recycler<PinnedVec<GenericPacket<N>>>;

pub type PacketBatch = GenericPacketBatch<PACKET_DATA_SIZE>;
pub type PacketBatchRecycler = Recycler<PinnedVec<Packet>>;

pub type PacketBatchSingle = GenericPacketBatch<PACKET_DATA_1X_SIZE>;
pub type PacketBatchDouble = GenericPacketBatch<PACKET_DATA_2X_SIZE>;
pub type PacketBatchTriple = GenericPacketBatch<PACKET_DATA_3X_SIZE>;

pub enum VarPacketBatch {
    Single(PacketBatchSingle),
    Double(PacketBatchDouble),
    Triple(PacketBatchTriple),
}

macro_rules! dispatch {
    ($vis:vis fn $func_name:ident(&self $(, $arg:ident : $type:ty)*) $(-> $out:ty)?) => {
        $vis fn $func_name(&self $(, $arg: $ty)*) $(-> $out)? {
            match self {
                Self::Single(batch) => batch.$func_name(),
                Self::Double(batch) => batch.$func_name(),
                Self::Triple(batch) => batch.$func_name(),
            }
        }
    }
}

impl VarPacketBatch {
    dispatch!(pub fn len(&self) -> usize);

    pub fn iter(&self) -> VarPacketBatchIter<'_> {
        match self {
            Self::Single(batch) => VarPacketBatchIter::Single(batch.iter()),
            Self::Double(batch) => VarPacketBatchIter::Double(batch.iter()),
            Self::Triple(batch) => VarPacketBatchIter::Triple(batch.iter()),
        }
    }

    pub fn iter_mut(&mut self) -> VarPacketBatchIterMut<'_> {
        match self {
            Self::Single(batch) => VarPacketBatchIterMut::Single(batch.iter_mut()),
            Self::Double(batch) => VarPacketBatchIterMut::Double(batch.iter_mut()),
            Self::Triple(batch) => VarPacketBatchIterMut::Triple(batch.iter_mut()),
        }
    }
}

/// An immutable view of a packet in a VarPacketBatch. This view exposes a
/// a reference to the packet's Meta as well as a slice for its' buffer.
/// The buffer slice can have variable length depending on what typee of
/// packets are actually contained in the VarPacketBatch.
pub struct BatchPacketView<'a> {
    pub meta: &'a Meta,
    pub buffer: &'a [u8],
}

impl BatchPacketView<'_> {
    /// Returns an immutable reference to the underlying buffer up to
    /// packet.meta.size. The rest of the buffer is not valid to read from.
    /// packet.data(..) returns packet.buffer.get(..packet.meta.size).
    /// Returns None if the index is invalid or if the packet is already marked
    /// as discard.
    #[inline]
    pub fn data<I>(&self, index: I) -> Option<&<I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        // If the packet is marked as discard, it is either invalid or
        // otherwise should be ignored, and so the payload should not be read
        // from.
        if self.meta.discard() {
            None
        } else {
            self.buffer.get(..self.meta.size)?.get(index)
        }
    }
}

impl<'a, const N: usize> From<&'a GenericPacket<N>> for BatchPacketView<'a> {
    fn from(packet: &'a GenericPacket<N>) -> Self {
        Self {
            meta: &packet.meta,
            buffer: &packet.buffer,
        }
    }
}

/// Similar to BatchPacketView, but mutable access to Meta and buffer.
pub struct BatchPacketViewMut<'a> {
    pub meta: &'a mut Meta,
    pub buffer: &'a mut [u8],
}

impl BatchPacketViewMut<'_> {
    /// Returns an immutable reference to the underlying buffer up to
    /// packet.meta.size. The rest of the buffer is not valid to read from.
    /// packet.data(..) returns packet.buffer.get(..packet.meta.size).
    /// Returns None if the index is invalid or if the packet is already marked
    /// as discard.
    #[inline]
    pub fn data<I>(&self, index: I) -> Option<&<I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        // If the packet is marked as discard, it is either invalid or
        // otherwise should be ignored, and so the payload should not be read
        // from.
        if self.meta.discard() {
            None
        } else {
            self.buffer.get(..self.meta.size)?.get(index)
        }
    }
}

impl<'a, const N: usize> From<&'a mut GenericPacket<N>> for BatchPacketViewMut<'a> {
    fn from(packet: &'a mut GenericPacket<N>) -> Self {
        Self {
            meta: &mut packet.meta,
            buffer: &mut packet.buffer,
        }
    }
}

/*
impl<'a> From<BatchPacketViewMut<'a>> for BatchPacketView<'a> {
    fn from(view_mut: BatchPacketViewMut<'a>) -> Self {
        Self {
            meta: view_mut.meta,
            buffer: view_mut.buffer,
        }
    }
}
*/

/// An iterator that provides TxPacketView's over an entire batch
pub enum VarPacketBatchIter<'a> {
    Single(Iter<'a, GenericPacket<PACKET_DATA_1X_SIZE>>),
    Double(Iter<'a, GenericPacket<PACKET_DATA_2X_SIZE>>),
    Triple(Iter<'a, GenericPacket<PACKET_DATA_3X_SIZE>>),
}

/// An iterator that provides TxPacketViewMut's over an entire batch
pub enum VarPacketBatchIterMut<'a> {
    Single(IterMut<'a, GenericPacket<PACKET_DATA_1X_SIZE>>),
    Double(IterMut<'a, GenericPacket<PACKET_DATA_2X_SIZE>>),
    Triple(IterMut<'a, GenericPacket<PACKET_DATA_3X_SIZE>>),
}

macro_rules! dispatch_iter {
    ($iter_type:ty => $item_type:ty) => {
        impl<'a> Iterator for $iter_type {
            type Item = $item_type;
            fn next(&mut self) -> Option<Self::Item> {
                match self {
                    Self::Single(iter) => iter.next().map(|p| Self::Item::from(p)),
                    Self::Double(iter) => iter.next().map(|p| Self::Item::from(p)),
                    Self::Triple(iter) => iter.next().map(|p| Self::Item::from(p)),
                }
            }
        }

        impl<'a> DoubleEndedIterator for $iter_type {
            fn next_back(&mut self) -> Option<Self::Item> {
                match self {
                    Self::Single(iter) => iter.next_back().map(|p| Self::Item::from(p)),
                    Self::Double(iter) => iter.next_back().map(|p| Self::Item::from(p)),
                    Self::Triple(iter) => iter.next_back().map(|p| Self::Item::from(p)),
                }
            }
        }
    };
}

dispatch_iter!(VarPacketBatchIter<'a> => BatchPacketView<'a>);
dispatch_iter!(VarPacketBatchIterMut<'a> => BatchPacketViewMut<'a>);

impl<const N: usize> GenericPacketBatch<N> {
    pub fn new(packets: Vec<GenericPacket<N>>) -> Self {
        let packets = PinnedVec::from_vec(packets);
        Self { packets }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let packets = PinnedVec::with_capacity(capacity);
        Self { packets }
    }

    pub fn new_pinned_with_capacity(capacity: usize) -> Self {
        let mut batch = Self::with_capacity(capacity);
        batch.packets.reserve_and_pin(capacity);
        batch
    }

    pub fn new_unpinned_with_recycler(
        recycler: GenericPacketBatchRecycler<N>,
        capacity: usize,
        name: &'static str,
    ) -> Self {
        let mut packets = recycler.allocate(name);
        packets.reserve(capacity);
        Self { packets }
    }

    pub fn new_with_recycler(
        recycler: GenericPacketBatchRecycler<N>,
        capacity: usize,
        name: &'static str,
    ) -> Self {
        let mut packets = recycler.allocate(name);
        packets.reserve_and_pin(capacity);
        Self { packets }
    }

    pub fn new_with_recycler_data(
        recycler: &GenericPacketBatchRecycler<N>,
        name: &'static str,
        mut packets: Vec<GenericPacket<N>>,
    ) -> Self {
        let mut batch = Self::new_with_recycler(recycler.clone(), packets.len(), name);
        batch.packets.append(&mut packets);
        batch
    }

    pub fn new_unpinned_with_recycler_data_and_dests<T: Serialize>(
        recycler: GenericPacketBatchRecycler<N>,
        name: &'static str,
        dests_and_data: &[(SocketAddr, T)],
    ) -> Self {
        let mut batch = Self::new_unpinned_with_recycler(recycler, dests_and_data.len(), name);
        batch
            .packets
            .resize(dests_and_data.len(), GenericPacket::default());

        for ((addr, data), packet) in dests_and_data.iter().zip(batch.packets.iter_mut()) {
            if !addr.ip().is_unspecified() && addr.port() != 0 {
                if let Err(e) = GenericPacket::populate_packet(packet, Some(addr), &data) {
                    // TODO: This should never happen. Instead the caller should
                    // break the payload into smaller messages, and here any errors
                    // should be propagated.
                    error!("Couldn't write to packet {:?}. Data skipped.", e);
                }
            } else {
                trace!("Dropping packet, as destination is unknown");
            }
        }
        batch
    }

    pub fn new_unpinned_with_recycler_data(
        recycler: &GenericPacketBatchRecycler<N>,
        name: &'static str,
        mut packets: Vec<GenericPacket<N>>,
    ) -> Self {
        let mut batch = Self::new_unpinned_with_recycler(recycler.clone(), packets.len(), name);
        batch.packets.append(&mut packets);
        batch
    }

    pub fn resize(&mut self, new_len: usize, value: GenericPacket<N>) {
        self.packets.resize(new_len, value)
    }

    pub fn truncate(&mut self, len: usize) {
        self.packets.truncate(len);
    }

    pub fn push(&mut self, packet: GenericPacket<N>) {
        self.packets.push(packet);
    }

    pub fn set_addr(&mut self, addr: &SocketAddr) {
        for p in self.iter_mut() {
            p.meta_mut().set_socket_addr(addr);
        }
    }

    pub fn len(&self) -> usize {
        self.packets.len()
    }

    pub fn capacity(&self) -> usize {
        self.packets.capacity()
    }

    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    pub fn as_ptr(&self) -> *const GenericPacket<N> {
        self.packets.as_ptr()
    }

    pub fn iter(&self) -> Iter<'_, GenericPacket<N>> {
        self.packets.iter()
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, GenericPacket<N>> {
        self.packets.iter_mut()
    }

    /// See Vector::set_len() for more details
    ///
    /// # Safety
    ///
    /// - `new_len` must be less than or equal to [`self.capacity`].
    /// - The elements at `old_len..new_len` must be initialized. Packet data
    ///   will likely be overwritten when populating the packet, but the meta
    ///   should specifically be initialized to known values.
    pub unsafe fn set_len(&mut self, new_len: usize) {
        self.packets.set_len(new_len);
    }
}

impl<const N: usize, I: SliceIndex<[GenericPacket<N>]>> Index<I> for GenericPacketBatch<N> {
    type Output = I::Output;

    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        &self.packets[index]
    }
}

impl<const N: usize, I: SliceIndex<[GenericPacket<N>]>> IndexMut<I> for GenericPacketBatch<N> {
    #[inline]
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        &mut self.packets[index]
    }
}

impl<'a, const N: usize> IntoIterator for &'a GenericPacketBatch<N> {
    type Item = &'a GenericPacket<N>;
    type IntoIter = Iter<'a, GenericPacket<N>>;

    fn into_iter(self) -> Self::IntoIter {
        self.packets.iter()
    }
}

impl<'a, const N: usize> IntoParallelIterator for &'a GenericPacketBatch<N> {
    type Iter = rayon::slice::Iter<'a, GenericPacket<N>>;
    type Item = &'a GenericPacket<N>;
    fn into_par_iter(self) -> Self::Iter {
        self.packets.par_iter()
    }
}

impl<'a, const N: usize> IntoParallelIterator for &'a mut GenericPacketBatch<N> {
    type Iter = rayon::slice::IterMut<'a, GenericPacket<N>>;
    type Item = &'a mut GenericPacket<N>;
    fn into_par_iter(self) -> Self::Iter {
        self.packets.par_iter_mut()
    }
}

impl<const N: usize> From<GenericPacketBatch<N>> for Vec<GenericPacket<N>> {
    fn from(batch: GenericPacketBatch<N>) -> Self {
        batch.packets.into()
    }
}

pub fn to_packet_batches<T: Serialize>(items: &[T], chunk_size: usize) -> Vec<PacketBatch> {
    items
        .chunks(chunk_size)
        .map(|batch_items| {
            let mut batch = PacketBatch::with_capacity(batch_items.len());
            batch.resize(batch_items.len(), Packet::default());
            for (item, packet) in batch_items.iter().zip(batch.packets.iter_mut()) {
                Packet::populate_packet(packet, None, item).expect("serialize request");
            }
            batch
        })
        .collect()
}

#[cfg(test)]
pub fn to_packet_batches_for_tests<T: Serialize>(items: &[T]) -> Vec<PacketBatch> {
    to_packet_batches(items, NUM_PACKETS)
}

pub fn deserialize_from_with_limit<R, T>(reader: R) -> bincode::Result<T>
where
    R: Read,
    T: DeserializeOwned,
{
    // with_limit causes pre-allocation size to be limited
    // to prevent against memory exhaustion attacks.
    bincode::options()
        .with_limit(PACKET_DATA_SIZE as u64)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from(reader)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sdk::{
            hash::Hash,
            signature::{Keypair, Signer},
            system_transaction,
        },
    };

    #[test]
    fn test_to_packet_batches() {
        let keypair = Keypair::new();
        let hash = Hash::new(&[1; 32]);
        let tx = system_transaction::transfer(&keypair, &keypair.pubkey(), 1, hash);
        let rv = to_packet_batches_for_tests(&[tx.clone(); 1]);
        assert_eq!(rv.len(), 1);
        assert_eq!(rv[0].len(), 1);

        #[allow(clippy::useless_vec)]
        let rv = to_packet_batches_for_tests(&vec![tx.clone(); NUM_PACKETS]);
        assert_eq!(rv.len(), 1);
        assert_eq!(rv[0].len(), NUM_PACKETS);

        #[allow(clippy::useless_vec)]
        let rv = to_packet_batches_for_tests(&vec![tx; NUM_PACKETS + 1]);
        assert_eq!(rv.len(), 2);
        assert_eq!(rv[0].len(), NUM_PACKETS);
        assert_eq!(rv[1].len(), 1);
    }

    #[test]
    fn test_to_packets_pinning() {
        let recycler = PacketBatchRecycler::default();
        for i in 0..2 {
            let _first_packets =
                PacketBatch::new_with_recycler(recycler.clone(), i + 1, "first one");
        }
    }
}
