use anyhow::Result;
use heed::{BytesDecode, BytesEncode};
use rkyv::api::high::{HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Error;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Serialize};

/// Wrapper for using Rkyv serialization/access with Heed
/// for zero-copy access to the database
pub struct RkyvCodec<T>(std::marker::PhantomData<T>);

impl<'a, T> BytesEncode<'a> for RkyvCodec<T>
where
    T: for<'b> Serialize<HighSerializer<AlignedVec, ArenaHandle<'b>, Error>> + 'a,
{
    type EItem = T;

    fn bytes_encode(
        item: &'a Self::EItem,
    ) -> Result<
        std::borrow::Cow<'a, [u8]>,
        Box<dyn std::error::Error + std::marker::Send + std::marker::Sync>,
    > {
        let bytes = rkyv::to_bytes::<Error>(item).map_err(|e| Box::new(e) as Box<_>)?;
        Ok(std::borrow::Cow::Owned(bytes.to_vec()))
    }
}

impl<'a, T> BytesDecode<'a> for RkyvCodec<T>
where
    T: Archive,
    T::Archived: 'a + for<'b> CheckBytes<HighValidator<'b, Error>>,
{
    type DItem = &'a T::Archived;

    fn bytes_decode(
        bytes: &'a [u8],
    ) -> Result<Self::DItem, Box<dyn std::error::Error + std::marker::Send + std::marker::Sync>>
    {
        Ok(rkyv::access::<T::Archived, _>(bytes)?)
    }
}
