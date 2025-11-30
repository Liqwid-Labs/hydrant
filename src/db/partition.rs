use std::ops::{Bound, RangeBounds};

use anyhow::{Context, Result};
use bincode::config::{BigEndian, Configuration};
use bincode::{Decode, Encode};
use fjall::{Batch, PartitionHandle};

static CONFIG: Configuration<BigEndian> = bincode::config::standard().with_big_endian();

/// Type safe wrapper around `fjall::PartitionHandle`
#[derive(Clone)]
pub struct Partition<K: Encode + Decode<()>, V: Encode + Decode<()>> {
    _phantom: std::marker::PhantomData<(K, V)>,
    pub partition: PartitionHandle,
}

impl<K: Encode + Decode<()>, V: Encode + Decode<()>> Partition<K, V> {
    // Serialization
    pub fn encode_key(&self, key: &K) -> Result<Vec<u8>> {
        bincode::encode_to_vec(key, CONFIG)
            .with_context(|| format!("failed to encode {} key", self.partition.name))
    }
    pub fn encode_value(&self, value: &V) -> Result<Vec<u8>> {
        bincode::encode_to_vec(value, CONFIG)
            .with_context(|| format!("failed to encode {} value", self.partition.name))
    }
    pub fn decode_key(&self, key: &[u8]) -> Result<K> {
        bincode::decode_from_slice(key, CONFIG)
            .with_context(|| format!("failed to decode {} key", self.partition.name))
            .map(|k| k.0)
    }
    pub fn decode_value(&self, value: &[u8]) -> Result<V> {
        bincode::decode_from_slice(value, CONFIG)
            .with_context(|| format!("failed to decode {} value", self.partition.name))
            .map(|v| v.0)
    }

    // Getters

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let encoded_key = bincode::encode_to_vec(key, CONFIG).context("failed to encode key")?;
        let encoded_value = self.partition.get(&encoded_key)?;
        encoded_value
            .map(|v| {
                bincode::decode_from_slice(&v, CONFIG)
                    .context("failed to decode value")
                    .map(|v| v.0)
            })
            .transpose()
    }

    pub fn contains_key(&self, key: &K) -> Result<bool> {
        let encoded_key = self.encode_key(key)?;
        Ok(self.partition.contains_key(&encoded_key)?)
    }

    pub fn range<R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> Result<impl DoubleEndedIterator<Item = Result<(K, V)>>> {
        let start_bound = match range.start_bound() {
            Bound::Included(start) => Bound::Included(self.encode_key(start)?),
            Bound::Excluded(start) => Bound::Excluded(self.encode_key(start)?),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bound = match range.end_bound() {
            Bound::Included(end) => Bound::Included(self.encode_key(end)?),
            Bound::Excluded(end) => Bound::Excluded(self.encode_key(end)?),
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(self
            .partition
            .range((start_bound, end_bound))
            .map(move |x| {
                let (k, v) = x?;
                Ok((self.decode_key(&k)?, self.decode_value(&v)?))
            }))
    }

    // Setters

    pub fn insert(&self, batch: &mut Batch, key: &K, value: &V) -> Result<()> {
        let encoded_key = self.encode_key(key)?;
        let encoded_value = self.encode_value(value)?;
        batch.insert(&self.partition, &encoded_key, &encoded_value);
        Ok(())
    }

    pub fn remove(&self, batch: &mut Batch, key: &K) -> Result<()> {
        let encoded_key = self.encode_key(key)?;
        batch.remove(&self.partition, &encoded_key);
        Ok(())
    }
}

impl<K, V> From<PartitionHandle> for Partition<K, V>
where
    K: Encode + Decode<()>,
    V: Encode + Decode<()>,
{
    fn from(handle: PartitionHandle) -> Self {
        Self {
            _phantom: std::marker::PhantomData,
            partition: handle,
        }
    }
}
