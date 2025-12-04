use minicbor::bytes::ByteVec;
use minicbor::decode::Error;
use minicbor::{Decode, Decoder, Encode};
use num_bigint::{BigInt as _BigInt, Sign};

use crate::primitives::Hash;

pub type POSIXTime = i64;

pub type PubKeyHash = Hash<28>;

pub type CurrencySymbol = ByteVec;
pub type TokenName = ByteVec;

#[derive(Debug, Decode, PartialEq, Clone)]
pub struct Rational {
    #[n(0)]
    pub numerator: BigInt,
    #[n(1)]
    pub denominator: BigInt,
}

#[cfg(test)]
impl Rational {
    pub fn new(numerator: i128, denominator: i128) -> Self {
        Self {
            numerator: numerator.into(),
            denominator: denominator.into(),
        }
    }
}

fn decode_cbor_bigint<'b, C>(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<_BigInt, Error> {
    let pos = d.position();

    // Check if we're looking at a tag
    match d.datatype()? {
        minicbor::data::Type::Tag => {
            let tag = d.tag()?;
            match tag.as_u64() {
                2 => {
                    // Positive big integer
                    let bytes = d.bytes()?;
                    bytes_to_bigint(bytes, false, pos)
                }
                3 => {
                    // Negative big integer
                    let bytes = d.bytes()?;
                    bytes_to_bigint(bytes, true, pos)
                }
                _ => {
                    // Not a big integer tag, restore position and try regular integer
                    d.set_position(pos);
                    decode_regular_integer(d)
                }
            }
        }
        _ => {
            // Not a tag, try regular CBOR integer
            decode_regular_integer(d)
        }
    }
}

fn decode_regular_integer<'b>(d: &mut Decoder<'b>) -> Result<_BigInt, Error> {
    let pos = d.position();

    // Try different integer sizes, starting with the most general
    match d.datatype()? {
        minicbor::data::Type::U8
        | minicbor::data::Type::U16
        | minicbor::data::Type::U32
        | minicbor::data::Type::U64 => {
            let val = d.u64()?;
            Ok(val.into())
        }
        minicbor::data::Type::I8
        | minicbor::data::Type::I16
        | minicbor::data::Type::I32
        | minicbor::data::Type::I64
        | minicbor::data::Type::Int => {
            let val = d.i64()?;
            Ok(val.into())
        }
        other => Err(Error::type_mismatch(other)
            .at(pos)
            .with_message("expected integer or big integer")),
    }
}

fn bytes_to_bigint(bytes: &[u8], is_negative: bool, pos: usize) -> Result<_BigInt, Error> {
    if bytes.is_empty() {
        return Err(Error::message("empty byte string for big integer").at(pos));
    }

    let bigint = _BigInt::from_bytes_be(if is_negative { Sign::Minus } else { Sign::Plus }, bytes);
    if is_negative {
        // For negative big integers: value = -1 - (unsigned_value)
        return Ok(bigint.checked_add(&_BigInt::from(-1)).unwrap());
    }
    Ok(bigint)
}

// Helper function to implement Decode trait for a wrapper type
#[derive(Debug, Clone, PartialEq)]
pub struct BigInt(pub _BigInt);

impl From<i128> for BigInt {
    fn from(val: i128) -> Self {
        BigInt(_BigInt::from(val))
    }
}

impl<'b, C> minicbor::Decode<'b, C> for BigInt {
    fn decode(d: &mut Decoder<'b>, ctx: &mut C) -> Result<Self, Error> {
        decode_cbor_bigint(d, ctx).map(BigInt)
    }
}

#[derive(Debug, Decode, PartialEq, Clone)]
pub struct FixedTokenExtendedAssetClassFields {
    #[n(0)]
    pub symbol: CurrencySymbol,
    #[n(1)]
    pub name: TokenName,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ExtendedAssetClass {
    AnyToken(CurrencySymbol),
    FixedToken(FixedTokenExtendedAssetClassFields),
}

impl<'b, C> Decode<'b, C> for ExtendedAssetClass {
    fn decode(d: &mut minicbor::Decoder<'b>, ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let tag = d.tag()?;
        match tag.as_u64() {
            121 => {
                _ = d.array();
                let inner = <CurrencySymbol>::decode(d, ctx)?;
                _ = d.skip();
                Ok(ExtendedAssetClass::AnyToken(inner))
            }
            122 => {
                _ = d.array();
                let inner = <FixedTokenExtendedAssetClassFields>::decode(d, ctx)?;
                _ = d.skip();
                Ok(ExtendedAssetClass::FixedToken(inner))
            }
            _ => Err(minicbor::decode::Error::message(
                "Invalid tag for ExtendedAssetClass",
            )),
        }
    }
}

/// Implements the tagged enum pattern for optional values. For values that may not be present at
/// all, use `Option<T>` instead. Or if both cases are possible, use `Option<Optional<T>>`
#[derive(Debug, PartialEq, Clone)]
pub enum Optional<T> {
    Some(T),
    None,
}

impl<'b, C, T: Decode<'b, C>> Decode<'b, C> for Optional<T> {
    fn decode(d: &mut minicbor::Decoder<'b>, ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let tag = d.tag()?;
        match tag.as_u64() {
            121 => {
                _ = d.array();
                let inner = <T>::decode(d, ctx)?;
                _ = d.skip();
                Ok(Optional::Some(inner))
            }
            122 => {
                _ = d.skip(); // skip over null
                Ok(Optional::None)
            }
            _ => Err(minicbor::decode::Error::message("Invalid tag for Optional")),
        }
    }
}

impl<C, T: Encode<C>> Encode<C> for Optional<T> {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        ctx: &mut C,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        match &self {
            Optional::Some(v) => {
                _ = e.tag(minicbor::data::Tag::new(121));
                _ = e.begin_array();
                _ = v.encode(e, ctx);
                _ = e.end();
                Ok(())
            }
            Optional::None => {
                _ = e.tag(minicbor::data::Tag::new(122));
                _ = e.begin_array();
                _ = e.end();
                Ok(())
            }
        }
    }
}
