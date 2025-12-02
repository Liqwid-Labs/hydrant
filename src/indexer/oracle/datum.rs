use minicbor::{self, Decode};

use super::primitives::*;

#[derive(Debug, Decode, PartialEq, Clone)]
pub struct OracleDatum {
    #[n(0)]
    pub trusted_public_key_hash: PubKeyHash,
    #[n(1)]
    pub base_asset: ExtendedAssetClass,
    #[n(2)]
    pub exchange_rate: Rational,
    #[n(3)]
    pub exchange_rate_date: POSIXTime,
    #[n(4)]
    pub prices_time_out: POSIXTime,
    #[n(5)]
    pub hard_caps: Option<Optional<HardCaps>>,
}

#[derive(Debug, Decode, PartialEq, Clone)]
pub struct HardCaps {
    #[n(0)]
    pub low_cap: Rational,
    #[n(1)]
    pub high_cap: Rational,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx::Hash;

    #[test]
    fn test_decode() {
        let expected = OracleDatum {
            trusted_public_key_hash: Hash::<28>(
                hex::decode("2f97c124ebe90d23fac1be4111b5fda977021cb3f88cbe5d87145b61")
                    .expect("hex decode failed")
                    .try_into()
                    .expect("slice with incorrect length"),
            ),
            base_asset: ExtendedAssetClass::FixedToken(FixedTokenExtendedAssetClassFields {
                symbol: hex::decode("1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e")
                    .expect("hex decode failed")
                    .into(),
                name: hex::decode("776f726c646d6f62696c65746f6b656e")
                    .expect("hex decode failed")
                    .into(),
            }),
            exchange_rate: Rational::new(244907, 1000000),
            exchange_rate_date: 1727893977701,
            prices_time_out: 900000,
            hard_caps: None,
        };

        let cbor = hex::decode("9f581c2f97c124ebe90d23fac1be4111b5fda977021cb3f88cbe5d87145b61d87a9f9f581c1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e50776f726c646d6f62696c65746f6b656effff9f1a0003bcab1a000f4240ff1b000001924e81ba651a000dbba0ff").expect("hex decode failed");
        let decoded = minicbor::decode::<OracleDatum>(cbor.as_slice()).unwrap();

        assert_eq!(expected, decoded);
    }

    #[test]
    fn test_decode_with_hard_caps() {
        let expected = OracleDatum {
            trusted_public_key_hash: Hash::<28>(
                hex::decode("4952d7a9288abbd7b36625309c264c089ddbb6042fd7699e3d401b94")
                    .expect("hex decode failed")
                    .try_into()
                    .expect("slice with incorrect length"),
            ),
            base_asset: ExtendedAssetClass::AnyToken(
                hex::decode("a63b0c97f9b1dac7098599ed7fa7b6be1225d0624a78759e1ff6881b")
                    .expect("hex decode failed")
                    .into(),
            ),
            exchange_rate: Rational::new(10000000, 1),
            exchange_rate_date: 1729001581809,
            prices_time_out: 600000,
            hard_caps: Some(Optional::Some(HardCaps {
                low_cap: Rational::new(10000000, 1),
                high_cap: Rational::new(100000000, 1),
            })),
        };

        let cbor = hex::decode("9f581c4952d7a9288abbd7b36625309c264c089ddbb6042fd7699e3d401b94d8799f581ca63b0c97f9b1dac7098599ed7fa7b6be1225d0624a78759e1ff6881bff9f1a0098968001ff1b0000019290866cf11a000927c0d8799f9f9f1a0098968001ff9f1a05f5e10001ffffffff").expect("hex decode failed");
        let decoded = minicbor::decode::<OracleDatum>(cbor.as_slice()).unwrap();

        assert_eq!(expected, decoded);
    }

    #[test]
    fn test_decode_optim_bond1() {
        let expected = OracleDatum {
            trusted_public_key_hash: Hash::<28>(
                hex::decode("2f97c124ebe90d23fac1be4111b5fda977021cb3f88cbe5d87145b61")
                    .expect("hex decode failed")
                    .try_into()
                    .expect("slice with incorrect length"),
            ),
            base_asset: ExtendedAssetClass::AnyToken(
                hex::decode("1f3dd1b685f0d87eaf92120aca3cf56da6ed91e2dbc32a95a26bce4a")
                    .expect("hex decode failed")
                    .into(),
            ),
            exchange_rate: Rational::new(34062600, 1),
            exchange_rate_date: 1727893196280,
            prices_time_out: 900000,
            hard_caps: None,
        };

        let cbor = hex::decode("9f581c2f97c124ebe90d23fac1be4111b5fda977021cb3f88cbe5d87145b61d8799f581c1f3dd1b685f0d87eaf92120aca3cf56da6ed91e2dbc32a95a26bce4aff9f1a0207c10801ff1b000001924e75cdf81a000dbba0ff").expect("hex decode failed");
        let decoded = minicbor::decode::<OracleDatum>(cbor.as_slice()).unwrap();

        assert_eq!(expected, decoded);
    }
}
