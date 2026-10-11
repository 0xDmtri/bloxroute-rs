//! Supported EVM submission networks and route capabilities.

/// EVM networks supported by this client's bloXroute submission routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BloxrouteNetwork {
    /// [Ethereum public transactions](https://docs.bloxroute.com/eth/sending-transactions/public-transactions).
    Ethereum,
    /// [BNB Smart Chain mainnet](https://docs.bloxroute.com/bsc/submit-transactions/public-transactions).
    Bnb,
    /// [Base mainnet](https://docs.bloxroute.com/base/submit-transactions/base-transactions).
    Base,
    /// [Robinhood Chain](https://docs.bloxroute.com/robinhood/submit-transactions/robinhood-transactions).
    Robinhood,
}

impl BloxrouteNetwork {
    pub(crate) const fn rpc_method(self) -> &'static str {
        match self {
            Self::Ethereum | Self::Bnb | Self::Base => "blxr_tx",
            Self::Robinhood => "robinhood_tx",
        }
    }

    pub(crate) const fn blockchain_network(self) -> Option<&'static str> {
        match self {
            Self::Ethereum | Self::Robinhood => None,
            Self::Bnb => Some("BSC-Mainnet"),
            Self::Base => Some("Base-Mainnet"),
        }
    }

    /// Whether this submission route accepts `backrunme_reward_address`.
    ///
    /// Returns `true` for BNB Smart Chain, Base, and Robinhood Chain. Ethereum's
    /// public route requires submissions without a reward address.
    pub const fn supports_reward_address(self) -> bool {
        matches!(self, Self::Bnb | Self::Base | Self::Robinhood)
    }
}
