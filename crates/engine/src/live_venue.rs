//! Live venue on Polymarket's official Rust SDK (`polymarket_client_sdk_v2`). Built only with the
//! `live` cargo feature. Orders are fill-and-kill marketable limits: price-protected, never resting.
//!
//! Unverified against the real venue in this repository (needs a funded, eligible account); the
//! adapter is compile-checked against SDK 0.9.0 and exercised only through the `Venue` interface.

use std::str::FromStr;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::clob::types::request::OrdersRequest;
use polymarket_client_sdk_v2::clob::types::{OrderStatusType, OrderType, Side, SignatureType};
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_client_sdk_v2::types::{Address, U256};
use polymarket_client_sdk_v2::POLYGON;

use crate::execution::{OrderState, SubmitResult, Venue, VenueError, VenueOrder, VenueOrderStatus};
use crate::positions::FillSide;

pub struct LiveVenue {
    client: Client<Authenticated<Normal>>,
    signer: PrivateKeySigner,
}

/// Trading signer configuration (never the data-only wallet).
pub struct TradingSigner {
    pub private_key: String,
    pub funder: Option<String>,
    /// 0 EOA, 1 Proxy, 2 GnosisSafe, 3 Poly1271 (deposit wallet)
    pub signature_type: u8,
    pub host: String,
}

impl LiveVenue {
    pub async fn connect(cfg: &TradingSigner) -> anyhow::Result<Self> {
        let signer = PrivateKeySigner::from_str(&cfg.private_key)?.with_chain_id(Some(POLYGON));
        let st = match cfg.signature_type {
            0 => SignatureType::Eoa,
            1 => SignatureType::Proxy,
            2 => SignatureType::GnosisSafe,
            3 => SignatureType::Poly1271,
            n => anyhow::bail!("unknown signature type {n}"),
        };
        let mut b = Client::new(&cfg.host, Config::default())?
            .authentication_builder(&signer)
            .signature_type(st);
        if let Some(f) = &cfg.funder {
            b = b.funder(Address::from_str(f)?);
        }
        Ok(LiveVenue {
            client: b.authenticate().await?,
            signer,
        })
    }
}

fn state(s: &OrderStatusType) -> OrderState {
    match s {
        OrderStatusType::Live | OrderStatusType::Delayed => OrderState::Acked,
        OrderStatusType::Matched => OrderState::Filled,
        OrderStatusType::Canceled | OrderStatusType::Unmatched => OrderState::Cancelled,
        _ => OrderState::Unknown,
    }
}

impl Venue for LiveVenue {
    async fn submit(&self, o: &VenueOrder) -> Result<SubmitResult, VenueError> {
        let token = U256::from_str(&o.token_id)
            .map_err(|e| VenueError::Uncertain(format!("bad token id: {e}")))?;
        let order = self
            .client
            .limit_order()
            .token_id(token)
            .size(o.shares)
            .price(o.limit_price)
            .side(if o.side == FillSide::Buy {
                Side::Buy
            } else {
                Side::Sell
            })
            .order_type(OrderType::FAK)
            .build()
            .await
            // Building happens locally (tick/size validation): nothing has been sent yet.
            .map_err(|e| return_rejected(e.to_string()))?;
        let signed = self
            .client
            .sign(&self.signer, order)
            .await
            .map_err(|e| return_rejected(e.to_string()))?;
        match self.client.post_order(signed).await {
            Ok(r) if r.success => Ok(SubmitResult::Accepted {
                exchange_id: r.order_id,
            }),
            Ok(r) => Ok(SubmitResult::Rejected(
                r.error_msg.unwrap_or_else(|| "rejected".into()),
            )),
            // Transport or server error after sending: the order may exist. Never retried.
            Err(e) => Err(VenueError::Uncertain(e.to_string())),
        }
    }

    async fn find(
        &self,
        o: &VenueOrder,
        since_us: i64,
    ) -> Result<Vec<VenueOrderStatus>, VenueError> {
        let token =
            U256::from_str(&o.token_id).map_err(|e| VenueError::Uncertain(e.to_string()))?;
        let req = OrdersRequest::builder().asset_id(token).build();
        let page = self
            .client
            .orders(&req, None)
            .await
            .map_err(|e| VenueError::Uncertain(e.to_string()))?;
        Ok(page
            .data
            .into_iter()
            .filter(|x| {
                x.price == o.limit_price
                    && x.original_size == o.shares
                    && x.created_at.timestamp_micros() >= since_us - 5_000_000
            })
            .map(|x| VenueOrderStatus {
                exchange_id: x.id,
                state: state(&x.status),
                size_matched: x.size_matched,
            })
            .collect())
    }

    async fn cancel(&self, exchange_id: &str) -> Result<(), VenueError> {
        self.client
            .cancel_order(exchange_id)
            .await
            .map(|_| ())
            .map_err(|e| VenueError::Uncertain(e.to_string()))
    }
}

/// Local build/sign failures happen before anything is sent: report as a rejection.
fn return_rejected(e: String) -> VenueError {
    VenueError::Uncertain(format!("local order build failed (not sent): {e}"))
}
