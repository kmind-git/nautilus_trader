// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under
//  the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied. See the License for the specific language governing
//  permissions and limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Minimal OrderHub client for script acceptance: submits one limit order,
//! prints the admission, snapshots state, and follows the event stream.

use std::time::Duration;

use orderhub_proto::orderhub::v1::journal_event::Kind;
use orderhub_proto::orderhub::v1::order_hub_client::OrderHubClient;
use orderhub_proto::orderhub::v1::{GetStateRequest, SubmitOrderRequest, WatchEventsRequest};
use tonic::Request as RpcRequest;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

/// Adds the shared bearer token to a request.
fn authorized<T>(message: T, token: &str) -> RpcRequest<T> {
    let mut request = RpcRequest::new(message);
    let value: MetadataValue<_> = format!("Bearer {token}").parse().unwrap();
    request.metadata_mut().insert("authorization", value);
    request
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("ORDERHUB_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let token =
        std::env::var("ORDERHUB_TOKEN").unwrap_or_else(|_| "orderhub-dev-token".to_string());
    let request_id = std::env::var("ORDERHUB_REQUEST_ID").unwrap_or_else(|_| {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or_default();
        format!("req-{millis}")
    });

    let channel = Channel::builder(format!("http://{addr}").parse()?)
        .connect()
        .await?;
    let mut client = OrderHubClient::new(channel);

    let admitted = client
        .submit_order(authorized(
            SubmitOrderRequest {
                submitter_id: "trader-1".to_string(),
                request_id: request_id.clone(),
                strategy_id: "ORDERHUB-S001".to_string(),
                instrument_id: "AAPL.XNAS".to_string(),
                side: "BUY".to_string(),
                quantity: "10".to_string(),
                price: "150.00".to_string(),
                time_in_force: "GTC".to_string(),
                post_only: false,
                reduce_only: false,
                submit_before_ns: None,
            },
            &token,
        ))
        .await?
        .into_inner();
    println!("ADMITTED: {admitted:?}");

    let state = client
        .get_state(authorized(GetStateRequest {}, &token))
        .await?
        .into_inner();
    println!(
        "STATE: cursor={} active_orders={}",
        state.state_cursor,
        state.active_orders.len()
    );

    let stream = client
        .watch_events(authorized(
            WatchEventsRequest {
                after: "0".to_string(),
            },
            &token,
        ))
        .await?
        .into_inner();
    tokio::pin!(stream);
    let mut business_events = 0;
    while business_events < 2 {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.message()).await??;
        let Some(event) = event else { break };
        match event.kind {
            Some(Kind::Admission(admission)) => {
                println!(
                    "EVENT admission seq={} request={}",
                    event.seq, admission.request_id
                );
                business_events += 1;
            }
            Some(Kind::Outcome(outcome)) => {
                println!(
                    "EVENT outcome seq={} origin={} result={}",
                    event.seq, outcome.origin_seq, outcome.outcome
                );
                business_events += 1;
            }
            Some(Kind::Progress(progress)) => {
                println!("EVENT progress cursor={}", progress.state_cursor);
            }
            None => {}
        }
    }
    println!("ACCEPTANCE OK");
    Ok(())
}
