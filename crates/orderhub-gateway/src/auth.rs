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

//! Per-submitter credential mapping (M2).
//!
//! Server-side mapping from bearer tokens to submitter identities and their
//! authorized strategy set, per the risk contract: `strategy_id` is an
//! attribution label validated against the credential, never a credential
//! itself. Requests referencing strategies outside the credential's
//! authorization are rejected without disclosing other submitters' data.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// One submitter's credentials and authorization scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitterCredentials {
    /// Shared bearer token for this submitter.
    pub token: String,
    /// Server-side submitter identity.
    pub submitter_id: String,
    /// Strategy IDs this submitter may submit and query for.
    pub strategies: Vec<String>,
}

/// Registry of all authorized submitters, keyed by token.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubmitterRegistry {
    submitters: Vec<SubmitterCredentials>,
    #[serde(skip)]
    by_token: HashMap<String, usize>,
}

impl SubmitterRegistry {
    /// Creates a registry from a credential list.
    ///
    /// # Panics
    ///
    /// Panics on duplicate tokens: they are a configuration error and must
    /// fail fast at startup rather than silently granting another identity.
    #[must_use]
    pub fn new(submitters: Vec<SubmitterCredentials>) -> Self {
        let mut by_token = HashMap::with_capacity(submitters.len());
        for (index, credentials) in submitters.iter().enumerate() {
            assert!(
                by_token.insert(credentials.token.clone(), index).is_none(),
                "duplicate submitter token for {}",
                credentials.submitter_id
            );
        }
        Self {
            submitters,
            by_token,
        }
    }

    /// Authenticates a raw bearer token to its submitter credentials.
    #[must_use]
    pub fn authenticate(&self, token: &str) -> Option<&SubmitterCredentials> {
        self.by_token
            .get(token)
            .and_then(|&index| self.submitters.get(index))
    }

    /// Authenticates a bearer-token header value ("Bearer <token>").
    #[must_use]
    pub fn authenticate_header(&self, header: &str) -> Option<&SubmitterCredentials> {
        header
            .strip_prefix("Bearer ")
            .and_then(|token| self.authenticate(token))
    }

    /// Whether `strategy_id` is within the credential's authorization set.
    #[must_use]
    pub fn is_authorized(credentials: &SubmitterCredentials, strategy_id: &str) -> bool {
        credentials.strategies.iter().any(|s| s == strategy_id)
    }
}
