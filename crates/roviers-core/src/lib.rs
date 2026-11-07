use chrono::Utc;
use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::rngs::OsRng;
use roviers_ledger::{DedupLog, Ledger, LedgerError, TopicTrie};
use roviers_proto::{
    CitizenCredentials, IntimationNotice, ProtoError, Receipt, ReceiptStatus, ServiceOutcome,
    ServiceRequest, SignedCitizenToken,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use thiserror::Error;
use tokio::sync::{broadcast, Mutex};
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum CoreError {
    #[error("Authentication failed: {0}")]
    Auth(#[from] ProtoError),
    #[error("Ledger storage error: {0}")]
    Ledger(#[from] LedgerError),
    #[error("Duplicate transaction detected (Receipt/Request ID: {0})")]
    DuplicateRequest(String),
    #[error("Receipt {0} not found")]
    ReceiptNotFound(Uuid),
    #[error("Unauthorized: Citizen ID does not match receipt owner")]
    CitizenMismatch,
    #[error("Outcome for receipt {0} is not ready yet")]
    OutcomeNotReady(Uuid),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// The core civic coordination engine
pub struct RoviersEngine {
    pub gov_signing_key: SigningKey,
    pub gov_verifying_key: VerifyingKey,
    pub ledger: Arc<Mutex<Ledger>>,
    pub dedup_log: Arc<DedupLog>,
    pub topic_trie: Arc<TopicTrie>,
    vault_dir: PathBuf,
    receipts: Arc<RwLock<HashMap<Uuid, Receipt>>>,
    outcomes: Arc<RwLock<HashMap<Uuid, ServiceOutcome>>>,
    intimation_bus: broadcast::Sender<IntimationNotice>,
    http_client: reqwest::Client,
}

impl RoviersEngine {
    pub fn new(ledger_dir: impl AsRef<Path>) -> Result<Self, CoreError> {
        let dir = ledger_dir.as_ref();
        std::fs::create_dir_all(dir)?;

        // 1. Persistent Authority Key Management
        let key_path = dir.join("gov_key.bin");
        let gov_signing_key = if key_path.exists() {
            let bytes = std::fs::read(&key_path)?;
            let key_array: [u8; 32] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| CoreError::Auth(ProtoError::KeyError("Invalid gov key length".into())))?;
            SigningKey::from_bytes(&key_array)
        } else {
            let mut rng = OsRng;
            let key = SigningKey::generate(&mut rng);
            std::fs::write(&key_path, key.to_bytes())?;
            key
        };
        let gov_verifying_key = gov_signing_key.verifying_key();

        // 2. Open Append-Only Ledger
        let ledger = Ledger::open(dir, 64 * 1024 * 1024)?;
        let (intimation_bus, _) = broadcast::channel(50000);

        // 3. Persistent Outcome Vault Initialization
        let vault_dir = dir.join("vault");
        std::fs::create_dir_all(&vault_dir)?;

        let mut receipts_map = HashMap::new();
        let mut outcomes_map = HashMap::new();

        if let Ok(entries) = std::fs::read_dir(&vault_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if file_name.ends_with(".receipt.json") {
                    if let Ok(data) = std::fs::read_to_string(&path) {
                        if let Ok(r) = serde_json::from_str::<Receipt>(&data) {
                            receipts_map.insert(r.receipt_id, r);
                        }
                    }
                } else if file_name.ends_with(".outcome.json") {
                    if let Ok(data) = std::fs::read_to_string(&path) {
                        if let Ok(o) = serde_json::from_str::<ServiceOutcome>(&data) {
                            outcomes_map.insert(o.receipt_id, o);
                        }
                    }
                }
            }
        }

        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Ok(Self {
            gov_signing_key,
            gov_verifying_key,
            ledger: Arc::new(Mutex::new(ledger)),
            dedup_log: Arc::new(DedupLog::new(100000)),
            topic_trie: Arc::new(TopicTrie::new()),
            vault_dir,
            receipts: Arc::new(RwLock::new(receipts_map)),
            outcomes: Arc::new(RwLock::new(outcomes_map)),
            intimation_bus,
            http_client,
        })
    }

    /// Issue a new official Citizen ID token
    pub fn issue_citizen_id(
        &self,
        citizen_id: &str,
        full_name: &str,
        validity_secs: i64,
    ) -> Result<SignedCitizenToken, CoreError> {
        let creds = CitizenCredentials::new(citizen_id, full_name, validity_secs);
        let token = SignedCitizenToken::issue(creds, &self.gov_signing_key)?;
        Ok(token)
    }

    /// Public service intake counter:
    /// 1. Statelessly verifies citizen card (sub-microsecond)
    /// 2. Checks idempotency
    /// 3. Appends transaction to immutable ledger
    /// 4. Generates claim-check Receipt and persists to Vault
    /// 5. Publishes to topic trie for async workers
    pub async fn submit_service_request(
        &self,
        request: ServiceRequest,
    ) -> Result<Receipt, CoreError> {
        // Step 1: Stateless verification of Citizen ID using Government Public Key
        let creds = request.citizen_token.verify(&self.gov_verifying_key)?;
        let citizen_id = creds.citizen_id.clone();
        let req_id_str = request.request_id.to_string();

        // Step 2: Idempotency deduplication check
        if self.dedup_log.check_and_record(&req_id_str) {
            return Err(CoreError::DuplicateRequest(req_id_str));
        }

        // Step 3: Serialize and append transaction to Ledger
        let payload_bytes = serde_json::to_vec(&request)?;
        let topic = format!("service.{}", request.service_name);

        let offset = {
            let mut l = self.ledger.lock().await;
            l.append(&topic, &req_id_str, &payload_bytes)?
        };

        // Step 4: Issue the Receipt (Claim-Check Token)
        let receipt = Receipt::from_request(&request, Some(5));

        {
            let mut r = self.receipts.write().unwrap();
            r.insert(receipt.receipt_id, receipt.clone());
        }

        // Persist receipt to disk vault
        let receipt_path = self.vault_dir.join(format!("{}.receipt.json", receipt.receipt_id));
        if let Ok(data) = serde_json::to_string_pretty(&receipt) {
            let _ = std::fs::write(receipt_path, data);
        }

        // Step 5: Notify workers through topic routing trie
        self.topic_trie.publish(&topic, offset);

        tracing::info!(
            "Accepted request {} from citizen {} on topic {} at ledger offset {}",
            req_id_str,
            citizen_id,
            topic,
            offset
        );

        Ok(receipt)
    }

    /// Worker completion step:
    /// 1. Records final outcome in memory and disk vault
    /// 2. Updates Receipt status to ReadyForPickup on disk and memory
    /// 3. Dispatches an Intimation Notice over SSE broadcast
    /// 4. Triggers active Webhook Callback if configured on the receipt
    pub fn complete_service(&self, outcome: ServiceOutcome) -> Result<(), CoreError> {
        let receipt_id = outcome.receipt_id;
        let citizen_id = outcome.citizen_id.clone();
        let service_name = outcome.service_name.clone();

        {
            let mut outcomes = self.outcomes.write().unwrap();
            outcomes.insert(receipt_id, outcome.clone());
        }

        // Persist outcome to disk vault
        let outcome_path = self.vault_dir.join(format!("{}.outcome.json", receipt_id));
        if let Ok(data) = serde_json::to_string_pretty(&outcome) {
            let _ = std::fs::write(outcome_path, data);
        }

        let callback_url = {
            let mut receipts = self.receipts.write().unwrap();
            if let Some(r) = receipts.get_mut(&receipt_id) {
                r.status = ReceiptStatus::ReadyForPickup;
                let receipt_path = self.vault_dir.join(format!("{}.receipt.json", receipt_id));
                if let Ok(data) = serde_json::to_string_pretty(r) {
                    let _ = std::fs::write(receipt_path, data);
                }
                r.callback_url.clone()
            } else {
                None
            }
        };

        // Emit Intimation Notice over the notification bus (SSE)
        let notice = IntimationNotice {
            intimation_id: Uuid::now_v7(),
            receipt_id,
            citizen_id: citizen_id.clone(),
            service_name,
            status: ReceiptStatus::ReadyForPickup,
            message: format!("Service outcome for receipt #{} is ready for collection", receipt_id),
            pickup_url: format!("/api/v1/receipts/{}/pickup", receipt_id),
            timestamp: Utc::now().timestamp(),
        };

        let _ = self.intimation_bus.send(notice.clone());

        // Trigger Active Webhook Callback if present
        if let Some(url) = callback_url {
            let client = self.http_client.clone();
            tokio::spawn(async move {
                tracing::info!("Dispatching webhook callback to {}", url);
                match client.post(&url).json(&notice).send().await {
                    Ok(resp) => {
                        tracing::info!("Webhook delivered to {} (HTTP {})", url, resp.status());
                    }
                    Err(e) => {
                        tracing::warn!("Webhook callback delivery to {} failed: {}", url, e);
                    }
                }
            });
        }

        tracing::info!(
            "Service outcome recorded for receipt {}. Intimation dispatched for citizen {}",
            receipt_id,
            citizen_id
        );

        Ok(())
    }

    /// Citizen presents receipt token to collect their outcome from the Vault
    pub fn pickup_outcome(
        &self,
        receipt_id: Uuid,
        token: &SignedCitizenToken,
    ) -> Result<ServiceOutcome, CoreError> {
        let creds = token.verify(&self.gov_verifying_key)?;

        let receipts = self.receipts.read().unwrap();
        let receipt = receipts
            .get(&receipt_id)
            .ok_or(CoreError::ReceiptNotFound(receipt_id))?;

        if receipt.citizen_id != creds.citizen_id {
            return Err(CoreError::CitizenMismatch);
        }

        let outcomes = self.outcomes.read().unwrap();
        let outcome = outcomes
            .get(&receipt_id)
            .ok_or(CoreError::OutcomeNotReady(receipt_id))?;

        drop(receipts);
        let mut receipts = self.receipts.write().unwrap();
        if let Some(r) = receipts.get_mut(&receipt_id) {
            r.status = ReceiptStatus::Fulfilled;
            let receipt_path = self.vault_dir.join(format!("{}.receipt.json", receipt_id));
            if let Ok(data) = serde_json::to_string_pretty(r) {
                let _ = std::fs::write(receipt_path, data);
            }
        }

        Ok(outcome.clone())
    }

    /// Get current status of a receipt
    pub fn get_receipt(&self, receipt_id: Uuid) -> Option<Receipt> {
        self.receipts.read().unwrap().get(&receipt_id).cloned()
    }

    /// List all receipts belonging to a citizen
    pub fn list_citizen_receipts(&self, citizen_id: &str) -> Vec<Receipt> {
        self.receipts
            .read()
            .unwrap()
            .values()
            .filter(|r| r.citizen_id == citizen_id)
            .cloned()
            .collect()
    }

    /// Subscribe to live intimation signals (used by SSE server stream)
    pub fn subscribe_intimations(&self) -> broadcast::Receiver<IntimationNotice> {
        self.intimation_bus.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_full_civic_engine_lifecycle() {
        let temp_dir = std::env::temp_dir().join(format!("roviers_test_{}", Uuid::now_v7()));
        let engine = RoviersEngine::new(&temp_dir).expect("Failed to init engine");

        // 1. Issue Citizen ID Card
        let citizen_token = engine
            .issue_citizen_id("CTZ-TEST-42", "Alexander Hamilton", 86400)
            .expect("Failed to issue ID");

        assert_eq!(citizen_token.credentials.citizen_id, "CTZ-TEST-42");

        // 2. Subscribe to Intimations (Citizen opens real-time listener)
        let mut intimation_rx = engine.subscribe_intimations();

        // 3. Submit Service Request at the Counter
        let request = ServiceRequest::new(
            "treasury.mint_bond",
            citizen_token.clone(),
            serde_json::json!({ "denomination": 1000 }),
            None,
        );
        let req_id = request.request_id;

        let receipt = engine
            .submit_service_request(request)
            .await
            .expect("Failed to submit request");

        assert_eq!(receipt.receipt_id, req_id);
        assert_eq!(receipt.citizen_id, "CTZ-TEST-42");
        assert_eq!(receipt.status, ReceiptStatus::Queued);

        // 4. Test Idempotency (Duplicate submission rejected immediately)
        let dup_request = ServiceRequest {
            request_id: req_id,
            service_name: "treasury.mint_bond".into(),
            citizen_token: citizen_token.clone(),
            payload: serde_json::json!({ "denomination": 1000 }),
            callback_url: None,
        };
        let dup_err = engine.submit_service_request(dup_request).await;
        assert!(matches!(dup_err, Err(CoreError::DuplicateRequest(_))));

        // 5. Worker completes service asynchronously
        let outcome = ServiceOutcome {
            receipt_id: req_id,
            citizen_id: "CTZ-TEST-42".into(),
            service_name: "treasury.mint_bond".into(),
            completed_at: Utc::now().timestamp(),
            result: serde_json::json!({ "bond_id": "BOND-9999", "status": "ISSUED" }),
        };

        engine.complete_service(outcome).expect("Failed to complete service");

        // 6. Verify Intimation was received over the broadcast bus
        let notice = intimation_rx.recv().await.expect("Failed to receive notice");
        assert_eq!(notice.receipt_id, req_id);
        assert_eq!(notice.citizen_id, "CTZ-TEST-42");
        assert_eq!(notice.status, ReceiptStatus::ReadyForPickup);

        // 7. Pickup outcome with Citizen Token
        let final_outcome = engine
            .pickup_outcome(req_id, &citizen_token)
            .expect("Pickup failed");

        assert_eq!(final_outcome.result["bond_id"], "BOND-9999");

        // Verify receipt status is now Fulfilled
        let updated_receipt = engine.get_receipt(req_id).unwrap();
        assert_eq!(updated_receipt.status, ReceiptStatus::Fulfilled);

        // 8. Attempting pickup with wrong citizen ID fails
        let wrong_token = engine
            .issue_citizen_id("CTZ-IMPOSTER", "Aaron Burr", 86400)
            .unwrap();
        let impostor_pickup = engine.pickup_outcome(req_id, &wrong_token);
        assert!(matches!(impostor_pickup, Err(CoreError::CitizenMismatch)));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_vault_persistence_across_restarts() {
        let temp_dir = std::env::temp_dir().join(format!("roviers_vault_test_{}", Uuid::now_v7()));

        let req_id;
        let citizen_token;

        // Session 1: Engine starts, creates key, accepts request, completes outcome
        {
            let engine = RoviersEngine::new(&temp_dir).expect("Failed to init engine");
            citizen_token = engine
                .issue_citizen_id("CTZ-PERSIST-01", "Ada Lovelace", 86400)
                .expect("Failed to issue token");

            let request = ServiceRequest::new(
                "computation.analytic_engine",
                citizen_token.clone(),
                serde_json::json!({ "algorithm": "bernoulli_numbers" }),
                Some("http://127.0.0.1:9999/callback".into()),
            );
            req_id = request.request_id;

            engine
                .submit_service_request(request)
                .await
                .expect("Failed to submit");

            let outcome = ServiceOutcome {
                receipt_id: req_id,
                citizen_id: "CTZ-PERSIST-01".into(),
                service_name: "computation.analytic_engine".into(),
                completed_at: Utc::now().timestamp(),
                result: serde_json::json!({ "status": "COMPUTED", "result_val": 42 }),
            };
            engine.complete_service(outcome).expect("Failed to complete");
        } // engine 1 dropped here!

        // Session 2: New engine instance opens the same data directory
        {
            let engine2 = RoviersEngine::new(&temp_dir).expect("Failed to re-open engine");

            // Key should have persisted, so token issued in Session 1 should still verify!
            let receipt = engine2.get_receipt(req_id).expect("Receipt missing after restart");
            assert_eq!(receipt.status, ReceiptStatus::ReadyForPickup);
            assert_eq!(receipt.callback_url, Some("http://127.0.0.1:9999/callback".into()));

            // Outcome should have persisted and be retrievable from Vault with original token
            let outcome = engine2
                .pickup_outcome(req_id, &citizen_token)
                .expect("Failed to pick up outcome from persistent vault");

            assert_eq!(outcome.result["result_val"], 42);

            let fulfilled_receipt = engine2.get_receipt(req_id).unwrap();
            assert_eq!(fulfilled_receipt.status, ReceiptStatus::Fulfilled);
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
