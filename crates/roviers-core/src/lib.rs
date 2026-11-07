use chrono::Utc;
use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::rngs::OsRng;
use roviers_ledger::{DedupLog, Ledger, LedgerError, TopicTrie};
use roviers_proto::{
    CitizenCredentials, IntimationNotice, ProtoError, Receipt, ReceiptStatus, ServiceOutcome,
    ServiceRequest, SignedCitizenToken,
};
use std::collections::HashMap;
use std::path::Path;
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
}

/// The core civic coordination engine
pub struct RoviersEngine {
    pub gov_signing_key: SigningKey,
    pub gov_verifying_key: VerifyingKey,
    pub ledger: Arc<Mutex<Ledger>>,
    pub dedup_log: Arc<DedupLog>,
    pub topic_trie: Arc<TopicTrie>,
    receipts: Arc<RwLock<HashMap<Uuid, Receipt>>>,
    outcomes: Arc<RwLock<HashMap<Uuid, ServiceOutcome>>>,
    intimation_bus: broadcast::Sender<IntimationNotice>,
}

impl RoviersEngine {
    pub fn new(ledger_dir: impl AsRef<Path>) -> Result<Self, CoreError> {
        let mut rng = OsRng;
        let gov_signing_key = SigningKey::generate(&mut rng);
        let gov_verifying_key = gov_signing_key.verifying_key();

        let ledger = Ledger::open(ledger_dir, 64 * 1024 * 1024)?;
        let (intimation_bus, _) = broadcast::channel(50000);

        Ok(Self {
            gov_signing_key,
            gov_verifying_key,
            ledger: Arc::new(Mutex::new(ledger)),
            dedup_log: Arc::new(DedupLog::new(100000)),
            topic_trie: Arc::new(TopicTrie::new()),
            receipts: Arc::new(RwLock::new(HashMap::new())),
            outcomes: Arc::new(RwLock::new(HashMap::new())),
            intimation_bus,
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
    /// 4. Generates claim-check Receipt
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
    /// Records final outcome and dispatches an Intimation Notice to summon the citizen
    pub fn complete_service(&self, outcome: ServiceOutcome) -> Result<(), CoreError> {
        let receipt_id = outcome.receipt_id;
        let citizen_id = outcome.citizen_id.clone();
        let service_name = outcome.service_name.clone();

        {
            let mut outcomes = self.outcomes.write().unwrap();
            outcomes.insert(receipt_id, outcome);
        }

        {
            let mut receipts = self.receipts.write().unwrap();
            if let Some(r) = receipts.get_mut(&receipt_id) {
                r.status = ReceiptStatus::ReadyForPickup;
            }
        }

        // Emit Intimation Notice over the notification bus
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

        let _ = self.intimation_bus.send(notice);

        tracing::info!(
            "Service outcome recorded for receipt {}. Intimation dispatched for citizen {}",
            receipt_id,
            citizen_id
        );

        Ok(())
    }

    /// Citizen presents receipt token to collect their outcome
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
}

