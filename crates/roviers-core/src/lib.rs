use chrono::Utc;
use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::rngs::OsRng;
use roviers_ledger::{DedupLog, Ledger, LedgerError, TopicTrie};
use roviers_proto::{
    AgentCredentials, AgentSignal, ClusterNodeInfo, ClusterTopologyResponse, PeerHeartbeatRequest,
    ProtoError, Receipt, ReceiptStatus, ResumePayload, ServiceBackoff, ServiceOutcome,
    ServiceRequest, SessionResumeRequest, SignedAgentToken, SignedResumeToken, WorkerLeaseRequest,
    WorkerLeaseResponse,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use thiserror::Error;
use tokio::sync::{Mutex, broadcast};
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
    #[error("Unauthorized: Agent ID does not match receipt owner")]
    AgentMismatch,
    #[error("Outcome for receipt {0} is not ready yet")]
    OutcomeNotReady(Uuid),
    #[error("Receipt {0} is not parked awaiting input")]
    NotParked(Uuid),
    #[error("Business case locked for key '{0}' (active receipt {1})")]
    BusinessCaseLocked(String, Uuid),
    #[error("Forbidden: Agent missing required scope '{0}'")]
    ScopeForbidden(String),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

// Backwards compatibility aliases
#[allow(non_upper_case_globals)]
pub const CitizenMismatch: fn() -> CoreError = || CoreError::AgentMismatch;
#[allow(non_upper_case_globals)]
pub const ClientMismatch: fn() -> CoreError = || CoreError::AgentMismatch;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ActiveWorkerLease {
    pub lease_id: Uuid,
    pub worker_id: String,
    pub service_filter: Option<String>,
    pub task_ids: Vec<Uuid>,
    pub granted_at: i64,
    pub expires_at: i64,
}

/// The core broker engine
pub struct RoviersEngine {
    pub broker_signing_key: SigningKey,
    pub broker_verifying_key: VerifyingKey,
    // Aliases for backwards compatibility
    pub authority_signing_key: SigningKey,
    pub authority_verifying_key: VerifyingKey,
    pub gov_signing_key: SigningKey,
    pub gov_verifying_key: VerifyingKey,
    pub ledger: Arc<Mutex<Ledger>>,
    pub dedup_log: Arc<DedupLog>,
    pub topic_trie: Arc<TopicTrie>,
    vault_dir: PathBuf,
    receipts: Arc<RwLock<HashMap<Uuid, Receipt>>>,
    outcomes: Arc<RwLock<HashMap<Uuid, ServiceOutcome>>>,
    pub service_backoffs: Arc<RwLock<HashMap<String, ServiceBackoff>>>,
    pub worker_leases: Arc<RwLock<HashMap<Uuid, ActiveWorkerLease>>>,
    pub dlq: Arc<RwLock<Vec<Receipt>>>,
    pub business_case_locks: Arc<RwLock<HashMap<String, Uuid>>>,
    pub cluster_nodes: Arc<RwLock<HashMap<String, ClusterNodeInfo>>>,
    pub pending_requests: Arc<RwLock<HashMap<Uuid, ServiceRequest>>>,
    pub node_id: String,
    pub is_leader: bool,
    pub num_partitions: u32,
    signal_bus: broadcast::Sender<AgentSignal>,
    http_client: reqwest::Client,
}

impl RoviersEngine {
    pub fn new(ledger_dir: impl AsRef<Path>) -> Result<Self, CoreError> {
        let dir = ledger_dir.as_ref();
        std::fs::create_dir_all(dir)?;

        // 1. Persistent Broker Key Management
        let key_path = if dir.join("broker_key.bin").exists() {
            dir.join("broker_key.bin")
        } else if dir.join("authority_key.bin").exists() {
            dir.join("authority_key.bin")
        } else if dir.join("gov_key.bin").exists() {
            dir.join("gov_key.bin")
        } else {
            dir.join("broker_key.bin")
        };

        let broker_signing_key = if key_path.exists() {
            let bytes = std::fs::read(&key_path)?;
            let key_array: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                CoreError::Auth(ProtoError::KeyError("Invalid broker key length".into()))
            })?;
            SigningKey::from_bytes(&key_array)
        } else {
            let mut rng = OsRng;
            let key = SigningKey::generate(&mut rng);
            std::fs::write(&key_path, key.to_bytes())?;
            key
        };
        let broker_verifying_key = broker_signing_key.verifying_key();

        // 2. Open Append-Only Ledger
        let ledger = Ledger::open(dir, 64 * 1024 * 1024)?;
        let (signal_bus, _) = broadcast::channel(50000);

        // 3. Persistent Outcome Vault Initialization
        let vault_dir = dir.join("vault");
        std::fs::create_dir_all(&vault_dir)?;

        let mut receipts_map = HashMap::new();
        let mut outcomes_map = HashMap::new();
        let mut pending_requests_map = HashMap::new();

        if let Ok(entries) = std::fs::read_dir(&vault_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if file_name.ends_with(".receipt.json") {
                    let receipt_res = std::fs::read_to_string(&path)
                        .ok()
                        .and_then(|data| serde_json::from_str::<Receipt>(&data).ok());
                    if let Some(r) = receipt_res {
                        receipts_map.insert(r.receipt_id, r);
                    }
                } else if file_name.ends_with(".outcome.json") {
                    let outcome_res = std::fs::read_to_string(&path)
                        .ok()
                        .and_then(|data| serde_json::from_str::<ServiceOutcome>(&data).ok());
                    if let Some(o) = outcome_res {
                        outcomes_map.insert(o.receipt_id, o);
                    }
                } else if file_name.ends_with(".request.json") {
                    let req_res = std::fs::read_to_string(&path)
                        .ok()
                        .and_then(|data| serde_json::from_str::<ServiceRequest>(&data).ok());
                    if let Some(req) = req_res {
                        pending_requests_map.insert(req.request_id, req);
                    }
                }
            }
        }

        // Filter pending requests to only non-terminal receipts
        pending_requests_map.retain(|id, _| {
            receipts_map.get(id).is_some_and(|r| {
                matches!(
                    r.status,
                    ReceiptStatus::Queued
                        | ReceiptStatus::Processing
                        | ReceiptStatus::ParkedAwaitingInput { .. }
                )
            })
        });

        // Initialize active business case locks from persisted receipts (Phase 6)
        let mut business_case_locks = HashMap::new();
        for (id, r) in &receipts_map {
            if let Some(ref bkey) = r.business_key
                && matches!(
                    r.status,
                    ReceiptStatus::Queued
                        | ReceiptStatus::Processing
                        | ReceiptStatus::ParkedAwaitingInput { .. }
                )
            {
                business_case_locks.insert(bkey.clone(), *id);
            }
        }

        // Initialize multi-broker node cluster state (Phase 3)
        let node_id = format!(
            "broker-node-{}",
            broker_verifying_key.to_bytes()[..4]
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>()
        );
        let num_partitions = 16;
        let mut cluster_nodes_map = HashMap::new();
        cluster_nodes_map.insert(
            node_id.clone(),
            ClusterNodeInfo {
                node_id: node_id.clone(),
                listen_addr: "127.0.0.1:8080".into(),
                is_leader: true,
                assigned_partitions: (0..num_partitions).collect(),
                last_heartbeat: Utc::now().timestamp(),
            },
        );

        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Ok(Self {
            broker_signing_key: broker_signing_key.clone(),
            broker_verifying_key,
            authority_signing_key: broker_signing_key.clone(),
            authority_verifying_key: broker_verifying_key,
            gov_signing_key: broker_signing_key,
            gov_verifying_key: broker_verifying_key,
            ledger: Arc::new(Mutex::new(ledger)),
            dedup_log: Arc::new(DedupLog::new(100000)),
            topic_trie: Arc::new(TopicTrie::new()),
            vault_dir,
            receipts: Arc::new(RwLock::new(receipts_map)),
            outcomes: Arc::new(RwLock::new(outcomes_map)),
            service_backoffs: Arc::new(RwLock::new(HashMap::new())),
            worker_leases: Arc::new(RwLock::new(HashMap::new())),
            dlq: Arc::new(RwLock::new(Vec::new())),
            business_case_locks: Arc::new(RwLock::new(business_case_locks)),
            cluster_nodes: Arc::new(RwLock::new(cluster_nodes_map)),
            pending_requests: Arc::new(RwLock::new(pending_requests_map)),
            node_id,
            is_leader: true,
            num_partitions,
            signal_bus,
            http_client,
        })
    }

    /// Issue a new official Agent identity token
    pub fn issue_agent_token(
        &self,
        agent_id: &str,
        agent_name: &str,
        validity_secs: i64,
    ) -> Result<SignedAgentToken, CoreError> {
        let creds = AgentCredentials::new(agent_id, agent_name, validity_secs);
        let token = SignedAgentToken::issue(creds, &self.broker_signing_key)?;
        Ok(token)
    }

    /// Issue a scoped identity token with fine-grained RBAC scopes and tenant isolation
    pub fn issue_scoped_agent_token(
        &self,
        agent_id: &str,
        agent_name: &str,
        validity_secs: i64,
        scopes: Vec<String>,
        tenant_id: Option<String>,
    ) -> Result<SignedAgentToken, CoreError> {
        let mut creds = AgentCredentials::new(agent_id, agent_name, validity_secs);
        creds.scopes = scopes;
        creds.tenant_id = tenant_id;
        let token = SignedAgentToken::issue(creds, &self.broker_signing_key)?;
        Ok(token)
    }

    // Backwards compatibility aliases
    pub fn issue_client_token(
        &self,
        client_id: &str,
        client_name: &str,
        validity_secs: i64,
    ) -> Result<SignedAgentToken, CoreError> {
        self.issue_agent_token(client_id, client_name, validity_secs)
    }

    pub fn issue_citizen_id(
        &self,
        citizen_id: &str,
        full_name: &str,
        validity_secs: i64,
    ) -> Result<SignedAgentToken, CoreError> {
        self.issue_agent_token(citizen_id, full_name, validity_secs)
    }

    /// Broker service intake:
    /// 1. Statelessly verifies agent credential (sub-microsecond)
    /// 2. Checks idempotency
    /// 3. Appends transaction to immutable ledger
    /// 4. Generates claim-check Receipt and persists to Vault
    /// 5. Publishes to topic trie for async workers
    pub async fn submit_service_request(
        &self,
        request: ServiceRequest,
    ) -> Result<Receipt, CoreError> {
        // Step 1: Stateless verification of Agent ID using Broker Public Key
        let creds = request.agent_token.verify(&self.broker_verifying_key)?;
        let agent_id = creds.agent_id.clone();
        let req_id_str = request.request_id.to_string();

        // Step 2: Idempotency deduplication check
        if self.dedup_log.check_and_record(&req_id_str) {
            return Err(CoreError::DuplicateRequest(req_id_str));
        }

        // Step 2.1: Long-Lived Business Idempotency & Case Leasing (Phase 6)
        if let Some(ref bkey) = request.business_key {
            let existing_id = {
                let locks = self.business_case_locks.read().unwrap();
                locks.get(bkey).copied()
            };
            if let Some(existing_id) = existing_id {
                let is_locked = {
                    let receipts = self.receipts.read().unwrap();
                    receipts.get(&existing_id).is_some_and(|existing_receipt| {
                        matches!(
                            existing_receipt.status,
                            ReceiptStatus::Queued
                                | ReceiptStatus::Processing
                                | ReceiptStatus::ParkedAwaitingInput { .. }
                        )
                    })
                };
                if is_locked {
                    return Err(CoreError::BusinessCaseLocked(bkey.clone(), existing_id));
                }
            }
            let mut locks = self.business_case_locks.write().unwrap();
            locks.insert(bkey.clone(), request.request_id);
        }

        // Step 3: Serialize and append transaction to Ledger
        let payload_bytes = serde_json::to_vec(&request)?;
        let topic = format!("service.{}", request.service_name);

        let offset = {
            let mut l = self.ledger.lock().await;
            l.append(&topic, &req_id_str, &payload_bytes)?
        };

        // Step 4: Issue the Receipt (Claim-Check Token) with dynamic ETA accounting for active service backoff
        let estimated_ready_secs =
            if let Some(backoff) = self.get_service_backoff(&request.service_name) {
                let now = Utc::now().timestamp();
                if backoff.backed_off_until > now {
                    Some(5 + (backoff.backed_off_until - now) as u64)
                } else {
                    Some(5)
                }
            } else {
                Some(5)
            };

        let partition_id = self.calculate_partition(&request);
        let mut receipt = Receipt::from_request(&request, estimated_ready_secs);
        receipt.partition_id = Some(partition_id);

        {
            let mut r = self.receipts.write().unwrap();
            r.insert(receipt.receipt_id, receipt.clone());
        }
        self.pending_requests
            .write()
            .unwrap()
            .insert(receipt.receipt_id, request.clone());

        // Persist receipt and request to disk vault
        let receipt_path = self
            .vault_dir
            .join(format!("{}.receipt.json", receipt.receipt_id));
        if let Ok(data) = serde_json::to_string_pretty(&receipt) {
            let _ = std::fs::write(receipt_path, data);
        }
        let request_path = self
            .vault_dir
            .join(format!("{}.request.json", receipt.receipt_id));
        if let Ok(data) = serde_json::to_string_pretty(&request) {
            let _ = std::fs::write(request_path, data);
        }

        // Step 5: Notify workers through topic routing trie
        self.topic_trie.publish(&topic, offset);

        tracing::info!(
            "Accepted request {} from agent {} on topic {} at ledger offset {}",
            req_id_str,
            agent_id,
            topic,
            offset
        );

        Ok(receipt)
    }

    /// Worker completion step:
    /// 1. Records final outcome in memory and disk vault
    /// 2. Updates Receipt status to ReadyForPickup on disk and memory
    /// 3. Dispatches an AgentSignal over SSE broadcast
    /// 4. Triggers active Webhook Callback if configured on the receipt
    pub fn complete_service(&self, outcome: ServiceOutcome) -> Result<(), CoreError> {
        let receipt_id = outcome.receipt_id;
        let agent_id = outcome.agent_id.clone();
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

        let (
            callback_url,
            tenant_id,
            session_id,
            business_key,
            traceparent,
            tracestate,
            partition_id,
        ) = {
            let mut receipts = self.receipts.write().unwrap();
            if let Some(r) = receipts.get_mut(&receipt_id) {
                r.status = ReceiptStatus::ReadyForPickup;
                let receipt_path = self.vault_dir.join(format!("{}.receipt.json", receipt_id));
                if let Ok(data) = serde_json::to_string_pretty(r) {
                    let _ = std::fs::write(receipt_path, data);
                }
                (
                    r.callback_url.clone(),
                    r.tenant_id.clone(),
                    r.session_id.clone(),
                    r.business_key.clone(),
                    r.traceparent.clone(),
                    r.tracestate.clone(),
                    r.partition_id,
                )
            } else {
                (None, None, None, None, None, None, None)
            }
        };

        // Release business case lock upon completion (Phase 6)
        if let Some(ref bkey) = business_key {
            let mut locks = self.business_case_locks.write().unwrap();
            locks.remove(bkey);
        }
        self.pending_requests.write().unwrap().remove(&receipt_id);
        let _ = std::fs::remove_file(self.vault_dir.join(format!("{}.request.json", receipt_id)));

        // Emit AgentSignal over the notification bus (SSE)
        let signal = AgentSignal {
            signal_id: Uuid::now_v7(),
            receipt_id,
            agent_id: agent_id.clone(),
            service_name,
            status: ReceiptStatus::ReadyForPickup,
            message: format!(
                "Service outcome for receipt #{} is ready in the Vault",
                receipt_id
            ),
            pickup_url: format!("/api/v1/receipts/{}/pickup", receipt_id),
            timestamp: Utc::now().timestamp(),
            tenant_id,
            session_id,
            business_key,
            traceparent,
            tracestate,
            partition_id,
        };

        let _ = self.signal_bus.send(signal.clone());

        // Trigger Active Webhook Callback if present
        if let Some(url) = callback_url {
            let client = self.http_client.clone();
            tokio::spawn(async move {
                tracing::info!("Dispatching webhook callback to {}", url);
                match client.post(&url).json(&signal).send().await {
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
            "Service outcome recorded for receipt {}. Signal dispatched for agent {}",
            receipt_id,
            agent_id
        );

        Ok(())
    }

    /// Agent presents receipt token to collect their outcome from the Vault
    pub fn pickup_outcome(
        &self,
        receipt_id: Uuid,
        token: &SignedAgentToken,
    ) -> Result<ServiceOutcome, CoreError> {
        let creds = token.verify(&self.broker_verifying_key)?;

        let receipts = self.receipts.read().unwrap();
        let receipt = receipts
            .get(&receipt_id)
            .ok_or(CoreError::ReceiptNotFound(receipt_id))?;

        if receipt.agent_id != creds.agent_id {
            return Err(CoreError::AgentMismatch);
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

    /// Worker parks a task awaiting interactive human or agent clarification (HITL).
    /// Generates and returns an Ed25519-signed ResumeToken containing session context.
    pub fn park_for_input(
        &self,
        receipt_id: Uuid,
        question: String,
        schema_for_answer: Option<serde_json::Value>,
        checkpoint_uri: Option<String>,
        ttl_secs: i64,
    ) -> Result<SignedResumeToken, CoreError> {
        let mut receipts = self.receipts.write().unwrap();
        let receipt = receipts
            .get_mut(&receipt_id)
            .ok_or(CoreError::ReceiptNotFound(receipt_id))?;

        receipt.status = ReceiptStatus::ParkedAwaitingInput {
            question: question.clone(),
            schema_for_answer: schema_for_answer.clone(),
        };

        // Persist updated status to disk vault
        let receipt_path = self.vault_dir.join(format!("{}.receipt.json", receipt_id));
        if let Ok(data) = serde_json::to_string_pretty(&*receipt) {
            let _ = std::fs::write(receipt_path, data);
        }

        let agent_id = receipt.agent_id.clone();
        let service_name = receipt.service_name.clone();
        let session_id = format!("sess-{}", receipt_id);

        let resume_payload = ResumePayload {
            session_id: session_id.clone(),
            turn_id: 1,
            receipt_id,
            question: question.clone(),
            schema_for_answer,
            checkpoint_uri,
            expires_at: Utc::now().timestamp() + ttl_secs,
        };

        let resume_token = SignedResumeToken::issue(resume_payload, &self.broker_signing_key)?;

        // Emit AgentSignal alerting the client/UI that user input is needed
        let signal = AgentSignal {
            signal_id: Uuid::now_v7(),
            receipt_id,
            agent_id: agent_id.clone(),
            service_name,
            status: receipt.status.clone(),
            message: format!("Interactive input required: {}", question),
            pickup_url: format!("/api/v1/sessions/{}/resume", session_id),
            timestamp: Utc::now().timestamp(),
            tenant_id: receipt.tenant_id.clone(),
            session_id: Some(session_id.clone()),
            business_key: receipt.business_key.clone(),
            traceparent: receipt.traceparent.clone(),
            tracestate: receipt.tracestate.clone(),
            partition_id: receipt.partition_id,
        };

        let _ = self.signal_bus.send(signal);

        tracing::info!(
            "Task for receipt {} parked awaiting input. ResumeToken issued for session {}",
            receipt_id,
            session_id
        );

        Ok(resume_token)
    }

    /// User/client supplies answer along with SignedResumeToken to continue execution.
    /// Resumption is stateless and verifiable by any worker without sticky routing.
    pub fn resume_session(&self, req: SessionResumeRequest) -> Result<Receipt, CoreError> {
        // 1. Stateless verification of agent token
        let creds = req.agent_token.verify(&self.broker_verifying_key)?;

        // 2. Stateless verification of resumption token
        let resume_payload = req.resume_token.verify(&self.broker_verifying_key)?;
        let receipt_id = resume_payload.receipt_id;

        let mut receipts = self.receipts.write().unwrap();
        let receipt = receipts
            .get_mut(&receipt_id)
            .ok_or(CoreError::ReceiptNotFound(receipt_id))?;

        if receipt.agent_id != creds.agent_id {
            return Err(CoreError::AgentMismatch);
        }

        // Verify task was actually parked
        if !matches!(receipt.status, ReceiptStatus::ParkedAwaitingInput { .. }) {
            return Err(CoreError::NotParked(receipt_id));
        }

        // Transition back to Processing
        receipt.status = ReceiptStatus::Processing;
        let receipt_path = self.vault_dir.join(format!("{}.receipt.json", receipt_id));
        if let Ok(data) = serde_json::to_string_pretty(&*receipt) {
            let _ = std::fs::write(receipt_path, data);
        }

        let service_name = receipt.service_name.clone();
        let agent_id = receipt.agent_id.clone();
        let topic = format!("service.{}.resumed", service_name);

        // Notify workers via topic trie
        self.topic_trie.publish(&topic, 0);

        // Emit AgentSignal over SSE
        let signal = AgentSignal {
            signal_id: Uuid::now_v7(),
            receipt_id,
            agent_id: agent_id.clone(),
            service_name,
            status: ReceiptStatus::Processing,
            message: format!(
                "Session {} resumed with user input; processing continued",
                resume_payload.session_id
            ),
            pickup_url: format!("/api/v1/receipts/{}/pickup", receipt_id),
            timestamp: Utc::now().timestamp(),
            tenant_id: receipt.tenant_id.clone(),
            session_id: Some(resume_payload.session_id.clone()),
            business_key: receipt.business_key.clone(),
            traceparent: receipt.traceparent.clone(),
            tracestate: receipt.tracestate.clone(),
            partition_id: receipt.partition_id,
        };

        let _ = self.signal_bus.send(signal);

        tracing::info!(
            "Session {} (receipt {}) successfully resumed. Answer recorded: {:?}",
            resume_payload.session_id,
            receipt_id,
            req.answer
        );

        Ok(receipt.clone())
    }

    /// Cancel an in-flight receipt. Immediately updates receipt status to Failed/Cancelled,
    /// persists to disk vault, and emits a cancellation AgentSignal so running workers abort inference loops.
    pub fn cancel_receipt(
        &self,
        receipt_id: Uuid,
        token: &SignedAgentToken,
        reason: Option<String>,
    ) -> Result<Receipt, CoreError> {
        let creds = token.verify(&self.broker_verifying_key)?;

        let cancel_reason = reason.unwrap_or_else(|| "Client/Agent requested cancellation".into());
        let (updated_receipt, bkey_to_release) = {
            let mut receipts = self.receipts.write().unwrap();
            let receipt = receipts
                .get_mut(&receipt_id)
                .ok_or(CoreError::ReceiptNotFound(receipt_id))?;

            if receipt.agent_id != creds.agent_id {
                return Err(CoreError::AgentMismatch);
            }

            receipt.status = ReceiptStatus::Failed(format!("Cancelled: {}", cancel_reason));

            // Persist to disk vault
            let receipt_path = self.vault_dir.join(format!("{}.receipt.json", receipt_id));
            if let Ok(data) = serde_json::to_string_pretty(&*receipt) {
                let _ = std::fs::write(receipt_path, data);
            }

            (receipt.clone(), receipt.business_key.clone())
        };

        // Release business case lock upon cancellation (Phase 6)
        if let Some(ref bkey) = bkey_to_release {
            let mut locks = self.business_case_locks.write().unwrap();
            locks.remove(bkey);
        }
        self.pending_requests.write().unwrap().remove(&receipt_id);
        let _ = std::fs::remove_file(self.vault_dir.join(format!("{}.request.json", receipt_id)));

        let signal = AgentSignal {
            signal_id: Uuid::now_v7(),
            receipt_id,
            agent_id: creds.agent_id.clone(),
            service_name: updated_receipt.service_name.clone(),
            status: updated_receipt.status.clone(),
            message: format!("Task cancelled: {}", cancel_reason),
            pickup_url: format!("/api/v1/receipts/{}", receipt_id),
            timestamp: Utc::now().timestamp(),
            tenant_id: updated_receipt.tenant_id.clone(),
            session_id: updated_receipt.session_id.clone(),
            business_key: updated_receipt.business_key.clone(),
            traceparent: updated_receipt.traceparent.clone(),
            tracestate: updated_receipt.tracestate.clone(),
            partition_id: updated_receipt.partition_id,
        };

        let _ = self.signal_bus.send(signal);

        tracing::info!("Receipt {} cancelled: {}", receipt_id, cancel_reason);

        Ok(updated_receipt)
    }

    /// Dynamically trip or set backoff for a downstream service (e.g. slow legacy ERP, rate-limited API).
    /// Incoming requests are still accepted and held safely in the append-only ledger, while worker dispatch is held.
    pub fn set_service_backoff(
        &self,
        service_name: &str,
        duration_secs: u64,
        reason: String,
        concurrency_cap: Option<usize>,
    ) -> ServiceBackoff {
        let backed_off_until = Utc::now().timestamp() + duration_secs as i64;
        let backoff = ServiceBackoff {
            service_name: service_name.to_string(),
            backed_off_until,
            reason,
            concurrency_cap: concurrency_cap.unwrap_or(1),
        };

        {
            let mut backoffs = self.service_backoffs.write().unwrap();
            backoffs.insert(service_name.to_string(), backoff.clone());
        }

        tracing::warn!(
            "Service '{}' backoff active until timestamp {} (reason: '{}', concurrency_cap: {})",
            service_name,
            backed_off_until,
            backoff.reason,
            backoff.concurrency_cap
        );

        backoff
    }

    /// Unset backoff and resume steady-state dispatch for a downstream service.
    pub fn unset_service_backoff(&self, service_name: &str) -> bool {
        let mut backoffs = self.service_backoffs.write().unwrap();
        let was_backed_off = backoffs.remove(service_name).is_some();
        if was_backed_off {
            tracing::info!(
                "Service '{}' backoff cleared; steady-state dispatch resumed",
                service_name
            );
        }
        was_backed_off
    }

    /// Get current backoff state for a service (if any)
    pub fn get_service_backoff(&self, service_name: &str) -> Option<ServiceBackoff> {
        let backoffs = self.service_backoffs.read().unwrap();
        backoffs.get(service_name).cloned()
    }

    /// List all currently active service backoffs
    pub fn list_service_backoffs(&self) -> Vec<ServiceBackoff> {
        let backoffs = self.service_backoffs.read().unwrap();
        let now = Utc::now().timestamp();
        backoffs
            .values()
            .filter(|b| b.backed_off_until > now)
            .cloned()
            .collect()
    }

    /// Worker capacity lease allocation:
    /// Workers request batches up to their current capacity slot ceiling.
    /// Implements Multi-Tenant Fair Scheduling (Deficit Round-Robin) across tenants
    /// to prevent noisy neighbor starvation, while strictly respecting active service backoffs.
    pub fn lease_tasks(
        &self,
        req: WorkerLeaseRequest,
        lease_ttl_secs: i64,
    ) -> Result<WorkerLeaseResponse, CoreError> {
        let receipts = self.receipts.read().unwrap();
        let mut tenant_queues: HashMap<String, Vec<Uuid>> = HashMap::new();

        for (id, r) in receipts.iter() {
            if r.status == ReceiptStatus::Queued {
                if req
                    .service_filter
                    .as_deref()
                    .is_some_and(|f| f != r.service_name)
                {
                    continue;
                }
                // Honor active service backoff!
                if self
                    .get_service_backoff(&r.service_name)
                    .is_some_and(|b| b.backed_off_until > Utc::now().timestamp())
                {
                    continue;
                }
                let tenant = r.tenant_id.clone().unwrap_or_else(|| "default".into());
                tenant_queues.entry(tenant).or_default().push(*id);
            }
        }
        drop(receipts);

        // Fair interleave across tenant queues
        let mut eligible_ids = Vec::new();
        let mut active = true;
        while active && eligible_ids.len() < req.max_tasks {
            active = false;
            for queue in tenant_queues.values_mut() {
                if let Some(id) = queue.pop() {
                    eligible_ids.push(id);
                    active = true;
                    if eligible_ids.len() >= req.max_tasks {
                        break;
                    }
                }
            }
        }

        // Transition leased tasks to Processing
        let mut receipts = self.receipts.write().unwrap();
        for id in &eligible_ids {
            if let Some(r) = receipts.get_mut(id) {
                r.status = ReceiptStatus::Processing;
                let receipt_path = self.vault_dir.join(format!("{}.receipt.json", id));
                if let Ok(data) = serde_json::to_string_pretty(r) {
                    let _ = std::fs::write(receipt_path, data);
                }
            }
        }
        drop(receipts);

        let lease_id = Uuid::now_v7();
        let now = Utc::now().timestamp();
        let expires_at = now + lease_ttl_secs;

        let active_lease = ActiveWorkerLease {
            lease_id,
            worker_id: req.worker_id.clone(),
            service_filter: req.service_filter.clone(),
            task_ids: eligible_ids.clone(),
            granted_at: now,
            expires_at,
        };

        {
            let mut leases = self.worker_leases.write().unwrap();
            leases.insert(lease_id, active_lease);
        }

        let mut granted_tasks = Vec::new();
        {
            let reqs = self.pending_requests.read().unwrap();
            for id in &eligible_ids {
                if let Some(r) = reqs.get(id) {
                    granted_tasks.push(r.clone());
                }
            }
        }

        Ok(WorkerLeaseResponse {
            lease_id,
            worker_id: req.worker_id,
            granted_tasks,
            lease_expires_at: expires_at,
        })
    }

    /// Worker heartbeat keepalive: extends lease expiration
    pub fn worker_heartbeat(&self, lease_id: Uuid, extend_secs: i64) -> Result<(), CoreError> {
        let mut leases = self.worker_leases.write().unwrap();
        if let Some(l) = leases.get_mut(&lease_id) {
            l.expires_at = Utc::now().timestamp() + extend_secs;
            Ok(())
        } else {
            Err(CoreError::ReceiptNotFound(lease_id))
        }
    }

    /// Reclaims tasks from expired worker leases.
    /// If a task has failed/crashed 3 or more times (poison pill or persistent worker crash),
    /// it is diverted to the Dead Letter Queue (DLQ) with a permanent failure notice.
    pub fn reclaim_expired_leases(&self) -> Vec<Uuid> {
        let now = Utc::now().timestamp();
        let mut expired_leases = Vec::new();

        {
            let mut leases = self.worker_leases.write().unwrap();
            let expired_keys: Vec<Uuid> = leases
                .iter()
                .filter(|(_, l)| l.expires_at <= now)
                .map(|(k, _)| *k)
                .collect();

            for k in expired_keys {
                if let Some(l) = leases.remove(&k) {
                    expired_leases.push(l);
                }
            }
        }

        let mut reclaimed_tasks = Vec::new();
        let mut bkeys_to_release = Vec::new();
        let mut dead_lettered_ids = Vec::new();
        let mut signals_to_send = Vec::new();

        {
            let mut receipts = self.receipts.write().unwrap();
            let mut dlq = self.dlq.write().unwrap();

            for lease in expired_leases {
                for task_id in lease.task_ids {
                    if let Some(receipt) = receipts.get_mut(&task_id)
                        && receipt.status == ReceiptStatus::Processing
                    {
                        receipt.delivery_attempts += 1;
                        if receipt.delivery_attempts >= 3 {
                            let fail_msg = format!(
                                "Exceeded max delivery attempts ({}). Diverted to Dead Letter Queue (DLQ).",
                                receipt.delivery_attempts
                            );
                            receipt.status = ReceiptStatus::Failed(fail_msg.clone());
                            dlq.push(receipt.clone());
                            dead_lettered_ids.push(task_id);

                            if let Some(ref bkey) = receipt.business_key {
                                bkeys_to_release.push(bkey.clone());
                            }

                            signals_to_send.push(AgentSignal {
                                signal_id: Uuid::now_v7(),
                                receipt_id: task_id,
                                agent_id: receipt.agent_id.clone(),
                                service_name: receipt.service_name.clone(),
                                status: receipt.status.clone(),
                                message: fail_msg,
                                pickup_url: format!("/api/v1/receipts/{}", task_id),
                                timestamp: now,
                                tenant_id: receipt.tenant_id.clone(),
                                session_id: receipt.session_id.clone(),
                                business_key: receipt.business_key.clone(),
                                traceparent: receipt.traceparent.clone(),
                                tracestate: receipt.tracestate.clone(),
                                partition_id: receipt.partition_id,
                            });
                        } else {
                            receipt.status = ReceiptStatus::Queued;
                            reclaimed_tasks.push(task_id);
                        }

                        let receipt_path = self.vault_dir.join(format!("{}.receipt.json", task_id));
                        if let Ok(data) = serde_json::to_string_pretty(&*receipt) {
                            let _ = std::fs::write(receipt_path, data);
                        }
                    }
                }
            }
        }

        // Release business keys without holding receipts lock
        if !bkeys_to_release.is_empty() {
            let mut locks = self.business_case_locks.write().unwrap();
            for bkey in bkeys_to_release {
                locks.remove(&bkey);
            }
        }

        // Clean up pending requests and request files for dead-lettered tasks
        if !dead_lettered_ids.is_empty() {
            let mut pending = self.pending_requests.write().unwrap();
            for task_id in dead_lettered_ids {
                pending.remove(&task_id);
                let _ =
                    std::fs::remove_file(self.vault_dir.join(format!("{}.request.json", task_id)));
            }
        }

        for signal in signals_to_send {
            let _ = self.signal_bus.send(signal);
        }

        reclaimed_tasks
    }

    /// List all tasks in the Dead Letter Queue
    pub fn list_dlq(&self) -> Vec<Receipt> {
        self.dlq.read().unwrap().clone()
    }

    /// Get current status of a receipt
    pub fn get_receipt(&self, receipt_id: Uuid) -> Option<Receipt> {
        self.receipts.read().unwrap().get(&receipt_id).cloned()
    }

    /// List all receipts belonging to an agent
    pub fn list_agent_receipts(&self, agent_id: &str) -> Vec<Receipt> {
        self.receipts
            .read()
            .unwrap()
            .values()
            .filter(|r| r.agent_id == agent_id)
            .cloned()
            .collect()
    }

    // Backwards compatibility aliases
    pub fn list_client_receipts(&self, client_id: &str) -> Vec<Receipt> {
        self.list_agent_receipts(client_id)
    }

    pub fn list_citizen_receipts(&self, citizen_id: &str) -> Vec<Receipt> {
        self.list_agent_receipts(citizen_id)
    }

    /// Subscribe to live signals (used by SSE server stream)
    pub fn subscribe_signals(&self) -> broadcast::Receiver<AgentSignal> {
        self.signal_bus.subscribe()
    }

    // Backwards compatibility aliases
    pub fn subscribe_notifications(&self) -> broadcast::Receiver<AgentSignal> {
        self.subscribe_signals()
    }

    pub fn subscribe_intimations(&self) -> broadcast::Receiver<AgentSignal> {
        self.subscribe_signals()
    }

    // --- Phase 3: Multi-Broker Partition Clustering ---

    /// Calculate virtual partition for a request
    pub fn calculate_partition(&self, req: &ServiceRequest) -> u32 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        if let Some(ref tid) = req.tenant_id {
            tid.hash(&mut hasher);
        } else if let Some(ref turn) = req.session_turn {
            turn.session_id.hash(&mut hasher);
        } else if let Some(ref bkey) = req.business_key {
            bkey.hash(&mut hasher);
        } else {
            req.agent_token.credentials.agent_id.hash(&mut hasher);
        }
        (hasher.finish() % (self.num_partitions as u64)) as u32
    }

    /// Register or heartbeat a peer broker node in the cluster
    pub fn register_or_heartbeat_peer(&self, peer: PeerHeartbeatRequest) -> ClusterNodeInfo {
        let now = Utc::now().timestamp();
        let info = ClusterNodeInfo {
            node_id: peer.node_id.clone(),
            listen_addr: peer.listen_addr,
            is_leader: peer.is_leader,
            assigned_partitions: peer.assigned_partitions,
            last_heartbeat: now,
        };
        let mut nodes = self.cluster_nodes.write().unwrap();
        nodes.insert(peer.node_id, info.clone());
        info
    }

    /// Retrieve full cluster topology
    pub fn get_cluster_topology(&self) -> ClusterTopologyResponse {
        let nodes = self.cluster_nodes.read().unwrap();
        let list: Vec<ClusterNodeInfo> = nodes.values().cloned().collect();
        let leader_id = list.iter().find(|n| n.is_leader).map(|n| n.node_id.clone());

        ClusterTopologyResponse {
            cluster_id: "roviers-primary-cluster".into(),
            leader_node_id: leader_id.or_else(|| {
                if self.is_leader {
                    Some(self.node_id.clone())
                } else {
                    None
                }
            }),
            total_partitions: self.num_partitions,
            nodes: list,
        }
    }

    // --- Phase 4: Multi-Tenant Scoped Security ---

    /// Verify agent token has required scope (supports wildcard "service:all" or specific scope)
    pub fn verify_scope(
        &self,
        token: &SignedAgentToken,
        required_scope: &str,
    ) -> Result<(), CoreError> {
        let creds = token.verify(&self.broker_verifying_key)?;
        if creds
            .scopes
            .iter()
            .any(|s| s == "service:all" || s == "*" || s == required_scope)
        {
            Ok(())
        } else {
            Err(CoreError::ScopeForbidden(required_scope.to_string()))
        }
    }

    // --- Phase 6: Long-Lived Business Idempotency & Case Leasing ---

    /// Inspect the active receipt holding a business case lock
    pub fn get_business_case(&self, business_key: &str) -> Option<Receipt> {
        let locks = self.business_case_locks.read().unwrap();
        let receipt_id = locks.get(business_key)?;
        let receipts = self.receipts.read().unwrap();
        receipts.get(receipt_id).cloned()
    }

    /// Explicitly release a business case lock
    pub fn release_business_case(&self, business_key: &str) -> bool {
        let mut locks = self.business_case_locks.write().unwrap();
        locks.remove(business_key).is_some()
    }

    // --- Phase 11: Vault Retention, Compaction & Lifecycle GC ---

    /// Purges terminal receipts (Fulfilled, Failed) and associated outcomes
    /// older than `max_age_secs` from in-memory index and disk storage.
    /// Returns the number of purged receipts.
    pub fn purge_expired_receipts(&self, max_age_secs: i64) -> usize {
        let now = chrono::Utc::now().timestamp();
        let cutoff = now.saturating_sub(max_age_secs);

        let mut to_purge = Vec::new();

        {
            let receipts = self.receipts.read().unwrap();
            for (id, r) in receipts.iter() {
                let is_terminal = matches!(
                    r.status,
                    ReceiptStatus::Fulfilled | ReceiptStatus::Failed(_)
                );
                if is_terminal && r.issued_at <= cutoff {
                    to_purge.push((*id, r.business_key.clone()));
                }
            }
        }

        if to_purge.is_empty() {
            return 0;
        }

        let mut purged_count = 0;
        {
            let mut receipts = self.receipts.write().unwrap();
            let mut outcomes = self.outcomes.write().unwrap();
            let mut pending = self.pending_requests.write().unwrap();

            for (id, _) in &to_purge {
                receipts.remove(id);
                outcomes.remove(id);
                pending.remove(id);

                // Remove files from vault disk
                let receipt_file = self.vault_dir.join(format!("{}.receipt.json", id));
                let outcome_file = self.vault_dir.join(format!("{}.outcome.json", id));
                let request_file = self.vault_dir.join(format!("{}.request.json", id));
                let _ = std::fs::remove_file(receipt_file);
                let _ = std::fs::remove_file(outcome_file);
                let _ = std::fs::remove_file(request_file);

                purged_count += 1;
            }
        }

        let mut locks = self.business_case_locks.write().unwrap();
        for (id, bkey) in to_purge {
            if let Some(ref k) = bkey
                && locks.get(k) == Some(&id)
            {
                locks.remove(k);
            }
        }

        purged_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_full_broker_engine_lifecycle() {
        let temp_dir = std::env::temp_dir().join(format!("roviers_test_{}", Uuid::now_v7()));
        let engine = RoviersEngine::new(&temp_dir).expect("Failed to init engine");

        // 1. Issue Agent Token
        let agent_token = engine
            .issue_agent_token("AGENT-TEST-42", "Autonomous Agent", 86400)
            .expect("Failed to issue token");

        assert_eq!(agent_token.credentials.agent_id, "AGENT-TEST-42");

        // 2. Subscribe to Signals (Agent opens real-time listener)
        let mut signal_rx = engine.subscribe_signals();

        // 3. Submit Service Request at the Ingress
        let request = ServiceRequest::new(
            "treasury.mint_bond",
            agent_token.clone(),
            serde_json::json!({ "denomination": 1000 }),
            None,
        );
        let req_id = request.request_id;

        let receipt = engine
            .submit_service_request(request)
            .await
            .expect("Failed to submit request");

        assert_eq!(receipt.receipt_id, req_id);
        assert_eq!(receipt.agent_id, "AGENT-TEST-42");
        assert_eq!(receipt.status, ReceiptStatus::Queued);

        // 4. Test Idempotency (Duplicate submission rejected immediately)
        let mut dup_request = ServiceRequest::new(
            "treasury.mint_bond",
            agent_token.clone(),
            serde_json::json!({ "denomination": 1000 }),
            None,
        );
        dup_request.request_id = req_id;
        let dup_err = engine.submit_service_request(dup_request).await;
        assert!(matches!(dup_err, Err(CoreError::DuplicateRequest(_))));

        // 5. Worker completes service asynchronously
        let outcome = ServiceOutcome {
            receipt_id: req_id,
            agent_id: "AGENT-TEST-42".into(),
            service_name: "treasury.mint_bond".into(),
            completed_at: Utc::now().timestamp(),
            result: serde_json::json!({ "bond_id": "BOND-9999", "status": "ISSUED" }),
            blob_ref: None,
        };

        engine
            .complete_service(outcome)
            .expect("Failed to complete service");

        // 6. Verify Signal was received over the broadcast bus
        let signal = signal_rx.recv().await.expect("Failed to receive signal");
        assert_eq!(signal.receipt_id, req_id);
        assert_eq!(signal.agent_id, "AGENT-TEST-42");
        assert_eq!(signal.status, ReceiptStatus::ReadyForPickup);

        // 7. Pickup outcome with Agent Token
        let final_outcome = engine
            .pickup_outcome(req_id, &agent_token)
            .expect("Pickup failed");

        assert_eq!(final_outcome.result["bond_id"], "BOND-9999");

        // Verify receipt status is now Fulfilled
        let updated_receipt = engine.get_receipt(req_id).unwrap();
        assert_eq!(updated_receipt.status, ReceiptStatus::Fulfilled);

        // 8. Attempting pickup with wrong agent ID fails
        let wrong_token = engine
            .issue_agent_token("AGENT-IMPOSTER", "Hostile Agent", 86400)
            .unwrap();
        let impostor_pickup = engine.pickup_outcome(req_id, &wrong_token);
        assert!(matches!(impostor_pickup, Err(CoreError::AgentMismatch)));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_vault_persistence_across_restarts() {
        let temp_dir = std::env::temp_dir().join(format!("roviers_vault_test_{}", Uuid::now_v7()));

        let req_id;
        let queued_req_id;
        let agent_token;

        // Session 1: Engine starts, creates key, accepts requests, completes one and leaves one queued
        {
            let engine = RoviersEngine::new(&temp_dir).expect("Failed to init engine");
            agent_token = engine
                .issue_agent_token("AGENT-PERSIST-01", "Ada Lovelace", 86400)
                .expect("Failed to issue token");

            let request = ServiceRequest::new(
                "computation.analytic_engine",
                agent_token.clone(),
                serde_json::json!({ "algorithm": "bernoulli_numbers" }),
                Some("http://127.0.0.1:9999/callback".into()),
            );
            req_id = request.request_id;

            engine
                .submit_service_request(request)
                .await
                .expect("Failed to submit");

            let queued_request = ServiceRequest::new(
                "computation.analytic_engine",
                agent_token.clone(),
                serde_json::json!({ "algorithm": "diffie_hellman", "key_size": 2048 }),
                None,
            );
            queued_req_id = queued_request.request_id;
            engine
                .submit_service_request(queued_request)
                .await
                .expect("Failed to submit queued request");

            let outcome = ServiceOutcome {
                receipt_id: req_id,
                agent_id: "AGENT-PERSIST-01".into(),
                service_name: "computation.analytic_engine".into(),
                completed_at: Utc::now().timestamp(),
                result: serde_json::json!({ "status": "COMPUTED", "result_val": 42 }),
                blob_ref: None,
            };
            engine
                .complete_service(outcome)
                .expect("Failed to complete");
        } // engine 1 dropped here!

        // Session 2: New engine instance opens the same data directory
        {
            let engine2 = RoviersEngine::new(&temp_dir).expect("Failed to re-open engine");

            // Key should have persisted, so token issued in Session 1 should still verify!
            let receipt = engine2
                .get_receipt(req_id)
                .expect("Receipt missing after restart");
            assert_eq!(receipt.status, ReceiptStatus::ReadyForPickup);
            assert_eq!(
                receipt.callback_url,
                Some("http://127.0.0.1:9999/callback".into())
            );

            // Outcome should have persisted and be retrievable from Vault with original token
            let outcome = engine2
                .pickup_outcome(req_id, &agent_token)
                .expect("Failed to pick up outcome from persistent vault");

            assert_eq!(outcome.result["result_val"], 42);

            let fulfilled_receipt = engine2.get_receipt(req_id).unwrap();
            assert_eq!(fulfilled_receipt.status, ReceiptStatus::Fulfilled);

            // Verify queued request persisted in pending_requests and can be leased with full payload!
            let lease_resp = engine2
                .lease_tasks(
                    WorkerLeaseRequest {
                        worker_id: "worker-recovery-test".into(),
                        service_filter: None,
                        max_tasks: 10,
                        available_tpm_budget: None,
                        active_inflight: 0,
                    },
                    30,
                )
                .expect("Failed to lease tasks after restart");

            assert_eq!(lease_resp.granted_tasks.len(), 1);
            let leased_task = &lease_resp.granted_tasks[0];
            assert_eq!(leased_task.request_id, queued_req_id);
            assert_eq!(leased_task.payload["algorithm"], "diffie_hellman");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_hitl_park_and_resume_lifecycle() {
        let temp_dir = std::env::temp_dir().join(format!("roviers_hitl_test_{}", Uuid::now_v7()));
        let engine = RoviersEngine::new(&temp_dir).expect("Failed to init engine");

        let agent_token = engine
            .issue_agent_token("AGENT-USER-1", "User Agent", 86400)
            .expect("Failed to issue token");

        let mut signal_rx = engine.subscribe_signals();

        // 1. Submit task
        let request = ServiceRequest::new(
            "quote.generate",
            agent_token.clone(),
            serde_json::json!({ "items": ["sku_1", "sku_2"] }),
            None,
        );
        let req_id = request.request_id;
        let receipt = engine
            .submit_service_request(request)
            .await
            .expect("Submit failed");
        assert_eq!(receipt.status, ReceiptStatus::Queued);

        // 2. Worker begins work, needs clarification, and parks for HITL input
        let resume_token = engine
            .park_for_input(
                req_id,
                "What is your tax jurisdiction?".into(),
                Some(serde_json::json!({ "type": "string", "enum": ["US-CA", "US-NY"] })),
                Some("vault://checkpoint/turn-1".into()),
                3600,
            )
            .expect("Park for input failed");

        // 3. Verify signal sent to user/client over SSE
        let park_signal = signal_rx
            .recv()
            .await
            .expect("Failed to receive park signal");
        assert_eq!(park_signal.receipt_id, req_id);
        assert!(matches!(
            park_signal.status,
            ReceiptStatus::ParkedAwaitingInput { .. }
        ));

        // 4. Verify receipt status is now ParkedAwaitingInput
        let parked_receipt = engine.get_receipt(req_id).unwrap();
        assert!(matches!(
            parked_receipt.status,
            ReceiptStatus::ParkedAwaitingInput { .. }
        ));

        // 5. User provides answer using ResumeToken
        let resume_request = SessionResumeRequest {
            resume_token,
            answer: serde_json::json!({ "jurisdiction": "US-CA" }),
            agent_token: agent_token.clone(),
        };

        let resumed_receipt = engine
            .resume_session(resume_request)
            .expect("Resume session failed");
        assert_eq!(resumed_receipt.status, ReceiptStatus::Processing);

        // 6. Verify resume signal was emitted over SSE
        let resume_signal = signal_rx
            .recv()
            .await
            .expect("Failed to receive resume signal");
        assert_eq!(resume_signal.receipt_id, req_id);
        assert_eq!(resume_signal.status, ReceiptStatus::Processing);

        // 7. Worker completes the resumed task
        let outcome = ServiceOutcome {
            receipt_id: req_id,
            agent_id: "AGENT-USER-1".into(),
            service_name: "quote.generate".into(),
            completed_at: Utc::now().timestamp(),
            result: serde_json::json!({ "total": 120.50, "tax": 10.50 }),
            blob_ref: None,
        };
        engine.complete_service(outcome).expect("Completion failed");

        let final_outcome = engine
            .pickup_outcome(req_id, &agent_token)
            .expect("Pickup failed");
        assert_eq!(final_outcome.result["total"], 120.50);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_service_backpressure_and_dynamic_backoff() {
        let temp_dir =
            std::env::temp_dir().join(format!("roviers_backoff_test_{}", Uuid::now_v7()));
        let engine = RoviersEngine::new(&temp_dir).expect("Failed to init engine");

        let agent_token = engine
            .issue_agent_token("AGENT-PROD-01", "Production Agent", 86400)
            .expect("Failed to issue token");

        let service = "legacy_erp.reconcile";

        // 1. Normal submission has default baseline ETA (5s)
        let req1 = ServiceRequest::new(
            service,
            agent_token.clone(),
            serde_json::json!({ "batch": 1 }),
            None,
        );
        let receipt1 = engine
            .submit_service_request(req1)
            .await
            .expect("Submit 1 failed");
        assert_eq!(receipt1.estimated_ready_secs, Some(5));

        // 2. Worker detects downstream 429/timeout and dynamically sets backoff (e.g. 60s)
        let backoff = engine.set_service_backoff(
            service,
            60,
            "ERP connection pool saturated; cooling down".into(),
            Some(2),
        );
        assert_eq!(backoff.service_name, service);
        assert_eq!(backoff.concurrency_cap, 2);

        // 3. New requests submitted during backoff are still accepted (shock absorber) with adjusted ETA
        let req2 = ServiceRequest::new(
            service,
            agent_token.clone(),
            serde_json::json!({ "batch": 2 }),
            None,
        );
        let receipt2 = engine
            .submit_service_request(req2)
            .await
            .expect("Submit 2 during backoff should succeed");
        assert_eq!(receipt2.status, ReceiptStatus::Queued);
        assert!(receipt2.estimated_ready_secs.unwrap() >= 60);

        // 4. Verify backoff lookup
        let active_backoffs = engine.list_service_backoffs();
        assert_eq!(active_backoffs.len(), 1);
        assert_eq!(active_backoffs[0].service_name, service);

        // 5. Unset backoff (downstream recovered)
        let cleared = engine.unset_service_backoff(service);
        assert!(cleared);
        assert!(engine.get_service_backoff(service).is_none());

        // 6. Next request returns to normal baseline ETA
        let req3 = ServiceRequest::new(
            service,
            agent_token.clone(),
            serde_json::json!({ "batch": 3 }),
            None,
        );
        let receipt3 = engine
            .submit_service_request(req3)
            .await
            .expect("Submit 3 failed");
        assert_eq!(receipt3.estimated_ready_secs, Some(5));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_cancellation_and_worker_leases() {
        let temp_dir = std::env::temp_dir().join(format!("roviers_cancel_test_{}", Uuid::now_v7()));
        let engine = RoviersEngine::new(&temp_dir).expect("Failed to init engine");

        let agent_token = engine
            .issue_agent_token("AGENT-CANCEL-01", "Cancel Test Agent", 86400)
            .expect("Failed to issue token");

        // 1. Submit task and cancel it in-flight
        let req = ServiceRequest::new(
            "ai.heavy_reasoning",
            agent_token.clone(),
            serde_json::json!({ "prompt": "long reasoning" }),
            None,
        );
        let req_id = req.request_id;
        let _ = engine.submit_service_request(req).await.unwrap();

        let cancelled_receipt = engine
            .cancel_receipt(req_id, &agent_token, Some("User closed tab".into()))
            .expect("Cancellation failed");
        assert!(matches!(cancelled_receipt.status, ReceiptStatus::Failed(_)));

        // 2. Submit 3 tasks and lease them via capacity leasing
        for i in 0..3 {
            let r = ServiceRequest::new(
                "batch.worker_job",
                agent_token.clone(),
                serde_json::json!({ "idx": i }),
                None,
            );
            let _ = engine.submit_service_request(r).await.unwrap();
        }

        let lease_req = WorkerLeaseRequest {
            worker_id: "WORKER-NODE-01".into(),
            service_filter: Some("batch.worker_job".into()),
            max_tasks: 2,
            available_tpm_budget: None,
            active_inflight: 0,
        };

        let lease_resp = engine.lease_tasks(lease_req, 45).expect("Lease failed");
        assert_eq!(lease_resp.worker_id, "WORKER-NODE-01");

        // Verify heartbeat extension
        let hb_res = engine.worker_heartbeat(lease_resp.lease_id, 60);
        assert!(hb_res.is_ok());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_two_tier_vault_and_dlq_reclaim() {
        let temp_dir = std::env::temp_dir().join(format!("roviers_dlq_test_{}", Uuid::now_v7()));
        let engine = RoviersEngine::new(&temp_dir).expect("Failed to init engine");

        let agent_token = engine
            .issue_agent_token("AGENT-ENTERPRISE-01", "Enterprise Agent", 86400)
            .expect("Failed to issue token");

        // 1. Two-Tier Vault: Large payload stored as BlobReference
        let blob_ref = roviers_proto::BlobReference {
            blob_url: "s3://roviers-vault-blobs/outcomes/heavy_quote_1001.pdf".into(),
            content_type: "application/pdf".into(),
            size_bytes: 4_500_000,
            sha256_hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
        };

        let req = ServiceRequest::new(
            "quote.pdf",
            agent_token.clone(),
            serde_json::json!({ "quote_id": 1001 }),
            None,
        );
        let req_id = req.request_id;
        let _ = engine.submit_service_request(req).await.unwrap();

        let blob_outcome = ServiceOutcome::new_blob(
            req_id,
            "AGENT-ENTERPRISE-01",
            "quote.pdf",
            serde_json::json!({ "summary": "4.5MB PDF quote generated" }),
            blob_ref.clone(),
        );

        engine
            .complete_service(blob_outcome)
            .expect("Complete blob service failed");

        let picked_up = engine
            .pickup_outcome(req_id, &agent_token)
            .expect("Pickup failed");
        assert!(picked_up.blob_ref.is_some());
        assert_eq!(picked_up.blob_ref.unwrap().size_bytes, 4_500_000);

        // 2. Dead Letter Queue & Expired Lease Reclaim
        // Submit a poison pill task
        let poison_req = ServiceRequest::new(
            "flaky.task",
            agent_token.clone(),
            serde_json::json!({ "flaky": true }),
            None,
        );
        let poison_id = poison_req.request_id;
        let _ = engine.submit_service_request(poison_req).await.unwrap();

        // Lease with 0-second TTL (immediately expired)
        let lease_req = WorkerLeaseRequest {
            worker_id: "CRASHING-WORKER-01".into(),
            service_filter: Some("flaky.task".into()),
            max_tasks: 1,
            available_tpm_budget: None,
            active_inflight: 0,
        };
        let _ = engine
            .lease_tasks(lease_req.clone(), -1)
            .expect("Lease failed");

        // Reclaim attempt 1: task is re-queued
        let reclaimed_1 = engine.reclaim_expired_leases();
        assert_eq!(reclaimed_1, vec![poison_id]);
        assert_eq!(
            engine.get_receipt(poison_id).unwrap().status,
            ReceiptStatus::Queued
        );

        // Re-lease and crash attempt 2
        let _ = engine
            .lease_tasks(lease_req.clone(), -1)
            .expect("Lease 2 failed");
        let reclaimed_2 = engine.reclaim_expired_leases();
        assert_eq!(reclaimed_2, vec![poison_id]);

        // Re-lease and crash attempt 3: max retries exceeded -> Diverts to DLQ!
        let _ = engine.lease_tasks(lease_req, -1).expect("Lease 3 failed");
        let reclaimed_3 = engine.reclaim_expired_leases();
        assert!(reclaimed_3.is_empty()); // Not re-queued; moved to DLQ

        let dlq_tasks = engine.list_dlq();
        assert_eq!(dlq_tasks.len(), 1);
        assert_eq!(dlq_tasks[0].receipt_id, poison_id);
        assert!(matches!(dlq_tasks[0].status, ReceiptStatus::Failed(_)));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_multi_tenant_fair_scheduling() {
        let temp_dir = std::env::temp_dir().join(format!("roviers_fair_test_{}", Uuid::now_v7()));
        let engine = RoviersEngine::new(&temp_dir).expect("Failed to init engine");

        let token_a = engine
            .issue_agent_token("AGENT-A", "Tenant A Agent", 86400)
            .unwrap();
        let token_b = engine
            .issue_agent_token("AGENT-B", "Tenant B Agent", 86400)
            .unwrap();

        // Tenant A submits 3 batch jobs
        for i in 0..3 {
            let req = ServiceRequest::new(
                "analytics.crunch",
                token_a.clone(),
                serde_json::json!({ "job": i }),
                None,
            )
            .with_tenant("tenant-A");
            let _ = engine.submit_service_request(req).await.unwrap();
        }

        // Tenant B submits 1 urgent interactive job
        let req_b = ServiceRequest::new(
            "analytics.crunch",
            token_b.clone(),
            serde_json::json!({ "urgent": true }),
            None,
        )
        .with_tenant("tenant-B");
        let b_id = req_b.request_id;
        let _ = engine.submit_service_request(req_b).await.unwrap();

        // Worker requests a batch of 2 tasks
        let lease_req = WorkerLeaseRequest {
            worker_id: "WORKER-FAIR-01".into(),
            service_filter: Some("analytics.crunch".into()),
            max_tasks: 2,
            available_tpm_budget: None,
            active_inflight: 0,
        };

        let lease = engine.lease_tasks(lease_req, 60).unwrap();
        let active_lease = engine
            .worker_leases
            .read()
            .unwrap()
            .get(&lease.lease_id)
            .unwrap()
            .clone();

        // Fair scheduling guarantees Tenant B is included in the very first batch of 2,
        // avoiding starvation behind Tenant A's batch!
        assert!(active_lease.task_ids.contains(&b_id));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_business_case_idempotency_locking() {
        let temp_dir =
            std::env::temp_dir().join(format!("roviers_case_lock_test_{}", Uuid::now_v7()));
        let engine = RoviersEngine::new(&temp_dir).expect("Failed to init engine");

        let token = engine
            .issue_agent_token("AGENT-CASE-01", "Case Worker", 86400)
            .unwrap();

        let business_key = "claim:CLM-2026-9999";

        // 1. Submit initial request with business_key
        let req1 = ServiceRequest::new(
            "claims.process",
            token.clone(),
            serde_json::json!({ "amount": 500 }),
            None,
        )
        .with_business_key(business_key);
        let req1_id = req1.request_id;

        let receipt1 = engine
            .submit_service_request(req1)
            .await
            .expect("Submit 1 failed");
        assert_eq!(receipt1.business_key, Some(business_key.to_string()));

        // Verify case lock is held
        let locked_receipt = engine.get_business_case(business_key);
        assert!(locked_receipt.is_some());
        assert_eq!(locked_receipt.unwrap().receipt_id, req1_id);

        // 2. Submit second request with the same business_key while first is active -> Should fail with BusinessCaseLocked
        let req2 = ServiceRequest::new(
            "claims.process",
            token.clone(),
            serde_json::json!({ "amount": 600 }),
            None,
        )
        .with_business_key(business_key);

        let err = engine.submit_service_request(req2).await;
        match err {
            Err(CoreError::BusinessCaseLocked(k, existing_id)) => {
                assert_eq!(k, business_key);
                assert_eq!(existing_id, req1_id);
            }
            other => panic!("Expected BusinessCaseLocked, got {:?}", other),
        }

        // 3. Complete first service -> Should release lock
        let outcome = ServiceOutcome {
            receipt_id: req1_id,
            agent_id: "AGENT-CASE-01".into(),
            service_name: "claims.process".into(),
            completed_at: Utc::now().timestamp(),
            result: serde_json::json!({ "status": "APPROVED" }),
            blob_ref: None,
        };
        engine.complete_service(outcome).expect("Completion failed");

        // Verify case lock is now freed
        assert!(engine.get_business_case(business_key).is_none());

        // 4. Submitting a new case under the same business_key should now succeed
        let req3 = ServiceRequest::new(
            "claims.process",
            token.clone(),
            serde_json::json!({ "amount": 700 }),
            None,
        )
        .with_business_key(business_key);
        let receipt3 = engine
            .submit_service_request(req3)
            .await
            .expect("Submit 3 failed");
        assert_eq!(receipt3.business_key, Some(business_key.to_string()));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_cluster_topology_and_partitioning() {
        let temp_dir =
            std::env::temp_dir().join(format!("roviers_cluster_test_{}", Uuid::now_v7()));
        let engine = RoviersEngine::new(&temp_dir).expect("Failed to init engine");

        // 1. Initial topology
        let topology = engine.get_cluster_topology();
        assert_eq!(topology.nodes.len(), 1);
        assert!(topology.nodes[0].is_leader);
        assert_eq!(topology.total_partitions, 16);

        // 2. Register peer node heartbeat
        let peer_hb = PeerHeartbeatRequest {
            node_id: "roviers-node-02".into(),
            listen_addr: "http://10.0.0.2:8080".into(),
            is_leader: false,
            assigned_partitions: vec![8, 9, 10, 11, 12, 13, 14, 15],
        };
        engine.register_or_heartbeat_peer(peer_hb);

        let updated_topology = engine.get_cluster_topology();
        assert_eq!(updated_topology.nodes.len(), 2);

        // 3. Test deterministic virtual partition calculation
        let token = engine
            .issue_agent_token("AGENT-PART-01", "Agent", 86400)
            .unwrap();
        let req = ServiceRequest::new("test.svc", token, serde_json::json!({}), None)
            .with_tenant("tenant-corp-x");
        let partition = engine.calculate_partition(&req);
        assert!(partition < 16);

        // Same request calculates same partition
        assert_eq!(engine.calculate_partition(&req), partition);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_scoped_tokens_and_security() {
        let temp_dir = std::env::temp_dir().join(format!("roviers_scope_test_{}", Uuid::now_v7()));
        let engine = RoviersEngine::new(&temp_dir).expect("Failed to init engine");

        // 1. Issue token with restricted scopes
        let token = engine
            .issue_scoped_agent_token(
                "AGENT-SCOPED-01",
                "Scoped Agent",
                86400,
                vec!["tasks:read".into(), "tasks:write".into()],
                Some("tenant-42".into()),
            )
            .expect("Failed to issue scoped token");

        assert_eq!(token.credentials.tenant_id, Some("tenant-42".into()));
        assert_eq!(
            token.credentials.scopes,
            vec!["tasks:read".to_string(), "tasks:write".to_string()]
        );

        // 2. Verify valid scope
        assert!(engine.verify_scope(&token, "tasks:read").is_ok());
        assert!(engine.verify_scope(&token, "tasks:write").is_ok());

        // 3. Verify unauthorized scope
        let scope_err = engine.verify_scope(&token, "admin:cluster");
        assert!(matches!(scope_err, Err(CoreError::ScopeForbidden(_))));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_vault_retention_and_compaction() {
        let temp_dir =
            std::env::temp_dir().join(format!("roviers_compaction_test_{}", Uuid::now_v7()));
        let engine = RoviersEngine::new(&temp_dir).expect("Failed to init engine");

        let token = engine
            .issue_agent_token("AGENT-GC", "GC Agent", 86400)
            .unwrap();

        // 1. Submit and fulfill an old task
        let req1 = ServiceRequest::new(
            "gc.test",
            token.clone(),
            serde_json::json!({ "idx": 1 }),
            None,
        );
        let id1 = req1.request_id;
        let mut r1 = engine.submit_service_request(req1).await.unwrap();
        // Artificially age the receipt issued_at by 100 seconds
        r1.issued_at -= 100;
        engine.receipts.write().unwrap().insert(id1, r1);

        let outcome1 = ServiceOutcome {
            receipt_id: id1,
            agent_id: "AGENT-GC".into(),
            service_name: "gc.test".into(),
            completed_at: chrono::Utc::now().timestamp() - 100,
            result: serde_json::json!({ "done": true }),
            blob_ref: None,
        };
        engine.complete_service(outcome1).unwrap();
        // Pick it up so status becomes Fulfilled
        let _ = engine.pickup_outcome(id1, &token).unwrap();

        // 2. Submit a fresh task (not yet fulfilled or aged)
        let req2 = ServiceRequest::new(
            "gc.test",
            token.clone(),
            serde_json::json!({ "idx": 2 }),
            None,
        );
        let id2 = req2.request_id;
        let _ = engine.submit_service_request(req2).await.unwrap();

        // 3. Purge terminal receipts older than 50 seconds
        let purged = engine.purge_expired_receipts(50);
        assert_eq!(purged, 1);

        // id1 should be purged from memory and disk
        assert!(engine.get_receipt(id1).is_none());
        assert!(
            !temp_dir
                .join("vault")
                .join(format!("{}.receipt.json", id1))
                .exists()
        );

        // id2 should remain intact
        assert!(engine.get_receipt(id2).is_some());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
