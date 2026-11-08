use chrono::Utc;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum ProtoError {
    #[error("Invalid signature: {0}")]
    InvalidSignature(String),
    #[error("Agent token has expired at {0}")]
    TokenExpired(i64),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Invalid key format: {0}")]
    KeyError(String),
    #[error("Resume token has expired at {0}")]
    ResumeTokenExpired(i64),
}

/// The verified agent identity payload
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentCredentials {
    pub agent_id: String,
    pub agent_name: String,
    pub issued_at: i64,
    pub expires_at: i64,
    pub scopes: Vec<String>,
}

impl AgentCredentials {
    pub fn new(
        agent_id: impl Into<String>,
        agent_name: impl Into<String>,
        validity_secs: i64,
    ) -> Self {
        let now = Utc::now().timestamp();
        Self {
            agent_id: agent_id.into(),
            agent_name: agent_name.into(),
            issued_at: now,
            expires_at: now + validity_secs,
            scopes: vec!["service:all".into()],
        }
    }

    pub fn is_expired(&self) -> bool {
        Utc::now().timestamp() > self.expires_at
    }
}

/// Cryptographically signed agent token (The Stateless Identity Credential)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedAgentToken {
    pub credentials: AgentCredentials,
    pub signature_bytes: Vec<u8>,
}

impl SignedAgentToken {
    /// Issues a new signed agent credential using broker signing key
    pub fn issue(
        credentials: AgentCredentials,
        signing_key: &SigningKey,
    ) -> Result<Self, ProtoError> {
        let payload_bytes = serde_json::to_vec(&credentials)?;
        let signature = signing_key.sign(&payload_bytes);
        Ok(Self {
            credentials,
            signature_bytes: signature.to_bytes().to_vec(),
        })
    }

    /// Statelessly verifies the agent credential against the broker public key
    pub fn verify(&self, verifying_key: &VerifyingKey) -> Result<&AgentCredentials, ProtoError> {
        let payload_bytes = serde_json::to_vec(&self.credentials)?;
        let sig_bytes: [u8; 64] = self
            .signature_bytes
            .as_slice()
            .try_into()
            .map_err(|_| ProtoError::InvalidSignature("Signature must be 64 bytes".into()))?;
        let signature = Signature::from_bytes(&sig_bytes);

        verifying_key
            .verify(&payload_bytes, &signature)
            .map_err(|e| ProtoError::InvalidSignature(e.to_string()))?;

        if self.credentials.is_expired() {
            return Err(ProtoError::TokenExpired(self.credentials.expires_at));
        }

        Ok(&self.credentials)
    }
}

/// Session turn correlation for multi-turn Human-In-The-Loop (HITL) workflows
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionTurn {
    pub session_id: String,
    pub turn_id: u64,
    pub parent_receipt_id: Option<Uuid>,
}

/// Service request presented to the Broker ingress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceRequest {
    pub request_id: Uuid,
    pub service_name: String,
    pub agent_token: SignedAgentToken,
    pub payload: serde_json::Value,
    pub callback_url: Option<String>,
    #[serde(default)]
    pub session_turn: Option<SessionTurn>,
}

impl ServiceRequest {
    pub fn new(
        service_name: impl Into<String>,
        agent_token: SignedAgentToken,
        payload: serde_json::Value,
        callback_url: Option<String>,
    ) -> Self {
        Self {
            request_id: Uuid::now_v7(),
            service_name: service_name.into(),
            agent_token,
            payload,
            callback_url,
            session_turn: None,
        }
    }

    pub fn with_session(mut self, session_id: impl Into<String>, turn_id: u64, parent: Option<Uuid>) -> Self {
        self.session_turn = Some(SessionTurn {
            session_id: session_id.into(),
            turn_id,
            parent_receipt_id: parent,
        });
        self
    }
}

/// Status of an issued receipt
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReceiptStatus {
    Queued,
    Processing,
    ParkedAwaitingInput {
        question: String,
        schema_for_answer: Option<serde_json::Value>,
    },
    ReadyForPickup,
    Fulfilled,
    Failed(String),
}

/// The verifiable cryptographic resumption token issued when an agent parks for HITL input
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResumePayload {
    pub session_id: String,
    pub turn_id: u64,
    pub receipt_id: Uuid,
    pub question: String,
    pub schema_for_answer: Option<serde_json::Value>,
    pub checkpoint_uri: Option<String>,
    pub expires_at: i64,
}

impl ResumePayload {
    pub fn is_expired(&self) -> bool {
        Utc::now().timestamp() > self.expires_at
    }
}

/// Cryptographically signed resume token (Stateless HITL continuation)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedResumeToken {
    pub payload: ResumePayload,
    pub signature_bytes: Vec<u8>,
}

impl SignedResumeToken {
    pub fn issue(payload: ResumePayload, signing_key: &SigningKey) -> Result<Self, ProtoError> {
        let payload_bytes = serde_json::to_vec(&payload)?;
        let signature = signing_key.sign(&payload_bytes);
        Ok(Self {
            payload,
            signature_bytes: signature.to_bytes().to_vec(),
        })
    }

    pub fn verify(&self, verifying_key: &VerifyingKey) -> Result<&ResumePayload, ProtoError> {
        let payload_bytes = serde_json::to_vec(&self.payload)?;
        let sig_bytes: [u8; 64] = self
            .signature_bytes
            .as_slice()
            .try_into()
            .map_err(|_| ProtoError::InvalidSignature("Signature must be 64 bytes".into()))?;
        let signature = Signature::from_bytes(&sig_bytes);

        verifying_key
            .verify(&payload_bytes, &signature)
            .map_err(|e| ProtoError::InvalidSignature(e.to_string()))?;

        if self.payload.is_expired() {
            return Err(ProtoError::ResumeTokenExpired(self.payload.expires_at));
        }

        Ok(&self.payload)
    }
}

/// Request to resume a parked HITL turn with user input
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionResumeRequest {
    pub resume_token: SignedResumeToken,
    pub answer: serde_json::Value,
    pub agent_token: SignedAgentToken,
}

/// Worker capacity lease request to prevent LLM quota stampedes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLeaseRequest {
    pub worker_id: String,
    pub service_filter: Option<String>,
    pub max_tasks: usize,
    pub available_tpm_budget: Option<u64>,
    pub active_inflight: usize,
}

/// Granted lease of tasks for a worker node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLeaseResponse {
    pub lease_id: Uuid,
    pub worker_id: String,
    pub granted_tasks: Vec<ServiceRequest>,
    pub lease_expires_at: i64,
}

/// Receipt (The Claim-Check token given to the agent)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Receipt {
    pub receipt_id: Uuid,
    pub agent_id: String,
    pub service_name: String,
    pub issued_at: i64,
    pub status: ReceiptStatus,
    pub estimated_ready_secs: Option<u64>,
    #[serde(default)]
    pub callback_url: Option<String>,
}

impl Receipt {
    pub fn from_request(req: &ServiceRequest, estimated_ready_secs: Option<u64>) -> Self {
        Self {
            receipt_id: req.request_id,
            agent_id: req.agent_token.credentials.agent_id.clone(),
            service_name: req.service_name.clone(),
            issued_at: Utc::now().timestamp(),
            status: ReceiptStatus::Queued,
            estimated_ready_secs,
            callback_url: req.callback_url.clone(),
        }
    }
}

/// The final outcome / output of the service stored in the Broker Vault
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceOutcome {
    pub receipt_id: Uuid,
    pub agent_id: String,
    pub service_name: String,
    pub completed_at: i64,
    pub result: serde_json::Value,
}

/// Notification signal pushed to the agent when their outcome is ready
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSignal {
    pub signal_id: Uuid,
    pub receipt_id: Uuid,
    pub agent_id: String,
    pub service_name: String,
    pub status: ReceiptStatus,
    pub message: String,
    pub pickup_url: String,
    pub timestamp: i64,
}

// Backwards-compatibility aliases
pub type CitizenCredentials = AgentCredentials;
pub type SignedCitizenToken = SignedAgentToken;
pub type IntimationNotice = AgentSignal;
pub type ClientCredentials = AgentCredentials;
pub type SignedClientToken = SignedAgentToken;
pub type NotificationNotice = AgentSignal;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::OsRng;

    #[test]
    fn test_agent_id_lifecycle_and_verification() {
        let mut rng = OsRng;
        let signing_key = SigningKey::generate(&mut rng);
        let verifying_key = signing_key.verifying_key();

        // 1. Issue valid agent credential
        let creds = AgentCredentials::new("AGENT-RESEARCH-01", "Research Agent", 3600);
        let token = SignedAgentToken::issue(creds, &signing_key).expect("Token issue failed");

        // 2. Stateless verify
        let verified = token.verify(&verifying_key).expect("Verification failed");
        assert_eq!(verified.agent_id, "AGENT-RESEARCH-01");
        assert_eq!(verified.agent_name, "Research Agent");

        // 3. Verify with wrong key
        let wrong_key = SigningKey::generate(&mut rng).verifying_key();
        assert!(token.verify(&wrong_key).is_err());

        // 4. Test expired token
        let expired_creds = AgentCredentials {
            agent_id: "AGENT-EXPIRED".into(),
            agent_name: "Expired Agent".into(),
            issued_at: Utc::now().timestamp() - 7200,
            expires_at: Utc::now().timestamp() - 3600,
            scopes: vec!["all".into()],
        };
        let expired_token =
            SignedAgentToken::issue(expired_creds, &signing_key).expect("Token issue failed");
        match expired_token.verify(&verifying_key) {
            Err(ProtoError::TokenExpired(_)) => {}
            other => panic!("Expected TokenExpired, got {:?}", other),
        }

        // 5. Test SignedResumeToken lifecycle
        let resume_payload = ResumePayload {
            session_id: "SESSION-99".into(),
            turn_id: 2,
            receipt_id: Uuid::now_v7(),
            question: "Please confirm invoice total?".into(),
            schema_for_answer: None,
            checkpoint_uri: Some("vault://checkpoints/SESSION-99/turn-1.json".into()),
            expires_at: Utc::now().timestamp() + 3600,
        };
        let resume_token = SignedResumeToken::issue(resume_payload.clone(), &signing_key)
            .expect("Failed to issue resume token");
        let verified_resume = resume_token.verify(&verifying_key).expect("Resume token verify failed");
        assert_eq!(verified_resume.session_id, "SESSION-99");
        assert_eq!(verified_resume.turn_id, 2);
    }
}
