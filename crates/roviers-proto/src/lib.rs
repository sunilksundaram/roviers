use chrono::Utc;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum ProtoError {
    #[error("Invalid signature: {0}")]
    InvalidSignature(String),
    #[error("Citizen ID token has expired at {0}")]
    TokenExpired(i64),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Invalid key format: {0}")]
    KeyError(String),
}

/// The verified citizen identity payload
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CitizenCredentials {
    pub citizen_id: String,
    pub full_name: String,
    pub issued_at: i64,
    pub expires_at: i64,
    pub scopes: Vec<String>,
}

impl CitizenCredentials {
    pub fn new(citizen_id: impl Into<String>, full_name: impl Into<String>, validity_secs: i64) -> Self {
        let now = Utc::now().timestamp();
        Self {
            citizen_id: citizen_id.into(),
            full_name: full_name.into(),
            issued_at: now,
            expires_at: now + validity_secs,
            scopes: vec!["service:all".into()],
        }
    }

    pub fn is_expired(&self) -> bool {
        Utc::now().timestamp() > self.expires_at
    }
}

/// Cryptographically signed citizen token (The Digital ID Card)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedCitizenToken {
    pub credentials: CitizenCredentials,
    pub signature_bytes: Vec<u8>,
}

impl SignedCitizenToken {
    /// Issues a new signed citizen card using government authority signing key
    pub fn issue(credentials: CitizenCredentials, signing_key: &SigningKey) -> Result<Self, ProtoError> {
        let payload_bytes = serde_json::to_vec(&credentials)?;
        let signature = signing_key.sign(&payload_bytes);
        Ok(Self {
            credentials,
            signature_bytes: signature.to_bytes().to_vec(),
        })
    }

    /// Statelessly verifies the citizen card against the government public key
    pub fn verify(&self, verifying_key: &VerifyingKey) -> Result<&CitizenCredentials, ProtoError> {
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

/// Citizen request presented at the service counter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceRequest {
    pub request_id: Uuid,
    pub service_name: String,
    pub citizen_token: SignedCitizenToken,
    pub payload: serde_json::Value,
    pub callback_url: Option<String>,
}

impl ServiceRequest {
    pub fn new(
        service_name: impl Into<String>,
        citizen_token: SignedCitizenToken,
        payload: serde_json::Value,
        callback_url: Option<String>,
    ) -> Self {
        Self {
            request_id: Uuid::now_v7(),
            service_name: service_name.into(),
            citizen_token,
            payload,
            callback_url,
        }
    }
}

/// Receipt (The Claim-Check token given to the citizen)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReceiptStatus {
    Queued,
    Processing,
    ReadyForPickup,
    Fulfilled,
    Failed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Receipt {
    pub receipt_id: Uuid,
    pub citizen_id: String,
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
            citizen_id: req.citizen_token.credentials.citizen_id.clone(),
            service_name: req.service_name.clone(),
            issued_at: Utc::now().timestamp(),
            status: ReceiptStatus::Queued,
            estimated_ready_secs,
            callback_url: req.callback_url.clone(),
        }
    }
}

/// The final outcome / output of the service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceOutcome {
    pub receipt_id: Uuid,
    pub citizen_id: String,
    pub service_name: String,
    pub completed_at: i64,
    pub result: serde_json::Value,
}

/// The intimation signal sent to notify the citizen to pick up their outcome
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntimationNotice {
    pub intimation_id: Uuid,
    pub receipt_id: Uuid,
    pub citizen_id: String,
    pub service_name: String,
    pub status: ReceiptStatus,
    pub message: String,
    pub pickup_url: String,
    pub timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::OsRng;

    #[test]
    fn test_citizen_id_lifecycle_and_verification() {
        let mut rng = OsRng;
        let signing_key = SigningKey::generate(&mut rng);
        let verifying_key = signing_key.verifying_key();

        // 1. Issue valid ID
        let creds = CitizenCredentials::new("CTZ-2026-999", "Sarah Connor", 3600);
        let token = SignedCitizenToken::issue(creds, &signing_key).expect("Token issue failed");

        // 2. Stateless verify
        let verified = token.verify(&verifying_key).expect("Verification failed");
        assert_eq!(verified.citizen_id, "CTZ-2026-999");
        assert_eq!(verified.full_name, "Sarah Connor");

        // 3. Verify with wrong key
        let wrong_key = SigningKey::generate(&mut rng).verifying_key();
        assert!(token.verify(&wrong_key).is_err());

        // 4. Test expired token
        let expired_creds = CitizenCredentials {
            citizen_id: "CTZ-EXPIRED".into(),
            full_name: "John Doe".into(),
            issued_at: Utc::now().timestamp() - 7200,
            expires_at: Utc::now().timestamp() - 3600,
            scopes: vec!["all".into()],
        };
        let expired_token =
            SignedCitizenToken::issue(expired_creds, &signing_key).expect("Token issue failed");
        match expired_token.verify(&verifying_key) {
            Err(ProtoError::TokenExpired(_)) => {}
            other => panic!("Expected TokenExpired, got {:?}", other),
        }
    }
}

