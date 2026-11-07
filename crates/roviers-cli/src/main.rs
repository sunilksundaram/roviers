use clap::{Parser, Subcommand};
use reqwest::Client;
use roviers_proto::{Receipt, ServiceOutcome, ServiceRequest, SignedCitizenToken};
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about = "Roviers Civic Protocol CLI")]
struct Cli {
    #[arg(short, long, default_value = "http://127.0.0.1:8080")]
    server: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Issue a new citizen ID card from the server
    IssueCitizen {
        #[arg(short, long)]
        id: String,
        #[arg(short, long)]
        name: String,
        #[arg(short, long, default_value = "365")]
        validity_days: i64,
        #[arg(short, long, default_value = "citizen_id.json")]
        out: PathBuf,
    },

    /// Submit a service request at the counter and receive a Receipt
    Submit {
        #[arg(short, long)]
        service: String,
        #[arg(short, long, default_value = "citizen_id.json")]
        token_file: PathBuf,
        #[arg(short, long, default_value = "{}")]
        data: String,
    },

    /// Check status of a receipt
    Status {
        #[arg(short, long)]
        receipt: Uuid,
    },

    /// Redeem receipt and collect final service outcome
    Pickup {
        #[arg(short, long)]
        receipt: Uuid,
        #[arg(short, long, default_value = "citizen_id.json")]
        token_file: PathBuf,
    },

    /// Listen for real-time intimation notifications via SSE stream (No polling!)
    Listen {
        #[arg(short, long)]
        citizen_id: String,
    },

    /// Run full automated end-to-end demonstration
    Demo,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client = Client::new();

    match cli.command {
        Commands::IssueCitizen {
            id,
            name,
            validity_days,
            out,
        } => {
            println!("🪪 Requesting new Citizen ID Card from Roviers Authority...");
            let res = client
                .post(format!("{}/api/v1/auth/citizen", cli.server))
                .json(&serde_json::json!({
                    "citizen_id": id,
                    "full_name": name,
                    "validity_days": validity_days
                }))
                .send()
                .await?;

            if !res.status().is_success() {
                eprintln!("❌ Failed to issue ID: {}", res.text().await?);
                return Ok(());
            }

            let token: SignedCitizenToken = res.json().await?;
            let json = serde_json::to_string_pretty(&token)?;
            fs::write(&out, json)?;
            println!("✅ Citizen ID Card issued and saved to {}", out.display());
            println!("   Citizen ID: {}", token.credentials.citizen_id);
            println!("   Holder Name: {}", token.credentials.full_name);
            println!("   Expires at (Unix): {}", token.credentials.expires_at);
        }

        Commands::Submit {
            service,
            token_file,
            data,
        } => {
            let token_str = fs::read_to_string(&token_file)?;
            let token: SignedCitizenToken = serde_json::from_str(&token_str)?;
            let payload: serde_json::Value = serde_json::from_str(&data)?;

            println!("🏛️ Visiting Roviers Service Counter for '{}'...", service);
            let req = ServiceRequest::new(service.clone(), token, payload, None);

            let res = client
                .post(format!("{}/api/v1/services/{}", cli.server, service))
                .json(&req)
                .send()
                .await?;

            if res.status().as_u16() != 202 {
                eprintln!("❌ Service submission rejected: {}", res.text().await?);
                return Ok(());
            }

            let receipt: Receipt = res.json().await?;
            println!("🎟️ Claim-Check Receipt Issued!");
            println!("   Receipt ID:   {}", receipt.receipt_id);
            println!("   Citizen ID:   {}", receipt.citizen_id);
            println!("   Service:      {}", receipt.service_name);
            println!("   Status:       {:?}", receipt.status);
            println!("   Est. Ready:   {:?} seconds", receipt.estimated_ready_secs);
            println!("\n💡 You can now disconnect. You will receive an Intimation when ready.");
        }

        Commands::Status { receipt } => {
            let res = client
                .get(format!("{}/api/v1/receipts/{}", cli.server, receipt))
                .send()
                .await?;

            if !res.status().is_success() {
                eprintln!("❌ Receipt not found: {}", res.text().await?);
                return Ok(());
            }

            let r: Receipt = res.json().await?;
            println!("📋 Receipt Status:");
            println!("   ID:     {}", r.receipt_id);
            println!("   Status: {:?}", r.status);
        }

        Commands::Pickup {
            receipt,
            token_file,
        } => {
            let token_str = fs::read_to_string(&token_file)?;
            let token: SignedCitizenToken = serde_json::from_str(&token_str)?;

            println!("📦 Presenting Receipt #{} at pickup counter...", receipt);
            let res = client
                .post(format!("{}/api/v1/receipts/{}/pickup", cli.server, receipt))
                .json(&serde_json::json!({
                    "citizen_token": token
                }))
                .send()
                .await?;

            if !res.status().is_success() {
                eprintln!("❌ Pickup failed: {}", res.text().await?);
                return Ok(());
            }

            let outcome: ServiceOutcome = res.json().await?;
            println!("🎉 Outcome Retrieved Successfully!");
            println!("   Completed At: {}", outcome.completed_at);
            println!(
                "   Outcome Result:\n{}",
                serde_json::to_string_pretty(&outcome.result)?
            );
        }

        Commands::Listen { citizen_id } => {
            println!("👂 Listening for real-time intimations for citizen '{}' (SSE)...", citizen_id);
            let mut res = client
                .get(format!("{}/api/v1/citizens/{}/stream", cli.server, citizen_id))
                .send()
                .await?;

            while let Some(chunk) = res.chunk().await? {
                let text = String::from_utf8_lossy(&chunk);
                if text.contains("data:") {
                    println!("🔔 INTIMATION RECEIVED:\n{}", text);
                }
            }
        }

        Commands::Demo => {
            println!("🚀 Running Full Civic Protocol Demonstration...\n");
            // 1. Issue ID
            let issue_res = client
                .post(format!("{}/api/v1/auth/citizen", cli.server))
                .json(&serde_json::json!({
                    "citizen_id": "CTZ-2026-DEMO-01",
                    "full_name": "Eleanor Vance",
                    "validity_days": 365
                }))
                .send()
                .await?;

            let token: SignedCitizenToken = issue_res.json().await?;
            println!("1️⃣ [CITIZEN ID] Issued verified ID for: {} ({})", token.credentials.full_name, token.credentials.citizen_id);

            // 2. Submit Request
            let req = ServiceRequest::new(
                "vital_records.birth_certificate",
                token.clone(),
                serde_json::json!({ "applicant_notes": "Official certified copy" }),
                None,
            );

            let submit_res = client
                .post(format!("{}/api/v1/services/vital_records.birth_certificate", cli.server))
                .json(&req)
                .send()
                .await?;

            let receipt: Receipt = submit_res.json().await?;
            println!("2️⃣ [RECEIPT TOKEN] Service counter accepted request!");
            println!("   Issued Receipt: #{} (Status: {:?})", receipt.receipt_id, receipt.status);
            println!("3️⃣ [ASYNC WORK] Worker pool processing in background... (Waiting for worker completion)");

            // Wait for worker completion (worker takes ~1.5s in simulation)
            tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

            // 4. Pickup outcome
            let pickup_res = client
                .post(format!("{}/api/v1/receipts/{}/pickup", cli.server, receipt.receipt_id))
                .json(&serde_json::json!({ "citizen_token": token }))
                .send()
                .await?;

            let outcome: ServiceOutcome = pickup_res.json().await?;
            println!("4️⃣ [OUTCOME PICKUP] Citizen presented receipt and collected final outcome:");
            println!("{}", serde_json::to_string_pretty(&outcome.result)?);
            println!("\n✨ Full end-to-end flow verified with zero polling and decoupled state!");
        }
    }

    Ok(())
}

