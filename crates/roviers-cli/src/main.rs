use clap::{Parser, Subcommand};
use reqwest::Client;
use roviers_proto::{Receipt, ServiceOutcome, ServiceRequest, SignedAgentToken};
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about = "Roviers: Broker & Claim-Check Vault CLI")]
struct Cli {
    #[arg(short, long, default_value = "http://127.0.0.1:8080")]
    server: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Issue a new agent identity token from the Broker
    #[command(alias = "issue-citizen")]
    IssueAgent {
        #[arg(short, long)]
        id: String,
        #[arg(short, long)]
        name: String,
        #[arg(short, long, default_value = "365")]
        validity_days: i64,
        #[arg(short, long, default_value = "agent_id.json")]
        out: PathBuf,
    },

    /// Submit a service request to the Broker ingress and receive a Receipt
    Submit {
        #[arg(short, long)]
        service: String,
        #[arg(short, long, default_value = "agent_id.json")]
        token_file: PathBuf,
        #[arg(short, long, default_value = "{}")]
        data: String,
        #[arg(short, long)]
        callback: Option<String>,
    },

    /// Check status of an issued receipt
    Status {
        #[arg(short, long)]
        receipt: Uuid,
    },

    /// Redeem receipt and collect final service outcome from the Broker Vault
    Pickup {
        #[arg(short, long)]
        receipt: Uuid,
        #[arg(short, long, default_value = "agent_id.json")]
        token_file: PathBuf,
    },

    /// Worker command: Deposit final task outcome into the Vault for a receipt
    Complete {
        #[arg(short, long)]
        receipt: Uuid,
        #[arg(short, long, default_value = "{}")]
        data: String,
    },

    /// Listen for real-time completion signals via SSE stream (Zero polling!)
    Listen {
        #[arg(short, long, alias = "citizen-id")]
        agent_id: String,
    },

    /// Run full automated end-to-end demonstration
    Demo,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client = Client::new();

    match cli.command {
        Commands::IssueAgent {
            id,
            name,
            validity_days,
            out,
        } => {
            println!("🤖 Requesting new Agent Token from Roviers Broker...");
            let res = client
                .post(format!("{}/api/v1/auth/agent", cli.server))
                .json(&serde_json::json!({
                    "agent_id": id,
                    "agent_name": name,
                    "validity_days": validity_days
                }))
                .send()
                .await?;

            if !res.status().is_success() {
                eprintln!("❌ Failed to issue Agent Token: {}", res.text().await?);
                return Ok(());
            }

            let token: SignedAgentToken = res.json().await?;
            let json = serde_json::to_string_pretty(&token)?;
            fs::write(&out, json)?;
            println!("✅ Agent Token issued and saved to {}", out.display());
            println!("   Agent ID:   {}", token.credentials.agent_id);
            println!("   Agent Name: {}", token.credentials.agent_name);
            println!("   Expires at: {}", token.credentials.expires_at);
        }

        Commands::Submit {
            service,
            token_file,
            data,
            callback,
        } => {
            let token_str = fs::read_to_string(&token_file)?;
            let token: SignedAgentToken = serde_json::from_str(&token_str)?;
            let payload: serde_json::Value = serde_json::from_str(&data)?;

            println!("🏛️ Dispatching task to Roviers Broker for '{}'...", service);
            let req = ServiceRequest::new(service.clone(), token, payload, callback);

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
            println!("   Agent ID:     {}", receipt.agent_id);
            println!("   Service:      {}", receipt.service_name);
            println!("   Status:       {:?}", receipt.status);
            println!(
                "   Est. Ready:   {:?} seconds",
                receipt.estimated_ready_secs
            );
            println!(
                "\n💡 Connection freed. You will receive an SSE Signal or Webhook when ready."
            );
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
            println!("   Agent:  {}", r.agent_id);
            println!("   Status: {:?}", r.status);
        }

        Commands::Complete { receipt, data } => {
            let result_val: serde_json::Value =
                serde_json::from_str(&data).map_err(|e| format!("Invalid JSON data: {}", e))?;

            println!(
                "👷 Worker depositing outcome into Broker Vault for receipt #{}...",
                receipt
            );
            let res = client
                .post(format!(
                    "{}/api/v1/receipts/{}/complete",
                    cli.server, receipt
                ))
                .json(&serde_json::json!({ "result": result_val }))
                .send()
                .await?;

            if !res.status().is_success() {
                eprintln!("❌ Failed to complete receipt: {}", res.text().await?);
                return Ok(());
            }

            println!("✅ Service outcome recorded in Vault. Signals and webhooks dispatched.");
        }

        Commands::Pickup {
            receipt,
            token_file,
        } => {
            let token_str = fs::read_to_string(&token_file)?;
            let token: SignedAgentToken = serde_json::from_str(&token_str)?;

            println!("📦 Presenting Receipt #{} at Broker Vault...", receipt);
            let res = client
                .post(format!("{}/api/v1/receipts/{}/pickup", cli.server, receipt))
                .json(&serde_json::json!({
                    "agent_token": token
                }))
                .send()
                .await?;

            if !res.status().is_success() {
                eprintln!("❌ Pickup failed: {}", res.text().await?);
                return Ok(());
            }

            let outcome: ServiceOutcome = res.json().await?;
            println!("🎉 Outcome Retrieved from Vault Successfully!");
            println!("   Completed At: {}", outcome.completed_at);
            println!(
                "   Outcome Result:\n{}",
                serde_json::to_string_pretty(&outcome.result)?
            );
        }

        Commands::Listen { agent_id } => {
            println!(
                "👂 Listening for real-time signals for agent '{}' (SSE)...",
                agent_id
            );
            let mut res = client
                .get(format!("{}/api/v1/agents/{}/stream", cli.server, agent_id))
                .send()
                .await?;

            while let Some(chunk) = res.chunk().await? {
                let text = String::from_utf8_lossy(&chunk);
                if text.contains("data:") {
                    println!("🔔 SIGNAL RECEIVED:\n{}", text);
                }
            }
        }

        Commands::Demo => {
            println!("🚀 Running Full Broker & Agent Demonstration...\n");
            // 1. Issue Agent Token
            let issue_res = client
                .post(format!("{}/api/v1/auth/agent", cli.server))
                .json(&serde_json::json!({
                    "agent_id": "AGENT-2026-DEMO-01",
                    "agent_name": "Autonomous Research Agent",
                    "validity_days": 365
                }))
                .send()
                .await?;

            let token: SignedAgentToken = issue_res.json().await?;
            println!(
                "1️⃣ [AGENT TOKEN] Issued verified token for: {} ({})",
                token.credentials.agent_name, token.credentials.agent_id
            );

            // 2. Submit Request
            let req = ServiceRequest::new(
                "ai.document_analysis",
                token.clone(),
                serde_json::json!({ "document_id": "DOC-9912", "priority": "high" }),
                None,
            );

            let submit_res = client
                .post(format!(
                    "{}/api/v1/services/ai.document_analysis",
                    cli.server
                ))
                .json(&req)
                .send()
                .await?;

            let receipt: Receipt = submit_res.json().await?;
            println!("2️⃣ [RECEIPT TOKEN] Broker accepted request!");
            println!(
                "   Issued Receipt: #{} (Status: {:?})",
                receipt.receipt_id, receipt.status
            );
            println!(
                "3️⃣ [ASYNC WORK] Worker pool processing in background... (Waiting for worker completion)"
            );

            // Wait for worker completion (worker takes ~1.5s in simulation)
            tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

            // 4. Pickup outcome
            let pickup_res = client
                .post(format!(
                    "{}/api/v1/receipts/{}/pickup",
                    cli.server, receipt.receipt_id
                ))
                .json(&serde_json::json!({ "agent_token": token }))
                .send()
                .await?;

            let outcome: ServiceOutcome = pickup_res.json().await?;
            println!(
                "4️⃣ [OUTCOME PICKUP] Agent presented receipt and collected final outcome from Vault:"
            );
            println!("{}", serde_json::to_string_pretty(&outcome.result)?);
            println!(
                "\n✨ Full end-to-end flow verified: non-blocking Broker ingress, async execution, and Vault redemption!"
            );
        }
    }

    Ok(())
}
