export interface AgentCredentials {
  agent_id: string;
  agent_name: string;
  issued_at: number;
  expires_at: number;
  scopes: string[];
  tenant_id?: string;
}

export interface SignedAgentToken {
  credentials: AgentCredentials;
  signature_bytes: number[];
}

export interface RoviersReceipt {
  receipt_id: string;
  agent_id: string;
  service_name: string;
  issued_at: number;
  status: 'Queued' | 'Processing' | 'ReadyForPickup' | 'Fulfilled' | { Failed: string };
  estimated_ready_secs?: number;
  callback_url?: string;
  business_key?: string;
  session_id?: string;
  partition_id?: number;
  traceparent?: string;
  tracestate?: string;
}

export interface BlobReference {
  blob_url: string;
  content_type: string;
  size_bytes: number;
  sha256_hash: string;
}

export interface ResumePayload {
  session_id: string;
  receipt_id: string;
  agent_id: string;
  question: string;
  schema_for_answer?: any;
  checkpoint_uri?: string;
  expires_at: number;
}

export interface SignedResumeToken {
  payload: ResumePayload;
  signature_bytes: number[];
}

export interface ServiceBackoff {
  service_name: string;
  backed_off_until: number;
  reason: string;
  concurrency_cap: number;
}

export interface WorkerLeaseResponse {
  lease_id: string;
  worker_id: string;
  granted_tasks: any[];
  lease_expires_at: number;
}

export interface ClusterNodeInfo {
  node_id: string;
  listen_addr: string;
  is_leader: boolean;
  assigned_partitions: number[];
  last_heartbeat: number;
}

export interface ClusterTopologyResponse {
  cluster_id: string;
  nodes: ClusterNodeInfo[];
  total_partitions: number;
}

export interface PeerHeartbeatRequest {
  node_id: string;
  listen_addr: string;
  is_leader: boolean;
  assigned_partitions: number[];
}

export interface RoviersOutcome<T = any> {
  receipt_id: string;
  agent_id: string;
  service_name: string;
  completed_at: number;
  result: T;
  blob_ref?: BlobReference;
}

export interface AgentSignal {
  signal_id: string;
  receipt_id: string;
  agent_id: string;
  service_name: string;
  status: any;
  message: string;
  pickup_url: string;
  timestamp: number;
  tenant_id?: string;
  session_id?: string;
  business_key?: string;
  traceparent?: string;
  tracestate?: string;
  partition_id?: number;
}

// Backwards-compatibility aliases
export type CitizenCredentials = AgentCredentials;
export type SignedCitizenToken = SignedAgentToken;
export type IntimationNotice = AgentSignal;
export type ClientCredentials = AgentCredentials;
export type SignedClientToken = SignedAgentToken;
export type NotificationNotice = AgentSignal;
