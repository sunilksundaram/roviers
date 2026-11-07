export interface AgentCredentials {
  agent_id: string;
  agent_name: string;
  issued_at: number;
  expires_at: number;
  scopes: string[];
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
}

export interface RoviersOutcome<T = any> {
  receipt_id: string;
  agent_id: string;
  service_name: string;
  completed_at: number;
  result: T;
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
}

// Backwards-compatibility aliases
export type CitizenCredentials = AgentCredentials;
export type SignedCitizenToken = SignedAgentToken;
export type IntimationNotice = AgentSignal;
export type ClientCredentials = AgentCredentials;
export type SignedClientToken = SignedAgentToken;
export type NotificationNotice = AgentSignal;
