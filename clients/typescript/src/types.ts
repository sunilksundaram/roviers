export interface CitizenCredentials {
  citizen_id: string;
  full_name: string;
  issued_at: number;
  expires_at: number;
  scopes: string[];
}

export interface SignedCitizenToken {
  credentials: CitizenCredentials;
  signature_bytes: number[];
}

export interface RoviersReceipt {
  receipt_id: string;
  citizen_id: string;
  service_name: string;
  issued_at: number;
  status: 'Queued' | 'Processing' | 'ReadyForPickup' | 'Fulfilled' | { Failed: string };
  estimated_ready_secs?: number;
  callback_url?: string;
}

export interface RoviersOutcome<T = any> {
  receipt_id: string;
  citizen_id: string;
  service_name: string;
  completed_at: number;
  result: T;
}

export interface IntimationNotice {
  intimation_id: string;
  receipt_id: string;
  citizen_id: string;
  service_name: string;
  status: any;
  message: string;
  pickup_url: string;
  timestamp: number;
}
