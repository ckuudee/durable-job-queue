export const VERSION = "0.1.0";

export type { Job, JobStatus, NewJob } from "./types.js";
export { IdempotencyConflictError, JobStateError, type Storage } from "./storage.js";
export { DEFAULT_LEASE_MS, SqliteStorage, type SqliteStorageOptions } from "./sqlite-storage.js";
export {
  DEFAULT_RETRY_POLICY,
  computeBackoff,
  validateRetryPolicy,
  type RetryPolicy,
} from "./backoff.js";
