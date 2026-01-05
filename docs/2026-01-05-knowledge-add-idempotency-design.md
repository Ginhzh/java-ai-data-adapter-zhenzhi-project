# Knowledge Add Idempotency Design (Java)

## Goal
Align Java `ReactiveKnowledgeAddService` idempotency behavior with the Python implementation using Redis for shared caching.

## Behavior
- Cache key: `knowledge_add:processed:<messageTaskId>`.
- Cache payload:
  - `status`: "success"
  - `result`: final response map
  - `cached_at`: ISO timestamp
- TTL: 7 days.

## Flow
1. Extract `messageTaskId` from the incoming message (if present).
2. Check Redis for a cached payload keyed by `knowledge_add:processed:<messageTaskId>`.
   - If `status == "success"`, return the cached `result` immediately.
3. Otherwise run the normal processing pipeline.
4. On success, write the payload above to Redis with a 7-day TTL.
5. If no `messageTaskId` is present, skip caching and process normally.

## Notes
- This mirrors the Python cache format and key naming exactly.
- No distributed lock is introduced; this is cache-based idempotency only.
