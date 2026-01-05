# Knowledge role offline merge design (Java + Python)

## Context
We have parallel Java and Python implementations of knowledge role permissions. We need both to follow the same behavior when a document was previously offline (status=0), and when the document is still not online.

## Requirements
- If the document was offline (`status=0`) but now the file status check is successful, compute the final role list as:
  `final_roles = existing_roles + add_roles - del_roles` (deduped), then call the permission API **once** with `action=add` and `final_roles`.
- If the document is still not online, compute the same merged role list and write it to the unstructured document table, while keeping `status=0`.
- Keep the rest of the flow (user binding, logging, counters) unchanged.
- Align Java and Python behavior to this same logic.

## Approach
### Python
- In `handle_file_online_operations`, if `context.current_status == 0` and there are add/del role changes, merge roles with `_merge_permissions` and call `set_doc_admin_wrapper` once with `action="add"` and the merged list. Skip the separate delete call in this branch.
- In `sync_unstructured_document`, if `is_online` is false, force `status_value = 0` (even if the current status is non-zero), while still persisting the merged role list.

### Java
- In `processOnlineDocumentEnhanced`, fetch the current unstructured document status/roles. If status is `0` and there are role changes, merge roles once and call the permission API once with `action=add`.
- Introduce a helper that updates unstructured document roles with an explicit `markOnline` flag. Use `markOnline=false` when the document is not online to avoid flipping status to `1`.
- In the retry failure path (document not online), merge roles and persist them while keeping `status=0`.

## Data flow notes
- The merged role list uses the existing role list as the base, then applies add and delete lists.
- If the final role list is empty, skip the API call but still persist the merged list to the database.

## Testing
Skipped at the user’s request for this change.
