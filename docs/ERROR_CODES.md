# Error Codes Reference

> **Status (checked 2026-09-24; bead 2l1b0.59).** The `E1xxx`–`E5xxx` codes
> below are defined for the Pages export and viewer in `src/pages/errors.rs`,
> but no user-facing path displays them today: `error_code()` is called only
> from that module's tests. The messages, causes and resolutions below remain
> accurate guidance for the same failures. CLI errors are identified instead
> by the process exit code plus the kebab-case `err.kind` in the JSON error
> envelope. The exit codes are listed by `cass robot-docs exit-codes` and
> under `exit_codes` in `cass capabilities --json`; the kind vocabulary (about
> 90 values) lives in `src/model/cli_error_kind.rs`. Branch on `err.kind`,
> never on an `E` code.

This document describes the Pages export error cases and provides guidance for resolution.

## Error Code Format

Error codes follow the format `E<category><number>`:
- **E1xxx**: Decryption/Authentication errors
- **E2xxx**: Database errors
- **E3xxx**: Browser compatibility errors
- **E4xxx**: Network errors
- **E5xxx**: Export errors

## CLI error kinds (`err.kind` in the robot error envelope)

The `E`-codes above belong to the Pages viewer. CLI failures are reported
as `{"error": {"code", "kind", "message", "hint", "retryable"}}`; the full
kind vocabulary lives in `src/model/cli_error_kind.rs` and in
`tests/golden/robot/error_envelope_kinds.json.golden`. Kinds added by the
local-only port:

| `err.kind` | Exit | Meaning |
| ---------- | ---- | ------- |
| `removed-command` | 2 (usage) | The invocation named a command, `command subcommand` pair, flag, or flag value removed because cass now runs fully local-only. The rejection fires before argument normalization and typo correction, so no other command runs in its place. `hint` (when present) names a local equivalent — it is a suggestion, never executed. |

## Decryption Errors (E1xxx)

### E1001: Authentication Failed

**Message**: "The password you entered is incorrect."

**Cause**: The provided password does not match the archive's encryption key.

**Resolution**:
- Double-check your password (passwords are case-sensitive)
- Ensure you're using the password set when the archive was created
- If you've forgotten the password, use your recovery key if available

### E1002: Empty Password

**Message**: "Please enter a password."

**Cause**: The password field was left empty.

**Resolution**: Enter your password before clicking "Unlock".

### E1003: Invalid Format

**Message**: "This file is not a valid archive."

**Cause**: The file is not a recognized CASS archive format, or has been modified.

**Resolution**:
- Verify you're opening the correct file
- Re-copy or re-export the archive
- Check that the file hasn't been modified or corrupted during transfer

### E1004: Integrity Check Failed

**Message**: "The archive appears to be corrupted or tampered with."

**Cause**: The archive's cryptographic integrity verification failed, indicating data corruption or modification.

**Resolution**:
- Re-copy or re-export the archive from the original source
- Check that the file transferred completely
- The archive may have been damaged during storage

### E1005: Unsupported Version

**Message**: "This archive requires a newer version of the software."

**Cause**: The archive version is newer than the viewer can handle.

**Resolution**:
- Rebuild and reinstall the current CASS (`cargo install --path . --locked --bin cass`)
- Re-export the archive with the current build if possible

### E1006: No Matching Key Slot

**Message**: "No matching key slot found for the provided credentials."

**Cause**: The provided credentials don't match any encryption slot in the archive.

**Resolution**:
- Try your password again
- If using a recovery key, ensure it's the correct one for this archive
- The archive may have been re-encrypted with different credentials

### E1007: Crypto Error

**Message**: "An error occurred during decryption."

**Cause**: The cryptographic operation failed unexpectedly.

**Resolution**:
- Try again - this may be a transient error
- If persisting, re-copy or re-export the archive
- Report the issue if it continues

### E1008: Unsupported Metadata

**Message**: "This archive uses unsupported encryption metadata."

**Cause**: The archive requests an encryption, compression, chunking, or key-derivation option that this CASS build does not support.

**Resolution**:
- Rebuild and reinstall the current CASS (`cargo install --path . --locked --bin cass`)
- If you created the archive, export it again with a supported format
- Do not retry the same archive as though the password were wrong

### E1009: Corrupt Payload

**Message**: "The encrypted archive payload is corrupted or tampered with."

**Cause**: Archive metadata was readable, but an encrypted chunk failed authentication or decompression.

**Resolution**:
- Re-copy the archive again from the original source
- Re-export the archive if the source remains available
- Treat unexpected payload changes as possible tampering

## Database Errors (E2xxx)

### E2001: Corrupt Database

**Message**: "The database appears to be corrupted."

**Cause**: The SQLite database inside the archive is damaged.

**Resolution**:
- Re-copy the archive
- Re-export from the original source if available
- The archive may have been damaged during creation

### E2002: Missing Table

**Message**: "The archive is missing required data."

**Cause**: The archive is incomplete or was created with an incompatible version.

**Resolution**:
- Re-export from the original CASS database
- Ensure you're using a compatible version of CASS

### E2003: Invalid Query

**Message**: "Your search could not be processed."

**Cause**: The search query contains syntax that cannot be interpreted.

**Resolution**:
- Simplify your search query
- Remove special characters
- Use quotes around phrases

### E2004: Database Locked

**Message**: "The database is currently in use by another process."

**Cause**: Another operation is currently accessing the database.

**Resolution**:
- Wait a moment and try again
- Close other browser tabs viewing the same archive
- Close any other applications that might be using this archive

### E2005: No Results

**Message**: "No results found."

**Cause**: The search returned no results.

**Resolution**:
- Try different search terms
- Check your filter settings
- Broaden your date range if filtering by date

## Browser Errors (E3xxx)

### E3001: Unsupported Browser

**Message**: "Your browser doesn't support required features."

**Cause**: The browser is missing Web Crypto API, IndexedDB, or other required APIs.

**Resolution**:
- Use a modern browser: Chrome 90+, Firefox 90+, Safari 15+, Edge 90+
- Update your browser to the latest version
- Disable privacy extensions that may block required features

### E3002: WebAssembly Not Supported

**Message**: "Your browser doesn't support WebAssembly."

**Cause**: WebAssembly is not available, possibly due to browser settings or version.

**Resolution**:
- Update your browser to a recent version
- Check that JavaScript is enabled
- Disable extensions that may block WebAssembly

### E3003: Cryptography Not Supported

**Message**: "Your browser doesn't support secure cryptography."

**Cause**: The Web Crypto API is not available, possibly due to insecure context (HTTP).

**Resolution**:
- Access the archive via HTTPS
- Serve the file from a local web server (not `file://`)
- Use a supported browser

### E3004: Storage Quota Exceeded

**Message**: "Not enough storage space available."

**Cause**: The browser's storage quota has been exceeded.

**Resolution**:
- Clear browser data for the site
- Close other tabs viewing large archives
- Increase storage allocation in browser settings

### E3005: Cross-Origin Isolation Required

**Message**: "Cross-origin isolation is required but not enabled."

**Cause**: SharedArrayBuffer is required but not available due to missing COOP/COEP headers.

**Resolution**:
- Serve the archive from a properly configured local server (the `cass
  pages --preview` loopback server sets the required headers)
- If you serve the bundle some other way, enable the COOP/COEP headers
  yourself

## Fetch Errors (E4xxx)

These fire inside the archive **viewer** when its `fetch` of the archive
payload fails — on the loopback preview server, or on any other local server
you point it at. cass itself never reaches the network.

### E4001: Fetch Failed

**Message**: "Failed to download the archive."

**Cause**: The viewer's request for the archive bytes failed.

**Resolution**:
- Check that the preview/local server serving the bundle is still running
- Try again in a few moments
- Verify the archive path/URL is correct

### E4002: Incomplete Download

**Message**: "The download was incomplete."

**Cause**: The archive payload was only partially transferred to the viewer.

**Resolution**:
- Retry loading the archive
- Check that the preview/local server is still running
- Clear browser cache and retry

### E4003: Timeout

**Message**: "The connection timed out."

**Cause**: The server took too long to respond.

**Resolution**:
- Try again later
- Check the local server's status
- The archive may be too large for the current connection

### E4004: Server Error

**Message**: "The server returned an error."

**Cause**: The HTTP server serving the bundle returned an error status code.

**Resolution**:
- Try again later
- Check that the archive path/URL is correct
- Check the local server's logs

## Export Errors (E5xxx)

### E5001: No Conversations

**Message**: "No conversations found to export."

**Cause**: The source database contains no conversations.

**Resolution**:
- Check that CASS has indexed some conversations
- Run `cass index` to scan for new conversations
- Make sure you have some agent sessions recorded

### E5002: Source Database Error

**Message**: "Could not read the source database."

**Cause**: The CASS database could not be opened or read.

**Resolution**:
- Verify the database path is correct
- Check file permissions
- Run `cass health` to diagnose issues

### E5003: Output Error

**Message**: "Could not write to the output location."

**Cause**: The export file could not be written.

**Resolution**:
- Check the output directory exists
- Verify write permissions
- Ensure sufficient disk space

### E5004: Filter Matched Nothing

**Message**: "No conversations matched your filter criteria."

**Cause**: The export filters excluded all conversations.

**Resolution**:
- Broaden your filter criteria
- Check agent and workspace filters
- Expand the date range

## Getting Help

If you encounter an error not listed here or need additional assistance:

1. **Check the logs**: Run with `--verbose` for detailed output
2. **Search existing issues**: https://github.com/Dicklesworthstone/coding_agent_session_search/issues
3. **File a new issue**: Include the error code, message, and steps to reproduce

## Reporting Bugs

When reporting an error, please include:

- Error code and message
- CASS version (`cass --version`)
- Browser and version (for web viewer)
- Operating system
- Steps to reproduce
- Any relevant log output

Do NOT include:
- Passwords or recovery keys
- Personal conversation content
- Sensitive file paths
