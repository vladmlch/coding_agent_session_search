/**
 * cass Archive Loader Module
 *
 * Loads unencrypted archive configuration and SQLite payload, initializes
 * Cross-Origin Isolation when available, and boots viewer.js.
 * CSP-safe: No inline event handlers, no eval.
 */

import {
  COI_STATE,
  getCOIState,
  initCOIDetection,
  onServiceWorkerActivated,
} from "./coi-detector.js";
import { registerServiceWorker } from "./sw-register.js";

let config = null;
let loadInFlight = false;
let activeAppInitToken = 0;
const MAX_BROWSER_DATABASE_SIZE = 512 * 1024 * 1024;
const ARCHIVE_SCOPE_URL = new URL("./", import.meta.url);

function beginAppInitAttempt() {
  activeAppInitToken += 1;
  return activeAppInitToken;
}

function isCurrentAppInitToken(token) {
  return token === activeAppInitToken;
}

async function loadConfig() {
  const response = await fetch(new URL("config.json", ARCHIVE_SCOPE_URL));
  if (!response.ok) {
    throw new Error(`Failed to load config: ${response.status}`);
  }
  return response.json();
}

function renderFatalError(message) {
  const main = document.getElementById("main-content");
  if (!main) {
    return;
  }
  main.replaceChildren();
  const box = document.createElement("div");
  box.className = "error";
  box.setAttribute("role", "alert");
  box.setAttribute("aria-live", "assertive");
  const text = document.createElement("span");
  text.className = "error-message";
  text.textContent = message;
  box.appendChild(text);
  main.appendChild(box);
}

async function init() {
  if (loadInFlight) {
    return;
  }

  try {
    config = await loadConfig();
  } catch (error) {
    console.error("Config load error:", error);
    renderFatalError("Failed to load archive configuration. The archive may be corrupted.");
    return;
  }

  if (config?.encrypted !== false) {
    renderFatalError("Encrypted archives are not supported in this build.");
    return;
  }

  await bootUnencryptedArchive();
}

async function bootUnencryptedArchive() {
  if (loadInFlight) {
    return;
  }

  const appInitToken = beginAppInitAttempt();
  loadInFlight = true;

  try {
    await loadViewerModule(appInitToken);
    const didLoad = await loadUnencryptedDatabase(appInitToken);
    if (!didLoad || !isCurrentAppInitToken(appInitToken)) {
      return;
    }
  } catch (error) {
    console.error("Failed to load unencrypted archive:", error);
    renderFatalError(error?.message || "Failed to load unencrypted database");
  } finally {
    loadInFlight = false;
  }
}

async function loadUnencryptedDatabase(initToken = activeAppInitToken) {
  const payloadPath = getUnencryptedPayloadPath();
  const expectedSize = getUnencryptedPayloadSize();
  const response = await fetch(new URL(payloadPath, ARCHIVE_SCOPE_URL));
  if (response.status !== 200) {
    throw new Error(`Failed to load database: ${response.status}`);
  }

  const dbBytes = await readResponseBytesExact(response, expectedSize, "Unencrypted database");
  if (!isCurrentAppInitToken(initToken)) {
    dbBytes.fill(0);
    return false;
  }

  let dbModule;
  try {
    dbModule = await import("./database.js");
    await dbModule.initDatabase(dbBytes);
  } finally {
    dbBytes.fill(0);
  }

  if (!isCurrentAppInitToken(initToken)) {
    dbModule.closeDatabase?.();
    return false;
  }

  const stats = dbModule.getStatistics();
  window.dispatchEvent(
    new CustomEvent("cass:db-ready", {
      detail: {
        conversationCount: stats.conversations || 0,
        messageCount: stats.messages || 0,
      },
    }),
  );
  return true;
}

function getUnencryptedPayloadSize() {
  const sizeBytes = config?.payload?.size_bytes;
  if (!Number.isSafeInteger(sizeBytes) || sizeBytes <= 0) {
    throw new Error("Unencrypted payload size_bytes must be a positive safe integer");
  }
  if (sizeBytes > MAX_BROWSER_DATABASE_SIZE) {
    throw new Error(
      `Unencrypted database exceeds the ${MAX_BROWSER_DATABASE_SIZE}-byte browser limit`,
    );
  }
  return sizeBytes;
}

async function readResponseBytesExact(response, expectedSize, label) {
  if (!Number.isSafeInteger(expectedSize) || expectedSize <= 0) {
    throw new Error(`${label} has an invalid expected size`);
  }
  if (expectedSize > MAX_BROWSER_DATABASE_SIZE) {
    throw new Error(`${label} exceeds the ${MAX_BROWSER_DATABASE_SIZE}-byte browser limit`);
  }

  const contentLengthHeader = response.headers?.get?.("content-length");
  if (contentLengthHeader !== null && contentLengthHeader !== undefined) {
    if (!/^\d+$/.test(contentLengthHeader)) {
      throw new Error(`${label} returned an invalid Content-Length header`);
    }
    const contentLength = Number(contentLengthHeader);
    if (!Number.isSafeInteger(contentLength) || contentLength > MAX_BROWSER_DATABASE_SIZE) {
      throw new Error(`${label} exceeds the ${MAX_BROWSER_DATABASE_SIZE}-byte browser limit`);
    }
    const contentEncoding = response.headers?.get?.("content-encoding");
    if (!contentEncoding && contentLength !== expectedSize) {
      throw new Error(
        `${label} size mismatch: received ${contentLength}, expected ${expectedSize}`,
      );
    }
  }

  if (!response.body?.getReader) {
    throw new Error(`${label} cannot be read safely without streaming response support`);
  }

  const reader = response.body.getReader();
  let bytes = null;
  let totalLength = 0;
  try {
    bytes = new Uint8Array(expectedSize);
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!(value instanceof Uint8Array)) {
        throw new Error(`${label} returned an invalid response chunk`);
      }
      try {
        const nextLength = totalLength + value.byteLength;
        if (
          !Number.isSafeInteger(nextLength) ||
          nextLength > expectedSize ||
          nextLength > MAX_BROWSER_DATABASE_SIZE
        ) {
          throw new Error(`${label} exceeds its declared ${expectedSize}-byte size`);
        }
        bytes.set(value, totalLength);
        totalLength = nextLength;
      } finally {
        value.fill(0);
      }
    }

    if (totalLength !== expectedSize) {
      throw new Error(`${label} size mismatch: received ${totalLength}, expected ${expectedSize}`);
    }

    return bytes;
  } catch (error) {
    bytes?.fill(0);
    try {
      await reader.cancel(error?.message || String(error));
    } catch {
      // The response stream may already be closed or errored.
    }
    throw error;
  }
}

function getUnencryptedPayloadPath() {
  const rawPath = config?.payload?.path;
  if (typeof rawPath === "string" && rawPath.trim().length > 0) {
    return normalizeUnencryptedPayloadPath(rawPath);
  }
  return "./payload/data.db";
}

function normalizeUnencryptedPayloadPath(rawPath) {
  const trimmed = rawPath.trim();
  if (!trimmed) {
    throw new Error("Unencrypted payload path cannot be empty");
  }
  if (trimmed.startsWith("/") || trimmed.startsWith("\\") || /^[A-Za-z]:[\\/]/.test(trimmed)) {
    throw new Error("Unencrypted payload path must be relative");
  }
  if (
    trimmed.includes("?") ||
    trimmed.includes("#") ||
    trimmed.includes("\\") ||
    trimmed.includes("%")
  ) {
    throw new Error("Unencrypted payload path contains invalid characters");
  }

  let normalized = trimmed;
  while (normalized.startsWith("./")) {
    normalized = normalized.slice(2);
  }

  const segments = normalized.split("/");
  if (segments.length < 2) {
    throw new Error("Unencrypted payload path must reference a file under payload/");
  }

  const safeSegments = [];
  for (const segment of segments) {
    if (!segment || segment === "." || segment === "..") {
      throw new Error("Unencrypted payload path contains traversal segments");
    }

    let decodedSegment;
    try {
      decodedSegment = decodeURIComponent(segment);
    } catch {
      throw new Error("Unencrypted payload path contains invalid escapes");
    }

    if (
      decodedSegment === "." ||
      decodedSegment === ".." ||
      decodedSegment.includes("/") ||
      decodedSegment.includes("\\") ||
      decodedSegment.includes("\0")
    ) {
      throw new Error("Unencrypted payload path contains invalid encoded segments");
    }

    safeSegments.push(segment);
  }

  if (safeSegments[0] !== "payload") {
    throw new Error("Unencrypted payload path must reside under payload/");
  }

  return `./${safeSegments.join("/")}`;
}

async function loadViewerModule(initToken = activeAppInitToken) {
  const module = await import("./viewer.js");
  if (!isCurrentAppInitToken(initToken)) {
    return;
  }
  module.init?.();
}

function bootstrapCrossOriginIsolation() {
  const coiStatus = document.getElementById("coi-status");
  const appScreen = document.getElementById("app-screen");

  const revealAppScreen = () => {
    appScreen?.classList.remove("hidden");
  };

  registerServiceWorker().catch((error) => {
    console.warn("Service worker registration failed:", error);
  });

  initCOIDetection({
    statusContainer: coiStatus,
    authContainer: null,
    onReady: revealAppScreen,
    maxWaitMs: 3000,
  })
    .then((state) => {
      console.log("[App] COI initialization complete, state:", state);
    })
    .catch((error) => {
      console.error("[App] COI initialization failed:", error);
      coiStatus?.classList.add("hidden");
      revealAppScreen();
    });

  onServiceWorkerActivated(async () => {
    const state = await getCOIState();
    if (state === COI_STATE.READY) {
      coiStatus?.classList.add("hidden");
      revealAppScreen();
    }
  });
}

function startApp() {
  bootstrapCrossOriginIsolation();
  void init();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", startApp);
} else {
  startApp();
}
