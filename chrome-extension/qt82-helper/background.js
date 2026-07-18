const DRAFT_KEY = "pnjQt82Draft";
const DISABLED_KEY = "pnjQt82Disabled";
const DRAFT_TTL_MS = 5 * 60 * 1000;
const DRAFT_EXPIRY_ALARM = "pnjQt82DraftExpiry";
const MAX_TEMPLATE_BYTES = 1024 * 1024;
const CREATE_WORKFLOW_URL = "https://eoffice.pnj.com.vn/workflow/sitepages/createworkflow.aspx?rcid=8&rscid=0&wid=0";

function validEofficeFormUrl(rawUrl) {
  try {
    const url = new URL(rawUrl || "");
    return url.protocol === "https:"
      && url.hostname === "eoffice.pnj.com.vn"
      && (url.port === "" || url.port === "443")
      && url.username === ""
      && url.password === ""
      && url.hash === ""
      && url.pathname.toLowerCase().startsWith("/workflow/");
  } catch (_error) {
    return false;
  }
}

function senderUrl(sender) {
  try {
    const rawUrl = sender && (sender.url || (sender.tab && sender.tab.url));
    return new URL(rawUrl || "");
  } catch (_error) {
    return null;
  }
}

function isAllowedWebSender(sender) {
  const url = senderUrl(sender);
  if (!url) return false;
  if (url.origin === "https://dangkhoa.io.vn") {
    return url.pathname === "/bk/eoffice" || url.pathname.startsWith("/bk/eoffice/");
  }
  return url.origin === "http://localhost:5050"
    && (url.pathname === "/eoffice" || url.pathname.startsWith("/eoffice/"));
}

function isAllowedEofficeSender(sender) {
  const url = senderUrl(sender);
  return Boolean(url && validEofficeFormUrl(url.href));
}

function validDraft(draft) {
  return Boolean(
    draft
    && draft.version === 1
    && Number.isInteger(Number(draft.phieuId))
    && Number(draft.phieuId) > 0
    && typeof draft.nonce === "string"
    && draft.nonce.length >= 8
    && validEofficeFormUrl(draft.formUrl)
    && (draft.openMode === undefined || draft.openMode === "workflow" || draft.openMode === "deeplink")
    && validTemplateFile(draft.templateFile)
    && Array.isArray(draft.detailDocuments)
    && draft.detailDocuments.length <= 50
    && draft.detailDocuments.every((item) => typeof item === "string" && item.length > 0 && item.length <= 40),
  );
}

function validTemplateFile(file) {
  if (!file || typeof file !== "object") return false;
  if (typeof file.name !== "string" || !/^[^\\/:*?"<>|]{1,180}\.xlsx$/i.test(file.name)) return false;
  if (file.type !== "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") return false;
  if (!Number.isInteger(file.size) || file.size <= 0 || file.size > MAX_TEMPLATE_BYTES) return false;
  if (typeof file.sha256 !== "string" || !/^[a-f0-9]{64}$/i.test(file.sha256)) return false;
  if (typeof file.base64 !== "string" || file.base64.length === 0 || file.base64.length > 1400000) return false;
  if (file.base64.length % 4 !== 0 || !/^[A-Za-z0-9+/]+={0,2}$/.test(file.base64)) return false;
  const padding = file.base64.endsWith("==") ? 2 : (file.base64.endsWith("=") ? 1 : 0);
  return ((file.base64.length * 3) / 4) - padding === file.size;
}

function clearStoredDraft() {
  return Promise.all([
    chrome.storage.session.remove(DRAFT_KEY),
    chrome.alarms.clear(DRAFT_EXPIRY_ALARM),
  ]);
}

chrome.runtime.onInstalled.addListener(() => {
  clearStoredDraft();
});

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm && alarm.name === DRAFT_EXPIRY_ALARM) clearStoredDraft();
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (!message || typeof message.type !== "string") return false;

  if (message.type === "STORE_AND_OPEN") {
    if (!isAllowedWebSender(sender) || !validDraft(message.draft)) {
      sendResponse({ok: false, error: "Nguồn hoặc dữ liệu bản nháp không hợp lệ."});
      return false;
    }
    const envelope = {
      draft: message.draft,
      expiresAt: Date.now() + DRAFT_TTL_MS,
    };
    const openUrl = message.draft.openMode === "deeplink" ? message.draft.formUrl : CREATE_WORKFLOW_URL;
    chrome.storage.session.set({[DRAFT_KEY]: envelope})
      .then(() => chrome.storage.session.remove(DISABLED_KEY))
      .then(() => chrome.alarms.create(DRAFT_EXPIRY_ALARM, {when: envelope.expiresAt}))
      .then(() => chrome.tabs.create({url: openUrl}))
      .then(() => sendResponse({ok: true}))
      .catch(() => sendResponse({ok: false, error: "Không thể mở QT82."}));
    return true;
  }

  if (message.type === "GET_DRAFT") {
    if (!isAllowedEofficeSender(sender)) {
      sendResponse({ok: false});
      return false;
    }
    chrome.storage.session.get([DRAFT_KEY, DISABLED_KEY]).then((stored) => {
      if (stored[DISABLED_KEY]) {
        sendResponse({ok: false, disabled: true});
        return;
      }
      const envelope = stored[DRAFT_KEY];
      if (!envelope || envelope.expiresAt <= Date.now() || !validDraft(envelope.draft)) {
        clearStoredDraft();
        sendResponse({ok: false, expired: Boolean(envelope)});
        return;
      }
      sendResponse({ok: true, draft: envelope.draft});
    }).catch(() => sendResponse({ok: false}));
    return true;
  }

  if (message.type === "CLEAR_DRAFT") {
    if (!isAllowedEofficeSender(sender)) {
      sendResponse({ok: false});
      return false;
    }
    clearStoredDraft()
      .then(() => sendResponse({ok: true}))
      .catch(() => sendResponse({ok: false}));
    return true;
  }

  if (message.type === "DISABLE_HELPER") {
    if (!isAllowedEofficeSender(sender)) {
      sendResponse({ok: false});
      return false;
    }
    clearStoredDraft()
      .then(() => chrome.storage.session.set({[DISABLED_KEY]: true}))
      .then(() => sendResponse({ok: true}))
      .catch(() => sendResponse({ok: false}));
    return true;
  }

  return false;
});
