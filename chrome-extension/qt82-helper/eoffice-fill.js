(() => {
  "use strict";

  let activeDraft = null;
  let filling = false;
  const templateImportAttempts = new Set();
  const SAFE_DIAGNOSTIC_MODE = false;
  const WORKFLOW_HUB_PATH = "/workflow/sitepages/createworkflow.aspx";
  const CREATE_WORKFLOW_URL = "https://eoffice.pnj.com.vn/workflow/sitepages/createworkflow.aspx?rcid=8&rscid=0&wid=0";
  const HUB_CLICK_NONCE_KEY = "pnjQt82HubClickNonce";
  const TARGET_LABELS = [
    "Mục đích", "Loại tiền", "TP phê duyệt", "Cửa hàng trưởng",
    "Đối tượng thanh toán", "Mã đối tượng", "Nhóm chi phí",
    "Nội dung đề nghị thanh toán", "Phương thức nhận tiền",
    "Số tiền cần thanh toán", "Mã chứng từ SAP", "Ngày mong muốn nhận tiền",
    "Tên tài khoản", "Số tài khoản", "Ngân hàng thụ hưởng",
  ];

  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  function pageHasVisibleLoadingIndicator() {
    const selector = [
      ".k-loading-mask", ".k-loading-image", ".spinner-border", ".loading-mask",
      ".dx-loadpanel", ".blockUI", "[aria-busy='true']",
    ].join(",");
    return Array.from(document.querySelectorAll(selector)).some(isVisible);
  }

  function readinessSignature() {
    const labels = [
      "Mục đích", "Loại tiền", "TP phê duyệt", "Nhóm chi phí",
      "Phương thức nhận tiền", "Mã chứng từ SAP", "Ngày mong muốn nhận tiền",
    ];
    const controls = labels.map((label) => findControl(label));
    if (controls.some((control) => !control || !isVisible(control))) return "";
    return controls.map((control) => [
      control.tagName,
      control.id || "",
      control.getAttribute("name") || "",
      Math.round(control.getBoundingClientRect().top),
    ].join(":" )).join("|");
  }

  async function waitForQt82Ready(timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    let stableSignature = "";
    let stableSince = 0;
    while (Date.now() < deadline) {
      const bridgeReady = document.documentElement.getAttribute("data-pnj-qt82-kendo-bridge") === "ready";
      const signature = document.readyState === "complete" && bridgeReady && !pageHasVisibleLoadingIndicator()
        ? readinessSignature()
        : "";
      if (signature && signature === stableSignature) {
        if (Date.now() - stableSince >= 1500) return true;
      } else {
        stableSignature = signature;
        stableSince = signature ? Date.now() : 0;
      }
      await delay(250);
    }
    return false;
  }

  function selectThroughKendo(selector, value, timeoutMs) {
    return new Promise((resolve) => {
      if (!selector) {
        resolve(false);
        return;
      }
      const requestId = `pnj-${Date.now()}-${Math.random().toString(16).slice(2)}`;
      const resultEvent = "pnj-qt82-kendo-result";
      let settled = false;
      const finish = (ok) => {
        if (settled) return;
        settled = true;
        document.removeEventListener(resultEvent, onResult);
        resolve(Boolean(ok));
      };
      const onResult = (event) => {
        if (!event.detail || event.detail.requestId !== requestId) return;
        finish(event.detail.ok);
      };
      document.addEventListener(resultEvent, onResult);
      document.dispatchEvent(new CustomEvent("pnj-qt82-kendo-select", {
        detail: {requestId, selector, value},
      }));
      setTimeout(() => finish(false), timeoutMs || 2500);
    });
  }

  function normalizeText(value) {
    return String(value || "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/đ/g, "d")
      .replace(/Đ/g, "D")
      .toLowerCase()
      .replace(/[\u2010-\u2015\u2212\-]/g, " ")
      .replace(/[()*:]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function isVisible(element) {
    if (!element || element.nodeType !== 1) return false;
    const view = (element.ownerDocument && element.ownerDocument.defaultView) || window;
    const style = view.getComputedStyle(element);
    if (style.display === "none" || style.visibility === "hidden" || Number(style.opacity) === 0) return false;
    const rect = element.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  }

  function elementText(element) {
    return String(element && (element.innerText || element.textContent) || "").trim();
  }

  function isWorkflowHubPage() {
    return String(window.location.pathname || "").toLowerCase() === WORKFLOW_HUB_PATH;
  }

  function isQt82FormPage() {
    return Boolean(findControl("Mục đích") && findControl("Mã chứng từ SAP"));
  }

  function isKnownNonFormWorkflowPage() {
    return String(window.location.pathname || "").toLowerCase() === "/workflow/default.aspx";
  }

  async function waitForQt82FormPage(timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (isQt82FormPage()) return true;
      await delay(250);
    }
    return false;
  }

  function redirectToWorkflowHub() {
    if (window.top !== window) return;
    window.location.replace(CREATE_WORKFLOW_URL);
  }

  function redirectForDraft(draft) {
    if (window.top !== window) return;
    const targetUrl = draft && draft.openMode === "deeplink" && draft.formUrl
      ? draft.formUrl
      : CREATE_WORKFLOW_URL;
    window.location.replace(targetUrl);
  }

  function qt82WorkflowTextMatches(value) {
    const text = normalizeText(value);
    return text === "qt82 quy trinh thanh toan"
      || (text.startsWith("qt82") && text.includes("quy trinh thanh toan") && text.length <= 60);
  }

  function findQt82WorkflowTile() {
    const labels = Array.from(document.querySelectorAll(
      "a, button, [role='button'], [onclick], div, span, p, h1, h2, h3, h4, h5",
    )).filter(isVisible).filter((element) => {
      const text = elementText(element);
      return text.length <= 120 && qt82WorkflowTextMatches(text);
    }).sort((a, b) => elementText(a).length - elementText(b).length);
    if (!labels.length) return null;

    const label = labels[0];
    const rankedTargets = [];
    let node = label;
    for (let depth = 0; node && depth < 7; depth += 1, node = node.parentElement) {
      const tag = String(node.tagName || "").toLowerCase();
      const style = window.getComputedStyle(node);
      const rect = node.getBoundingClientRect();
      const className = safeClassName(node).toLowerCase();
      const looksLikeCard = rect.width >= 180 && rect.height >= 90
        && rect.width <= 520 && rect.height <= 360
        && normalizeText(elementText(node)).includes("qt82")
        && (
          className.includes("card")
          || className.includes("item")
          || className.includes("workflow")
          || className.includes("process")
          || style.cursor === "pointer"
        );
      if (looksLikeCard) rankedTargets.push({element: node, score: 1000 - depth});
      if (
        tag === "a"
        || tag === "button"
        || node.getAttribute("role") === "button"
        || node.hasAttribute("onclick")
        || node.tabIndex >= 0
        || style.cursor === "pointer"
      ) rankedTargets.push({element: node, score: 800 - depth});
    }
    rankedTargets.push({element: label, score: 1});
    rankedTargets.sort((a, b) => b.score - a.score);
    return rankedTargets[0].element;
  }

  function directHrefFromTarget(target) {
    let node = target;
    for (let depth = 0; node && depth < 5; depth += 1, node = node.parentElement) {
      if (node.href && /^https:\/\/eoffice\.pnj\.com\.vn\/workflow\//i.test(node.href)) return node.href;
      const link = node.querySelector && node.querySelector("a[href*='/workflow/']");
      if (link && link.href && /^https:\/\/eoffice\.pnj\.com\.vn\/workflow\//i.test(link.href)) return link.href;
    }
    return "";
  }

  function dispatchSingleClick(target) {
    if (!target) return false;
    target.scrollIntoView({behavior: "auto", block: "center", inline: "center"});
    const view = (target.ownerDocument && target.ownerDocument.defaultView) || window;
    const rect = target.getBoundingClientRect();
    const clientX = Math.round(rect.left + rect.width / 2);
    const clientY = Math.round(rect.top + rect.height / 2);
    const centerTarget = document.elementFromPoint(clientX, clientY) || target;
    const targets = [centerTarget, target].filter((item, index, items) => item && items.indexOf(item) === index);
    targets.forEach((clickTarget) => {
      ["pointerdown", "mousedown", "pointerup", "mouseup", "click"].forEach((name) => {
        clickTarget.dispatchEvent(new view.MouseEvent(name, {
          bubbles: true,
          cancelable: true,
          view,
          button: 0,
          buttons: name.endsWith("down") ? 1 : 0,
          clientX,
          clientY,
        }));
      });
    });
    if (typeof target.click === "function") target.click();
    const href = directHrefFromTarget(target);
    if (href) {
      setTimeout(() => {
        if (window.location.href === CREATE_WORKFLOW_URL) window.location.assign(href);
      }, 1200);
    }
    return true;
  }

  function dispatchDoubleClick(target) {
    if (!target) return false;
    const view = (target.ownerDocument && target.ownerDocument.defaultView) || window;
    const rect = target.getBoundingClientRect();
    target.dispatchEvent(new view.MouseEvent("dblclick", {
        bubbles: true,
        cancelable: true,
        view,
        button: 0,
        clientX: Math.round(rect.left + rect.width / 2),
        clientY: Math.round(rect.top + rect.height / 2),
      }));
    return true;
  }

  async function openQt82FromWorkflowHub(draft, manualRetry) {
    if (window.top !== window || !draft) return false;
    activeDraft = draft;
    const nonce = String(draft.nonce || "");
    const alreadyClicked = sessionStorage.getItem(HUB_CLICK_NONCE_KEY) === nonce;
    if (alreadyClicked && !manualRetry) {
      renderStatus(
        "QT82 đã được nhấn một lần cho bản nháp này. Nếu form chưa mở, hãy kiểm tra đăng nhập rồi nhấn Thử lại.",
        ["Mở form QT82"],
        false,
      );
      return false;
    }

    renderStatus("Đang tìm QT82 trong danh sách Tạo yêu cầu...", [], true);
    const deadline = Date.now() + 30000;
    let tile = null;
    while (Date.now() < deadline) {
      tile = findQt82WorkflowTile();
      if (tile) break;
      await delay(300);
    }
    if (!tile) {
      renderStatus(
        "Không tìm thấy QT82 sau 30 giây. Hãy kiểm tra phiên đăng nhập eOffice hoặc tải lại trang Tạo yêu cầu.",
        ["QT82. Quy trình thanh toán"],
        false,
      );
      return false;
    }

    sessionStorage.setItem(HUB_CLICK_NONCE_KEY, nonce);
    renderStatus("Đã tìm thấy QT82. Đang mở form tạo yêu cầu...", [], true);
    const pageUrlBeforeClick = window.location.href;
    dispatchSingleClick(tile);
    setTimeout(() => {
      if (
        window.location.href === pageUrlBeforeClick
        && activeDraft
        && String(activeDraft.nonce || "") === nonce
      ) {
        dispatchDoubleClick(tile);
      }
    }, 1800);
    setTimeout(() => {
      if (
        window.location.href === pageUrlBeforeClick
        && activeDraft
        && String(activeDraft.nonce || "") === nonce
      ) {
        renderStatus(
          "Đã nhấn QT82 nhưng tab hiện tại chưa chuyển trang. Nếu form chưa mở ở tab khác, hãy nhấn Thử mở QT82 lại.",
          ["Mở form QT82"],
          false,
        );
      }
    }, 8000);
    return true;
  }

  function findLabel(labelText) {
    const wanted = normalizeText(labelText);
    const candidates = Array.from(document.querySelectorAll("label, span, div, td"))
      .filter(isVisible)
      .map((element) => ({element, text: normalizeText(elementText(element))}))
      .filter((item) => item.text === wanted || (item.text.startsWith(wanted) && item.text.length <= wanted.length + 18));
    candidates.sort((a, b) => {
      if (a.text === wanted && b.text !== wanted) return -1;
      if (b.text === wanted && a.text !== wanted) return 1;
      return elementText(a.element).length - elementText(b.element).length;
    });
    return candidates.length ? candidates[0].element : null;
  }

  function visibleControls(root) {
    return Array.from(root.querySelectorAll(
      "input:not([type='hidden']):not([disabled]), textarea:not([disabled]), select:not([disabled]), [role='combobox'], .dx-selectbox, .k-dropdown, .select2-selection",
    )).filter(isVisible);
  }

  const EXACT_CONTROL_SELECTORS = {
    "Loại tiền": "input[name='Currency_input']",
    "Đối tượng thanh toán": "input[name='SAP_Vendor_Name']",
    "Mã đối tượng": "input[name='Mã đối tượng'], input.kdeVendorSAPNo",
    "Nội dung đề nghị thanh toán": "textarea[name='Nội dung đề nghị thanh toán']",
    "Phương thức nhận tiền": "input[name='Paymentmethod_input']",
    "Ngân hàng thụ hưởng": "input[name='BeneficiaryBank_input']",
    "Mã chứng từ SAP": "input.kdeSAPDocsNo",
    "Ngày mong muốn nhận tiền": "input[name='Ngày mong muốn nhận tiền']",
  };

  function pickControl(controls, labelText) {
    if (!controls.length) return null;
    const exactSelector = EXACT_CONTROL_SELECTORS[labelText];
    if (exactSelector) {
      const exact = controls.find((control) => control.matches(exactSelector));
      if (exact) return exact;
    }
    if (labelText === "Nội dung đề nghị thanh toán") {
      const textarea = controls.find((control) => control.matches("textarea, [contenteditable='true']"));
      if (textarea) return textarea;
    }
    if (labelText === "Số tiền cần thanh toán") {
      const numeric = controls.find((control) => control.getAttribute("role") === "spinbutton");
      if (numeric) return numeric;
    }
    if (["Mục đích", "Loại tiền", "TP phê duyệt", "Nhóm chi phí", "Phương thức nhận tiền"].includes(labelText)) {
      const dropdown = controls.find((control) => {
        const role = control.getAttribute("role");
        return role === "combobox" || role === "listbox" || control.matches("select");
      });
      if (dropdown) return dropdown;
    }
    return controls[0];
  }

  function findControl(labelText) {
    const exactSelector = EXACT_CONTROL_SELECTORS[labelText];
    if (exactSelector) {
      const exact = Array.from(document.querySelectorAll(exactSelector)).find(isVisible);
      if (exact) return exact;
    }
    const label = findLabel(labelText);
    if (!label) return null;
    const htmlFor = label.getAttribute && label.getAttribute("for");
    if (htmlFor) {
      const direct = document.getElementById(htmlFor);
      if (direct && isVisible(direct)) return direct;
    }

    // Trên QT82, nhiều "nhãn" chính là cả khối ItemRow/col-group và control
    // nằm trực tiếp bên trong. Phải kiểm tra khối này trước khi leo lên hàng cha.
    const ownControls = visibleControls(label);
    const ownControl = pickControl(ownControls, labelText);
    if (ownControl) return ownControl;

    let node = label.parentElement;
    for (let depth = 0; node && depth < 5; depth += 1, node = node.parentElement) {
      const controls = visibleControls(node);
      if (controls.length === 1) return controls[0];
      if (controls.length > 1) {
        const preferred = pickControl(controls, labelText);
        if (preferred && exactSelector && preferred.matches(exactSelector)) return preferred;
        const labelRect = label.getBoundingClientRect();
        controls.sort((a, b) => {
          const ar = a.getBoundingClientRect();
          const br = b.getBoundingClientRect();
          const ad = Math.abs(ar.left - labelRect.left) + Math.abs(ar.top - labelRect.bottom);
          const bd = Math.abs(br.left - labelRect.left) + Math.abs(br.top - labelRect.bottom);
          return ad - bd;
        });
        return controls[0];
      }
    }
    return null;
  }

  async function waitForControl(labelText, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const control = findControl(labelText);
      if (control) return control;
      await delay(150);
    }
    return null;
  }

  function setNativeValue(control, value, blurAfter) {
    if (!control) return false;
    const target = control.matches("input, textarea") ? control : control.querySelector("input, textarea");
    if (!target) return false;
    const prototype = target instanceof HTMLTextAreaElement
      ? HTMLTextAreaElement.prototype
      : HTMLInputElement.prototype;
    const descriptor = Object.getOwnPropertyDescriptor(prototype, "value");
    if (descriptor && descriptor.set) descriptor.set.call(target, String(value ?? ""));
    else target.value = String(value ?? "");
    ["input", "change", "keyup"].forEach((name) => target.dispatchEvent(new Event(name, {bubbles: true})));
    if (blurAfter !== false) target.dispatchEvent(new Event("blur", {bubbles: true}));
    return true;
  }

  async function fillInput(label, value) {
    const control = await waitForControl(label, 3000);
    if (!control || !setNativeValue(control, value)) return {label, ok: false};
    await delay(80);
    return {label, ok: true};
  }

  function optionDocuments() {
    const documents = [document];
    try {
      if (window.top && window.top.document && window.top.document !== document) {
        documents.push(window.top.document);
      }
    } catch (_error) {
      // Bỏ qua frame khác origin.
    }
    return Array.from(new Set(documents));
  }

  function visibleOptions() {
    const selector = [
      "[role='option']", ".dropdown-item", ".dx-list-item-content", ".dx-item-content",
      ".ant-select-item-option-content", ".k-list-item", ".k-item", ".ui-menu-item",
      ".ms-core-menu-item", "li",
    ].join(",");
    return optionDocuments().flatMap((rootDocument) => Array.from(rootDocument.querySelectorAll(selector))).filter((item) => {
      const text = normalizeText(elementText(item));
      return isVisible(item) && text && text.length < 220;
    });
  }

  function optionTextMatches(actual, expected) {
    const text = normalizeText(actual);
    const wanted = normalizeText(expected);
    if (!text || !wanted) return false;
    if (text === wanted || text.includes(wanted) || wanted.includes(text)) return true;
    const tokens = wanted.split(" ").filter((token) => token.length > 1);
    return tokens.length > 0 && tokens.every((token) => text.includes(token));
  }

  async function waitForOption(expected, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const options = visibleOptions();
      const wanted = normalizeText(expected);
      const exact = options.find((item) => normalizeText(elementText(item)) === wanted);
      if (exact) return exact;
      const contains = options.find((item) => optionTextMatches(elementText(item), expected));
      if (contains) return contains;
      await delay(150);
    }
    return null;
  }

  async function chooseValue(label, value) {
    const control = await waitForControl(label, 3000);
    if (!control) return {label, ok: false};
    const exactSelector = EXACT_CONTROL_SELECTORS[label];
    if (exactSelector && await selectThroughKendo(exactSelector, value, 3000)) {
      await delay(250);
      return {label, ok: true};
    }
    if (control instanceof HTMLSelectElement) {
      const wanted = normalizeText(value);
      const option = Array.from(control.options).find((item) => {
        const text = normalizeText(item.textContent);
        return text === wanted || text.includes(wanted);
      });
      if (!option) return {label, ok: false};
      control.value = option.value;
      control.dispatchEvent(new Event("change", {bubbles: true}));
      return {label, ok: true};
    }

    // Kendo render input combobox bên trong một wrapper .k-dropdown. Click trực tiếp
    // vào input không mở danh sách trên QT82; phải click wrapper/listbox bao ngoài.
    const kendoDropdown = control.closest && control.closest(".k-dropdown");
    const clickable = kendoDropdown || (control.matches("input, [role='combobox']")
      ? control
      : (control.querySelector("input, [role='combobox']") || control));
    clickable.click();
    await delay(200);
    const option = await waitForOption(value, 4000);
    if (!option) return {label, ok: false};
    const optionView = (option.ownerDocument && option.ownerDocument.defaultView) || window;
    option.dispatchEvent(new optionView.MouseEvent("mousedown", {bubbles: true, cancelable: true, view: optionView}));
    option.dispatchEvent(new optionView.MouseEvent("mouseup", {bubbles: true, cancelable: true, view: optionView}));
    option.click();
    await delay(250);
    return {label, ok: true};
  }

  async function chooseSuggestion(label, query, expected) {
    const control = await waitForControl(label, 4000);
    if (!control) return {label, ok: false};
    const exactSelector = EXACT_CONTROL_SELECTORS[label];
    if (exactSelector && await selectThroughKendo(exactSelector, expected || query, 3500)) {
      await delay(500);
      return {label, ok: true};
    }
    const input = control.matches("input, textarea")
      ? control
      : control.querySelector("input, textarea");
    if (!input) return {label, ok: false};
    input.focus();
    input.click();
    if (!setNativeValue(input, query, false)) return {label, ok: false};
    input.dispatchEvent(new InputEvent("input", {
      bubbles: true,
      inputType: "insertText",
      data: String(query || ""),
    }));
    input.dispatchEvent(new KeyboardEvent("keyup", {bubbles: true, key: "a"}));
    const target = await waitForOption(expected || query, 3000);
    if (!target) return {label, ok: false};
    const targetView = (target.ownerDocument && target.ownerDocument.defaultView) || window;
    target.dispatchEvent(new targetView.MouseEvent("mousedown", {
      bubbles: true, cancelable: true, view: targetView,
    }));
    target.dispatchEvent(new targetView.MouseEvent("mouseup", {
      bubbles: true, cancelable: true, view: targetView,
    }));
    target.click();
    await delay(250);
    input.dispatchEvent(new KeyboardEvent("keydown", {
      bubbles: true, cancelable: true, key: "Tab", code: "Tab", keyCode: 9, which: 9,
    }));
    input.dispatchEvent(new Event("change", {bubbles: true}));
    input.dispatchEvent(new Event("blur", {bubbles: true}));
    await delay(350);
    return {label, ok: true};
  }

  function fieldValueMatches(label, expected) {
    const control = findControl(label);
    if (!control) return false;
    const input = control.matches("input, textarea")
      ? control
      : control.querySelector("input, textarea");
    if (!input) return false;
    return optionTextMatches(input.value, expected);
  }

  async function waitForFieldValue(label, expected, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (fieldValueMatches(label, expected)) return true;
      await delay(150);
    }
    return false;
  }

  async function chooseCurrency(value) {
    const label = "Loại tiền";
    const control = await waitForControl(label, 3000);
    if (!control) return {label, ok: false};
    const input = control.matches("input, textarea")
      ? control
      : control.querySelector("input, textarea");
    if (!input) return {label, ok: false};

    input.focus();
    input.click();
    if (!setNativeValue(input, value, false)) return {label, ok: false};
    input.dispatchEvent(new InputEvent("input", {
      bubbles: true,
      inputType: "insertText",
      data: String(value || ""),
    }));
    input.dispatchEvent(new KeyboardEvent("keyup", {
      bubbles: true, key: "D", code: "KeyD", keyCode: 68, which: 68,
    }));
    const option = await waitForOption(value, 3000);
    if (option) {
      const optionView = (option.ownerDocument && option.ownerDocument.defaultView) || window;
      option.dispatchEvent(new optionView.MouseEvent("mousedown", {
        bubbles: true, cancelable: true, view: optionView,
      }));
      option.dispatchEvent(new optionView.MouseEvent("mouseup", {
        bubbles: true, cancelable: true, view: optionView,
      }));
      option.click();
    }
    await delay(250);
    input.dispatchEvent(new KeyboardEvent("keydown", {
      bubbles: true, cancelable: true, key: "Tab", code: "Tab", keyCode: 9, which: 9,
    }));
    input.dispatchEvent(new Event("change", {bubbles: true}));
    input.dispatchEvent(new Event("blur", {bubbles: true}));
    return {label, ok: await waitForFieldValue(label, value, 1500)};
  }

  function findTemplateFileInput() {
    const label = findLabel("Đính kèm excel");
    let node = label;
    for (let depth = 0; node && depth < 6; depth += 1, node = node.parentElement) {
      const localInput = node.querySelector && node.querySelector("input[type='file']");
      if (localInput) return localInput;
    }
    const inputs = Array.from(document.querySelectorAll("input[type='file']"));
    return inputs.find((input) => /xls|spreadsheet/i.test(input.accept || "")) || inputs[0] || null;
  }

  function findClickableByText(expected) {
    const wanted = normalizeText(expected);
    const candidates = Array.from(document.querySelectorAll("button, a, [role='button'], span, div"))
      .filter(isVisible)
      .filter((element) => normalizeText(elementText(element)) === wanted)
      .sort((a, b) => elementText(a).length - elementText(b).length);
    if (!candidates.length) return null;
    const element = candidates[0];
    return element.closest("button, a, [role='button']") || element;
  }

  function base64ToBytes(base64) {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
    return bytes;
  }

  async function sha256Hex(bytes) {
    if (!crypto || !crypto.subtle) throw new Error("Trình duyệt không hỗ trợ SHA-256");
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
    return Array.from(digest).map((value) => value.toString(16).padStart(2, "0")).join("");
  }

  function templateImportVerified(draft) {
    const bodyText = String(document.body && document.body.innerText || "");
    const compactText = bodyText.replace(/\s+/g, "");
    const documents = Array.isArray(draft.detailDocuments) ? draft.detailDocuments : [];
    const documentsFound = documents.length > 0
      && documents.every((item) => compactText.includes(String(item || "").replace(/\s+/g, "")));
    const amount = String(Math.round(Number(draft.paymentAmount) || 0));
    const numericCompact = bodyText.replace(/[.,\s]/g, "");
    const amountFound = amount !== "0" && numericCompact.includes(amount);
    return documentsFound && amountFound;
  }

  async function waitForTemplateImport(draft, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (templateImportVerified(draft)) return true;
      await delay(300);
    }
    return false;
  }

  async function importTemplateFromMemory(draft) {
    const label = "Template TT";
    const nonce = String(draft && draft.nonce || "");
    if (!nonce || templateImportAttempts.has(nonce)) return {label, ok: false};
    templateImportAttempts.add(nonce);
    try {
      const template = draft.templateFile;
      if (!template || typeof template.base64 !== "string") return {label, ok: false};
      const bytes = base64ToBytes(template.base64);
      if (bytes.length !== template.size || bytes.length < 4 || bytes[0] !== 0x50 || bytes[1] !== 0x4b) {
        return {label, ok: false};
      }
      if ((await sha256Hex(bytes)).toLowerCase() !== String(template.sha256 || "").toLowerCase()) {
        return {label, ok: false};
      }
      const fileInput = findTemplateFileInput();
      const importButton = findClickableByText("Nhập từ excel");
      if (!fileInput || !importButton) return {label, ok: false};

      const file = new File([bytes], template.name, {type: template.type, lastModified: Date.now()});
      const transfer = new DataTransfer();
      transfer.items.add(file);
      fileInput.files = transfer.files;
      fileInput.dispatchEvent(new Event("input", {bubbles: true}));
      fileInput.dispatchEvent(new Event("change", {bubbles: true}));
      await delay(350);
      if (!fileInput.files || fileInput.files.length !== 1 || fileInput.files[0].name !== template.name) {
        return {label, ok: false};
      }

      const buttonView = (importButton.ownerDocument && importButton.ownerDocument.defaultView) || window;
      importButton.dispatchEvent(new buttonView.MouseEvent("mousedown", {
        bubbles: true, cancelable: true, view: buttonView,
      }));
      importButton.dispatchEvent(new buttonView.MouseEvent("mouseup", {
        bubbles: true, cancelable: true, view: buttonView,
      }));
      importButton.click();
      return {label, ok: await waitForTemplateImport(draft, 12000)};
    } catch (_error) {
      return {label, ok: false};
    }
  }

  function createStatusPanel() {
    if (window.top !== window) return null;
    let panel = document.getElementById("pnj-qt82-helper-status");
    if (panel) return panel;
    panel = document.createElement("section");
    panel.id = "pnj-qt82-helper-status";
    panel.style.cssText = [
      "position:fixed", "right:18px", "bottom:18px", "z-index:2147483647",
      "width:min(380px,calc(100vw - 36px))", "background:#fff", "color:#172033",
      "border:1px solid #2f80ed", "border-radius:10px", "box-shadow:0 12px 35px rgba(0,0,0,.24)",
      "padding:14px", "font:14px/1.4 Arial,sans-serif",
    ].join(";");
    document.documentElement.appendChild(panel);
    return panel;
  }

  function safeClassName(element) {
    return String(element && element.className || "").slice(0, 300);
  }

  function controlDescriptor(control, labelRect) {
    const rect = control.getBoundingClientRect();
    return {
      tag: control.tagName.toLowerCase(),
      type: control.getAttribute("type") || "",
      id: control.id || "",
      name: control.getAttribute("name") || "",
      className: safeClassName(control),
      role: control.getAttribute("role") || "",
      contenteditable: control.getAttribute("contenteditable") || "",
      ariaLabel: control.getAttribute("aria-label") || "",
      placeholder: control.getAttribute("placeholder") || "",
      dataBind: control.getAttribute("data-bind") || "",
      ngModel: control.getAttribute("ng-model") || "",
      deltaLeft: Math.round(rect.left - labelRect.left),
      deltaTop: Math.round(rect.top - labelRect.bottom),
      width: Math.round(rect.width),
      height: Math.round(rect.height),
    };
  }

  function buildDiagnosticReport() {
    const allControls = Array.from(document.querySelectorAll(
      "input:not([type='hidden']), textarea, select, [role='combobox'], [contenteditable='true'], .dx-selectbox, .k-dropdown, .select2-selection",
    )).filter(isVisible);

    const fields = TARGET_LABELS.map((labelText) => {
      const label = findLabel(labelText);
      if (!label) return {label: labelText, labelFound: false, candidates: []};
      const labelRect = label.getBoundingClientRect();
      const candidates = allControls
        .map((control) => {
          const rect = control.getBoundingClientRect();
          const vertical = Math.abs(rect.top - labelRect.bottom);
          const horizontal = Math.abs(rect.left - labelRect.left);
          return {control, score: vertical * 2 + horizontal};
        })
        .filter((item) => {
          const rect = item.control.getBoundingClientRect();
          return rect.top >= labelRect.top - 20
            && rect.top <= labelRect.bottom + 220
            && Math.abs(rect.left - labelRect.left) <= 750;
        })
        .sort((a, b) => a.score - b.score)
        .slice(0, 6)
        .map((item) => controlDescriptor(item.control, labelRect));

      const parents = [];
      let parent = label.parentElement;
      for (let level = 0; parent && level < 4; level += 1, parent = parent.parentElement) {
        parents.push({
          level,
          tag: parent.tagName.toLowerCase(),
          id: parent.id || "",
          className: safeClassName(parent),
        });
      }
      return {
        label: labelText,
        labelFound: true,
        labelTag: label.tagName.toLowerCase(),
        labelId: label.id || "",
        labelClassName: safeClassName(label),
        labelFor: label.getAttribute("for") || "",
        parents,
        candidates,
      };
    });

    return JSON.stringify({
      extensionVersion: chrome.runtime.getManifest().version,
      documentReadyState: document.readyState,
      fields,
    }, null, 2);
  }

  async function copyDiagnosticReport(button) {
    try {
      await navigator.clipboard.writeText(buildDiagnosticReport());
      button.textContent = "Đã sao chép — dán vào chat";
      button.style.borderColor = "#16a34a";
      button.style.color = "#166534";
    } catch (_error) {
      button.textContent = "Không sao chép được";
      button.style.borderColor = "#dc2626";
      button.style.color = "#b42318";
    }
  }

  function renderStatus(message, failures, busy) {
    const panel = createStatusPanel();
    if (!panel) return;
    const safeFailures = Array.isArray(failures) ? failures : [];
    panel.replaceChildren();

    const title = document.createElement("strong");
    title.textContent = `PNJ QT82 Draft Helper v${chrome.runtime.getManifest().version}`;
    panel.appendChild(title);

    const text = document.createElement("div");
    text.style.marginTop = "6px";
    text.textContent = message;
    panel.appendChild(text);

    if (safeFailures.length) {
      const detail = document.createElement("div");
      detail.style.cssText = "margin-top:6px;color:#b42318;font-size:12px";
      detail.textContent = "Chưa điền được: " + safeFailures.join(", ");
      panel.appendChild(detail);
    }

    const warning = document.createElement("div");
    warning.style.cssText = "margin-top:8px;padding:7px;background:#fff7e6;border-radius:6px;font-size:12px";
    warning.textContent = "Tiện ích không bấm Lưu/Gửi. Hãy kiểm tra Template TT, mã SAP và toàn bộ thông tin trước khi tự gửi.";
    panel.appendChild(warning);

    const buttons = document.createElement("div");
    buttons.style.cssText = "display:flex;flex-wrap:wrap;gap:7px;margin-top:9px";
    const diagnostic = document.createElement("button");
    diagnostic.type = "button";
    diagnostic.textContent = "Sao chép cấu trúc trường";
    diagnostic.addEventListener("click", () => copyDiagnosticReport(diagnostic));
    const retry = document.createElement("button");
    retry.type = "button";
    retry.textContent = busy ? "Đang xử lý..." : (isWorkflowHubPage() ? "Thử mở QT82 lại" : "Điền lại");
    retry.disabled = Boolean(busy || !activeDraft);
    retry.addEventListener("click", () => handleDraftOnCurrentPage(activeDraft, true));
    const clear = document.createElement("button");
    clear.type = "button";
    clear.textContent = "Xóa dữ liệu tạm";
    clear.addEventListener("click", () => {
      chrome.runtime.sendMessage({type: "CLEAR_DRAFT"});
      if (activeDraft && activeDraft.templateFile) activeDraft.templateFile.base64 = "";
      activeDraft = null;
      panel.remove();
    });
    const disable = document.createElement("button");
    disable.type = "button";
    disable.textContent = "Tắt helper";
    disable.addEventListener("click", () => {
      chrome.runtime.sendMessage({type: "DISABLE_HELPER"});
      if (activeDraft && activeDraft.templateFile) activeDraft.templateFile.base64 = "";
      activeDraft = null;
      panel.remove();
    });
    [diagnostic, retry, clear, disable].forEach((button) => {
      button.style.cssText = "border:1px solid #b8c2d1;background:#fff;border-radius:5px;padding:5px 9px;cursor:pointer";
      buttons.appendChild(button);
    });
    panel.appendChild(buttons);
  }

  async function fillDraft(draft) {
    if (!draft || filling) return;
    activeDraft = draft;
    if (SAFE_DIAGNOSTIC_MODE) {
      renderStatus(
        "Đã nhận bản nháp. Chế độ an toàn đang bật nên chưa ghi vào form. Hãy sao chép cấu trúc trường và dán vào chat.",
        [],
        false,
      );
      return;
    }
    filling = true;
    renderStatus("Đang chờ eOffice và Kendo tải ổn định...", [], true);
    if (!(await waitForQt82Ready(20000))) {
      filling = false;
      renderStatus(
        "eOffice chưa sẵn sàng sau 20 giây. Bản nháp vẫn được giữ tối đa 5 phút.",
        ["Trạng thái tải trang"],
        false,
      );
      return;
    }
    renderStatus("eOffice đã sẵn sàng. Đang điền bản nháp...", [], true);
    const results = [];
    let currencyRetryUsed = false;

    results.push(await chooseValue("Mục đích", draft.purpose));
    let currencyResult = await chooseCurrency(draft.currency);
    if (!currencyResult.ok) {
      currencyRetryUsed = true;
      await delay(1000);
      currencyResult = await chooseCurrency(draft.currency);
    }
    results.push(currencyResult);
    results.push(await chooseValue("TP phê duyệt", draft.managerApproval));
    results.push(await chooseSuggestion("Cửa hàng trưởng", draft.storeManagerQuery, draft.storeManagerName));
    results.push(await fillInput("Đối tượng thanh toán", draft.paymentObjectName));
    results.push(await fillInput("Mã đối tượng", draft.paymentObjectCode));
    results.push(await chooseValue("Nhóm chi phí", draft.costGroup));
    results.push(await fillInput("Nội dung đề nghị thanh toán", draft.requestContent));
    results.push(await chooseValue("Phương thức nhận tiền", draft.paymentMethod));
    await delay(700);
    // Không ghi trực tiếp trường tiền. Tổng thanh toán trên QT82 được tính từ các
    // dòng Chi tiết thanh toán sau khi người dùng nhập Template TT.
    results.push(await fillInput("Mã chứng từ SAP", draft.sapDocument));
    results.push(await fillInput("Ngày mong muốn nhận tiền", draft.desiredDate));
    results.push(await fillInput("Tên tài khoản", draft.accountName));
    results.push(await fillInput("Số tài khoản", draft.accountNumber));
    results.push(await chooseSuggestion("Ngân hàng thụ hưởng", draft.bankQuery, draft.bankQuery.replace(/-$/, "")));

    let failures = results.filter((item) => !item.ok).map((item) => item.label);
    if (!failures.length) {
      const templateResult = await importTemplateFromMemory(draft);
      results.push(templateResult);
      failures = results.filter((item) => !item.ok).map((item) => item.label);
    }
    if (!failures.length && !(await waitForFieldValue("Loại tiền", draft.currency, 1000))) {
      const currencyRetry = currencyRetryUsed
        ? {label: "Loại tiền", ok: false}
        : await chooseCurrency(draft.currency);
      results.push(currencyRetry);
      failures = results.filter((item) => !item.ok).map((item) => item.label);
    }
    filling = false;
    if (!failures.length) {
      chrome.runtime.sendMessage({type: "CLEAR_DRAFT"});
      if (draft.templateFile) draft.templateFile.base64 = "";
      activeDraft = null;
      renderStatus(
        draft.sapPlaceholder
          ? "Đã điền và nhập Template TT. Mã SAP đang là 1234 — bắt buộc thay trước khi gửi."
          : "Đã điền bản nháp và nhập Template TT. Hãy kiểm tra tổng tiền trước khi tự Lưu/Gửi.",
        [],
        false,
      );
    } else {
      renderStatus("Đã điền được một phần. Dữ liệu tạm vẫn được giữ tối đa 5 phút.", failures, false);
    }
  }

  async function handleDraftOnCurrentPage(draft, manualRetry) {
    if (!draft) return;
    activeDraft = draft;
    if (isWorkflowHubPage()) {
      await openQt82FromWorkflowHub(draft, Boolean(manualRetry));
      return;
    }
    if (isKnownNonFormWorkflowPage() || !(await waitForQt82FormPage(20000))) {
      const targetText = draft.openMode === "deeplink" ? "form QT82" : "Tạo yêu cầu";
      renderStatus(`Trang hiện tại chưa phải QT82. Đang chuyển đến ${targetText}...`, [], true);
      redirectForDraft(draft);
      return;
    }
    await fillDraft(draft);
  }

  function requestDraft(attempt) {
    chrome.runtime.sendMessage({type: "GET_DRAFT"}, (response) => {
      if (chrome.runtime.lastError) {
        if (window.top === window) {
          renderStatus("Tiện ích không kết nối được với tiến trình nền. Hãy tải lại tiện ích rồi thử lại.", ["Kết nối tiện ích"], false);
        }
        return;
      }
      if (response && response.ok && response.draft) {
        activeDraft = response.draft;
        if (window.top === window) {
          renderStatus("Đã nhận bản nháp. Đang chờ form QT82 sẵn sàng...", [], true);
        }
        delay(250).then(() => handleDraftOnCurrentPage(activeDraft, false));
        return;
      }
      if (attempt < 8) {
        setTimeout(() => requestDraft(attempt + 1), 750);
        return;
      }
      if (response && response.disabled) return;
      if (window.top === window) {
        renderStatus(
          response && response.expired
            ? "Bản nháp đã hết hạn. Quay lại website và nhấn tạo bản nháp một lần nữa."
            : "Không tìm thấy bản nháp QT82. Quay lại website và nhấn Tạo bản nháp QT82.",
          ["Dữ liệu bản nháp"],
          false,
        );
      }
    });
  }

  if (window.top === window) {
    renderStatus("Tiện ích đã chạy. Đang tìm bản nháp QT82...", [], true);
  }
  requestDraft(0);
})();
