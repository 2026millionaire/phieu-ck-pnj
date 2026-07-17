(() => {
  "use strict";

  const REQUEST_EVENT = "pnj-qt82-kendo-select";
  const RESULT_EVENT = "pnj-qt82-kendo-result";

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

  function textMatches(actual, expected) {
    const text = normalizeText(actual);
    const wanted = normalizeText(expected);
    if (!text || !wanted) return false;
    if (text === wanted || text.includes(wanted) || wanted.includes(text)) return true;
    const tokens = wanted.split(" ").filter((token) => token.length > 1);
    return tokens.length > 0 && tokens.every((token) => text.includes(token));
  }

  function candidateElements(control) {
    const container = control.closest(".ItemRow, .k-widget") || control.parentElement;
    const candidates = [control];
    if (container) candidates.push(...container.querySelectorAll("input, select, span.k-widget"));
    return Array.from(new Set(candidates));
  }

  function findWidget(control) {
    const jq = window.jQuery || (window.kendo && window.kendo.jQuery);
    if (!jq) return null;
    const keys = ["kendoDropDownList", "kendoComboBox"];
    const candidates = candidateElements(control);
    // Ưu tiên widget lựa chọn thật. AutoComplete của ô lọc chỉ giữ từ khóa tìm
    // kiếm và không cập nhật model của eOffice.
    for (const key of keys) {
      for (const candidate of candidates) {
        const widget = jq(candidate).data(key);
        if (widget) return widget;
      }
    }
    return null;
  }

  function itemText(widget, item) {
    const textField = widget.options && widget.options.dataTextField;
    if (textField && item && item[textField] != null) return item[textField];
    if (item && item.text != null) return item.text;
    if (item && item.Text != null) return item.Text;
    return String(item || "");
  }

  async function selectWidget(widget, expected) {
    if (typeof widget.open === "function") widget.open();
    await new Promise((resolve) => setTimeout(resolve, 150));
    const view = widget.dataSource && typeof widget.dataSource.view === "function"
      ? Array.from(widget.dataSource.view())
      : [];
    const data = view.length || !widget.dataSource || typeof widget.dataSource.data !== "function"
      ? view
      : Array.from(widget.dataSource.data());
    const index = data.findIndex((item) => textMatches(itemText(widget, item), expected));
    if (index >= 0 && typeof widget.select === "function") {
      widget.select(index);
    } else if (typeof widget.value === "function") {
      widget.value(expected);
    } else {
      return false;
    }
    if (typeof widget.trigger === "function") widget.trigger("change");
    const eventTargets = [
      widget.element && widget.element[0],
      widget.input && widget.input[0],
    ].filter(Boolean);
    for (const target of eventTargets) {
      target.dispatchEvent(new Event("change", {bubbles: true}));
      target.dispatchEvent(new Event("blur", {bubbles: true}));
    }
    if (typeof widget.close === "function") widget.close();
    await new Promise((resolve) => setTimeout(resolve, 100));
    const currentText = typeof widget.text === "function" ? widget.text() : "";
    const currentValue = typeof widget.value === "function" ? widget.value() : "";
    return textMatches(currentText, expected) || textMatches(currentValue, expected);
  }

  document.addEventListener(REQUEST_EVENT, async (event) => {
    const detail = event.detail || {};
    let ok = false;
    let reason = "Không tìm thấy control";
    try {
      const control = detail.selector ? document.querySelector(detail.selector) : null;
      if (control) {
        const widget = findWidget(control);
        if (widget) {
          ok = await selectWidget(widget, detail.value);
          reason = ok ? "" : "Kendo không xác nhận giá trị đã chọn";
        } else {
          reason = "Không tìm thấy Kendo widget";
        }
      }
    } catch (error) {
      reason = String(error && error.message || error || "Lỗi Kendo").slice(0, 160);
    }
    document.dispatchEvent(new CustomEvent(RESULT_EVENT, {
      detail: {requestId: detail.requestId || "", ok, reason},
    }));
  });

  function announceKendoReady(attempt) {
    if (window.jQuery && window.kendo) {
      document.documentElement.setAttribute("data-pnj-qt82-kendo-bridge", "ready");
      return;
    }
    if (attempt < 80) setTimeout(() => announceKendoReady(attempt + 1), 250);
  }

  announceKendoReady(0);
})();
