(() => {
  "use strict";

  function postToPage(type, extra) {
    window.postMessage(
      Object.assign({source: "PNJ_QT82_EXTENSION", type}, extra || {}),
      window.location.origin,
    );
  }

  window.addEventListener("message", (event) => {
    if (
      event.source !== window
      || event.origin !== window.location.origin
      || !event.data
      || event.data.source !== "PNJ_QT82_WEB"
      || event.data.type !== "STORE_AND_OPEN"
    ) return;

    chrome.runtime.sendMessage(
      {type: "STORE_AND_OPEN", draft: event.data.draft},
      (response) => {
        if (chrome.runtime.lastError || !response || !response.ok) {
          postToPage("ERROR", {
            message: response && response.error
              ? response.error
              : "Tiện ích Chrome không nhận được bản nháp.",
          });
          return;
        }
        postToPage("DRAFT_ACCEPTED");
      },
    );
  });

  postToPage("READY");
})();
