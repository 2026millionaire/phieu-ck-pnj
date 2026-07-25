(() => {
    const root = document.getElementById('proposalEditor');
    if (!root) return;

    const $ = id => document.getElementById(id);
    const fieldInputs = Array.from(root.querySelectorAll('[data-field]'));
    const titleInput = $('proposalTitle');
    const contentInput = $('proposalContent');
    const reasonInput = $('reasonContent');
    const organizationInput = $('organization');
    const attachmentsInput = $('attachments');
    const approvalInput = $('approvalLevel');
    const saveMessage = $('saveMessage');
    let proposalId = root.dataset.id || null;

    function values() {
        const result = {};
        fieldInputs.forEach(input => {
            result[input.dataset.field] = input.value.trim();
        });
        return result;
    }

    function displayValues() {
        const result = values();
        fieldInputs.forEach(input => {
            if (input.dataset.type !== 'date' || !input.value) return;
            const parts = input.value.split('-');
            if (parts.length === 3) {
                result[input.dataset.field] = `${parts[2]}/${parts[1]}/${parts[0]}`;
            }
        });
        return result;
    }

    function fillPattern(pattern, data) {
        return (pattern || '').replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_match, key) => {
            return data[key] || '……';
        });
    }

    function formatMoney(value) {
        const digits = String(value || '').replace(/[^\d]/g, '');
        if (!digits) return '';
        return Number(digits).toLocaleString('vi-VN');
    }

    fieldInputs.forEach(input => {
        if (input.dataset.type === 'money') {
            input.value = formatMoney(input.value);
            input.addEventListener('input', () => {
                const cursorAtEnd = input.selectionStart === input.value.length;
                input.value = formatMoney(input.value);
                if (cursorAtEnd) input.setSelectionRange(input.value.length, input.value.length);
                updatePreview();
            });
        } else {
            input.addEventListener('input', updatePreview);
            input.addEventListener('change', updatePreview);
        }
    });

    function regenerate() {
        const data = displayValues();
        titleInput.value = fillPattern(root.dataset.titleTemplate, data);
        contentInput.value = fillPattern(root.dataset.contentTemplate, data);
        reasonInput.value = fillPattern(root.dataset.reasonTemplate, data);
        updatePreview();
    }

    function updatePreview() {
        $('previewTitle').textContent = titleInput.value || 'ĐỀ XUẤT';
        $('previewContent').textContent = contentInput.value || '……';
        $('previewReason').textContent = reasonInput.value || '……';
        $('previewOrganization').textContent = organizationInput.value || '';
        $('previewAttachments').textContent = attachmentsInput.value || '……';
    }

    [titleInput, contentInput, reasonInput, organizationInput, attachmentsInput]
        .forEach(input => input.addEventListener('input', updatePreview));
    $('regenerateContent').addEventListener('click', regenerate);

    function showMessage(message, type) {
        saveMessage.textContent = message;
        saveMessage.className = `dx-save-message ${type}`;
        saveMessage.classList.remove('d-none');
        saveMessage.scrollIntoView({behavior: 'smooth', block: 'nearest'});
    }

    function payload(status) {
        return {
            id: proposalId,
            template_id: Number(root.dataset.templateId),
            status,
            title: titleInput.value.trim(),
            proposal_content: contentInput.value.trim(),
            reason_content: reasonInput.value.trim(),
            approval_level: approvalInput.value.trim(),
            organization: organizationInput.value.trim(),
            attachments: attachmentsInput.value.trim(),
            form_data: values(),
        };
    }

    async function save(status = 'draft') {
        const response = await fetch(root.dataset.saveUrl, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload(status)),
        });
        let result;
        try {
            result = await response.json();
        } catch (_error) {
            throw new Error('Máy chủ trả về dữ liệu không hợp lệ.');
        }
        if (!response.ok || !result.ok) throw new Error(result.error || 'Không thể lưu đề xuất.');
        proposalId = String(result.id);
        root.dataset.id = proposalId;
        window.history.replaceState({}, '', result.edit_url);
        showMessage(status === 'draft' ? 'Đã lưu bản nháp.' : 'Đã lưu và hoàn thiện đề xuất.', 'success');
        return result;
    }

    async function guardedSave(status, button) {
        const original = button.innerHTML;
        button.disabled = true;
        button.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Đang lưu';
        try {
            return await save(status);
        } catch (error) {
            showMessage(error.message, 'error');
            return null;
        } finally {
            button.disabled = false;
            button.innerHTML = original;
        }
    }

    $('saveDraftButton').addEventListener('click', event => guardedSave('draft', event.currentTarget));
    $('downloadPdfButton').addEventListener('click', async event => {
        const result = await guardedSave('completed', event.currentTarget);
        if (result) window.location.href = result.pdf_url;
    });

    const previewModal = $('previewModal');
    const mobileHost = $('mobilePreviewHost');
    const livePreview = $('livePreview');

    function openPreview() {
        updatePreview();
        mobileHost.innerHTML = '';
        mobileHost.appendChild(livePreview.cloneNode(true));
        previewModal.classList.remove('d-none');
        document.body.style.overflow = 'hidden';
    }

    function closePreview() {
        previewModal.classList.add('d-none');
        mobileHost.innerHTML = '';
        document.body.style.overflow = '';
    }

    $('mobilePreviewButton').addEventListener('click', openPreview);
    $('openMobilePreview').addEventListener('click', openPreview);
    $('closePreview').addEventListener('click', closePreview);
    previewModal.addEventListener('click', event => {
        if (event.target === previewModal) closePreview();
    });

    updatePreview();
})();
