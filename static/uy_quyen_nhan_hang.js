(function() {
    'use strict';

    const section = document.getElementById('sectionUQ01');
    if (!section) return;

    const FORM_CODE = 'PNJ-QYD-PLPLNL-NS-UQ-F1';
    const DEFAULT_ACTION = 'ký/thực hiện nhận gói/hộp niêm phong và giao hàng';
    const DEFAULT_RESPONSIBILITY = 'Người ủy quyền hoàn toàn chịu trách nhiệm về việc ủy quyền này.';
    const MANUAL_PLANT_VALUE = '__OTHER__';
    let plantDirectory = [];
    try {
        const plantDirectoryData = document.getElementById('uqPlantDirectoryData');
        const parsedPlants = JSON.parse(plantDirectoryData ? plantDirectoryData.textContent : '[]');
        plantDirectory = Array.isArray(parsedPlants) ? parsedPlants : [];
    } catch (_error) {
        plantDirectory = [];
    }
    let documentIdentity = {
        plant: text(section.dataset.plant) || '1305',
        document_no: text(section.dataset.documentNo),
        created_at: text(section.dataset.createdAt),
        issue_date: text(section.dataset.issueDate),
        issue_place: text(section.dataset.issuePlace) || 'Huế',
        copies: 2
    };
    let personnelProfiles = [];
    let selectedAuthorizerId = '';
    let selectedRecipientId = '';
    let personnelModal = null;
    let profilesLoaded = false;
    let items = [emptyItem()];
    let generatedContent = '';
    let contentCustomized = false;
    let currentSourceMode = 'manual';
    let previewTimer = null;
    let pendingPreviewAction = null;
    let initialized = false;

    function byId(id) {
        return document.getElementById(id);
    }

    function fieldValue(id) {
        const element = byId(id);
        return element ? String(element.value || '').trim() : '';
    }

    function appRoute(path) {
        const prefix = window.location.pathname.startsWith('/bk/') || window.location.pathname === '/bk'
            ? '/bk'
            : '';
        return prefix + path;
    }

    function plantByCode(code) {
        const normalizedCode = text(code).toUpperCase();
        return plantDirectory.find(function(plant) {
            return text(plant.code) === normalizedCode;
        }) || null;
    }

    function fillPlantSelect(selectId, placeholder, includeManual) {
        const select = byId(selectId);
        select.replaceChildren();
        const blankOption = document.createElement('option');
        blankOption.value = '';
        blankOption.textContent = placeholder;
        select.appendChild(blankOption);
        plantDirectory.forEach(function(plant) {
            const option = document.createElement('option');
            option.value = text(plant.code);
            option.textContent = text(plant.code) + ' - ' + text(plant.name);
            select.appendChild(option);
        });
        if (includeManual) {
            const manualOption = document.createElement('option');
            manualOption.value = MANUAL_PLANT_VALUE;
            manualOption.textContent = 'Khác - nhập thủ công';
            select.appendChild(manualOption);
        }
    }

    function updateLocationSummary(summaryId, plant) {
        const summary = byId(summaryId);
        summary.hidden = !plant;
        summary.textContent = plant
            ? text(plant.code) + ' - ' + text(plant.name)
            : '';
    }

    function syncPickupLocation() {
        const isStore = fieldValue('uqPickupType').toLowerCase() === 'cửa hàng';
        const plantSelect = byId('uqPickupPlant');
        const codeField = byId('uqPickupCode');
        const nameField = byId('uqPickupName');
        byId('uqPickupPlantField').hidden = !isStore;
        byId('uqPickupCodeField').hidden = isStore;
        nameField.readOnly = isStore;

        if (!isStore) {
            updateLocationSummary('uqPickupPlantSummary', null);
            return;
        }
        if (!plantSelect.value) {
            const currentPlant = plantByCode(codeField.value);
            if (currentPlant) plantSelect.value = currentPlant.code;
        }
        const selectedPlant = plantByCode(plantSelect.value);
        codeField.value = selectedPlant ? selectedPlant.code : '';
        nameField.value = selectedPlant ? selectedPlant.name : '';
        updateLocationSummary('uqPickupPlantSummary', selectedPlant);
    }

    function syncDestinationLocation(clearKnownForManual) {
        const plantSelect = byId('uqDestinationPlant');
        const codeField = byId('uqDestinationCode');
        const nameField = byId('uqDestinationName');
        const manual = plantSelect.value === MANUAL_PLANT_VALUE;
        byId('uqDestinationCodeField').hidden = !manual;
        nameField.readOnly = !manual;

        if (manual) {
            if (clearKnownForManual && plantByCode(codeField.value)) {
                codeField.value = '';
                nameField.value = '';
            }
            updateLocationSummary('uqDestinationPlantSummary', null);
            return;
        }
        const selectedPlant = plantByCode(plantSelect.value);
        codeField.value = selectedPlant ? selectedPlant.code : '';
        nameField.value = selectedPlant ? selectedPlant.name : '';
        updateLocationSummary('uqDestinationPlantSummary', selectedPlant);
    }

    function initializePlantControls() {
        fillPlantSelect('uqPickupPlant', '-- Chọn cửa hàng nhận --', false);
        fillPlantSelect('uqDestinationPlant', '-- Chọn cửa hàng giao --', true);

        const pickupPlant = plantByCode(fieldValue('uqPickupCode'));
        byId('uqPickupPlant').value = pickupPlant ? pickupPlant.code : '';
        syncPickupLocation();

        const destinationPlant = plantByCode(fieldValue('uqDestinationCode'));
        byId('uqDestinationPlant').value = destinationPlant
            ? destinationPlant.code
            : MANUAL_PLANT_VALUE;
        syncDestinationLocation(false);

        byId('uqPickupType').addEventListener('change', function() {
            syncPickupLocation();
            onStructuredChange();
        });
        byId('uqPickupPlant').addEventListener('change', function() {
            syncPickupLocation();
            onStructuredChange();
        });
        byId('uqDestinationPlant').addEventListener('change', function() {
            syncDestinationLocation(true);
            onStructuredChange();
        });
    }

    function profileId(value) {
        const id = parseInt(String(value || ''), 10);
        return Number.isFinite(id) && id > 0 ? String(id) : '';
    }

    function profileById(id) {
        const normalizedId = profileId(id);
        return personnelProfiles.find(function(profile) {
            return String(profile.id) === normalizedId;
        }) || null;
    }

    function selectedAuthorizer() {
        return profileById(selectedAuthorizerId);
    }

    function selectedRecipient() {
        return profileById(selectedRecipientId);
    }

    function partyFromProfile(profile, includeIdentity) {
        if (!profile) {
            return includeIdentity
                ? {
                    full_name: '', id_type: 'CCCD', id_number: '',
                    issue_date: '', issue_place: '', job_title: '', employee_code: '',
                    unit_code: '', unit_name: ''
                }
                : {
                    full_name: '', job_title: '', employee_code: '',
                    unit_code: '', unit_name: ''
                };
        }
        const party = {
            full_name: text(profile.full_name),
            job_title: text(profile.job_title),
            employee_code: text(profile.employee_code),
            unit_code: text(profile.unit_code),
            unit_name: text(profile.unit_name)
        };
        if (includeIdentity) {
            party.id_type = text(profile.id_type) || 'CCCD';
            party.id_number = text(profile.id_number);
            party.issue_date = text(profile.id_issue_date);
            party.issue_place = text(profile.id_issue_place);
        }
        return party;
    }

    function setProfileSummary(containerId, profile) {
        const container = byId(containerId);
        container.replaceChildren();
        if (!profile) {
            container.textContent = 'Chưa có hồ sơ phù hợp. Dùng “Cập nhật dữ liệu nhân sự” để thêm.';
            return;
        }
        const name = document.createElement('strong');
        name.textContent = text(profile.full_name);
        const detail = document.createElement('small');
        const unit = [profile.unit_code, profile.unit_name].filter(Boolean).join(' - ');
        detail.textContent = [profile.job_title, unit].filter(Boolean).join(' · ');
        container.append(name, detail);
    }

    function fillProfileSelect(selectId, profiles, selectedId, emptyLabel) {
        const select = byId(selectId);
        select.replaceChildren();
        const placeholder = document.createElement('option');
        placeholder.value = '';
        placeholder.textContent = emptyLabel;
        select.appendChild(placeholder);
        profiles.forEach(function(profile) {
            const option = document.createElement('option');
            option.value = String(profile.id);
            option.textContent = [profile.full_name, profile.job_title]
                .filter(Boolean)
                .join(' · ');
            select.appendChild(option);
        });
        select.value = profileId(selectedId);
    }

    function renderProfileSelectors(preferredProfileId) {
        const authorizers = personnelProfiles.filter(function(profile) {
            return Boolean(profile.can_authorize);
        });
        const recipients = personnelProfiles.filter(function(profile) {
            return Boolean(profile.can_receive);
        });
        const preferred = profileById(preferredProfileId);
        if (preferred && preferred.can_authorize) selectedAuthorizerId = String(preferred.id);
        if (preferred && preferred.can_receive) selectedRecipientId = String(preferred.id);
        if (!authorizers.some(function(profile) { return String(profile.id) === selectedAuthorizerId; })) {
            selectedAuthorizerId = authorizers.length ? String(authorizers[0].id) : '';
        }
        if (!recipients.some(function(profile) { return String(profile.id) === selectedRecipientId; })) {
            selectedRecipientId = recipients.length ? String(recipients[0].id) : '';
        }

        fillProfileSelect(
            'uqAuthorizerProfile',
            authorizers,
            selectedAuthorizerId,
            authorizers.length ? '-- Chọn người ủy quyền --' : 'Chưa có hồ sơ người ủy quyền'
        );
        fillProfileSelect(
            'uqRecipientProfile',
            recipients,
            selectedRecipientId,
            recipients.length ? '-- Chọn người được ủy quyền --' : 'Chưa có hồ sơ người được ủy quyền'
        );
        updateProfileSummaries();
    }

    function updateProfileSummaries() {
        const authorizer = selectedAuthorizer();
        const recipient = selectedRecipient();
        setProfileSummary('uqAuthorizerSummary', authorizer);
        setProfileSummary('uqRecipientSummary', recipient);
        const missingIdentity = Boolean(recipient && !text(recipient.id_number));
        byId('uqRecipientProfileWarning').hidden = !missingIdentity;
    }

    async function loadPersonnelProfiles(preferredProfileId) {
        const response = await window.fetch(appRoute('/api/uq01/personnel-profiles?role=all'), {
            credentials: 'same-origin',
            headers: { Accept: 'application/json' }
        });
        const result = await response.json();
        if (!response.ok || !result.ok) {
            throw new Error(result.error || 'Không thể tải hồ sơ nhân sự.');
        }
        personnelProfiles = Array.isArray(result.profiles) ? result.profiles : [];
        profilesLoaded = true;
        renderProfileSelectors(preferredProfileId);
        renderProfileList();
        onStructuredChange();
        return personnelProfiles;
    }

    function emptyItem() {
        return {
            material_code: '',
            batch: '',
            description: '',
            quantity: '1',
            unit: 'món',
            sale_price: '',
            note: ''
        };
    }

    function text(value) {
        return String(value == null ? '' : value).trim();
    }

    function firstValue(source, keys) {
        for (const key of keys) {
            if (source && source[key] != null && text(source[key])) return text(source[key]);
        }
        return '';
    }

    function normalizeReferenceType(value) {
        const referenceType = text(value).toUpperCase();
        if (referenceType === 'PO') return 'PXK';
        if (['STO', 'PXK', 'KHÁC'].includes(referenceType)) return referenceType;
        return referenceType ? 'KHÁC' : 'STO';
    }

    function normalizeStoData(raw) {
        const source = raw && typeof raw === 'object' ? raw : {};
        const normalizedItems = Array.isArray(source.items) ? source.items.slice(0, 40).map(function(item) {
            const row = item && typeof item === 'object' ? item : {};
            return {
                material_code: firstValue(row, ['material_code', 'material', 'matnr']),
                batch: firstValue(row, ['batch', 'charg']),
                description: firstValue(
                    row,
                    ['description', 'product_name', 'material_description', 'maktx']
                ),
                quantity: firstValue(row, ['quantity', 'qty', 'menge']) || '1',
                unit: firstValue(row, ['unit', 'uom', 'meins']) || 'món',
                sale_price: firstValue(row, ['sale_price', 'price', 'net_price']),
                note: firstValue(row, ['note', 'remark'])
            };
        }) : [];
        const sourceMode = text(source.source_mode).toLowerCase();
        return {
            source_mode: sourceMode === 'erp' ? 'erp' : 'manual',
            reference_type: normalizeReferenceType(source.reference_type),
            reference_number: firstValue(source, ['reference_number', 'sto_number', 'ebeln']),
            approved_date: firstValue(source, ['approved_date', 'approval_date']),
            items: normalizedItems
        };
    }

    function applyStoData(raw) {
        const normalized = normalizeStoData(raw);
        currentSourceMode = normalized.source_mode;
        byId('uqReferenceType').value = ['STO', 'PXK', 'KHÁC'].includes(normalized.reference_type)
            ? normalized.reference_type
            : 'KHÁC';
        byId('uqReferenceNumber').value = normalized.reference_number;
        byId('uqApprovedDate').value = normalized.approved_date;
        items = normalized.items.length ? normalized.items : [emptyItem()];
        renderItems();
        onStructuredChange();
        return normalized;
    }

    window.UQ01StoAdapter = Object.freeze({
        normalizeStoData: normalizeStoData,
        applyStoData: applyStoData
    });

    function dateDisplay(value) {
        const match = text(value).match(/^(\d{4})-(\d{2})-(\d{2})$/);
        return match ? match[3] + '/' + match[2] + '/' + match[1] : text(value);
    }

    function locationText(code, name, address) {
        const lead = [text(code), text(name)].filter(Boolean).join(' - ');
        if (lead && text(address)) return lead + ', ' + text(address);
        return lead || text(address);
    }

    function generatedAuthorizationContent() {
        const authorizerProfile = selectedAuthorizer();
        const recipientProfile = selectedRecipient();
        const authorizer = text(authorizerProfile && authorizerProfile.full_name) || '[chưa chọn]';
        const recipient = text(recipientProfile && recipientProfile.full_name) || '[chưa chọn]';
        const action = fieldValue('uqAuthorizationAction') || DEFAULT_ACTION;
        const packageCount = Math.max(1, parseInt(fieldValue('uqPackageCount') || '1', 10) || 1);
        const packageText = byId('uqSealedPackage').checked
            ? packageCount + ' gói/hộp niêm phong chứa hàng hóa'
            : packageCount + ' kiện hàng hóa';
        const referenceType = normalizeReferenceType(fieldValue('uqReferenceType'));
        const referenceNumber = fieldValue('uqReferenceNumber');
        let referenceText = '';
        if (referenceNumber) {
            referenceText = ' theo ' + referenceType + ' số ' + referenceNumber;
        }
        const pickup = locationText(
            fieldValue('uqPickupCode'),
            fieldValue('uqPickupName'),
            fieldValue('uqPickupAddress')
        ) || 'địa điểm nhận hàng';
        const destination = locationText(
            fieldValue('uqDestinationCode'),
            fieldValue('uqDestinationName'),
            fieldValue('uqDestinationAddress')
        ) || 'địa điểm giao hàng';
        return (
            'Người ủy quyền Ông/Bà ' + authorizer
            + ' ủy quyền cho Người được ủy quyền Ông/Bà ' + recipient
            + ' thay mặt Người ủy quyền ' + action.replace(/\.+$/, '')
            + '. Phạm vi ủy quyền gồm ' + packageText + referenceText
            + '; hàng hóa được nhận tại ' + pickup + ' và giao về ' + destination + '.'
        );
    }

    function updateContentStatus() {
        const badge = byId('uqContentStatus');
        if (contentCustomized) {
            badge.textContent = 'Đã tùy chỉnh';
            badge.classList.add('customized');
        } else {
            badge.textContent = 'Đang sinh từ dữ liệu';
            badge.classList.remove('customized');
        }
    }

    function regenerateContent() {
        generatedContent = generatedAuthorizationContent();
        byId('uqContentOverride').value = generatedContent;
        contentCustomized = false;
        updateContentStatus();
        schedulePreview();
    }

    function itemHasData(item) {
        if (
            text(item.material_code)
            || text(item.description)
        ) return true;
        if (!['', '1'].includes(text(item.quantity))) return true;
        return !['', 'món'].includes(text(item.unit).toLowerCase());
    }

    function parseNumber(value) {
        let normalized = text(value).replace(/\s+/g, '');
        if (!normalized) return null;
        if (/^-?\d{1,3}(?:\.\d{3})+$/.test(normalized)) {
            normalized = normalized.replace(/\./g, '');
        } else if (normalized.includes(',') && !normalized.includes('.')) {
            normalized = normalized.replace(',', '.');
        }
        const number = Number(normalized);
        return Number.isFinite(number) ? number : null;
    }

    function renderItems() {
        const list = byId('uqItemsList');
        const template = byId('uqItemTemplate');
        list.replaceChildren();
        items.forEach(function(item, index) {
            const fragment = template.content.cloneNode(true);
            const card = fragment.querySelector('.uq01-item-card');
            fragment.querySelector('[data-uq-item-number]').textContent = String(index + 1);
            fragment.querySelectorAll('[data-uq-field]').forEach(function(input) {
                const key = input.dataset.uqField;
                input.value = item[key] == null ? '' : item[key];
                input.addEventListener('input', function() {
                    items[index][key] = this.value;
                    onStructuredChange();
                });
                if (input.classList.contains('uq01-uppercase')) {
                    input.addEventListener('blur', function() {
                        this.value = this.value.trim().toUpperCase();
                        items[index][key] = this.value;
                        onStructuredChange();
                    });
                }
            });
            fragment.querySelector('[data-uq-remove-item]').addEventListener('click', function() {
                items.splice(index, 1);
                if (!items.length) items.push(emptyItem());
                renderItems();
                onStructuredChange();
            });
            card.dataset.itemIndex = String(index);
            list.appendChild(fragment);
        });
    }

    function collectPayload() {
        return {
            template_code: 'UQ-01',
            form_code: FORM_CODE,
            plant: documentIdentity.plant,
            document_no: documentIdentity.document_no,
            created_at: documentIdentity.created_at,
            issue_place: documentIdentity.issue_place,
            issue_date: documentIdentity.issue_date,
            copies: 2,
            authorizer: partyFromProfile(selectedAuthorizer(), false),
            authorized_person: partyFromProfile(selectedRecipient(), true),
            authorization: {
                authorization_action: fieldValue('uqAuthorizationAction') || DEFAULT_ACTION,
                pickup_type: fieldValue('uqPickupType'),
                pickup: {
                    code: fieldValue('uqPickupCode'),
                    name: fieldValue('uqPickupName'),
                    address: fieldValue('uqPickupAddress')
                },
                destination: {
                    code: fieldValue('uqDestinationCode'),
                    name: fieldValue('uqDestinationName'),
                    address: fieldValue('uqDestinationAddress')
                },
                package_count: parseInt(fieldValue('uqPackageCount') || '1', 10) || 1,
                sealed_package: byId('uqSealedPackage').checked,
                valid_from: fieldValue('uqValidFrom'),
                valid_to: fieldValue('uqValidTo'),
                responsibility_clause: fieldValue('uqResponsibilityClause') || DEFAULT_RESPONSIBILITY,
                additional_notes: fieldValue('uqAdditionalNotes'),
                content_override: contentCustomized ? byId('uqContentOverride').value.trim() : '',
                content_customized: contentCustomized
            },
            sto: {
                source_mode: currentSourceMode,
                reference_type: normalizeReferenceType(fieldValue('uqReferenceType')),
                reference_number: fieldValue('uqReferenceNumber'),
                approved_date: fieldValue('uqApprovedDate'),
                items: items.map(function(item) { return Object.assign({}, item); })
            }
        };
    }

    function validatePayload(payload) {
        const warnings = [];
        function add(message) {
            if (!warnings.includes(message)) warnings.push(message);
        }
        [
            ['người ủy quyền', payload.authorizer],
            ['người được ủy quyền', payload.authorized_person]
        ].forEach(function(entry) {
            const label = entry[0];
            const party = entry[1];
            if (!party.full_name) {
                add('Chưa chọn hồ sơ ' + label + '.');
                return;
            }
            if (!party.job_title) add('Chưa nhập chức vụ của ' + label + '.');
            if (!party.unit_code && !party.unit_name) add('Chưa nhập đơn vị của ' + label + '.');
        });

        const idNumber = payload.authorized_person.id_number.replace(/\s+/g, '');
        if (!idNumber) {
            add('Chưa nhập số giấy tờ của người được ủy quyền.');
        } else if (payload.authorized_person.id_type === 'CCCD' && !/^\d{12}$/.test(idNumber)) {
            add('CCCD thường phải gồm đúng 12 chữ số.');
        } else if (payload.authorized_person.id_type === 'CMND' && !/^(\d{9}|\d{12})$/.test(idNumber)) {
            add('CMND cần kiểm tra lại; dạng lịch sử thường gồm 9 hoặc 12 chữ số.');
        }

        if (!payload.sto.reference_number) {
            add('Chưa nhập số STO/tham chiếu nhận hàng.');
        } else if (payload.sto.reference_type === 'STO' && !/^\d{10}$/.test(payload.sto.reference_number)) {
            add('Số STO quan sát thường gồm 10 chữ số; vui lòng kiểm tra lại.');
        }

        const pickup = payload.authorization.pickup;
        const destination = payload.authorization.destination;
        if (!locationText(pickup.code, pickup.name, pickup.address)) add('Chưa nhập nơi nhận hàng.');
        if (!locationText(destination.code, destination.name, destination.address)) add('Chưa nhập nơi giao hàng.');
        if (
            (pickup.code && destination.code && pickup.code.toLowerCase() === destination.code.toLowerCase())
            || (pickup.name && destination.name && pickup.name.toLowerCase() === destination.name.toLowerCase())
        ) add('Nơi nhận và nơi giao đang trùng nhau; vui lòng kiểm tra.');

        if (!payload.authorization.valid_from && !payload.authorization.valid_to) {
            add('Chưa nhập ngày hiệu lực ủy quyền.');
        }
        if (
            payload.authorization.valid_from
            && payload.authorization.valid_to
            && payload.authorization.valid_to < payload.authorization.valid_from
        ) add('Ngày kết thúc hiệu lực không được trước ngày bắt đầu.');

        const activeItems = payload.sto.items.filter(itemHasData);
        if (!activeItems.length) add('Chưa có danh sách hàng hóa.');
        activeItems.forEach(function(item, index) {
            if (!item.material_code && !item.description) {
                add('Dòng hàng ' + (index + 1) + ': thiếu mã hoặc tên sản phẩm.');
            }
            const quantity = parseNumber(item.quantity);
            if (quantity === null || quantity <= 0) {
                add('Dòng hàng ' + (index + 1) + ': số lượng phải lớn hơn 0.');
            }
        });

        if (payload.authorization.content_customized && !payload.authorization.content_override) {
            add('Nội dung đã đánh dấu tùy chỉnh nhưng đang để trống.');
        }
        return warnings;
    }

    function renderWarnings(warnings) {
        const container = byId('uqWarnings');
        container.replaceChildren();
        if (!warnings.length) {
            container.classList.remove('show');
            return;
        }
        const title = document.createElement('strong');
        title.textContent = warnings.length + ' cảnh báo cần kiểm tra trước khi in';
        const list = document.createElement('ul');
        warnings.forEach(function(message) {
            const item = document.createElement('li');
            item.textContent = message;
            list.appendChild(item);
        });
        container.append(title, list);
        container.classList.add('show');
    }

    function currentWarnings() {
        const warnings = validatePayload(collectPayload());
        renderWarnings(warnings);
        return warnings;
    }

    function confirmWarnings(warnings) {
        if (!warnings.length) return true;
        return window.confirm(
            'Biểu mẫu còn ' + warnings.length + ' cảnh báo dữ liệu.\n'
            + warnings.slice(0, 5).map(function(message) { return '• ' + message; }).join('\n')
            + (warnings.length > 5 ? '\n• ...' : '')
            + '\n\nBạn vẫn muốn tiếp tục?'
        );
    }

    function showProfileMessage(message, tone) {
        const container = byId('uqProfileFormMessage');
        container.textContent = message || '';
        container.className = 'uq01-profile-form-message ' + (tone || 'error');
        container.hidden = !message;
    }

    function resetProfileForm() {
        byId('uqProfileId').value = '';
        byId('uqProfileFormTitle').textContent = 'Thêm hồ sơ';
        byId('uqProfileCanAuthorize').checked = false;
        byId('uqProfileCanReceive').checked = true;
        byId('uqProfileFullName').value = '';
        byId('uqProfileJobTitle').value = '';
        byId('uqProfileEmployeeCode').value = '';
        byId('uqProfileUnitCode').value = documentIdentity.plant || '1305';
        byId('uqProfileUnitName').value = fieldValue('uqDestinationName');
        byId('uqProfileIdType').value = 'CCCD';
        byId('uqProfileIdNumber').value = '';
        byId('uqProfileIdIssueDate').value = '';
        byId('uqProfileIdIssuePlace').value = '';
        showProfileMessage('', '');
    }

    function editProfile(profileIdValue) {
        const profile = profileById(profileIdValue);
        if (!profile) {
            resetProfileForm();
            return;
        }
        byId('uqProfileId').value = String(profile.id);
        byId('uqProfileFormTitle').textContent = 'Sửa hồ sơ · ' + profile.full_name;
        byId('uqProfileCanAuthorize').checked = Boolean(profile.can_authorize);
        byId('uqProfileCanReceive').checked = Boolean(profile.can_receive);
        byId('uqProfileFullName').value = profile.full_name || '';
        byId('uqProfileJobTitle').value = profile.job_title || '';
        byId('uqProfileEmployeeCode').value = profile.employee_code || '';
        byId('uqProfileUnitCode').value = profile.unit_code || '';
        byId('uqProfileUnitName').value = profile.unit_name || '';
        byId('uqProfileIdType').value = ['CCCD', 'CMND', 'Khác'].includes(profile.id_type)
            ? profile.id_type
            : 'Khác';
        byId('uqProfileIdNumber').value = profile.id_number || '';
        byId('uqProfileIdIssueDate').value = profile.id_issue_date || '';
        byId('uqProfileIdIssuePlace').value = profile.id_issue_place || '';
        showProfileMessage('', '');
    }

    function renderProfileList() {
        const list = byId('uqProfileList');
        if (!list) return;
        list.replaceChildren();
        if (!personnelProfiles.length) {
            const empty = document.createElement('p');
            empty.className = 'text-muted small mb-0';
            empty.textContent = 'Chưa có hồ sơ. Chọn “Thêm hồ sơ” để bắt đầu.';
            list.appendChild(empty);
            return;
        }
        personnelProfiles.forEach(function(profile) {
            const item = document.createElement('div');
            item.className = 'uq01-profile-list-item';
            const copy = document.createElement('div');
            const name = document.createElement('strong');
            name.textContent = text(profile.full_name);
            const detail = document.createElement('small');
            detail.textContent = [profile.job_title, profile.unit_code].filter(Boolean).join(' · ');
            const roles = document.createElement('div');
            roles.className = 'uq01-profile-role-badges';
            if (profile.can_authorize) {
                const role = document.createElement('span');
                role.textContent = 'Ủy quyền';
                roles.appendChild(role);
            }
            if (profile.can_receive) {
                const role = document.createElement('span');
                role.textContent = 'Nhận ủy quyền';
                roles.appendChild(role);
                if (!profile.id_number) {
                    const missing = document.createElement('span');
                    missing.textContent = 'Thiếu giấy tờ';
                    roles.appendChild(missing);
                }
            }
            copy.append(name, detail, roles);
            const edit = document.createElement('button');
            edit.className = 'btn btn-outline-secondary btn-sm';
            edit.type = 'button';
            edit.textContent = 'Sửa';
            edit.addEventListener('click', function() {
                editProfile(profile.id);
            });
            item.append(copy, edit);
            list.appendChild(item);
        });
    }

    function openPersonnelManager(profileIdValue) {
        renderProfileList();
        if (profileIdValue) {
            editProfile(profileIdValue);
        } else {
            resetProfileForm();
        }
        if (!personnelModal && window.bootstrap && window.bootstrap.Modal) {
            personnelModal = new window.bootstrap.Modal(byId('uqPersonnelModal'));
        }
        if (personnelModal) personnelModal.show();
    }

    function collectProfileForm() {
        const profile = {
            full_name: fieldValue('uqProfileFullName'),
            job_title: fieldValue('uqProfileJobTitle'),
            employee_code: fieldValue('uqProfileEmployeeCode'),
            unit_code: fieldValue('uqProfileUnitCode'),
            unit_name: fieldValue('uqProfileUnitName'),
            id_type: fieldValue('uqProfileIdType') || 'CCCD',
            id_number: fieldValue('uqProfileIdNumber'),
            id_issue_date: fieldValue('uqProfileIdIssueDate'),
            id_issue_place: fieldValue('uqProfileIdIssuePlace'),
            can_authorize: byId('uqProfileCanAuthorize').checked,
            can_receive: byId('uqProfileCanReceive').checked
        };
        if (!profile.can_authorize && !profile.can_receive) {
            throw new Error('Hồ sơ phải có ít nhất một vai trò sử dụng.');
        }
        if (!profile.full_name) throw new Error('Vui lòng nhập họ và tên.');
        if (!profile.job_title) throw new Error('Vui lòng nhập chức vụ.');
        if (!profile.unit_code) throw new Error('Vui lòng nhập mã đơn vị.');
        if (!profile.unit_name) throw new Error('Vui lòng nhập tên đơn vị.');
        return profile;
    }

    async function savePersonnelProfile() {
        let profile;
        try {
            profile = collectProfileForm();
        } catch (error) {
            showProfileMessage(error.message, 'error');
            return;
        }
        const profileIdValue = profileId(byId('uqProfileId').value);
        const url = profileIdValue
            ? appRoute('/api/uq01/personnel-profiles/' + profileIdValue)
            : appRoute('/api/uq01/personnel-profiles');
        const button = byId('btnUqSaveProfile');
        button.disabled = true;
        showProfileMessage('Đang lưu hồ sơ...', 'warning');
        try {
            const response = await window.fetch(url, {
                method: profileIdValue ? 'PUT' : 'POST',
                credentials: 'same-origin',
                headers: {
                    Accept: 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(profile)
            });
            const result = await response.json();
            if (!response.ok || !result.ok) {
                throw new Error(result.error || 'Không thể lưu hồ sơ.');
            }
            await loadPersonnelProfiles(result.profile.id);
            editProfile(result.profile.id);
            const warnings = Array.isArray(result.warnings) ? result.warnings : [];
            showProfileMessage(
                warnings.length
                    ? 'Đã lưu hồ sơ. ' + warnings.join(' ')
                    : 'Đã lưu hồ sơ và cập nhật biểu mẫu.',
                warnings.length ? 'warning' : 'success'
            );
        } catch (error) {
            showProfileMessage(error.message || 'Không thể lưu hồ sơ.', 'error');
        } finally {
            button.disabled = false;
        }
    }

    async function createNewDocumentIdentity() {
        if (!window.confirm(
            'Tạo số văn bản và ngày lập mới?\nDữ liệu nghiệp vụ đang nhập sẽ được giữ nguyên.'
        )) return;
        try {
            const response = await window.fetch(appRoute('/api/uq01/document-identity'), {
                credentials: 'same-origin',
                headers: { Accept: 'application/json' }
            });
            const result = await response.json();
            if (!response.ok || !result.ok || !result.document) {
                throw new Error(result.error || 'Không thể tạo số văn bản mới.');
            }
            documentIdentity = Object.assign({ copies: 2 }, result.document);
            byId('uqValidFrom').value = documentIdentity.issue_date || '';
            onStructuredChange();
            submitPreview();
        } catch (error) {
            window.alert(error.message || 'Không thể tạo số văn bản mới.');
        }
    }

    function submitPreview(afterLoad) {
        const payload = collectPayload();
        renderWarnings(validatePayload(payload));
        byId('uqPreviewState').textContent = 'Đang tải...';
        byId('uqPreviewPayload').value = JSON.stringify(payload);
        const form = byId('uqPreviewPostForm');
        form.action = appRoute('/uy-quyen-nhan-hang/print');
        pendingPreviewAction = typeof afterLoad === 'function' ? afterLoad : null;
        form.submit();
    }

    function schedulePreview() {
        if (!byId('rbUQ01').checked) return;
        clearTimeout(previewTimer);
        byId('uqPreviewState').textContent = 'Có thay đổi';
        previewTimer = window.setTimeout(function() {
            submitPreview();
        }, 650);
    }

    function onStructuredChange() {
        generatedContent = generatedAuthorizationContent();
        if (!contentCustomized) {
            byId('uqContentOverride').value = generatedContent;
        }
        updateContentStatus();
        currentWarnings();
        schedulePreview();
    }

    function initialize() {
        if (initialized) return;
        initialized = true;
        initializePlantControls();
        if (!fieldValue('uqValidFrom')) {
            byId('uqValidFrom').value = documentIdentity.issue_date || '';
        }
        renderItems();
        regenerateContent();
        currentWarnings();

        section.querySelector('.uq01-editor').querySelectorAll('input, select, textarea').forEach(function(element) {
            if (
                element.id === 'uqContentOverride'
                || element.id === 'uqAuthorizerProfile'
                || element.id === 'uqRecipientProfile'
                || element.id === 'uqPickupType'
                || element.id === 'uqPickupPlant'
                || element.id === 'uqDestinationPlant'
                || element.closest('.uq01-item-card')
                || element.type === 'hidden'
                || element.name === 'uqSourceMode'
            ) return;
            element.addEventListener('input', onStructuredChange);
            element.addEventListener('change', onStructuredChange);
            if (element.classList.contains('uq01-uppercase')) {
                element.addEventListener('blur', function() {
                    this.value = this.value.trim().toUpperCase();
                    onStructuredChange();
                });
            }
        });

        byId('uqAuthorizerProfile').addEventListener('change', function() {
            selectedAuthorizerId = profileId(this.value);
            updateProfileSummaries();
            onStructuredChange();
        });
        byId('uqRecipientProfile').addEventListener('change', function() {
            selectedRecipientId = profileId(this.value);
            updateProfileSummaries();
            onStructuredChange();
        });

        byId('uqContentOverride').addEventListener('input', function() {
            contentCustomized = this.value.trim() !== generatedContent.trim();
            updateContentStatus();
            currentWarnings();
            schedulePreview();
        });

        byId('btnManageUqPersonnel').addEventListener('click', function() {
            openPersonnelManager(selectedAuthorizerId);
        });
        byId('btnCompleteRecipientProfile').addEventListener('click', function() {
            openPersonnelManager(selectedRecipientId);
        });
        byId('btnUqNewProfile').addEventListener('click', resetProfileForm);
        byId('btnUqSaveProfile').addEventListener('click', savePersonnelProfile);
        byId('btnNewUq01').addEventListener('click', createNewDocumentIdentity);
        byId('btnRegenerateUqContent').addEventListener('click', regenerateContent);
        byId('btnAddUqItem').addEventListener('click', function() {
            items.push(emptyItem());
            renderItems();
            onStructuredChange();
        });
        byId('btnPreviewUQ01').addEventListener('click', function() {
            clearTimeout(previewTimer);
            submitPreview();
        });
        byId('btnPrintUQ01').addEventListener('click', function() {
            clearTimeout(previewTimer);
            const warnings = currentWarnings();
            if (!confirmWarnings(warnings)) return;
            submitPreview(function() {
                const frame = byId('uqPreviewFrame');
                frame.contentWindow.focus();
                frame.contentWindow.print();
            });
        });
        byId('btnPdfUQ01').addEventListener('click', function() {
            clearTimeout(previewTimer);
            const payload = collectPayload();
            const warnings = validatePayload(payload);
            renderWarnings(warnings);
            if (!confirmWarnings(warnings)) return;
            byId('uqPdfPayload').value = JSON.stringify(payload);
            const form = byId('uqPdfPostForm');
            form.action = appRoute('/uy-quyen-nhan-hang/pdf');
            form.submit();
        });

        byId('uqPreviewFrame').addEventListener('load', function() {
            byId('uqPreviewState').textContent = 'Đã cập nhật';
            if (pendingPreviewAction) {
                const action = pendingPreviewAction;
                pendingPreviewAction = null;
                window.setTimeout(action, 80);
            }
        });

        loadPersonnelProfiles().catch(function(error) {
            profilesLoaded = false;
            byId('uqAuthorizerProfile').innerHTML = '<option value="">Không thể tải hồ sơ</option>';
            byId('uqRecipientProfile').innerHTML = '<option value="">Không thể tải hồ sơ</option>';
            byId('uqAuthorizerSummary').textContent = error.message || 'Không thể tải hồ sơ nhân sự.';
            byId('uqRecipientSummary').textContent = 'Vui lòng tải lại trang hoặc đăng nhập lại.';
            currentWarnings();
            submitPreview();
        });
    }

    function switchUq01Mode(selectedValue) {
        const column = document.querySelector('.uq01-page-column');
        const isUq01 = selectedValue === 'uq01';
        if (column) column.classList.toggle('uq01-mode', isUq01);
        if (isUq01) {
            initialize();
            if (profilesLoaded) submitPreview();
        }
    }

    document.querySelectorAll('input[name="formType"]').forEach(function(radio) {
        radio.addEventListener('change', function() {
            switchUq01Mode(this.value);
        });
    });

    window.addEventListener('message', function(event) {
        if (event.origin !== window.location.origin) return;
        if (!event.data || event.data.type !== 'uq01-preview-metrics') return;
        byId('uqOverflowAlert').hidden = !event.data.overflow;
    });

    const requestedForm = new URLSearchParams(window.location.search).get('form');
    const requestedRadio = requestedForm
        ? document.querySelector('input[name="formType"][value="' + CSS.escape(requestedForm) + '"]')
        : null;
    if (requestedRadio) {
        requestedRadio.checked = true;
        requestedRadio.dispatchEvent(new Event('change', { bubbles: true }));
    } else {
        const selected = document.querySelector('input[name="formType"]:checked');
        switchUq01Mode(selected ? selected.value : '');
    }
})();
