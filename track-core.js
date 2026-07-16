/*! track-core.js — Engine tracking CAPI dùng chung cho MỌI landing (Phil + Malay)
 *  Cấu hình riêng từng trang: đặt window.LANDING_CONFIG TRƯỚC khi nạp file này.
 *  Đã vá: race flushInFlight (gửi submit trực tiếp keepalive) + dedup 6h.
 *  Sửa lỗi CHỈ sửa file này -> mọi landing tự cập nhật (nhớ bump ?v= khi nạp).
 */
    (function () {
    'use strict';

    
    var LC = (window.LANDING_CONFIG || {});
    if (!LC.WEBHOOK_URL) { try { console.warn("[TrackingScript] Thiếu window.LANDING_CONFIG.WEBHOOK_URL"); } catch(e){} }
    const WEBHOOK_CONFIG = Object.freeze({ URL: LC.WEBHOOK_URL, AUTH: LC.WEBHOOK_AUTH || null });

    const CONFIG = Object.freeze({
        WEBHOOK_URL: WEBHOOK_CONFIG.URL,
        WEBHOOK_AUTH: WEBHOOK_CONFIG.AUTH,

        EVENT_ID_PREFIX: 'KhoiND',
        SAFE_EVENT_ID_MAX: 80,

        PHONE: {
            COUNTRY_CALLING_CODE: (LC.COUNTRY_CALLING_CODE || '63'),
            LOCAL_MIN_DIGITS: 9,
            LOCAL_MAX_DIGITS: 11
        },
        CURRENCY: (LC.CURRENCY || 'PHP'),
        ACTION_SOURCE: 'website',

        DEBUG: location.hostname === 'localhost' || location.search.indexOf('debug=1') !== -1,

        TIME_THRESHOLDS: [3, 10, 30, 60, 120, 180, 300],
        SCROLL_THRESHOLDS: [25, 50, 75, 100],

        BATCH_INTERVAL: 15000,
        MAX_EVENTS_PER_BATCH: 20,
        MAX_QUEUE_SIZE: 200,
        MAX_RETRY: 8,
        IP_PREFETCH_TIMEOUT_MS: 5000,
        SUBMIT_FLUSH_TIMEOUT_MS: 3500,
        SUBMIT_QUEUE_MAX_AGE_MS: 120000,

        ENABLE_LOCAL_STORAGE_QUEUE: true,
        QUEUE_STORAGE_KEY: '__landing_tracking_queue__',

        HASH_PII_ON_CLIENT: false,

        FORM_RULE: {
            MIN_NAME: 3,
            MIN_PHONE: 8,
            MIN_ADDRESS: 3,
            DEBOUNCE_MS: 400
        },

        COOLDOWN_MS: {
            AddToCart: 1500,
            SubmitCluster: 4000
        },

        SUBMIT_EVENTS: ['Lead', 'CompleteRegistration', 'Mua_form'],
        SUBMIT_DISPATCH_PIXELS: true,
 
        PRODUCTS: (LC.PRODUCTS || {
            option_1: { name: 'Buy 1 Set 5m: ₱1999 + Free Shipping & COD', price: 1999, sku: 'OTC10M', quantity: 1, content_id: 'OTC10M-OPT1' },
            option_2: { name: 'Buy 1 Set 10m: ₱2299 + Free Shipping & COD', price: 2299, sku: 'OTC10M', quantity: 1, content_id: 'OTC10M-OPT2' }
        })
    });

    const SELECTORS = Object.freeze({
        addToCartButton: '[class*="addtocart"]',
        submitButton: '.KhoiND_submit',
        quantityRadio: 'input[type="radio"]',
        nameInput: 'input[name="name"]',
        phoneInput: 'input[name="phone"]',
        addressInput: 'input[name="address"]'
    });

    const state = {
        sessionId: makeId('sess'),
        queue: loadQueue(),
        retryCount: 0,
        retryTimer: null,
        flushInFlight: false,
        webhookDisabled: false,
        unloadFlushStarted: false,

        firedOnce: Object.create(null),
        scrollFired: Object.create(null),
        cooldownMap: Object.create(null),
        lastAddToCartByScope: [],

        interacted: false,
        typingStarted: false,
        initiateCheckoutFired: false,
        inputDebounceTimer: null,
        allowNativeSubmitOnce: false,
        submissionId: null,
        submitClusterFired: false,

        ip: null,
        country: null,
	externalIdHashed: null,
	countryHashed: null
    };

    const userContextBase = {
        ua: navigator.userAgent || '',
        url: location.href,
        ref: document.referrer || '',
        fbp: getFBP(),
        fbc: getFBC(),
        ttp: getCookie('_ttp') || null,
        ttclid: getTTClid()
    };

    log('init', { sessionId: state.sessionId, queueLength: state.queue.length });

    bootstrap();

    async function bootstrap() {
        // Chạy lấy IP/Country trước, sau đó mới đăng ký tracking
        await prefetchIpFirst();
	// Hash 1 lần, dùng cho mọi event sau này
        let _stableCid = getCookie('_lp_cid');
        if (!_stableCid) { _stableCid = makeId('cid'); persistCookie('_lp_cid', _stableCid, 60 * 60 * 24 * 180); }
        state.externalIdHashed = await sha256Safe(_stableCid);
	if (state.country) {
            state.countryHashed = await sha256Safe(state.country.toLowerCase());
    	}

        ensureDefaultProductSelection(document);

        registerTopFunnelEvents();
        registerMidFunnelEvents();
        registerBottomFunnelEvents();
        startBatchLoop();
        registerUnloadFlush();
    }

    // ----------------------- Lõi tracking -----------------------

    function enqueueEvent(eventName, customData, options) {
        const opts = options || {};
        const eventId = sanitizeEventId(opts.eventId || makeEventId(eventName));
        const finalCustomData = resolveEventCustomData(eventName, customData);

        // ★ GIẢM TẢI SERVER: Time_/Scroll_ chỉ bắn pixel trình duyệt (Meta vẫn nhận đủ tín hiệu),
        //   KHÔNG gửi về webhook nữa — n8n đang quá tải vì các event phụ này (sập -> mất khách).
        if (/^(Time|Scroll)_/i.test(eventName)) {
            if (opts.dispatchPixels !== false) {
                dispatchBrowserPixels(eventName, finalCustomData, eventId, buildUserData(opts.userData));
            }
            return;
        }

        const payload = {
            event_name: eventName,
            event_time: Math.floor(Date.now() / 1000),
            event_id: eventId,
            action_source: CONFIG.ACTION_SOURCE,
            event_source_url: location.href,
            user_data: buildUserData(opts.userData),
            custom_data: finalCustomData,
            context: {
                session_id: state.sessionId,
                page_title: document.title || '',
                ts_ms: Date.now()
            }
        };

        if (state.queue.length >= CONFIG.MAX_QUEUE_SIZE) {
            state.queue.shift();
        }

        state.queue.push(payload);
        persistQueue();

        log('enqueue', { event: eventName, eventId: eventId, queueLength: state.queue.length });
        if (opts.dispatchPixels !== false) {
            dispatchBrowserPixels(eventName, finalCustomData, eventId, payload.user_data);
        }
    }

    function buildUserData(extraUserData) {
        const user = {
            ua: userContextBase.ua,
            fbp: userContextBase.fbp,
            fbc: userContextBase.fbc,
            ttp: userContextBase.ttp,
            ttclid: userContextBase.ttclid,
            // client_ip_address: BỎ — n8n sẽ lấy IP thật của khách từ header request
            client_country_code: state.country,
            client_country: state.country,
            client_user_agent: userContextBase.ua,
	    external_id: state.externalIdHashed,
	    country: state.countryHashed
        };

        if (extraUserData && typeof extraUserData === 'object') {
            return mergeObjects(user, extraUserData);
        }

        return user;
    }

    function extractMetaPixelUserData(userData) {
        const source = userData && typeof userData === 'object' ? userData : {};
        const metaUserData = {};

        if (source.em) metaUserData.em = source.em;
        if (source.ph) metaUserData.ph = source.ph;
        if (source.fn) metaUserData.fn = source.fn;
        if (source.ln) metaUserData.ln = source.ln;

        return metaUserData;
    }

    const DEFAULT_SUBMIT_EVENTS = Object.freeze(['Lead', 'CompleteRegistration', 'Purchase']);
    const SUBMIT_EVENT_ALLOWLIST = Object.freeze({
        Lead: true,
        CompleteRegistration: true,
        Purchase: true,
	Mua_form: true
    });

    const COMMERCE_EVENT_ALLOWLIST = Object.freeze({
        AddToCart: true,
        InitiateCheckout: true,
        Lead: true,
        CompleteRegistration: true,
        Purchase: true
    });

    async function enqueueSubmitCluster(formData, productData, submitId) {
	if (state.submitClusterFired) {
            log('submit_cluster_skipped', { submitId: submitId });
            return;
        }
        // ★ VÁ NUỐT LEAD: chỉ chặn trùng khi CÙNG SĐT đặt lại trong 6h trên CÙNG trang.
        //   (Trước đây: 1 khoá chung cho CẢ TÊN MIỀN → đặt ở landing A xong,
        //    landing B nuốt Lead/Complete của khách khác → báo cáo thiếu mua.)
        const _phGuard = String((formData && formData.phone) || '').replace(/\D/g, '');
        const _prevRec = loadSubmissionId();
        if (_prevRec && _prevRec.ph && _phGuard && _prevRec.ph === _phGuard) {
            log('submit_cluster_skipped_same_phone', { submitId: submitId });
            return;
        }
        state.submitClusterFired = true;
        persistSubmissionId(submitId, _phGuard);
        const baseData = {
            name: formData.name,
            phone: formData.phone,
            address: formData.address
        };

        const pii = await preparePii(baseData);
        const merged = mergeObjects(pii, productData);  // đổi thứ tự: productData sau, ưu tiên hơn
        const submitEvents = getConfiguredSubmitEvents();
        const submitUserData = await buildSubmitUserData(formData);

        for (let i = 0; i < submitEvents.length; i++) {
            const eventName = submitEvents[i];
	    if (eventName === 'Mua_form') continue;   // ← Skip, đã được xử lý riêng phía dưới
            const _cid = (productData.content_ids || [])[0] || 'UNKNOWN';
            enqueueEvent(eventName, merged, {
                eventId: 'form_' + submitId + '_' + makeSubmitEventSuffix(eventName),
                userData: submitUserData,
                dispatchPixels: CONFIG.SUBMIT_DISPATCH_PIXELS
            });
        }

        if (submitEvents.indexOf('Mua_form') !== -1) {
            enqueueWebhookMuaFormItemEvents(merged, submitUserData, submitId);
        }
    }

    function enqueueWebhookPurchaseItemEvents(mergedData, submitUserData, submitId) {
        const normalizedPurchase = normalizePurchasePayload(mergedData || {});
        const itemEvents = normalizedPurchase && Array.isArray(normalizedPurchase.contents)
            ? normalizedPurchase.contents
            : [];

        for (let i = 0; i < itemEvents.length; i++) {
            const item = itemEvents[i] || {};
            const itemId = String(item.id || 'UNKNOWN');
            const eventName = 'Mua_' + itemId;
            const itemPayload = {
                value: Number(item.item_price || 0),
                currency: CONFIG.CURRENCY,
                content_name: String(item.content_name || itemId),
                content_ids: [itemId],
                contents: [{
                    id: itemId,
                    quantity: Number(item.quantity || 1),
                    item_price: Number(item.item_price || 0),
                    content_name: String(item.content_name || itemId)
                }],
                content_type: 'product'
            };

            enqueueEvent(eventName, itemPayload, {
                eventId: submitId + ':mua_' + itemId,
                userData: submitUserData,
                dispatchPixels: CONFIG.SUBMIT_DISPATCH_PIXELS
            });
        }
    }

    function enqueueWebhookMuaFormItemEvents(mergedData, submitUserData, submitId) {
        const normalizedPurchase = normalizePurchasePayload(mergedData || {});
        const itemEvents = normalizedPurchase && Array.isArray(normalizedPurchase.contents)
            ? normalizedPurchase.contents
            : [];

        for (let i = 0; i < itemEvents.length; i++) {
            const item = itemEvents[i] || {};
            const itemId = String(item.id || 'UNKNOWN');
            const eventName = 'Mua_form_' + itemId;
            const itemPayload = {
                value: Number(item.item_price || 0),
                currency: CONFIG.CURRENCY,
                content_name: String(item.content_name || itemId),
                content_ids: [itemId],
                contents: [{
                    id: itemId,
                    quantity: Number(item.quantity || 1),
                    item_price: Number(item.item_price || 0),
                    content_name: String(item.content_name || itemId)
                }],
                content_type: 'product'
            };

            enqueueEvent(eventName, itemPayload, {
                eventId: submitId + ':mua_form_' + itemId,
                userData: submitUserData,
                dispatchPixels: CONFIG.SUBMIT_DISPATCH_PIXELS
            });
        }
    }

    async function buildSubmitUserData(formData) {
        const parts = splitNameForSubmitUserData(formData && formData.name);
        const normalizedPhone = normalizePhoneForSubmitUserData(formData && formData.phone);

        const userData = {};

        if (parts.fn) userData.fn = await sha256Safe(parts.fn);
        if (parts.ln) userData.ln = await sha256Safe(parts.ln);
        if (normalizedPhone.meta) userData.ph = await sha256Safe(normalizedPhone.meta);
        if (normalizedPhone.tiktok) userData.ph_tiktok = await sha256Safe(normalizedPhone.tiktok);

        return userData;
    }

    function splitNameForSubmitUserData(name) {
        const normalized = String(name || '')
            .trim()
            .replace(/\s+/g, ' ')
            .toLowerCase();

        if (!normalized) return { fn: '', ln: '' };

        const parts = normalized.split(' ').filter(Boolean);
        if (parts.length === 1) {
            return { fn: parts[0], ln: '' };
        }

        return {
            fn: parts[0],
            ln: parts.slice(1).join(' ')
        };
    }

    function normalizePhoneForSubmitUserData(phone) {
        return normalizePhoneByCountry(phone);
    }

    function getConfiguredSubmitEvents() {
        const configured = Array.isArray(CONFIG.SUBMIT_EVENTS) ? CONFIG.SUBMIT_EVENTS : [];
        const deduped = [];

        for (let i = 0; i < configured.length; i++) {
            const raw = configured[i];
            const eventName = typeof raw === 'string' ? raw.trim() : '';
            if (!eventName) continue;
            if (!SUBMIT_EVENT_ALLOWLIST[eventName]) continue;
            if (deduped.indexOf(eventName) !== -1) continue;
            deduped.push(eventName);
        }

        return deduped.length ? deduped : DEFAULT_SUBMIT_EVENTS.slice();
    }

    function makeSubmitEventSuffix(eventName) {
        return String(eventName || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '') || 'submit_event';
    }

    function findAddToCartButton(target) {
        if (!target || !target.closest) return null;

        const candidate = target.closest(SELECTORS.addToCartButton);
        if (!candidate || !candidate.classList) return null;

        for (let i = 0; i < candidate.classList.length; i++) {
            if (/^addtocart\d+$/i.test(candidate.classList[i])) {
                return candidate;
            }
        }

        return null;
    }

    function isOptionSelectionTarget(target) {
        if (!target || !target.closest) return false;
        return !!target.closest('.ladi-form-checkbox, .ladi-form-checkbox-box-item, .ladi-form-checkbox-item, span[data-checked], ' + SELECTORS.quantityRadio);
    }

    function getAddToCartScope(scope) {
        return scope && scope.nodeType === 1 ? scope : document;
    }

    function getProductTrackingKey(product) {
        if (!product) return '';
        return String(product.content_id || product.sku || product.name || '').trim();
    }

    function getLastAddToCartTrackingKey(scope) {
        const root = getAddToCartScope(scope);

        for (let i = 0; i < state.lastAddToCartByScope.length; i++) {
            const entry = state.lastAddToCartByScope[i];
            if (entry && entry.scope === root) {
                return entry.key || '';
            }
        }

        return '';
    }

    function rememberLastAddToCartProduct(scope, product) {
        const root = getAddToCartScope(scope);
        const key = getProductTrackingKey(product);
        if (!key) return;

        for (let i = 0; i < state.lastAddToCartByScope.length; i++) {
            const entry = state.lastAddToCartByScope[i];
            if (entry && entry.scope === root) {
                entry.key = key;
                return;
            }
        }

        state.lastAddToCartByScope.push({ scope: root, key: key });
    }

    function maybeTrackAddToCartFromOptionSelection(scope) {
        const root = getAddToCartScope(scope);
        const lastKey = getLastAddToCartTrackingKey(root);
        if (!lastKey) return;

        const product = resolveSelectedProduct(root);
        const nextKey = getProductTrackingKey(product);
        if (!nextKey || nextKey === lastKey) return;

        enqueueEvent('AddToCart', toProductPayload(product));
        rememberLastAddToCartProduct(root, product);
    }

    function validateSubmitFormData(formData, scope) {
        const data = formData || { name: '', phone: '', address: '' };
        if (!isValidFormData(data)) {
            return { ok: false, reason: 'form_data_invalid' };
        }

        const root = scope || document;
        const radios = root.querySelectorAll(SELECTORS.quantityRadio);
        if (radios && radios.length && !getSelectedQuantityRadio(root)) {
            return { ok: false, reason: 'product_not_selected' };
        }

        return { ok: true, reason: '' };
    }

    // ----------------------- Bộ xử lý theo phễu -----------------------

    function registerTopFunnelEvents() {
        once('ViewContent', function () {
            const viewContentProduct = CONFIG.PRODUCTS.option_1 || getLowestPriceProduct();
            const vcPayload = toProductPayload(viewContentProduct);
 	    // Đảm bảo luôn có value và currency dù product có price = 0
            if (!vcPayload.value || vcPayload.value <= 0) {
        	vcPayload.value = viewContentProduct.price || CONFIG.PRODUCTS.option_1.price || 109;
    }
    	    enqueueEvent('ViewContent', vcPayload);
        });

        CONFIG.TIME_THRESHOLDS.forEach(function (sec) {
            setTimeout(function () {
                once('Time_' + sec, function () {
                    enqueueEvent('Time_' + sec, { time_on_page_seconds: sec });
                });
            }, sec * 1000);
        });

        window.addEventListener('scroll', function () {
            const doc = document.documentElement;
            const scrollTop = window.pageYOffset || doc.scrollTop || 0;
            const max = (doc.scrollHeight || 1) - window.innerHeight;
            if (max <= 0) return;

            const percent = Math.round((scrollTop / max) * 100);
            CONFIG.SCROLL_THRESHOLDS.forEach(function (threshold) {
                if (percent >= threshold && !state.scrollFired[threshold]) {
                    state.scrollFired[threshold] = true;
                    enqueueEvent('Scroll_' + threshold, { scroll_percent: percent });
                }
            });
        }, { passive: true });
    }

    function registerMidFunnelEvents() {
        document.addEventListener('click', function (e) {
            const target = findAddToCartButton(e.target);
            if (!target) return;

            if (!allowByCooldown('AddToCart', 'global')) return;

            const scope = target.closest('form, .ladi-form') || document;
            const product = resolveSelectedProduct(scope);
            enqueueEvent('AddToCart', toProductPayload(product));
            rememberLastAddToCartProduct(scope, product);
        }, true);

        document.addEventListener('change', function (e) {
            if (!e.target || !e.target.matches) return;
            if (!e.target.matches(SELECTORS.quantityRadio)) return;

            const scope = e.target.closest('form, .ladi-form') || document;
            setTimeout(function () {
                maybeTrackAddToCartFromOptionSelection(scope);
            }, 0);
        }, true);
    }

    function registerBottomFunnelEvents() {
        document.addEventListener('focusin', function (e) {
            if (!e.target || !e.target.matches) return;
            if (e.target.matches(SELECTORS.nameInput + ',' + SELECTORS.phoneInput + ',' + SELECTORS.addressInput)) {
                state.interacted = true;
            }
        }, true);

        document.addEventListener('input', function (e) {
            if (!e.target || !e.target.matches) return;
            if (!e.target.matches(SELECTORS.nameInput + ',' + SELECTORS.phoneInput + ',' + SELECTORS.addressInput)) return;

            state.typingStarted = true;
            clearTimeout(state.inputDebounceTimer);
            state.inputDebounceTimer = setTimeout(function () {
                maybeFireInitiateCheckout(e.target.closest('form, .ladi-form') || document);
            }, CONFIG.FORM_RULE.DEBOUNCE_MS);
        }, true);

        document.addEventListener('blur', function (e) {
            if (!e.target || !e.target.matches) return;
            if (!e.target.matches(SELECTORS.addressInput)) return;

            const form = e.target.closest('form, .ladi-form') || document;
            maybeFireInitiateCheckout(form);
        }, true);

        // ★ 2026-07-16 — tách phần bắn cụm submit ra hàm dùng chung (KHÔNG đụng logic, copy y nguyên
        // từ handler click bên dưới) để đường 'submit' (Enter/nút sai class) gọi lại được
        // mà KHÔNG cần preventDefault. Đơn hàng là bất khả xâm phạm.
        async function fireSubmitClusterFromForm(form) {
            if (!allowByCooldown('SubmitCluster', 'global')) return false;
            const formData = getFormData(form);
            const validation = validateSubmitFormData(formData, form);
            if (!validation.ok) {
                log('submit_soft_pass', { reason: validation.reason, formData: formData });
                const _digits = String(formData.phone || '').replace(/D/g, '');
                if (_digits.length < 7) return false;
            }
            const product = resolveSelectedProduct(form);
            const productPayload = toProductPayload(product);
            if (!state.submissionId) state.submissionId = makeId('sub');
            const _phoneForId = normalizePhoneByCountry(formData.phone).meta || String(formData.phone || '').replace(/D+/g, '');
            const submitParentId = _phoneForId ? ('p' + (await sha256Safe(_phoneForId)).slice(0, 24)) : state.submissionId;
            state.webhookDisabled = false;
            state.retryCount = 0;
            clearTimeout(state.retryTimer);
            state.retryTimer = null;
            await enqueueSubmitCluster(formData, productPayload, submitParentId);
            try {
                const _batch = enrichEventsBeforeSend(state.queue.slice(0));
                if (_batch.length && CONFIG.WEBHOOK_URL) {
                    await Promise.race([
                        fetch(CONFIG.WEBHOOK_URL, {
                            method: 'POST',
                            headers: buildWebhookHeaders(),
                            body: JSON.stringify({ events: _batch, flush_reason: 'submit_bridge' }),
                            keepalive: true
                        }).catch(function () {}),
                        new Promise(function (r) { setTimeout(r, CONFIG.SUBMIT_FLUSH_TIMEOUT_MS); })
                    ]);
                }
            } catch (e) {}
            flushByBeacon();
            return true;
        }

        // ★ 2026-07-16 — BẮT MỌI CÚ SUBMIT (Enter/'Go' bàn phím, nút ẩn ladi-hidden, nút gắn sai class).
        // TUYỆT ĐỐI KHÔNG preventDefault -> đơn của khách luôn đi, tracking chạy song song.
        // Bắn đôi? Không: cooldown SubmitCluster (4s) tự chặn khi đường click đã bắn.
        document.addEventListener('submit', function (e) {
            try {
                const t = e.target;
                if (!t) return;
                const form = (t.closest && t.closest('form, .ladi-form')) || t;
                fireSubmitClusterFromForm(form).catch(function () {});
            } catch (bridgeErr) {}
        }, true);

        document.addEventListener('click', async function (e) {
            if (isOptionSelectionTarget(e.target)) return;

            const btn = e.target && e.target.closest ? e.target.closest(SELECTORS.submitButton) : null;
            if (!btn) return;

            if (state.allowNativeSubmitOnce) {
                state.allowNativeSubmitOnce = false;
                return;
            }

            if (!allowByCooldown('SubmitCluster', 'global')) return;

            const form = btn.closest('form, .ladi-form') || document;
            const formData = getFormData(form);
            const validation = validateSubmitFormData(formData, form);
            if (!validation.ok) {
                log('submit_soft_pass', { reason: validation.reason, formData: formData });
                // KHÔNG chặn gửi CAPI nữa: đơn đang được đặt thật (sẽ sang Pancake).
                // Chỉ bỏ qua khi THỰC SỰ không có SĐT (không thể là đơn hợp lệ).
                const _digits = String(formData.phone || '').replace(/\D/g, '');
                if (_digits.length < 7) return;
            }

            e.preventDefault();

            const product = resolveSelectedProduct(form);
            const productPayload = toProductPayload(product);
	    if (!state.submissionId) state.submissionId = makeId('sub');
            // (persist chuyển vào enqueueSubmitCluster — lưu kèm SĐT để chặn trùng đúng người)
            const _phoneForId = normalizePhoneByCountry(formData.phone).meta || String(formData.phone || '').replace(/\D+/g, '');
            const submitParentId = _phoneForId ? ('p' + (await sha256Safe(_phoneForId)).slice(0, 24)) : state.submissionId;
            // ★ Đảm bảo cụm submit LUÔN gửi được, kể cả khi trước đó webhook bị lỗi/tắt tạm (n8n chậm/lỗi lúc cao điểm)
            state.webhookDisabled = false;
            state.retryCount = 0;
            clearTimeout(state.retryTimer);
            state.retryTimer = null;
            await enqueueSubmitCluster(formData, productPayload, submitParentId);
            // ★ VÁ RỚT ĐƠN: gửi thẳng cụm submit bằng keepalive, KHÔNG phụ thuộc flushInFlight
            try {
                const _batch = enrichEventsBeforeSend(state.queue.slice(0));
                if (_batch.length && CONFIG.WEBHOOK_URL) {
                    await Promise.race([
                        fetch(CONFIG.WEBHOOK_URL, {
                            method: 'POST',
                            headers: buildWebhookHeaders(),
                            body: JSON.stringify({ events: _batch, flush_reason: 'submit_direct' }),
                            keepalive: true
                        }).catch(function () {}),
                        new Promise(function (r) { setTimeout(r, CONFIG.SUBMIT_FLUSH_TIMEOUT_MS); })
                    ]);
                }
            } catch (e) {}
            flushByBeacon();

            state.allowNativeSubmitOnce = true;
            try {
                btn.click();
            } catch (clickErr) {
                state.allowNativeSubmitOnce = false;
            }
        }, true);
    }

    function maybeFireInitiateCheckout(scope) {
        if (state.initiateCheckoutFired) return;
        if (!state.interacted || !state.typingStarted) return;

        const data = getFormData(scope || document);
        if (!isValidFormData(data)) return;

        state.initiateCheckoutFired = true;

        const product = resolveSelectedProduct(scope || document);
        const customData = mergeObjects(toProductPayload(product), {
            name: data.name,
            phone: data.phone,
            address: data.address
        });

        buildSubmitUserData(data).then(function(submitUserData) {
        enqueueEvent('InitiateCheckout', customData, {
            userData: submitUserData
            });
        });
    }

    // ----------------------- Hàng đợi và gửi dữ liệu -----------------------

    function startBatchLoop() {
        setInterval(function () {
            flushQueue(false);
        }, CONFIG.BATCH_INTERVAL);
    }

    function flushQueue(forceAll) {
        if (state.flushInFlight) return;
        if (state.webhookDisabled) {
            log('skip_flush_webhook_disabled');
            return;
        }
        if (!CONFIG.WEBHOOK_URL || CONFIG.WEBHOOK_URL.indexOf('your-webhook-endpoint') !== -1) {
            log('skip_flush_missing_webhook');
            return;
        }
        if (!state.queue.length) return;

        state.flushInFlight = true;

        const maxItems = forceAll ? state.queue.length : Math.min(CONFIG.MAX_EVENTS_PER_BATCH, state.queue.length);
        const batch = enrichEventsBeforeSend(state.queue.slice(0, maxItems));

        fetch(CONFIG.WEBHOOK_URL, {
            method: 'POST',
            headers: buildWebhookHeaders(),
            body: JSON.stringify({ events: batch })
        })
            .then(function (res) {
                if (!res.ok) throw new Error('HTTP ' + res.status);

                state.queue.splice(0, maxItems);
                state.retryCount = 0;
                persistQueue();

                log('flush_ok', { sent: maxItems, remaining: state.queue.length });
            })
            .catch(function (err) {
                state.retryCount += 1;
                scheduleRetry(err);
            })
            .finally(function () {
                state.flushInFlight = false;
            });
    }

    function flushQueueWithTimeout(forceAll, timeoutMs) {
        const timeout = Number(timeoutMs || 0);
        if (!timeout || timeout < 1) {
            return flushQueueNow(forceAll);
        }

        return Promise.race([
            flushQueueNow(forceAll),
            new Promise(function (resolve) {
                setTimeout(function () { resolve(false); }, timeout);
            })
        ]);
    }

    function flushQueueNow(forceAll) {
        if (state.flushInFlight) return Promise.resolve(false);
        if (state.webhookDisabled) {
            log('skip_flush_now_webhook_disabled');
            return Promise.resolve(false);
        }
        if (!CONFIG.WEBHOOK_URL || CONFIG.WEBHOOK_URL.indexOf('your-webhook-endpoint') !== -1) {
            log('skip_flush_now_missing_webhook');
            return Promise.resolve(false);
        }
        if (!state.queue.length) return Promise.resolve(true);

        state.flushInFlight = true;

        const maxItems = forceAll ? state.queue.length : Math.min(CONFIG.MAX_EVENTS_PER_BATCH, state.queue.length);
        const batch = enrichEventsBeforeSend(state.queue.slice(0, maxItems));

        return fetch(CONFIG.WEBHOOK_URL, {
            method: 'POST',
            headers: buildWebhookHeaders(),
            body: JSON.stringify({ events: batch }),
            keepalive: true
        })
            .then(function (res) {
                if (!res.ok) throw new Error('HTTP ' + res.status);

                state.queue.splice(0, maxItems);
                state.retryCount = 0;
                persistQueue();

                log('flush_now_ok', { sent: maxItems, remaining: state.queue.length });
                return true;
            })
            .catch(function (err) {
                state.retryCount += 1;
                scheduleRetry(err);
                return false;
            })
            .finally(function () {
                state.flushInFlight = false;
            });
    }

    function scheduleRetry(err) {
        log('flush_fail', {
            error: String(err && err.message ? err.message : err),
            retry: state.retryCount
        });

        if (state.retryCount >= CONFIG.MAX_RETRY) {
            state.webhookDisabled = true;
            clearTimeout(state.retryTimer);
            state.retryTimer = null;
            log('webhook_disabled_after_max_retry', { retry: state.retryCount, maxRetry: CONFIG.MAX_RETRY });
            return;
        }

        clearTimeout(state.retryTimer);
        const delay = Math.min(Math.pow(2, state.retryCount) * 500, 15000);
        state.retryTimer = setTimeout(function () {
            flushQueue(false);
        }, delay);
    }

    function registerUnloadFlush() {
        window.addEventListener('visibilitychange', function () {
            if (document.visibilityState === 'hidden') {
                flushByBeacon();
            }
        });

        window.addEventListener('pagehide', function () {
            flushByBeacon();
        });

        window.addEventListener('beforeunload', function () {
            flushByBeacon();
        });
    }

    function flushByBeacon() {
        // ★ VÁ CAPTURE: KHÔNG bỏ cuộc khi đang kẹt (flushInFlight/webhookDisabled).
        //   Đây là lần gửi CUỐI lúc khách rời trang → phải cố gửi cho được
        //   (keepalive sống qua điều hướng + n8n dedup theo event_id nên không sợ trùng).
        state.webhookDisabled = false;
        if (state.unloadFlushStarted) return;
        if (!state.queue.length) return;
        if (!CONFIG.WEBHOOK_URL || CONFIG.WEBHOOK_URL.indexOf('your-webhook-endpoint') !== -1) return;

        const batch = enrichEventsBeforeSend(state.queue.slice(0));
        const body = JSON.stringify({ events: batch, flush_reason: 'unload' });

        state.unloadFlushStarted = true;

        try {
            if (navigator.sendBeacon && !hasWebhookAuth()) {
                const blob = new Blob([body], { type: 'application/json' });
                const ok = navigator.sendBeacon(CONFIG.WEBHOOK_URL, blob);
                if (ok) {
                    state.queue.splice(0, batch.length);
                    state.retryCount = 0;
                    clearTimeout(state.retryTimer);
                    state.retryTimer = null;
                    persistQueue();
                    return;
                }
            }
        } catch (e) {
            // bỏ qua lỗi và chuyển sang phương án dự phòng
        }

        try {
            fetch(CONFIG.WEBHOOK_URL, {
                method: 'POST',
                headers: buildWebhookHeaders(),
                body: body,
                keepalive: true
            });

            state.queue.splice(0, batch.length);
            state.retryCount = 0;
            clearTimeout(state.retryTimer);
            state.retryTimer = null;
            persistQueue();
        } catch (e2) {
            state.unloadFlushStarted = false;
            // bỏ qua lỗi
        }
    }

    // ----------------------- Bộ chọn sản phẩm -----------------------

    function ensureDefaultProductSelection(scope) {
        const root = scope || document;
        const radios = root.querySelectorAll(SELECTORS.quantityRadio);
        if (!radios || !radios.length) return;

        const hasChecked = getSelectedQuantityRadio(root);
        if (hasChecked) return;

        const productKeys = Object.keys(CONFIG.PRODUCTS || {});
        if (!productKeys.length) {
            radios[0].checked = true;
            return;
        }

        const firstProduct = CONFIG.PRODUCTS[productKeys[0]];

        for (let i = 0; i < radios.length; i++) {
            const matched = findProductByRadioValue(radios[i].value);
            if (matched && firstProduct && matched.content_id === firstProduct.content_id) {
                radios[i].checked = true;
                return;
            }
        }

        radios[0].checked = true;
    }

    function resolveSelectedProduct(scope) {
        const root = scope || document;
        const selected = getSelectedQuantityRadio(root);

        if (selected && selected.value) {
            const matched = findProductByRadioValue(selected.value);
            if (matched) return matched;
        }

        return getLowestPriceProduct();
    }

    function getSelectedQuantityRadio(scope) {
        const root = scope || document;
        const checked = root.querySelector(SELECTORS.quantityRadio + ':checked');
        if (checked) return checked;

        const radios = root.querySelectorAll(SELECTORS.quantityRadio);
        for (let i = 0; i < radios.length; i++) {
            const radio = radios[i];
            if (radio.checked) return radio;

            const item = radio.closest ? radio.closest('.ladi-form-checkbox-item') : null;
            const visualChecked = item && item.querySelector && item.querySelector('span[data-checked="true"]');
            if (visualChecked) return radio;
        }

        return null;
    }

    function findProductByRadioValue(rawValue) {
        const value = String(rawValue || '').trim();
        if (!value) return null;

        if (CONFIG.PRODUCTS[value]) {
            return CONFIG.PRODUCTS[value];
        }

        const normalizedInput = normalizeProductMatchValue(value);
        const keys = Object.keys(CONFIG.PRODUCTS || {});

        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            const product = CONFIG.PRODUCTS[key];
            const productName = String((product && product.name) || '').trim();

            if (productName && productName === value) {
                return product;
            }

            if (normalizeProductMatchValue(key) === normalizedInput) {
                return product;
            }

            if (productName && normalizeProductMatchValue(productName) === normalizedInput) {
                return product;
            }
        }

        return null;
    }

    function normalizeProductMatchValue(input) {
        let text = String(input || '').trim();
        if (!text) return '';

        try {
            text = text.normalize('NFKC');
        } catch (e) {
            // ignore if browser does not support normalize
        }

        return text.toLowerCase().replace(/\s+/g, ' ');
    }

    function getLowestPriceProduct() {
        const keys = Object.keys(CONFIG.PRODUCTS || {});
        if (!keys.length) return { name: 'Unknown Product', price: 0, sku: 'UNKNOWN', quantity: 1, content_id: 'UNKNOWN' };

        let min = CONFIG.PRODUCTS[keys[0]];
        for (let i = 1; i < keys.length; i++) {
            const p = CONFIG.PRODUCTS[keys[i]];
            if (Number(p.price || 0) < Number(min.price || 0)) min = p;
        }
        return min;
    }

    function toProductPayload(product) {
        const p = product || getLowestPriceProduct();
        return {
            value: Number(p.price || 0),
            currency: CONFIG.CURRENCY,
            content_name: p.name || p.sku || 'Product',
            content_ids: [p.content_id || p.sku || 'UNKNOWN'],
            content_type: 'product',
            quantity: Number(p.quantity || 1),
            sku: p.sku || null
        };
    }

    function resolveEventCustomData(eventName, customData) {
        const data = customData || {};
        if (!shouldAttachProductContext(eventName)) {
            return data;
        }

        const selectedProductPayload = toProductPayload(resolveSelectedProduct(document));
        return mergeObjects(selectedProductPayload, data);
    }

    function shouldAttachProductContext(eventName) {
        const name = String(eventName || '');
        if (!name) return false;
        if (COMMERCE_EVENT_ALLOWLIST[name]) return true;
        return /^Mua_/i.test(name);
    }

    // ----------------------- Hàm hỗ trợ form -----------------------

    function getFormData(scope) {
        const root = scope || document;
        const rawPhone = (root.querySelector(SELECTORS.phoneInput) || {}).value || '';
        const cleanPhone = String(rawPhone).replace(/\D+/g, '');   // ← xoá MỌI ký tự không phải số (kể cả + ở giữa)
        return {
            name: (root.querySelector(SELECTORS.nameInput) || {}).value ? root.querySelector(SELECTORS.nameInput).value.trim() : '',
            phone: cleanPhone,
            address: (root.querySelector(SELECTORS.addressInput) || {}).value ? root.querySelector(SELECTORS.addressInput).value.trim() : ''
        };
    }

    function isValidFormData(data) {
        const name = (data && data.name) || '';
        const phone = (data && data.phone) || '';
        const address = (data && data.address) || '';

        return (
            name.length >= CONFIG.FORM_RULE.MIN_NAME &&
            phone.length >= CONFIG.FORM_RULE.MIN_PHONE &&
            address.length >= CONFIG.FORM_RULE.MIN_ADDRESS
        );
    }

    // ----------------------- Hàm hỗ trợ định danh / PII -----------------------

    async function preparePii(data) {
        if (!CONFIG.HASH_PII_ON_CLIENT) return data;

        return {
            name: await sha256Safe(data.name),
            phone: await sha256Safe(data.phone),
            address: await sha256Safe(data.address)
        };
    }

    async function sha256Safe(input) {
        const text = String(input || '').trim().toLowerCase();
        if (!text) return '';

        try {
            if (!window.crypto || !window.crypto.subtle) return text;
            const encoded = new TextEncoder().encode(text);
            const hash = await window.crypto.subtle.digest('SHA-256', encoded);
            const bytes = Array.prototype.slice.call(new Uint8Array(hash));
            return bytes.map(function (b) { return b.toString(16).padStart(2, '0'); }).join('');
        } catch (e) {
            return text;
        }
    }

    function prefetchIpFirst() {
        const timeoutMs = CONFIG.IP_PREFETCH_TIMEOUT_MS || 5000;
        const providers = [
            function fromDbIp() {
                return fetchWithTimeout('https://api.db-ip.com/v2/free/self', timeoutMs)
                    .then(function (j) {
                        return {
                            ip: j && j.ipAddress ? j.ipAddress : null,
                            country: j && j.countryCode ? j.countryCode : null
                        };
                    });
            },
            function fromIpify() {
                return fetchWithTimeout('https://api64.ipify.org?format=json', timeoutMs)
                    .then(function (j) {
                        return { ip: j && j.ip ? j.ip : null, country: null };
                    });
            },
            function fromIpApiCo() {
                return fetchWithTimeout('https://ipapi.co/json/', timeoutMs)
                    .then(function (j) {
                        return {
                            ip: j && j.ip ? j.ip : null,
                            country: j && j.country ? j.country : null
                        };
                    });
            },
            function fromIpwho() {
                return fetchWithTimeout('https://ipwho.is/', timeoutMs)
                    .then(function (j) {
                        return {
                            ip: j && j.ip ? j.ip : null,
                            country: j && j.country_code ? j.country_code : null
                        };
                    });
            },
            function fromGeoDbJsonp() {
                return fetchGeoByJsonp(timeoutMs)
                    .then(function (j) {
                        return {
                            ip: j && (j.IPv4 || j.ip) ? (j.IPv4 || j.ip) : null,
                            country: j && (j.country_code || j.countryCode) ? (j.country_code || j.countryCode) : null
                        };
                    });
            }
        ];

        return new Promise(function (resolve) {
            (function tryNext(index) {
                if (index >= providers.length) {
                    if (state.ip && !state.country) {
                        fetchCountryByIp(state.ip, timeoutMs)
                            .then(function (c) {
                                if (c) state.country = c;
                                if (!state.country) state.country = inferCountryFromLocale();
                                resolve();
                            })
                            .catch(function () {
                                if (!state.country) state.country = inferCountryFromLocale();
                                resolve();
                            });
                        return;
                    }

                    if (!state.country) state.country = inferCountryFromLocale();
                    resolve();
                    return;
                }

                providers[index]()
                    .then(function (info) {
                        if (info && info.ip && !state.ip) state.ip = info.ip;
                        if (info && info.country && !state.country) state.country = String(info.country).toUpperCase();

                        if (state.ip && state.country) {
                            resolve();
                            return;
                        }

                        tryNext(index + 1);
                    })
                    .catch(function (err) {
                        log('ip_provider_fail', { index: index, error: String(err && err.message ? err.message : err) });
                        tryNext(index + 1);
                    });
            })(0);
        });
    }

    function fetchCountryByIp(ip, timeoutMs) {
        if (!ip) return Promise.resolve(null);

        return fetchWithTimeout('https://ipapi.co/' + encodeURIComponent(ip) + '/json/', timeoutMs)
            .then(function (j) {
                return j && j.country ? String(j.country).toUpperCase() : null;
            })
            .catch(function () {
                return null;
            });
    }

    function fetchGeoByJsonp(timeoutMs) {
        return new Promise(function (resolve, reject) {
            const cbName = '__geoJsonpCb_' + Date.now() + '_' + Math.floor(Math.random() * 100000);
            const script = document.createElement('script');
            let finished = false;

            const timer = setTimeout(function () {
                cleanup();
                reject(new Error('JSONP timeout'));
            }, timeoutMs || 5000);

            function cleanup() {
                if (finished) return;
                finished = true;
                clearTimeout(timer);
                if (script.parentNode) script.parentNode.removeChild(script);
                try { delete window[cbName]; } catch (e) { window[cbName] = void 0; }
            }

            window[cbName] = function (data) {
                cleanup();
                resolve(data || {});
            };

            script.onerror = function () {
                cleanup();
                reject(new Error('JSONP load error'));
            };

            script.src = 'https://geolocation-db.com/jsonp/' + cbName;
            (document.head || document.documentElement).appendChild(script);
        });
    }

    function inferCountryFromLocale() {
        const lang = navigator.language || '';
        const parts = lang.split('-');
        if (parts.length >= 2 && parts[1]) return String(parts[1]).toUpperCase();
        return null;
    }

    function fetchWithTimeout(url, timeoutMs) {
        const ctrl = new AbortController();
        const timer = setTimeout(function () { ctrl.abort(); }, timeoutMs || 2500);

        return fetch(url, { signal: ctrl.signal })
            .then(function (res) {
                if (!res.ok) throw new Error('HTTP ' + res.status);
                return res.json();
            })
            .finally(function () {
                clearTimeout(timer);
            });
    }

    function enrichEventsBeforeSend(events) {
        return (events || []).map(function (ev) {
            const userData = ev && ev.user_data ? ev.user_data : {};
            if (!userData.client_country_code && state.country) {
                userData.client_country_code = state.country;
            }
            if (!userData.client_country && state.country) {
                userData.client_country = state.country;
            }

            ev.user_data = userData;
            return ev;
        });
    }

    // ----------------------- Đồng bộ Meta Pixel + TikTok Pixel -----------------------

    function dispatchBrowserPixels(eventName, customData, eventId, pixelUserData) {
        const payload = customData || {};
        const eventIdBase = sanitizeEventId(eventId || makeEventId(eventName || 'pixel_event'));
        const metaUserData = extractMetaPixelUserData(pixelUserData);

        if (eventName === 'Purchase') {
            dispatchPurchasePixels(payload, eventIdBase, pixelUserData);
            return;
        }

        const generic = normalizeGenericPixelPayload(payload);

        // ===== META PIXEL (standard/custom theo eventName) =====
        try {
            if (typeof fbq === 'function') {
                const metaMethod = isMetaStandardEvent(eventName) ? 'track' : 'trackCustom';
                // Lead & CompleteRegistration đo bằng LƯỢT, không gửi value/currency cho fbq
                const noValue = ['Lead', 'CompleteRegistration'].includes(eventName);
                const metaParams = {
                    content_type: 'product',
                    content_ids: generic.contentIds,
                    contents: generic.contents,
                    ...metaUserData
                };
                if (!noValue) {
                    metaParams.value = generic.value || Number(payload.value || 0);
                    metaParams.currency = CONFIG.CURRENCY;
                }

                if (/^Time_\d+$/i.test(eventName) && generic.timeOnPageSeconds > 0) {
                    metaParams.time_on_page_seconds = generic.timeOnPageSeconds;
                }

                if (/^Scroll_\d+$/i.test(eventName) && generic.scrollPercent >= 0) {
                    metaParams.scroll_percent = generic.scrollPercent;
                }

                fbq(metaMethod, eventName, metaParams, { eventID: eventIdBase });
            }
        } catch (err) {
            log('meta_pixel_error', { error: String(err && err.message ? err.message : err), event: eventName });
        }

        // ===== TIKTOK PIXEL =====
        try {
            if (typeof ttq === 'object' && ttq && typeof ttq.track === 'function') {
                const tiktokParams = {
                    value: generic.value,
                    currency: CONFIG.CURRENCY,
                    content_type: 'product',
                    content_ids: generic.contentIds,
                    contents: generic.contents.map(function (c) {
                        return {
                            content_id: c.id,
                            content_name: c.content_name,
                            quantity: c.quantity,
                            price: c.item_price
                        };
                    }),
                    event_id: eventIdBase
                };

                if (/^Time_\d+$/i.test(eventName) && generic.timeOnPageSeconds > 0) {
                    tiktokParams.time_on_page_seconds = generic.timeOnPageSeconds;
                }

                if (/^Scroll_\d+$/i.test(eventName) && generic.scrollPercent >= 0) {
                    tiktokParams.scroll_percent = generic.scrollPercent;
                }

                ttq.track(eventName, tiktokParams);
            }
        } catch (err2) {
            log('tiktok_pixel_error', { error: String(err2 && err2.message ? err2.message : err2), event: eventName });
        }
    }

    function dispatchPurchasePixels(customData, eventIdBase, pixelUserData) {
        const payload = customData || {};
        const normalized = normalizePurchasePayload(payload);

        const normalizedPhone = normalizePhoneByCountry(payload.phone);
        const phoneMetaDigits = normalizedPhone.meta;
        const externalId = phoneMetaDigits || state.sessionId;
        const metaUserData = extractMetaPixelUserData(pixelUserData);

        // ===== META PIXEL =====
        try {
            if (typeof fbq === 'function') {
                const metaParams = {
                    value: normalized.totalPrice,
                    currency: CONFIG.CURRENCY,
                    content_ids: normalized.contentIds,
                    contents: normalized.contents,
                    content_type: 'product',
                    ...(externalId ? { external_id: externalId } : {}),
                    ...metaUserData
                };

                fbq('track', 'Purchase', metaParams, { eventID: eventIdBase });

                normalized.contents.forEach(function (item) {
                    const customMetaId = sanitizeEventId((eventIdBase + '__' + item.id).slice(0, CONFIG.SAFE_EVENT_ID_MAX));
                    const customParams = {
                        value: item.item_price,
                        currency: CONFIG.CURRENCY,
                        content_ids: [item.id],
                        contents: [item],
                        content_type: 'product',
                        ...(externalId ? { external_id: externalId } : {}),
                        ...metaUserData
                    };
                    fbq('trackCustom', 'Mua_' + item.id, customParams, { eventID: customMetaId });
                });
            }
        } catch (err) {
            log('meta_pixel_error', { error: String(err && err.message ? err.message : err), event: 'Purchase' });
        }

        // ===== TIKTOK PIXEL EVENTS =====
        try {
            if (typeof ttq === 'object' && ttq && typeof ttq.track === 'function') {
                ttq.track('CompletePayment', {
                    value: normalized.totalPrice,
                    currency: CONFIG.CURRENCY,
                    content_type: 'product',
                    content_ids: normalized.contentIds,
                    contents: normalized.contents.map(function (c) {
                        return {
                            content_id: c.id,
                            content_name: c.content_name,
                            quantity: c.quantity,
                            price: c.item_price
                        };
                    }),
                    event_id: eventIdBase
                });

                normalized.contents.forEach(function (item) {
                    ttq.track('Mua_' + item.id, {
                        content_type: 'product',
                        content_id: item.id,
                        content_name: item.content_name,
                        quantity: 1,
                        price: item.item_price,
                        value: item.item_price,
                        currency: CONFIG.CURRENCY,
                        event_id: sanitizeEventId((eventIdBase + '__' + item.id).slice(0, CONFIG.SAFE_EVENT_ID_MAX))
                    });
                });
            }
        } catch (err2) {
            log('tiktok_pixel_error', { error: String(err2 && err2.message ? err2.message : err2), event: 'Purchase' });
        }
    }

    function isMetaStandardEvent(eventName) {
        return eventName === 'ViewContent' ||
            eventName === 'AddToCart' ||
            eventName === 'InitiateCheckout' ||
            eventName === 'Lead' ||
            eventName === 'CompleteRegistration' ||
            eventName === 'Purchase';
    }

    function normalizeGenericPixelPayload(customData) {
        const data = customData || {};
        const baseContentIds = Array.isArray(data.content_ids) ? data.content_ids : [];
        const firstId = baseContentIds.length ? baseContentIds[0] : (data.content_id || data.sku || 'UNKNOWN');

        const contents = Array.isArray(data.contents) && data.contents.length
            ? data.contents.map(function (item, idx) {
                return {
                    id: String(item && (item.id || item.content_id || item.sku || (idx + 1))),
                    quantity: Number(item && item.quantity ? item.quantity : 1),
                    item_price: Number(item && (item.item_price || item.price || 0)),
                    content_name: String(item && (item.content_name || item.name || data.content_name || 'Product'))
                };
            })
            : [
                {
                    id: String(firstId),
                    quantity: Number(data.quantity || 1),
                    item_price: Number(data.item_price || data.price || data.value || 0),
                    content_name: String(data.content_name || data.sku || 'Product')
                }
            ];

        const rawScrollPercent = Number(data.scroll_percent);

        return {
            value: Number(data.value || 0),
            timeOnPageSeconds: Number(data.time_on_page_seconds || 0),
            scrollPercent: Number.isFinite(rawScrollPercent) ? rawScrollPercent : -1,
            contentIds: contents.map(function (c) { return c.id; }),
            contents: contents
        };
    }

    function normalizePurchasePayload(customData) {
        const data = customData || {};

        const contents = Array.isArray(data.contents) && data.contents.length
            ? data.contents.map(function (item, idx) {
                return {
                    id: String(item && (item.id || item.content_id || item.sku || idx + 1)),
                    quantity: Number(item && item.quantity ? item.quantity : 1),
                    item_price: Number(item && (item.item_price || item.price || 0)),
                    content_name: String(item && (item.content_name || item.name || data.content_name || 'Product'))
                };
            })
            : [
                {
                    id: String((Array.isArray(data.content_ids) && data.content_ids[0]) || data.content_id || data.sku || 'UNKNOWN'),
                    quantity: Number(data.quantity || 1),
                    item_price: Number(data.value || 0),
                    content_name: String(data.content_name || data.sku || 'Product')
                }
            ];

        const contentIds = contents.map(function (c) { return c.id; });

        let totalPrice = Number(data.value || 0);
        if (!totalPrice || totalPrice < 0) {
            totalPrice = contents.reduce(function (sum, item) {
                return sum + (Number(item.item_price || 0) * Number(item.quantity || 1));
            }, 0);
        }

        return {
            totalPrice: totalPrice,
            contentIds: contentIds,
            contents: contents
        };
    }

    function normalizePhoneByCountry(phone) {
        const rawDigits = String(phone || '').replace(/\D+/g, '');
        if (!rawDigits) return { meta: '', tiktok: '' };

        const phoneCfg = CONFIG.PHONE || {};
        const countryCode = String(phoneCfg.COUNTRY_CALLING_CODE || '').replace(/\D+/g, '');
        const localMinDigits = Number(phoneCfg.LOCAL_MIN_DIGITS || 9);
        const localMaxDigits = Number(phoneCfg.LOCAL_MAX_DIGITS || 11);

        if (!countryCode) {
            return {
                meta: rawDigits,
                tiktok: '+' + rawDigits
            };
        }

        let localDigits = rawDigits;

        if (localDigits.indexOf('00' + countryCode) === 0) {
            localDigits = localDigits.slice(2);
        }

        const startsWithCountryCode = localDigits.indexOf(countryCode) === 0;
        const hasLocalLeadingZero = localDigits.charAt(0) === '0';
        const isLocalLengthRange = localDigits.length >= localMinDigits && localDigits.length <= localMaxDigits;

        let national;

        // Trường hợp 1 (Malay mặc định): bắt đầu bằng 0 và dài 9-11 số -> bỏ 0, thêm 60
        if (hasLocalLeadingZero && isLocalLengthRange) {
            national = countryCode + localDigits.replace(/^0+/, '');
        }
        // Trường hợp 2: đã có mã quốc gia và đủ độ dài quốc gia + local tối thiểu -> giữ nguyên
        else if (startsWithCountryCode && localDigits.length >= (countryCode.length + localMinDigits)) {
            national = localDigits;
        }
        // Fallback an toàn
        else if (startsWithCountryCode) {
            national = localDigits;
        } else {
            national = countryCode + localDigits.replace(/^0+/, '');
        }

        const meta = national;         // chỉ digits, ví dụ 60346108999
        const tiktok = '+' + national; // E.164, ví dụ +60346108999
        return { meta: meta, tiktok: tiktok };
    }

    function normalizeEmail(email) {
        return String(email || '').trim().toLowerCase();
    }

    function splitName(name) {
        const full = String(name || '').trim().toLowerCase();
        if (!full) return { fn: '', ln: '' };

        const parts = full.split(/\s+/).filter(Boolean);
        if (parts.length === 1) return { fn: parts[0], ln: '' };

        return {
            fn: parts[0],
            ln: parts.slice(1).join(' ')
        };
    }

    function buildWebhookHeaders() {
        const headers = { 'Content-Type': 'application/json' };

        if (hasWebhookAuth()) {
            headers.Authorization = 'Basic ' + encodeBase64(CONFIG.WEBHOOK_AUTH.username + ':' + CONFIG.WEBHOOK_AUTH.password);
        }

        return headers;
    }

    function hasWebhookAuth() {
        return !!(
            CONFIG.WEBHOOK_AUTH &&
            typeof CONFIG.WEBHOOK_AUTH.username === 'string' &&
            typeof CONFIG.WEBHOOK_AUTH.password === 'string' &&
            CONFIG.WEBHOOK_AUTH.username.length
        );
    }

    function encodeBase64(input) {
        try {
            return btoa(unescape(encodeURIComponent(String(input || ''))));
        } catch (e) {
            return btoa(String(input || ''));
        }
    }

    // ----------------------- Hàm bảo vệ điều kiện -----------------------

    function once(key, fn) {
        if (state.firedOnce[key]) return;
        state.firedOnce[key] = true;
        fn();
    }

    function allowByCooldown(eventName, key) {
        const mapKey = eventName + ':' + key;
        const now = Date.now();
        const cooldown = CONFIG.COOLDOWN_MS[eventName] || 0;
        const last = state.cooldownMap[mapKey] || 0;

        if (now - last < cooldown) return false;

        state.cooldownMap[mapKey] = now;
        return true;
    }

    // ----------------------- Tiện ích dùng chung -----------------------

    function makeEventId(eventName) {
        return [
            CONFIG.EVENT_ID_PREFIX,
            eventName,
            state.sessionId,
            Date.now(),
            Math.random().toString(36).slice(2, 8)
        ].join('.');
    }

    function sanitizeEventId(input) {
        const value = String(input || '').replace(/\s+/g, '_');
        if (value.length <= CONFIG.SAFE_EVENT_ID_MAX) return value;
        return value.slice(0, CONFIG.SAFE_EVENT_ID_MAX);
    }

    function makeId(prefix) {
        return sanitizeEventId(prefix + '.' + Date.now() + '.' + Math.random().toString(36).slice(2, 10));
    }

    function getCookie(name) {
        const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const match = document.cookie.match(new RegExp('(?:^|; )' + escaped + '=([^;]*)'));
        return match ? decodeURIComponent(match[1]) : null;
    }

    function getCookieRootDomain() {
        const host = location.hostname || '';
        if (!host || host === 'localhost' || /^\d+\.\d+\.\d+\.\d+$/.test(host)) return '';

        const parts = host.split('.');
        return parts.length >= 2 ? '.' + parts.slice(-2).join('.') : host;
    }

    function persistCookie(name, value, maxAgeSeconds) {
        const attrs = [
            'path=/',
            'max-age=' + Math.max(1, Number(maxAgeSeconds) || 1),
            'SameSite=Lax'
        ];
        const rootDomain = getCookieRootDomain();

        if (rootDomain) attrs.push('domain=' + rootDomain);
        if (location.protocol === 'https:') attrs.push('Secure');

        document.cookie = name + '=' + encodeURIComponent(value) + '; ' + attrs.join('; ');
    }

    function makeFallbackFbpFromFbc(fbc) {
        const match = String(fbc || '').match(/^fb\.(\d)\.(\d{10,13})[.:][A-Za-z0-9._-]+$/i);
        if (!match) return null;

        const version = match[1] || '1';
        const timestamp = match[2] || String(Math.floor(Date.now() / 1000));
        const randomPart = String(Date.now()) + String(Math.floor(Math.random() * 10000000000));

        return 'fb.' + version + '.' + timestamp + '.' + randomPart;
    }

    function getFBP() {
        const FBP_RE = /^fb\.\d\.\d{10,13}\..+$/i;
        const candidates = getAllCookiesByName('_fbp').map(normalizeCookieVal);
        const valid = candidates
            .filter(Boolean)
            .filter(function (v) { return FBP_RE.test(v); })
            .sort(function (a, b) { return b.length - a.length; });

        if (valid[0]) return valid[0];

        const qp = new URLSearchParams(location.search || '');
        const fromUrl = normalizeCookieVal(qp.get('fbp'));
        if (fromUrl && FBP_RE.test(fromUrl)) return fromUrl;

        const fallbackFbc = getFBC();
        const repairedFbp = makeFallbackFbpFromFbc(fallbackFbc);
        if (!repairedFbp) return null;

        if (getCookie('_fbp') !== repairedFbp) {
            persistCookie('_fbp', repairedFbp, 60 * 60 * 24 * 90);
        }

        return repairedFbp;
    }

    function getFBC() {
        const FBC_RE = /^fb\.\d\.\d{10,13}[.:][A-Za-z0-9._-]+$/i;

        const candidates = getAllCookiesByName('_fbc').map(normalizeCookieVal);
        const repaired = candidates.map(function (v) {
            return (v && /fb\.\d\.\d{10,13}:/i.test(v)) ? v.replace(/:/, '.') : v;
        });

        const valid = candidates.concat(repaired)
            .filter(Boolean)
            .filter(function (v) { return FBC_RE.test(v); })
            .sort(function (a, b) { return b.length - a.length; });

        if (valid[0]) return valid[0];

        const qp = new URLSearchParams(location.search || '');
        const rawFbclid = qp.get('fbclid');
        const fbclid = rawFbclid ? normalizeCookieVal(rawFbclid) : null;

        if (fbclid && /^[A-Za-z0-9._-]+$/.test(fbclid)) {
            return 'fb.' + 1 + '.' + Math.floor(Date.now() / 1000) + '.' + fbclid;
        }

        return null;
    }
    function getTTClid() {
        const TTCLID_RE = /^[A-Za-z0-9._-]+$/;

        // 1) Lấy từ cookie nếu đã lưu lần trước
        const fromCookie = getCookie('_ttclid');
        if (fromCookie && TTCLID_RE.test(fromCookie)) return fromCookie;

        // 2) Lấy từ URL param ?ttclid=... và lưu cookie 90 ngày
        const qp = new URLSearchParams(location.search || '');
        const fromUrl = qp.get('ttclid');
        if (fromUrl && TTCLID_RE.test(fromUrl)) {
            persistCookie('_ttclid', fromUrl, 60 * 60 * 24 * 90);
            return fromUrl;
        }

        return null;
    }
    function getAllCookiesByName(name) {
        const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const re = new RegExp('(?:^|;\\s*)' + escaped + '=([^;]*)', 'g');
        const out = [];
        let m;

        while ((m = re.exec(document.cookie)) !== null) {
            out.push(m[1]);
        }

        return out;
    }

    function normalizeCookieVal(val) {
        if (val == null) return null;

        let v = String(val).trim();
        if (!v) return null;

        try {
            v = decodeURIComponent(v);
        } catch (e) {
            // ignore malformed URI sequences
        }

        v = v.trim().replace(/^"|"$/g, '');
        return v || null;
    }

    function mergeObjects(a, b) {
        const out = {};
        const k1 = Object.keys(a || {});
        const k2 = Object.keys(b || {});

        for (let i = 0; i < k1.length; i++) out[k1[i]] = a[k1[i]];
        for (let j = 0; j < k2.length; j++) out[k2[j]] = b[k2[j]];

        return out;
    }

    function isRecoverableQueuedSubmitEvent(eventName) {
        const name = String(eventName || '');
        if (!name) return false;
        if (name === 'Lead' || name === 'CompleteRegistration' || name === 'Purchase') return true;
        return /^Mua_/i.test(name);
    }

    function getQueuedEventTimestampMs(item) {
        const contextTs = item && item.context ? Number(item.context.ts_ms || 0) : 0;
        if (contextTs > 0) return contextTs;

        const eventTime = item ? Number(item.event_time || 0) : 0;
        if (eventTime > 0) return eventTime * 1000;

        return 0;
    }

    function loadQueue() {
        if (!CONFIG.ENABLE_LOCAL_STORAGE_QUEUE) return [];

        try {
            const raw = localStorage.getItem(CONFIG.QUEUE_STORAGE_KEY);
            if (!raw) return [];

            const parsed = JSON.parse(raw);
            const queue = Array.isArray(parsed) ? parsed : [];
            const nowMs = Date.now();
            const filtered = queue.filter(function (item) {
                if (!item || typeof item !== 'object') return false;
                if (!isRecoverableQueuedSubmitEvent(item.event_name)) return true;

                const tsMs = getQueuedEventTimestampMs(item);
                if (!tsMs) {
                    log('drop_stale_submit_queue_event_missing_ts', {
                        event: item.event_name,
                        eventId: item.event_id || null
                    });
                    return false;
                }

                const ageMs = nowMs - tsMs;
                if (ageMs <= CONFIG.SUBMIT_QUEUE_MAX_AGE_MS) return true;

                log('drop_stale_submit_queue_event', {
                    event: item.event_name,
                    eventId: item.event_id || null,
                    ageMs: ageMs,
                    maxAgeMs: CONFIG.SUBMIT_QUEUE_MAX_AGE_MS
                });
                return false;
            });

            if (filtered.length !== queue.length) {
                try {
                    if (!filtered.length) {
                        localStorage.removeItem(CONFIG.QUEUE_STORAGE_KEY);
                    } else {
                        localStorage.setItem(CONFIG.QUEUE_STORAGE_KEY, JSON.stringify(filtered));
                    }
                } catch (persistErr) {
                    // bỏ qua lỗi quota hoặc chế độ riêng tư
                }
            }

            return filtered;
        } catch (e) {
            return [];
        }
    }

    function persistQueue() {
        if (!CONFIG.ENABLE_LOCAL_STORAGE_QUEUE) return;

        try {
            if (!state.queue.length) {
                localStorage.removeItem(CONFIG.QUEUE_STORAGE_KEY);
                return;
            }
            localStorage.setItem(CONFIG.QUEUE_STORAGE_KEY, JSON.stringify(state.queue));
        } catch (e) {
            // bỏ qua lỗi quota hoặc chế độ riêng tư
        }
    }
    function SUBMISSION_KEY() { return '__landing_submission_id__:' + location.pathname; }

    function loadSubmissionId() {
        try {
            const raw = localStorage.getItem(SUBMISSION_KEY());
            if (!raw) return null;
            const obj = JSON.parse(raw);
            if (!obj || !obj.id || !obj.ts) return null;
            if (Date.now() - obj.ts > 6 * 60 * 60 * 1000) {
                localStorage.removeItem(SUBMISSION_KEY());
                return null;
            }
            return obj; // trả cả record {id, ts, ph} để so SĐT
        } catch (e) { return null; }
    }

    function persistSubmissionId(id, ph) {
        try {
            localStorage.setItem(SUBMISSION_KEY(), JSON.stringify({ id: id, ts: Date.now(), ph: String(ph || '') }));
        } catch (e) {}
    }

    function log(label, data) {
        if (!CONFIG.DEBUG) return;
        if (typeof data === 'undefined') {
            console.log('[TrackingScript]', label);
            return;
        }
        console.log('[TrackingScript]', label, data);
    }
})();
