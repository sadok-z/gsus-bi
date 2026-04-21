// GSUS Macro Dashboard - Core Logic

const App = (function () {
    // State
    let unifiedData = [];
    let filteredData = []; // Store filtered result
    let processedFiles = [];

    // Pagination / Infinite Scroll State
    let currentRenderLimit = 0;
    const CHUNK_SIZE = 500; // Updated to 500 as requested
    const SCROLL_BUFFER = 200;

    // Database
    let db = null;
    const DB_NAME = "GSUS_DB";
    const DB_VERSION = 2;
    const STORE_DATA = "dataStore";

    // Application Settings
    const settings = {
        useInternationDate: false,
        includePsychiatric: false, // Default: Do not count psychiatric
        filterAceite: false, // Default: Do not filter by aceite
        filterInternamento: false // Default: Do not filter by internamento
    };

    // Filter State
    const activeFilters = {
        years: new Set(),
        months: new Set(),
        situations: new Set(),
        types: new Set(['EXTERNA']), // Default to Externa
        executanteRegionais: new Set(),
        solicitanteRegionais: new Set(),
        executantes: new Set(),
        solicitantes: new Set()
    };

    const derivedData = {
        executanteRegionalMap: new Map(),
        solicitanteRegionalMap: new Map()
    };

    // DOM Elements
    const elements = {
        fileInput: null,
        fileList: null,
        stats: {
            totalFiles: null,
            totalRecords: null,
            filteredRecords: null,
        },
        settings: {
            modal: null,
            btnClose: null,
            btnSave: null,
            checkboxInternation: null,
            checkboxPsychiatric: null
        },
        filters: {
            year: null,
            month: null,
            situation: null,
            type: null,
            executanteRegional: null,
            solicitanteRegional: null,
            executante: null,
            solicitante: null,
            btnExecutanteRegional: null,
            btnSolicitanteRegional: null,
            panelExecutanteRegional: null,
            panelSolicitanteRegional: null,
            searchExec: null,
            searchSolic: null,
            switchAceite: null,
            switchInternamento: null
        },
        tableHead: null,
        tableBody: null,
        tableWrapper: null, // For scroll event
        btnExport: null,
        btnExportMensal: null,
        btnExportOrtopedia: null,
        btnClear: null,
        loading: null,
        dashboard: null,
    };

    // --- IndexedDB Logic ---
    function initDB() {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(DB_NAME, DB_VERSION);

            request.onerror = (event) => {
                console.error("IDB Error:", event.target.error);
                reject(event.target.error);
            };

            request.onupgradeneeded = (event) => {
                const db = event.target.result;
                if (!db.objectStoreNames.contains(STORE_DATA)) {
                    db.createObjectStore(STORE_DATA, { keyPath: "id" });
                }
            };

            request.onsuccess = (event) => {
                db = event.target.result;
                resolve(db);
            };
        });
    }

    function saveStateToDB() {
        if (!db) return;
        const transaction = db.transaction([STORE_DATA], "readwrite");
        const store = transaction.objectStore(STORE_DATA);

        const state = {
            id: "mainState",
            unifiedData: unifiedData,
            processedFiles: processedFiles,
            settings: settings
        };

        const req = store.put(state);
        req.onsuccess = () => console.log("State saved to DB");
        req.onerror = (e) => console.warn("Error saving state", e);
    }

    function loadStateFromDB() {
        return new Promise((resolve) => {
            if (!db) return resolve(false);
            try {
                const transaction = db.transaction([STORE_DATA], "readonly");
                const store = transaction.objectStore(STORE_DATA);
                const req = store.get("mainState");

                req.onsuccess = (event) => {
                    const result = event.target.result;
                    if (result) {
                        unifiedData = result.unifiedData || [];
                        
                        // Compatibility check: If old data doesn't have _sourceFile, clear it to avoid zombie data
                        if (unifiedData.length > 0 && !unifiedData[0].hasOwnProperty('_sourceFile')) {
                            console.warn("Old data format detected. Clearing DB to prevent zombie data.");
                            unifiedData = [];
                            processedFiles = [];
                            clearDB();
                        } else {
                            processedFiles = result.processedFiles || [];
                        }

                        if (result.settings) {
                            Object.assign(settings, result.settings);
                        }
                        console.log("State loaded from DB", result.unifiedData?.length);
                        resolve(true);
                    } else {
                        resolve(false);
                    }
                };
                req.onerror = () => resolve(false);
            } catch (e) {
                console.warn("Error loading DB transaction", e);
                resolve(false);
            }
        });
    }

    function clearDB() {
        if (!db) return;
        const transaction = db.transaction([STORE_DATA], "readwrite");
        const store = transaction.objectStore(STORE_DATA);
        store.delete("mainState");
        console.log("DB Cleared");
    }

    function init() {
        // Initialize DOM elements
        elements.fileInput = document.getElementById('fileInput');
        elements.fileList = document.getElementById('fileList');

        // Buttons
        const btnUpload = document.getElementById('btnUploadTrigger');
        const btnSettings = document.getElementById('btnSettings');

        elements.stats.totalFiles = document.getElementById('statTotalFiles');
        elements.stats.totalRecords = document.getElementById('statTotalRecords');
        elements.stats.filteredRecords = document.getElementById('statFilteredRecords');

        elements.filters.year = document.getElementById('filterYear');
        elements.filters.month = document.getElementById('filterMonth');
        elements.filters.situation = document.getElementById('filterSituation');
        elements.filters.type = document.getElementById('filterType');
        elements.filters.executanteRegional = document.getElementById('filterExecutanteRegional');
        elements.filters.solicitanteRegional = document.getElementById('filterSolicitanteRegional');
        elements.filters.executante = document.getElementById('filterExecutante');
        elements.filters.solicitante = document.getElementById('filterSolicitante');
        elements.filters.btnExecutanteRegional = document.getElementById('btnExecutanteRegional');
        elements.filters.btnSolicitanteRegional = document.getElementById('btnSolicitanteRegional');
        elements.filters.panelExecutanteRegional = document.getElementById('panelExecutanteRegional');
        elements.filters.panelSolicitanteRegional = document.getElementById('panelSolicitanteRegional');
        elements.filters.searchExec = document.getElementById('searchExecutante');
        elements.filters.searchSolic = document.getElementById('searchSolicitante');
        elements.filters.switchAceite = document.getElementById('checkAceite');
        elements.filters.switchInternamento = document.getElementById('checkInternamento');

        // Settings Elements
        elements.settings.modal = document.getElementById('settingsModal');
        elements.settings.btnClose = document.getElementById('btnCloseSettings');
        elements.settings.btnSave = document.getElementById('btnSaveSettings');
        elements.settings.checkboxInternation = document.getElementById('checkInternationDate');
        elements.settings.checkboxPsychiatric = document.getElementById('checkPsychiatric');

        elements.tableHead = document.getElementById('tableHead');
        elements.tableBody = document.getElementById('tableBody');
        elements.tableWrapper = document.querySelector('.table-wrapper');
        elements.btnExport = document.getElementById('btnExport');
        elements.btnExportMensal = document.getElementById('btnExportMensal');
        elements.btnExportOrtopedia = document.getElementById('btnExportOrtopedia');
        elements.btnClear = document.getElementById('btnClear');
        elements.loading = document.getElementById('loadingOverlay');
        elements.dashboard = document.getElementById('dashboardSection');

        setupEventListeners(btnUpload, btnSettings);

        // Init DB and Attempt Load
        initDB().then(async () => {
            const loaded = await loadStateFromDB();
            if (loaded && unifiedData.length > 0) {
                console.log("Restoring session...");
                showLoading(true);
                setTimeout(() => {
                    recalculateDates();
                    populateFilters();
                    applyFilters();
                    updateUI();
                    showLoading(false);
                }, 50);
            }
        });
    }

    function setupEventListeners(btnUpload, btnSettings) {
        // Drag and drop events
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            document.body.addEventListener(eventName, preventDefaults, false);
        });

        document.body.addEventListener('dragover', () => document.body.style.opacity = '0.8');
        document.body.addEventListener('dragleave', () => document.body.style.opacity = '1');
        document.body.addEventListener('drop', handleDrop, false);

        if (btnUpload) {
            btnUpload.addEventListener('click', () => {
                if (elements.fileInput) elements.fileInput.click();
            });
        }

        // Settings Modal
        if (btnSettings) {
            btnSettings.addEventListener('click', () => {
                if (elements.settings.modal) {
                    elements.settings.modal.style.display = 'flex';
                    // Sync state
                    if (elements.settings.checkboxInternation) {
                        elements.settings.checkboxInternation.checked = settings.useInternationDate;
                    }
                    if (elements.settings.checkboxPsychiatric) {
                        elements.settings.checkboxPsychiatric.checked = settings.includePsychiatric;
                    }
                }
            });
        }

        if (elements.settings.btnClose) {
            elements.settings.btnClose.addEventListener('click', () => {
                elements.settings.modal.style.display = 'none';
            });
        }

        if (elements.settings.btnSave) {
            elements.settings.btnSave.addEventListener('click', () => {
                // Save Settings
                const newInternation = elements.settings.checkboxInternation.checked;
                const newPsych = elements.settings.checkboxPsychiatric.checked;

                const hasChanged = (newInternation !== settings.useInternationDate) ||
                    (newPsych !== settings.includePsychiatric);

                settings.useInternationDate = newInternation;
                settings.includePsychiatric = newPsych;

                elements.settings.modal.style.display = 'none';

                if (hasChanged) {
                    saveStateToDB(); // Save new settings preference
                    if (unifiedData.length > 0) {
                        showLoading(true);
                        setTimeout(() => {
                            recalculateDates();
                            populateFilters();
                            applyFilters();
                            updateUI();
                            showLoading(false);
                        }, 50);
                    }
                }
            });
        }

        if (elements.fileInput) {
            elements.fileInput.addEventListener('click', function () {
                this.value = null;
            });
            elements.fileInput.addEventListener('change', handleFilesEvent);
        }

        if (elements.btnExport) elements.btnExport.addEventListener('click', exportData);
        if (elements.btnExportMensal) elements.btnExportMensal.addEventListener('click', exportAceitesMensal);
        if (elements.btnExportOrtopedia) elements.btnExportOrtopedia.addEventListener('click', exportAceitesOrtopedia);
        if (elements.btnClear) elements.btnClear.addEventListener('click', clearData);

        if (elements.filters.searchExec) setupSearch(elements.filters.searchExec, elements.filters.executante);
        if (elements.filters.searchSolic) setupSearch(elements.filters.searchSolic, elements.filters.solicitante);
        if (elements.filters.btnExecutanteRegional && elements.filters.panelExecutanteRegional) {
            setupRegionalPanelToggle(
                elements.filters.btnExecutanteRegional,
                elements.filters.panelExecutanteRegional,
                activeFilters.executanteRegionais
            );
        }
        if (elements.filters.btnSolicitanteRegional && elements.filters.panelSolicitanteRegional) {
            setupRegionalPanelToggle(
                elements.filters.btnSolicitanteRegional,
                elements.filters.panelSolicitanteRegional,
                activeFilters.solicitanteRegionais
            );
        }

        document.addEventListener('click', handleOutsideRegionalPanels);

        // Aceite Switch Logic
        if (elements.filters.switchAceite) {
            // Set initial state from loaded DB settings
            elements.filters.switchAceite.checked = settings.filterAceite;

            elements.filters.switchAceite.addEventListener('change', (e) => {
                settings.filterAceite = e.target.checked;
                saveStateToDB();
                applyFilters();
                updateUI();
            });
        }

        if (elements.filters.switchInternamento) {
            elements.filters.switchInternamento.checked = settings.filterInternamento;

            elements.filters.switchInternamento.addEventListener('change', (e) => {
                settings.filterInternamento = e.target.checked;
                saveStateToDB();
                applyFilters();
                updateUI();
            });
        }

        // Infinite Scroll Listener
        if (elements.tableWrapper) {
            elements.tableWrapper.addEventListener('scroll', handleTableScroll);
        }
    }

    function handleTableScroll(e) {
        const { scrollTop, scrollHeight, clientHeight } = e.target;
        if (scrollTop + clientHeight >= scrollHeight - SCROLL_BUFFER) {
            // Reached bottom
            if (filteredData.length > currentRenderLimit) {
                renderChunk();
            }
        }
    }

    function setupSearch(input, listContainer) {
        input.addEventListener('input', (e) => {
            applySearchTerm(e.target.value, listContainer);
        });
    }

    function applySearchTerm(term, listContainer) {
        const normalizedTerm = String(term || "").toLowerCase();
        const items = listContainer.querySelectorAll('.filter-item');
        items.forEach(item => {
            const text = item.textContent.toLowerCase();
            item.style.display = text.includes(normalizedTerm) ? 'block' : 'none';
        });
    }

    function refreshSearchFilters() {
        if (elements.filters.searchExec && elements.filters.executante) {
            applySearchTerm(elements.filters.searchExec.value, elements.filters.executante);
        }
        if (elements.filters.searchSolic && elements.filters.solicitante) {
            applySearchTerm(elements.filters.searchSolic.value, elements.filters.solicitante);
        }
    }

    function setupRegionalPanelToggle(button, panel, activeSet) {
        button.addEventListener('click', (e) => {
            e.stopPropagation();
            const shouldOpen = panel.hidden;
            closeRegionalPanels();
            setRegionalPanelState(button, panel, shouldOpen);
            updateRegionalButton(button, activeSet);
        });
        updateRegionalButton(button, activeSet);
    }

    function handleOutsideRegionalPanels(event) {
        closePanelIfOutside(event, elements.filters.btnExecutanteRegional, elements.filters.panelExecutanteRegional, activeFilters.executanteRegionais);
        closePanelIfOutside(event, elements.filters.btnSolicitanteRegional, elements.filters.panelSolicitanteRegional, activeFilters.solicitanteRegionais);
    }

    function closePanelIfOutside(event, button, panel, activeSet) {
        if (!button || !panel || panel.hidden) return;
        if (button.contains(event.target) || panel.contains(event.target)) return;
        setRegionalPanelState(button, panel, false);
        updateRegionalButton(button, activeSet);
    }

    function closeRegionalPanels() {
        setRegionalPanelState(elements.filters.btnExecutanteRegional, elements.filters.panelExecutanteRegional, false);
        setRegionalPanelState(elements.filters.btnSolicitanteRegional, elements.filters.panelSolicitanteRegional, false);
    }

    function setRegionalPanelState(button, panel, isOpen) {
        if (!button || !panel) return;
        panel.hidden = !isOpen;
        button.classList.toggle('is-open', isOpen);
        button.setAttribute('aria-expanded', String(isOpen));
    }

    function updateRegionalButtons() {
        updateRegionalButton(elements.filters.btnExecutanteRegional, activeFilters.executanteRegionais);
        updateRegionalButton(elements.filters.btnSolicitanteRegional, activeFilters.solicitanteRegionais);
    }

    function updateRegionalButton(button, activeSet) {
        if (!button) return;
        const count = activeSet.size;
        button.textContent = count > 0 ? `Regional (${count})` : 'Regional';
        button.classList.toggle('has-selection', count > 0);
    }

    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    function handleDrop(e) {
        document.body.style.opacity = '1';
        const dt = e.dataTransfer;
        const files = dt.files;
        handleFiles(files);
    }

    function handleFilesEvent(e) {
        const files = this.files;
        handleFiles(files);
    }

    // Date Parsing Helper
    function parseDateString(dateStr) {
        if (dateStr === null || dateStr === undefined || dateStr === '') return null;
        if (dateStr instanceof Date) {
            return isNaN(dateStr.getTime()) ? null : dateStr;
        }

        try {
            const str = String(dateStr).trim();
            
            // Check for ISO format (YYYY-MM-DD)
            if (str.match(/^\d{4}-\d{2}-\d{2}/)) {
                const d = new Date(str);
                if (!isNaN(d.getTime())) return d;
            }

            // Expected format: dd/mm/yyyy ...
            const parts = str.split('/');
            if (parts.length >= 3) {
                let day = parseInt(parts[0], 10);
                let month = parseInt(parts[1], 10) - 1;
                
                const yearPart = parts[2].split(' ')[0];
                let year = parseInt(yearPart, 10);
                
                // Adjust for 2-digit years (e.g. M/D/YY)
                if (year < 100) {
                    year += 2000;
                    // Assume US format M/D/YY if 2-digit year
                    const temp = day;
                    day = month + 1;
                    month = temp - 1;
                }

                if (!isNaN(day) && !isNaN(month) && !isNaN(year)) {
                    let hours = 0, mins = 0, secs = 0;
                    if (parts[2].includes(':')) {
                        const timeParts = parts[2].split(' ')[1].split(':');
                        if (timeParts.length >= 2) {
                            hours = parseInt(timeParts[0], 10) || 0;
                            mins = parseInt(timeParts[1], 10) || 0;
                            secs = parseInt(timeParts[2], 10) || 0;
                        }
                    }
                    return new Date(year, month, day, hours, mins, secs);
                }
            }
            
            // Fallback native parse
            const fallback = new Date(str);
            if (!isNaN(fallback.getTime())) return fallback;
            
        } catch (e) {
            console.warn("Date parse error", dateStr, e);
        }
        return null;
    }

    // Duration Parsing Helper
    function parseDuration(durationStr) {
        if (!durationStr) return 0;
        
        if (durationStr instanceof Date) {
            if (isNaN(durationStr.getTime())) return 0;
            const hours = durationStr.getHours();
            const mins = durationStr.getMinutes();
            const secs = durationStr.getSeconds();
            // If it's an Excel time, it might have an offset or just represent the hours since 00:00.
            return ((hours * 60 * 60) + (mins * 60) + secs) * 1000;
        }

        const str = String(durationStr).trim();

        // Simple case: HH:MM:SS
        const parts = str.split(':');
        if (parts.length >= 3) {
            // Check for days prefix (e.g. "4 10") in first part
            let hours = 0;
            let mins = parseInt(parts[1], 10);
            let secs = parseInt(parts[2], 10);

            const firstPart = parts[0];
            if (firstPart.includes(' ')) {
                const dayParts = firstPart.split(' ');
                const days = parseInt(dayParts[0], 10);
                const h = parseInt(dayParts[1], 10);
                hours = (days * 24) + h;
            } else {
                hours = parseInt(firstPart, 10);
            }

            return ((hours * 60 * 60) + (mins * 60) + secs) * 1000;
        }
        return 0;
    }

    // New Helper: Get the data subset based on Psychiatric filter
    function getEffectiveData() {
        if (settings.includePsychiatric) {
            return unifiedData;
        } else {
            return unifiedData.filter(d => {
                const leito = String(d["_rawLeito"] || "").toUpperCase();
                return !leito.includes("PSIQUIATRIA");
            });
        }
    }

    function recalculateDates() {
        console.log("Starting recalculateDates...");
        let successCount = 0;

        const monthNames = [
            "JANEIRO", "FEVEREIRO", "MARÇO", "ABRIL", "MAIO", "JUNHO",
            "JULHO", "AGOSTO", "SETEMBRO", "OUTUBRO", "NOVEMBRO", "DEZEMBRO"
        ];

        unifiedData.forEach((row, index) => {
            let finalDate = null;
            let dateSource = "CADASTRO"; // debug info

            const cadastroDate = parseDateString(row["_rawDateCadastro"]);

            if (settings.useInternationDate) {
                // Priority 1: Data de Internação
                const intDateStr = row["_rawDateInternacao"];
                const parsedIntDate = parseDateString(intDateStr);

                if (parsedIntDate) {
                    finalDate = parsedIntDate;
                    dateSource = "INTERNACAO";
                }
                else if (cadastroDate) {
                    // Priority 2: Cadastro + Espera
                    const durationStr = row["_rawEspera"];
                    const durationMs = parseDuration(durationStr);

                    if (durationMs > 0) {
                        finalDate = new Date(cadastroDate.getTime() + durationMs);
                        dateSource = "CALCULATED";
                    } else {
                        finalDate = cadastroDate;
                    }
                }
            } else {
                // Default: Use Cadastro
                finalDate = cadastroDate;
            }

            // Assign _year and _month logic
            if (finalDate && !isNaN(finalDate.getTime())) {
                row["_year"] = String(finalDate.getFullYear());
                let m = String(finalDate.getMonth() + 1);
                if (m.length === 1) m = "0" + m;
                row["_month"] = m;

                // Add the visible column (Written Month)
                row["Mês Referência"] = monthNames[finalDate.getMonth()];

                successCount++;
            } else {
                if (index < 5 && row["_situation"] !== "CANCELADA") console.warn(`Row ${index} Date Fail. Source: ${dateSource}, RawCad: ${row["_rawDateCadastro"]}, Final: ${finalDate}`);
                row["_year"] = "N/A";
                row["_month"] = "N/A";
                row["Mês Referência"] = "N/A";
            }
        });
        console.log(`RecalculateDates complete. Valid dates: ${successCount} / ${unifiedData.length}`);
    }

    function logError(msg, detail = "") {
        console.error(msg, detail);
        let errorContainer = document.getElementById('errorContainer');
        if (!errorContainer) {
            errorContainer = document.createElement('div');
            errorContainer.id = 'errorContainer';
            errorContainer.style.cssText = `
                position: fixed; bottom: 20px; right: 20px; 
                background: #fee2e2; border: 1px solid #ef4444; color: #b91c1c; 
                padding: 1rem; border-radius: 0.5rem; max-width: 400px; 
                z-index: 9999; box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                max-height: 200px; overflow-y: auto;
            `;
            document.body.appendChild(errorContainer);
        }
        const err = document.createElement('div');
        err.style.marginBottom = '0.5rem';
        err.innerHTML = `<strong>Error:</strong> ${msg} <br><small>${detail}</small>`;
        errorContainer.appendChild(err);
        showLoading(false);
    }

    function exportAceitesMensal() {
        if (!filteredData || filteredData.length === 0) {
            alert("Nenhum dado filtrado para exportar.");
            return;
        }

        const targetEAS = [
            "HOSPITAL REGIONAL DO SUDOESTE",
            "HOSPITAL MUNICIPAL DE CASCAVEL",
            "HOSPITAL REGIONAL DE TOLEDO HRT",
            "HOSPITAL UNIVERSITARIO OESTE DO PARANA",
            "HOESP",
            "HOSPITAL BENEFICENTE ASSISTEGUAIRA",
            "ASSOCIACAO HOSPITALAR BENEFICENTE MOACIR",
            "HOSPITAL DR AURELIO",
            "HOSPITAL DE ENSINO SAO LUCAS",
            "HOSPITAL ITAMED",
            "HOSPITAL DO CANCER DE CASCAVEL UOPECCAN",
            "HOSPITAL MUNICIPAL PADRE GERMANO LAUCK",
            "CEONC",
            "DEUS MENINO - CEONC",
            "INSTITUTO SAO RAFAEL",
            "HOSPITAL SAO FRANCISCO",
            "POLICLINICA PATO BRANCO",
            "HOSPITAL MUNICIPAL PREFEITO QUINTO ABRAO"
        ];

        // Ensure "Aceite" switch effect is within filteredData already by applyFilters()
        // Count occurrences
        const counts = {};
        targetEAS.forEach(eas => counts[eas] = 0);

        filteredData.forEach(row => {
            const exec = String(row["EAS Executante"] || "").trim();
            if (exec) {
                const upperExec = exec.toUpperCase();
                if (counts.hasOwnProperty(upperExec)) {
                    counts[upperExec]++;
                } else {
                    const matched = targetEAS.find(eas => upperExec.includes(eas));
                    if (matched) counts[matched]++;
                }
            }
        });

        // Generate XLSX instead of CSV for better Excel compatibility
        const exportDataList = [];
        targetEAS.forEach(eas => {
            if (counts[eas] > 0) { // Only export EAS with actual data
                exportDataList.push({
                    "EAS EXECUTANTE": eas,
                    "QUANTIDADE": counts[eas]
                });
            }
        });

        if (exportDataList.length === 0) {
            alert("Nenhum aceite mensal encontrado para os EAS monitorados no período.");
            return;
        }

        const ws = XLSX.utils.json_to_sheet(exportDataList);
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, "Aceites Mensal");
        try {
            // Robust download logic for Chrome/Safari compatibility
            const wbout = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
            const blob = new Blob([wbout], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
            const url = URL.createObjectURL(blob);
            const link = document.createElement("a");
            link.href = url;
            link.download = `Aceites_Mensal_${new Date().toISOString().split('T')[0]}.xlsx`;
            document.body.appendChild(link);
            link.click();
            setTimeout(() => {
                document.body.removeChild(link);
                window.URL.revokeObjectURL(url);
            }, 100);
        } catch (e) {
            console.error("Error exporting Aceites Mensal", e);
            alert("Erro ao exportar o arquivo Excel.");
        }
    }

    function exportAceitesOrtopedia() {
        if (!filteredData || filteredData.length === 0) {
            alert("Nenhum dado filtrado para exportar.");
            return;
        }

        const targetEAS = [
            "HOSPITAL REGIONAL DO SUDOESTE",
            "HOSPITAL UNIVERSITARIO OESTE DO PARANA",
            "ASSOCIACAO HOSPITALAR BENEFICENTE MOACIR",
            "HOESP",
            "HOSPITAL DE ENSINO SAO LUCAS",
            "HOSPITAL MUNICIPAL PADRE GERMANO LAUCK",
            "HOSPITAL E MATERNIDADE NOSSA SENHORA DA"
        ];

        // Count occurrences matching both EAS and Especialidade Regulada containing 'ortopedia'
        const counts = {};
        targetEAS.forEach(eas => counts[eas] = 0);

        filteredData.forEach(row => {
            const exec = String(row["EAS Executante"] || "").trim();
            const specialidade = String(row["Especialidade Regulada"] || "");

            if (exec && specialidade && specialidade.toLowerCase().includes("ortopedia")) {
                const upperExec = exec.toUpperCase();
                if (counts.hasOwnProperty(upperExec)) {
                    counts[upperExec]++;
                } else {
                    const matched = targetEAS.find(eas => upperExec.includes(eas));
                    if (matched) counts[matched]++;
                }
            }
        });

        // Generate XLSX
        const exportDataList = [];
        targetEAS.forEach(eas => {
            if (counts[eas] > 0) { // Only export EAS with actual data
                exportDataList.push({
                    "EAS EXECUTANTE": eas,
                    "QUANTIDADE": counts[eas]
                });
            }
        });

        if (exportDataList.length === 0) {
            alert("Nenhum aceite de ortopedia encontrado para os EAS monitorados no período.");
            return;
        }

        const ws = XLSX.utils.json_to_sheet(exportDataList);
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, "Aceites Ortopedia");
        try {
            const wbout = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
            const blob = new Blob([wbout], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
            const url = URL.createObjectURL(blob);
            const link = document.createElement("a");
            link.href = url;
            link.download = `Aceites_Ortopedia_${new Date().toISOString().split('T')[0]}.xlsx`;
            document.body.appendChild(link);
            link.click();
            setTimeout(() => {
                document.body.removeChild(link);
                window.URL.revokeObjectURL(url);
            }, 100);
        } catch (e) {
            console.error("Error exporting Aceites Ortopedia", e);
            alert("Erro ao exportar o arquivo Excel.");
        }
    }

    function showLoading(show) {
        if (elements.loading) {
            elements.loading.style.display = show ? 'flex' : 'none';
        }
    }

    async function handleFiles(fileList) {
        if (!fileList.length) return;

        if (typeof XLSX === 'undefined') {
            logError("Biblioteca SheetJS Nao Encontrada", "Por favor, verifique sua conexao com a internet ou adicione a biblioteca localmente.");
            return;
        }

        showLoading(true);
        const files = Array.from(fileList);

        try {
            for (const file of files) {
                if (!file.name.match(/\.(xls|xlsx|csv)$/i)) {
                    console.warn(`Skipping ${file.name} - not a supported file type`);
                    continue;
                }
                await processFile(file);
            }

            // Initial Processing
            try {
                recalculateDates();
                saveStateToDB(); // Persist changes
                populateFilters();
                applyFilters();
                updateUI();
            } catch (e) {
                logError("Erro na filtragem", e.message);
            }

        } catch (error) {
            logError("Erro ao processar arquivos", error.message);
        } finally {
            showLoading(false);
        }
    }

    function findBestColumn(keys, keywords, exclude = []) {
        const candidates = keys.filter(k => {
            const upper = k.toUpperCase();
            return keywords.every(kw => upper.includes(kw)) &&
                exclude.every(exc => !upper.includes(exc));
        });
        return candidates.length > 0 ? candidates[0] : null;
    }

    function processFile(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();

            reader.onload = function (e) {
                try {
                    const data = new Uint8Array(e.target.result);
                    const workbook = XLSX.read(data, { type: 'array', cellDates: true });
                    const firstSheetName = workbook.SheetNames[0];
                    const worksheet = workbook.Sheets[firstSheetName];

                    const jsonData = XLSX.utils.sheet_to_json(worksheet, {
                        defval: "",
                        raw: true
                    });

                    // CONTENT-BASED DETECTION FOR "SITUAÇÃO"
                    let detectedSituationKey = null;
                    const statusKeywords = ["CANCELADA", "INTERNADO", "INTERNADA", "AUTORIZADA", "NEGADA", "AGUARDANDO", "EM REGULAÇÃO", "PENDENTE"];

                    // Initialize generic keys
                    let detectedDateCadastroKey = null;
                    let detectedDateInternacaoKey = null;
                    let detectedEsperaKey = null;
                    let detectedLeitoKey = null;

                    if (jsonData.length > 0) {
                        try {
                            const headers = Object.keys(jsonData[0]);

                            // Detect Situation (Content based)
                            const sampleSize = Math.min(jsonData.length, 50);
                            const columnScores = {};
                            headers.forEach(k => columnScores[k] = 0);

                            for (let i = 0; i < sampleSize; i++) {
                                const row = jsonData[i];
                                headers.forEach(key => {
                                    const val = String(row[key]).toUpperCase();
                                    if (statusKeywords.some(kw => val.includes(kw))) {
                                        columnScores[key]++;
                                    }
                                });
                            }

                            let bestKey = null;
                            let maxScore = 0;
                            Object.entries(columnScores).forEach(([key, score]) => {
                                if (score > maxScore) {
                                    maxScore = score;
                                    bestKey = key;
                                }
                            });
                            if (maxScore > 0) detectedSituationKey = bestKey;

                            // Content/Name detection for Others
                            detectedDateCadastroKey = findBestColumn(headers, ["DATA", "CADASTRO"]) ||
                                findBestColumn(headers, ["DATA", "SOLICITA"]) ||
                                "Data de Cadastro";

                            detectedDateInternacaoKey = findBestColumn(headers, ["DATA", "INTERNA"]) ||
                                "Data da Internação";

                            detectedEsperaKey = findBestColumn(headers, ["ESPERA"]) ||
                                findBestColumn(headers, ["TEMPO", "DECORRIDO"]) ||
                                "Espera";

                            // Detect Tipo de Leito Regulado
                            detectedLeitoKey = findBestColumn(headers, ["TIPO", "LEITO"]) ||
                                findBestColumn(headers, ["LEITO", "REGULADO"]) ||
                                "Tipo de Leito Regulado";

                        } catch (e) {
                            console.warn("Error in detection", e);
                        }
                    }

                    // Post-process logic
                    const processed = jsonData.map(row => {
                        try {
                            row["_sourceFile"] = file.name;
                            // Normalize Standard Fields (internal usage)
                            row["_rawDateCadastro"] = row[detectedDateCadastroKey] || row["Data de Cadastro"] || "";
                            row["_rawDateInternacao"] = row[detectedDateInternacaoKey] || row["Data da Internação"] || "";
                            row["_rawLeito"] = row[detectedLeitoKey] || "";

                            let esperaVal = row[detectedEsperaKey] || row["Espera"] || "";
                            // Fix number separators in Espera if needed
                            if (esperaVal && typeof esperaVal === 'string') {
                                esperaVal = esperaVal.replace(/,/g, '.');
                            }
                            row["_rawEspera"] = esperaVal;


                            // Note: Date processing moved to recalculateDates() for dynamic updating

                            // Normalize Type logic
                            if (row["Tipo de Transferência"]) {
                                row["_type"] = row["Tipo de Transferência"].toString().toUpperCase();
                            } else {
                                // Fallback detection?
                                const typeKey = findBestColumn(Object.keys(row), ["TIPO", "TRANSFER"]);
                                row["_type"] = typeKey ? row[typeKey].toString().toUpperCase() : "N/A";
                            }

                            // Normalize Situation logic
                            let situacao = null;
                            if (detectedSituationKey) {
                                situacao = row[detectedSituationKey];
                            } else {
                                // Fallback
                                const keys = Object.keys(row);
                                let situationKey = keys.find(k => {
                                    const upper = k.toUpperCase();
                                    return upper.includes("SITUA") && !upper.includes("DATA") && !upper.includes("HORA");
                                });
                                if (situationKey) situacao = row[situationKey];
                            }

                            if (situacao) {
                                row["_situation"] = situacao.toString().toUpperCase().trim();
                            } else {
                                row["_situation"] = "N/A";
                            }

                        } catch (innerErr) {
                            console.warn("Row processing error", innerErr);
                        }
                        return row;
                    });

                    if (processed.length > 0) {
                        unifiedData = unifiedData.concat(processed);
                        processedFiles.push({
                            name: file.name,
                            rows: processed.length
                        });
                    }

                    resolve();
                } catch (err) {
                    reject(err);
                }
            };

            reader.onerror = reject;
            reader.readAsArrayBuffer(file);
        });
    }

    function populateFilters() {
        const data = getEffectiveData();
        rebuildRegionalMaps(data);

        if (data.length === 0) {
            activeFilters.years.clear();
            activeFilters.months.clear();
            activeFilters.situations.clear();
            activeFilters.types.clear();
            activeFilters.types.add('EXTERNA');
            activeFilters.executanteRegionais.clear();
            activeFilters.solicitanteRegionais.clear();
            activeFilters.executantes.clear();
            activeFilters.solicitantes.clear();

            if (elements.filters.year) elements.filters.year.innerHTML = '';
            if (elements.filters.month) elements.filters.month.innerHTML = '';
            if (elements.filters.situation) elements.filters.situation.innerHTML = '';
            if (elements.filters.type) elements.filters.type.innerHTML = '';
            if (elements.filters.executanteRegional) elements.filters.executanteRegional.innerHTML = '';
            if (elements.filters.solicitanteRegional) elements.filters.solicitanteRegional.innerHTML = '';
            if (elements.filters.executante) elements.filters.executante.innerHTML = '';
            if (elements.filters.solicitante) elements.filters.solicitante.innerHTML = '';
            if (elements.filters.btnExecutanteRegional) elements.filters.btnExecutanteRegional.disabled = true;
            if (elements.filters.btnSolicitanteRegional) elements.filters.btnSolicitanteRegional.disabled = true;
            closeRegionalPanels();
            updateRegionalButtons();
            return;
        }

        const years = [...new Set(data.map(d => String(d["_year"] || "")).filter(Boolean))].sort();
        const months = [...new Set(data.map(d => String(d["_month"] || "")).filter(Boolean))].sort();
        const situations = [...new Set(data.map(d => String(d["_situation"] || "")).filter(Boolean))].sort();
        const types = [...new Set(data.map(d => String(d["_type"] || "")).filter(Boolean))].sort();
        const execRegions = getRegionalValuesFromMap(derivedData.executanteRegionalMap);
        const solicRegions = getRegionalValuesFromMap(derivedData.solicitanteRegionalMap);

        cleanActiveFilters(activeFilters.years, years);
        cleanActiveFilters(activeFilters.months, months);
        cleanActiveFilters(activeFilters.situations, situations);
        cleanActiveFilters(activeFilters.types, types);
        cleanActiveFilters(activeFilters.executanteRegionais, execRegions);
        cleanActiveFilters(activeFilters.solicitanteRegionais, solicRegions);

        const execs = getExecutanteOptions(data);
        const solics = getSolicitanteOptions(data);

        cleanActiveFilters(activeFilters.executantes, execs);
        cleanActiveFilters(activeFilters.solicitantes, solics);

        if (elements.filters.year) createFilterList(elements.filters.year, years, activeFilters.years);
        if (elements.filters.month) createFilterList(elements.filters.month, months, activeFilters.months);
        if (elements.filters.situation) createFilterList(elements.filters.situation, situations, activeFilters.situations);
        if (elements.filters.type) createFilterList(elements.filters.type, types, activeFilters.types);
        if (elements.filters.executanteRegional) {
            createFilterList(
                elements.filters.executanteRegional,
                execRegions.map(region => ({ value: region, label: formatRegionalLabel(region), title: region })),
                activeFilters.executanteRegionais,
                { forceMulti: true }
            );
        }
        if (elements.filters.solicitanteRegional) {
            createFilterList(
                elements.filters.solicitanteRegional,
                solicRegions.map(region => ({ value: region, label: formatRegionalLabel(region), title: region })),
                activeFilters.solicitanteRegionais,
                { forceMulti: true }
            );
        }
        if (elements.filters.executante) createFilterList(elements.filters.executante, execs, activeFilters.executantes);
        if (elements.filters.solicitante) createFilterList(elements.filters.solicitante, solics, activeFilters.solicitantes);

        if (elements.filters.btnExecutanteRegional) elements.filters.btnExecutanteRegional.disabled = execRegions.length === 0;
        if (elements.filters.btnSolicitanteRegional) elements.filters.btnSolicitanteRegional.disabled = solicRegions.length === 0;
        updateRegionalButtons();
        refreshSearchFilters();
    }

    function cleanActiveFilters(activeSet, availableItems) {
        const availableSet = new Set(availableItems);
        for (let item of activeSet) {
            if (!availableSet.has(item)) activeSet.delete(item);
        }
    }

    function createFilterList(container, items, activeSet, options = {}) {
        container.innerHTML = '';
        items.forEach(item => {
            const option = typeof item === 'object'
                ? item
                : { value: item, label: item, title: item };

            const div = document.createElement('div');
            div.className = 'filter-item';
            div.textContent = option.label;
            div.title = option.title || option.label;
            div.dataset.value = option.value;

            if (activeSet.has(option.value)) {
                div.classList.add('selected');
            }

            div.addEventListener('click', (e) => {
                handleSelection(e, option.value, activeSet, container, options.forceMulti);
                updateRegionalButtons();
                populateFilters();
                applyFilters();
                updateUI();
            });

            container.appendChild(div);
        });
    }

    function handleSelection(e, value, activeSet, container, forceMulti = false) {
        const isMulti = forceMulti || e.metaKey || e.ctrlKey;
        if (activeSet.has(value)) {
            activeSet.delete(value);
        } else if (isMulti) {
            activeSet.add(value);
        } else {
            activeSet.clear();
            activeSet.add(value);
        }
        const allItems = container.querySelectorAll('.filter-item');
        allItems.forEach(item => {
            const itemValue = item.dataset.value || item.textContent;
            if (activeSet.has(itemValue)) item.classList.add('selected');
            else item.classList.remove('selected');
        });
    }

    function rebuildRegionalMaps(data) {
        derivedData.executanteRegionalMap = new Map();
        derivedData.solicitanteRegionalMap = new Map();

        data.forEach(row => {
            const solicitante = normalizeEntityName(row["EAS Solicitante"]);
            const regional = normalizeRegionalValue(row["Regional EAS Solicitante"]);
            if (!solicitante || !regional) return;
            addRegionToMap(derivedData.solicitanteRegionalMap, solicitante, regional);
        });

        const executantes = [...new Set(data.map(row => normalizeEntityName(row["EAS Executante"])).filter(Boolean))];
        executantes.forEach(executante => {
            const mappedRegions = derivedData.solicitanteRegionalMap.get(executante);
            if (!mappedRegions || mappedRegions.size === 0) return;
            derivedData.executanteRegionalMap.set(executante, new Set(mappedRegions));
        });
    }

    function addRegionToMap(map, key, value) {
        if (!map.has(key)) {
            map.set(key, new Set());
        }
        map.get(key).add(value);
    }

    function normalizeEntityName(value) {
        return String(value || "").trim().toUpperCase();
    }

    function normalizeRegionalValue(value) {
        return String(value || "").trim().toUpperCase();
    }

    function getRegionalValuesFromMap(map) {
        const values = new Set();
        map.forEach(regionSet => {
            regionSet.forEach(region => values.add(region));
        });
        return [...values].sort(compareRegionalValues);
    }

    function getExecutanteOptions(data) {
        const execs = [...new Set(data.map(d => String(d["EAS Executante"] || "").trim()).filter(Boolean))].sort();
        if (activeFilters.executanteRegionais.size === 0) return execs;

        return execs.filter(exec => matchSelectedRegions(
            derivedData.executanteRegionalMap.get(normalizeEntityName(exec)),
            activeFilters.executanteRegionais
        ));
    }

    function getSolicitanteOptions(data) {
        const solics = [...new Set(data.map(d => String(d["EAS Solicitante"] || "").trim()).filter(Boolean))].sort();
        if (activeFilters.solicitanteRegionais.size === 0) return solics;

        return solics.filter(solic => matchSelectedRegions(
            derivedData.solicitanteRegionalMap.get(normalizeEntityName(solic)),
            activeFilters.solicitanteRegionais
        ));
    }

    function matchSelectedRegions(regionSet, selectedSet) {
        if (selectedSet.size === 0) return true;
        if (!regionSet || regionSet.size === 0) return false;

        for (const region of regionSet) {
            if (selectedSet.has(region)) return true;
        }
        return false;
    }

    function compareRegionalValues(a, b) {
        const aNumber = extractRegionalNumber(a);
        const bNumber = extractRegionalNumber(b);
        if (aNumber !== bNumber) return aNumber - bNumber;
        return a.localeCompare(b, 'pt-BR');
    }

    function extractRegionalNumber(value) {
        const match = normalizeRegionalValue(value).match(/^RS\s*0?(\d+)/);
        return match ? parseInt(match[1], 10) : Number.MAX_SAFE_INTEGER;
    }

    function formatRegionalLabel(value) {
        const normalized = normalizeRegionalValue(value);
        const match = normalized.match(/^RS\s*0?(\d+)\s*-\s*(.+)$/);
        if (!match) return normalized;
        return `${parseInt(match[1], 10)}ª Regional - ${match[2]}`;
    }

    function applyFilters() {
        const data = getEffectiveData();
        rebuildRegionalMaps(data);

        const aceiteSituations = new Set([
            "AGUARDANDO REMOÇÃO",
            "ALTA",
            "EM TRÂNSITO",
            "INTERNADO",
            "PACIENTE AVALIADO E LIBERADO",
            "RESERVA CONFIRMADA",
            "TRANSFERÊNCIA PARA EAS NÃO REGULADO"
        ]);

        filteredData = data.filter(row => {
            const yearMatch = activeFilters.years.size === 0 || activeFilters.years.has(String(row["_year"] || ""));
            const monthMatch = activeFilters.months.size === 0 || activeFilters.months.has(String(row["_month"] || ""));

            // Situation matching modified by Aceite Switch
            let situationMatch = true;
            if (settings.filterAceite) {
                situationMatch = aceiteSituations.has(String(row["_situation"] || ""));
            } else {
                situationMatch = activeFilters.situations.size === 0 || activeFilters.situations.has(String(row["_situation"] || ""));
            }

            const typeMatch = activeFilters.types.size === 0 || activeFilters.types.has(String(row["_type"] || ""));
            const execRegionalMatch = matchSelectedRegions(
                derivedData.executanteRegionalMap.get(normalizeEntityName(row["EAS Executante"])),
                activeFilters.executanteRegionais
            );
            const solicRegionalMatch = activeFilters.solicitanteRegionais.size === 0 ||
                activeFilters.solicitanteRegionais.has(normalizeRegionalValue(row["Regional EAS Solicitante"]));
            const execMatch = activeFilters.executantes.size === 0 || activeFilters.executantes.has(String(row["EAS Executante"] || ""));
            const solicMatch = activeFilters.solicitantes.size === 0 || activeFilters.solicitantes.has(String(row["EAS Solicitante"] || ""));
            const internamentoMatch = !settings.filterInternamento || hasInternationData(row["_rawDateInternacao"]);

            return yearMatch &&
                monthMatch &&
                situationMatch &&
                typeMatch &&
                execRegionalMatch &&
                solicRegionalMatch &&
                execMatch &&
                solicMatch &&
                internamentoMatch;
        });
    }

    function hasInternationData(value) {
        if (value instanceof Date) {
            return !isNaN(value.getTime());
        }

        if (typeof value === 'number') {
            return !Number.isNaN(value);
        }

        return String(value || "").trim() !== "";
    }

    function removeFile(fileName) {
        showLoading(true);
        setTimeout(() => {
            // Remove from unifiedData
            unifiedData = unifiedData.filter(row => row["_sourceFile"] !== fileName);
            // Remove from processedFiles
            processedFiles = processedFiles.filter(f => f.name !== fileName);
            
            // Re-run processing
            recalculateDates();
            saveStateToDB();
            populateFilters();
            applyFilters();
            updateUI();
            
            showLoading(false);
        }, 50);
    }

    // --- Updated UI Update with Explicit Infinite Scroll Init ---
    function updateUI() {
        if (elements.stats.totalFiles) elements.stats.totalFiles.textContent = processedFiles.length;
        if (elements.stats.totalRecords) elements.stats.totalRecords.textContent = getEffectiveData().length.toLocaleString();
        if (elements.stats.filteredRecords) elements.stats.filteredRecords.textContent = filteredData.length.toLocaleString();

        if (elements.fileList) {
            elements.fileList.innerHTML = processedFiles.map(f =>
                `<div class="file-chip">
                    <span>📄</span> ${f.name} (${f.rows.toLocaleString()})
                    <button class="btn-remove-file" data-filename="${f.name.replace(/"/g, '&quot;')}" title="Remover arquivo">&times;</button>
                </div>`
            ).join('');

            elements.fileList.querySelectorAll('.btn-remove-file').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const fileName = e.target.getAttribute('data-filename');
                    removeFile(fileName);
                });
            });
        }

        // Reset scroll and render from scratch
        renderTable(null, true);

        if (unifiedData.length > 0) {
            if (elements.btnExport) elements.btnExport.disabled = false;
            if (elements.btnExportMensal) elements.btnExportMensal.disabled = false;
            if (elements.btnExportOrtopedia) elements.btnExportOrtopedia.disabled = false;
            if (elements.btnClear) elements.btnClear.disabled = false;
        } else {
            if (elements.btnExportMensal) elements.btnExportMensal.disabled = true;
            if (elements.btnExportOrtopedia) elements.btnExportOrtopedia.disabled = true;
        }
    }

    function renderTable(data, reset = false) {
        if (reset) {
            if (!elements.tableHead || !elements.tableBody) return;

            // Setup Headers if needed (from filteredData mainly)
            if (filteredData.length === 0) {
                elements.tableHead.innerHTML = '';
                elements.tableBody.innerHTML = '<tr><td colspan="5" style="text-align:center; padding: 2rem;">Nenhum dado encontrado</td></tr>';
                currentRenderLimit = 0;
                return;
            }

            const visibleColumns = Object.keys(filteredData[0]).filter(k => !k.startsWith('_'));
            elements.tableHead.innerHTML = `<tr>${visibleColumns.map(col => `<th>${col}</th>`).join('')}</tr>`;
            elements.tableBody.innerHTML = ''; // Clear existing
            currentRenderLimit = 0; // Reset counter
        }

        renderChunk();
    }

    function renderChunk() {
        if (filteredData.length === 0) return;

        const start = currentRenderLimit;
        const end = Math.min(start + CHUNK_SIZE, filteredData.length);

        if (start >= end) return; // All rendered

        const chunk = filteredData.slice(start, end);
        const visibleColumns = Object.keys(filteredData[0]).filter(k => !k.startsWith('_'));

        const html = chunk.map(row => {
            return `<tr>${visibleColumns.map(col => {
                let val = row[col];
                if (val instanceof Date) {
                    if (!isNaN(val.getTime())) {
                        val = val.toLocaleString('pt-BR');
                    } else {
                        val = "Data Inválida";
                    }
                }
                return `<td>${val !== null && val !== undefined ? val : ''}</td>`;
            }).join('')}</tr>`;
        }).join('');

        // Use insertAdjacentHTML for better performance than innerHTML +=
        elements.tableBody.insertAdjacentHTML('beforeend', html);

        currentRenderLimit = end;
    }

    function exportData() {
        if (filteredData.length === 0) return;
        const date = new Date().toISOString().split('T')[0];
        const filename = `GSUS_Filtered_${date}.xlsx`;
        const cleanData = filteredData.map(row => {
            const newRow = { ...row };
            Object.keys(newRow).forEach(key => {
                if (key.startsWith('_')) delete newRow[key];
            });
            return newRow;
        });
        const ws = XLSX.utils.json_to_sheet(cleanData);
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, "Dados Filtrados");
        XLSX.writeFile(wb, filename);
    }

    function clearData() {
        unifiedData = [];
        filteredData = [];
        processedFiles = [];
        currentRenderLimit = 0;

        activeFilters.years.clear();
        activeFilters.months.clear();
        activeFilters.situations.clear();
        activeFilters.types.clear();
        activeFilters.types.add('EXTERNA');
        activeFilters.executanteRegionais.clear();
        activeFilters.solicitanteRegionais.clear();
        activeFilters.executantes.clear();
        activeFilters.solicitantes.clear();
        derivedData.executanteRegionalMap = new Map();
        derivedData.solicitanteRegionalMap = new Map();

        clearDB(); // Clear persistence

        if (elements.fileList) elements.fileList.innerHTML = '';
        if (elements.filters.year) elements.filters.year.innerHTML = '';
        if (elements.filters.month) elements.filters.month.innerHTML = '';
        if (elements.filters.situation) elements.filters.situation.innerHTML = '';
        if (elements.filters.type) elements.filters.type.innerHTML = '';
        if (elements.filters.executanteRegional) elements.filters.executanteRegional.innerHTML = '';
        if (elements.filters.solicitanteRegional) elements.filters.solicitanteRegional.innerHTML = '';
        if (elements.filters.executante) elements.filters.executante.innerHTML = '';
        if (elements.filters.solicitante) elements.filters.solicitante.innerHTML = '';
        if (elements.filters.btnExecutanteRegional) elements.filters.btnExecutanteRegional.disabled = true;
        if (elements.filters.btnSolicitanteRegional) elements.filters.btnSolicitanteRegional.disabled = true;
        closeRegionalPanels();
        updateRegionalButtons();

        if (elements.stats.totalFiles) elements.stats.totalFiles.textContent = '0';
        if (elements.stats.totalRecords) elements.stats.totalRecords.textContent = '0';
        if (elements.stats.filteredRecords) elements.stats.filteredRecords.textContent = '0';

        if (elements.btnExport) elements.btnExport.disabled = true;
        if (elements.btnClear) elements.btnClear.disabled = true;
        renderTable(null, true); // Reset table

        if (elements.fileInput) elements.fileInput.value = '';
    }

    return {
        init: init
    };
})();

document.addEventListener('DOMContentLoaded', App.init);
