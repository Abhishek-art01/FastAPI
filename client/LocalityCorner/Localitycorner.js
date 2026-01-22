const API_BASE = "http://127.0.0.1:8001/api";
let masterLocalities = [];
let currentPage = 1;
let totalPages = 1;
let editingRowId = null; // Track which row is being edited
let currentPendingItem = null;
let bulkSelection = new Set();

document.addEventListener("DOMContentLoaded", () => {
    fetchMasterDropdown();
    fetchTableData(1);
    switchTab('view');
});

// --- FETCH MASTERS ---
async function fetchMasterDropdown() {
    try {
        const res = await fetch(`${API_BASE}/dropdown-localities/`);
        masterLocalities = await res.json();

        populateDropdown('singleLocalitySelect', masterLocalities);
        populateDropdown('bulkLocalitySelect', masterLocalities);

        const uniqueZones = [...new Set(masterLocalities.map(l => l.zone).filter(z => z))].sort();
        const zoneSelect = document.getElementById('newLocZone');
        zoneSelect.innerHTML = '<option value="">-- Select Existing Zone --</option>';
        uniqueZones.forEach(z => {
            const opt = document.createElement('option');
            opt.value = z; opt.textContent = z;
            zoneSelect.appendChild(opt);
        });
    } catch (err) { console.error("Error loading masters:", err); }
}

function populateDropdown(id, data) {
    const sel = document.getElementById(id);
    sel.innerHTML = '<option value="">-- Select --</option>';
    data.forEach(loc => {
        const opt = document.createElement('option');
        opt.value = loc.locality;
        opt.textContent = loc.locality;
        sel.appendChild(opt);
    });
}

// --- VIEW TAB ---
async function fetchTableData(page) {
    const search = document.getElementById('viewSearch').value;
    try {
        const res = await fetch(`${API_BASE}/localities/?page=${page}&search=${search}`);
        const data = await res.json();
        renderTable(data.results || []);

        currentPage = page;
        totalPages = data.pagination ? data.pagination.total_pages : 1;
        document.getElementById('pageIndicator').innerText = `Page ${page} of ${totalPages}`;
        document.getElementById('globalPendingCount').innerText = data.global_pending || 0;

        document.getElementById('btnPrev').disabled = page === 1;
        document.getElementById('btnNext').disabled = page === totalPages;
    } catch (err) { console.error(err); }
}

// ✅ FIXED: Restored Edit/Save/Cancel Buttons
function renderTable(rows) {
    const tbody = document.getElementById('viewTableBody');
    tbody.innerHTML = '';

    if (rows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;">No records found</td></tr>';
        return;
    }

    rows.forEach(row => {
        const tr = document.createElement('tr');
        const isEditing = editingRowId === row.id;

        // 1. Locality Cell: Show Text OR Select Box
        let localityCell = isEditing
            ? `<select id="edit-select-${row.id}" class="edit-select" style="width:100%"></select>`
            : `<strong>${row.locality || '-'}</strong>`;

        // 2. Action Cell: Show Edit Icon OR Save/Cancel Icons
        let actionCell = isEditing
            ? `<button onclick="saveEdit(${row.id})" class="btn-save" style="color:green; cursor:pointer; margin-right:8px;" title="Save"><i class="fa-solid fa-check"></i></button>
               <button onclick="cancelEdit()" class="btn-cancel" style="color:red; cursor:pointer;" title="Cancel"><i class="fa-solid fa-xmark"></i></button>`
            : `<button onclick="startEdit(${row.id})" class="btn-icon" title="Edit"><i class="fa-solid fa-pencil"></i></button>`;

        tr.innerHTML = `
            <td>${row.id}</td>
            <td><div style="max-height:60px; overflow-y:auto; font-size:0.9rem;">${row.address}</div></td>
            <td>${localityCell}</td>
            <td>${row.zone || '-'}</td>
            <td>${row.km || '-'}</td>
            <td><span class="status-badge status-${(row.status || 'pending').toLowerCase()}">${row.status}</span></td>
            <td style="text-align:center;">${actionCell}</td>
        `;
        tbody.appendChild(tr);

        // 3. If editing, populate the dropdown immediately
        if (isEditing) {
            const sel = document.getElementById(`edit-select-${row.id}`);
            // Add default option
            const def = document.createElement('option');
            def.value = ""; def.textContent = "-- Select --";
            sel.appendChild(def);

            masterLocalities.forEach(loc => {
                const opt = document.createElement('option');
                opt.value = loc.locality;
                opt.textContent = loc.locality;
                // Pre-select current value
                if (loc.locality === row.locality) opt.selected = true;
                sel.appendChild(opt);
            });
        }
    });
}

function changePage(delta) {
    const newPage = currentPage + delta;
    if (newPage > 0 && newPage <= totalPages) fetchTableData(newPage);
}

// --- ✅ INLINE EDIT LOGIC ---
function startEdit(id) {
    editingRowId = id;
    fetchTableData(currentPage); // Re-render to show input
}

function cancelEdit() {
    editingRowId = null;
    fetchTableData(currentPage); // Re-render to hide input
}

async function saveEdit(id) {
    const sel = document.getElementById(`edit-select-${id}`);
    const newLocName = sel.value;

    if (!newLocName) { alert("Please select a locality"); return; }

    // Send update
    const success = await postData('/save-mapping/', {
        address_id: id,
        locality_id: newLocName,
        locality_name: newLocName
    });

    if (success) {
        editingRowId = null;
        fetchTableData(currentPage);
    }
}

// --- SINGLE SET MODE ---
async function fetchNextPending() {
    try {
        const res = await fetch(`${API_BASE}/next-pending/`);
        const data = await res.json();

        if (data.found) {
            currentPendingItem = data.data;
            document.getElementById('pendingCard').classList.remove('hidden');
            document.getElementById('noPendingMsg').classList.add('hidden');
            document.getElementById('pendingAddress').textContent = currentPendingItem.address;

            document.getElementById('singleLocalitySelect').value = "";
            document.getElementById('previewZone').textContent = "-";
            document.getElementById('previewKM').textContent = "-";
        } else {
            currentPendingItem = null;
            document.getElementById('pendingCard').classList.add('hidden');
            document.getElementById('noPendingMsg').classList.remove('hidden');
        }
    } catch (err) { console.error(err); }
}

function updatePreview() {
    const locName = document.getElementById('singleLocalitySelect').value;
    const loc = masterLocalities.find(l => l.locality === locName);

    if (loc) {
        document.getElementById('previewZone').textContent = loc.zone || "-";
        document.getElementById('previewKM').textContent = loc.km || loc.billing_km || "-";
    } else {
        document.getElementById('previewZone').textContent = "-";
        document.getElementById('previewKM').textContent = "-";
    }
}

async function saveSinglePending() {
    const locName = document.getElementById('singleLocalitySelect').value;
    if (!currentPendingItem || !locName) { alert("Select a locality!"); return; }

    const success = await postData('/save-mapping/', {
        address_id: currentPendingItem.id,
        locality_id: locName,
        locality_name: locName
    });

    if (success) {
        fetchNextPending();
        fetchTableData(1);
        const res = await fetch(`${API_BASE}/localities/?page=1`);
        const data = await res.json();
        document.getElementById('globalPendingCount').innerText = data.global_pending || 0;
    }
}

// --- BULK MODE ---
async function fetchBulkData(page) {
    const q = document.getElementById('bulkSearch').value;
    const res = await fetch(`${API_BASE}/search-pending/?q=${q}&page=${page}`);
    const data = await res.json();

    document.getElementById('bulkCount').innerText = data.pagination.total_records;
    const tbody = document.getElementById('bulkTableBody');
    tbody.innerHTML = '';

    data.results.forEach(row => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td><input type="checkbox" class="bulk-chk" value="${row.id}" onchange="updateBulkSet(this)"></td>
            <td>${row.address}</td>
        `;
        tbody.appendChild(tr);
    });
    bulkSelection.clear();
}

function updateBulkSet(chk) {
    if (chk.checked) bulkSelection.add(parseInt(chk.value));
    else bulkSelection.delete(parseInt(chk.value));
}

function toggleAllBulk() {
    const mainChk = document.getElementById('bulkSelectAll');
    const chks = document.querySelectorAll('.bulk-chk');
    chks.forEach(c => {
        c.checked = mainChk.checked;
        updateBulkSet(c);
    });
}

async function saveBulk() {
    const locName = document.getElementById('bulkLocalitySelect').value;
    const ids = Array.from(bulkSelection);

    if (ids.length === 0 || !locName) { alert("Select addresses and locality!"); return; }

    const payload = {
        address_ids: ids,
        locality_id: locName,
        locality_name: locName
    };

    const res = await fetch(`${API_BASE}/bulk-save/`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });
    const data = await res.json();

    if (data.success) {
        alert(`Updated ${data.count} addresses!`);
        fetchBulkData(1);
        fetchTableData(1);
    }
}

// --- ADD MASTER ---
async function addNewMaster() {
    const name = document.getElementById('newLocName').value;
    const zone = document.getElementById('newLocZone').value;

    if (!name || !zone) { alert("Enter Name and Zone"); return; }

    const success = await postData('/add-master-locality/', { locality_name: name, zone_name: zone });
    if (success) {
        alert("Locality Added Successfully!");
        document.getElementById('newLocName').value = "";
        fetchMasterDropdown();
    }
}

// --- UTILITIES ---
function switchTab(tabId) {
    document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.btn-tab').forEach(el => el.classList.remove('active'));

    document.getElementById(`tab-${tabId}`).classList.add('active');
    document.querySelector(`button[onclick="switchTab('${tabId}')"]`).classList.add('active');

    if (tabId === 'set') fetchNextPending();
}

function setSetMode(mode) {
    document.getElementById('mode-single').classList.add('hidden');
    document.getElementById('mode-bulk').classList.add('hidden');
    document.getElementById('sub-single').classList.remove('active');
    document.getElementById('sub-bulk').classList.remove('active');

    document.getElementById(`mode-${mode}`).classList.remove('hidden');
    document.getElementById(`sub-${mode}`).classList.add('active');

    if (mode === 'bulk') fetchBulkData(1);
    else fetchNextPending();
}

async function postData(endpoint, body) {
    try {
        const res = await fetch(`${API_BASE}${endpoint}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });

        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            alert("Error: " + (err.detail ? JSON.stringify(err.detail) : "Validation Failed"));
            return false;
        }

        const data = await res.json();
        return data.success;
    } catch (e) { alert("Network Error"); return false; }
}
// --- NEW FUNCTION: Bulk Preview Logic ---
function updateBulkPreview() {
    const locName = document.getElementById('bulkLocalitySelect').value;
    const loc = masterLocalities.find(l => l.locality === locName);

    if (loc) {
        document.getElementById('bulkPreviewZone').textContent = loc.zone || "-";
        document.getElementById('bulkPreviewKM').textContent = loc.km || loc.billing_km || "-";
        // Visual cue: change text color if valid
        document.getElementById('bulkPreviewZone').style.color = "#333";
    } else {
        document.getElementById('bulkPreviewZone').textContent = "-";
        document.getElementById('bulkPreviewKM').textContent = "-";
    }
}