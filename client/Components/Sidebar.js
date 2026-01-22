document.addEventListener("DOMContentLoaded", function () {
    // 1. Define the HTML for the sidebar
    const sidebarHTML = `
    <button id="hamburger-btn" class="hamburger-btn">☰</button>
    <div id="sidebar-overlay" class="sidebar-overlay"></div>
    <div id="sidebar-container" class="sidebar-container">
        <div class="sidebar-header">
            <h2>Menu</h2>
            <button id="close-btn" class="close-btn">✖</button>
        </div>
        <nav class="sidebar-nav">
            <a href="/" class="nav-item">📊 Dashboard</a>
            <a href="/cleaner" class="nav-item">🚚 Cleaner</a>
            <a href="/gps-corner" class="nav-item">🛰️ GPS Corner</a>
            <a href="/locality-manager" class="nav-item">🏙️ Locality Manager</a>
            <a href="/operation-manager" class="nav-item">⬇️ Downloads</a>
            <a href="/admin" class="nav-item">🔧 Admin</a>
            <a href="/logout" class="nav-item" style="color:red; margin-top: auto;">🚪 Logout</a>
        </nav>
    </div>
    `;

    // 2. Inject it immediately after the opening <body> tag
    document.body.insertAdjacentHTML('afterbegin', sidebarHTML);

    // 3. Add Event Listeners for Open/Close
    const hamburgerBtn = document.getElementById('hamburger-btn');
    const closeBtn = document.getElementById('close-btn');
    const overlay = document.getElementById('sidebar-overlay');
    const sidebar = document.getElementById('sidebar-container');

    function toggleSidebar() {
        sidebar.classList.toggle('open');
        overlay.classList.toggle('active');
    }

    function closeSidebar() {
        sidebar.classList.remove('open');
        overlay.classList.remove('active');
    }

    if (hamburgerBtn) hamburgerBtn.addEventListener('click', toggleSidebar);
    if (closeBtn) closeBtn.addEventListener('click', closeSidebar);
    if (overlay) overlay.addEventListener('click', closeSidebar);

    // 4. Highlight Active Link
    const currentPath = window.location.pathname;
    document.querySelectorAll('.nav-item').forEach(link => {
        // Simple check to highlight active button
        if (link.getAttribute('href') === currentPath) {
            link.classList.add('active');
        }
    });
});