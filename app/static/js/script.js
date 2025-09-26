// --- Stars background ---
function createStars() {
    const starsContainer = document.getElementById('stars');
    if (!starsContainer) return;

    const starCount = 200;
    for (let i = 0; i < starCount; i++) {
        const star = document.createElement('div');
        star.className = 'star';

        const size = Math.random() * 3;
        const left = Math.random() * 100;
        const top = Math.random() * 100;
        const duration = Math.random() * 4 + 2;
        const delay = Math.random() * 5;

        star.style.width = `${size}px`;
        star.style.height = `${size}px`;
        star.style.left = `${left}%`;
        star.style.top = `${top}%`;
        star.style.animationDuration = `${duration}s`;
        star.style.animationDelay = `${delay}s`;

        starsContainer.appendChild(star);
    }
}

// --- Mobile menu toggle ---
function setupMenuToggle() {
    const sidebar = document.querySelector('.sidebar');
    if (!sidebar) return;

    const menuToggle = document.createElement('button');
    menuToggle.innerHTML = '<i class="fas fa-bars"></i>';
    menuToggle.classList.add('menu-toggle');

    document.body.appendChild(menuToggle);

    menuToggle.addEventListener('click', () => {
        sidebar.classList.toggle('active');
    });

    function checkMobile() {
        if (window.innerWidth <= 768) {
            menuToggle.style.display = 'block';
        } else {
            menuToggle.style.display = 'none';
            sidebar.classList.remove('active');
        }
    }

    checkMobile();
    window.addEventListener('resize', checkMobile);
}

// --- User profile dropdown ---
function setupUserDropdown() {
    const profile = document.querySelector('.user-profile');
    if (!profile) return;

    profile.addEventListener('click', (e) => {
        e.stopPropagation();
        profile.classList.toggle('active');
    });

    document.addEventListener('click', () => {
        profile.classList.remove('active');
    });
}

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
    createStars();
    setupMenuToggle();
    setupUserDropdown();
});
