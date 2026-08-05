        (function() {
            if (localStorage.getItem('darkMode') === 'true' ||
                (localStorage.getItem('darkMode') === null && window.matchMedia('(prefers-color-scheme: dark)').matches)) {
                document.documentElement.classList.add('dark');
            }
        })();
