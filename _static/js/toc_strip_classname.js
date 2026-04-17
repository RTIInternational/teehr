/**
 * Strip the "ClassName." prefix from "On this page" TOC entries.
 * Autodoc generates headings like "MyClass.my_method" — this reduces them
 * to just "my_method" in the secondary sidebar navigation.
 */
document.addEventListener("DOMContentLoaded", function () {
    const tocNav = document.querySelector(".bd-toc-nav");
    if (!tocNav) return;

    tocNav.querySelectorAll("a").forEach(function (link) {
        // Match and strip a single "Word." prefix (e.g. "MyClass.method" -> "method")
        link.textContent = link.textContent.replace(/^[A-Za-z_][A-Za-z0-9_]*\./, "");
    });
});
