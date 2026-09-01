const config = window.HIGHWATER_SITE || {};

document.querySelectorAll("[data-config-link]").forEach((link) => {
  const key = link.dataset.configLink;
  const destination = config[key];
  if (!destination || destination === "#") {
    link.setAttribute("aria-disabled", "true");
    link.setAttribute("tabindex", "-1");
    link.setAttribute("title", link.textContent.trim() + " link coming soon");
    link.addEventListener("click", (event) => event.preventDefault());
    return;
  }
  link.href = destination;
});

document.querySelector("[data-year]").textContent = new Date().getFullYear();

const menuButton = document.querySelector("[data-menu-button]");
const mobileNavigation = document.querySelector("[data-mobile-nav]");
const menuLabel = menuButton.querySelector(".sr-only");

menuButton.addEventListener("click", () => {
  const open = menuButton.getAttribute("aria-expanded") === "true";
  menuButton.setAttribute("aria-expanded", String(!open));
  menuLabel.textContent = open ? "Open navigation" : "Close navigation";
  mobileNavigation.hidden = open;
  document.body.classList.toggle("menu-open", !open);
});

mobileNavigation.querySelectorAll("a").forEach((link) => {
  link.addEventListener("click", () => {
    menuButton.setAttribute("aria-expanded", "false");
    menuLabel.textContent = "Open navigation";
    mobileNavigation.hidden = true;
    document.body.classList.remove("menu-open");
  });
});

document.addEventListener("keydown", (event) => {
  if (event.key !== "Escape" || mobileNavigation.hidden) {
    return;
  }
  menuButton.setAttribute("aria-expanded", "false");
  menuLabel.textContent = "Open navigation";
  mobileNavigation.hidden = true;
  document.body.classList.remove("menu-open");
  menuButton.focus();
});

const copyButton = document.querySelector("[data-copy]");
if (copyButton) {
  copyButton.addEventListener("click", async () => {
    const label = copyButton.querySelector(".copy-label");
    try {
      await navigator.clipboard.writeText(copyButton.dataset.copy);
      label.textContent = "Copied";
    } catch {
      label.textContent = "Select";
    }
    window.setTimeout(() => {
      label.textContent = "Copy";
    }, 1800);
  });
}

const header = document.querySelector("[data-header]");
const updateHeader = () => {
  header.classList.toggle("header-scrolled", window.scrollY > 16);
};
updateHeader();
window.addEventListener("scroll", updateHeader, { passive: true });
