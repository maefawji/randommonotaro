const form = document.getElementById("fetch-form");
const urlInput = document.getElementById("url-input");
const tagInput = document.getElementById("tag-input");
const sidInput = document.getElementById("sid-input");
const submitButton = document.getElementById("submit-button");
const statusText = document.getElementById("status");
const gallery = document.getElementById("gallery");
const shuffleButton = document.getElementById("shuffle-button");
const displayCountInput = document.getElementById("display-count-input");
const imageFitSelect = document.getElementById("image-fit-select");
const searchInput = document.getElementById("search-input");
const sortSelect = document.getElementById("sort-select");
const summaryPanel = document.getElementById("summary-panel");
const summaryTitle = document.getElementById("summary-title");
const summaryTimestamp = document.getElementById("summary-timestamp");
const summaryStats = document.getElementById("summary-stats");
const controlsMeta = document.getElementById("controls-meta");
const API_BASE = window.location.protocol === "file:" ? "http://127.0.0.1:8011" : "";
const STORAGE_KEY = "scrapbox-viewer-settings";
const PAGE_MODE = document.body.dataset.pageMode || "a";
const state = {
  items: [],
  sid: "",
  project: "",
  tag: "",
  lastShownIds: [],
  renderedItems: [],
  fetchedAt: 0,
  lastResponse: null,
};

function setStatus(message, isError = false) {
  statusText.textContent = message;
  statusText.style.color = isError ? "#8d2f2f" : "";
}

function setLoading(isLoading) {
  submitButton.disabled = isLoading;
  submitButton.textContent = isLoading ? "Loading..." : "Load Images";
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatUnixTime(timestamp) {
  if (!timestamp) {
    return "";
  }

  const date = new Date(timestamp * 1000);
  if (Number.isNaN(date.getTime())) {
    return "";
  }

  return new Intl.DateTimeFormat("ja-JP", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function updateSummary(data) {
  state.project = data.project || "";
  state.tag = data.tag || "";
  state.fetchedAt = Date.now();
  state.lastResponse = data;
  renderSummary(data);
}

function getRandomItems(items, count) {
  const pool = [...items];
  for (let index = pool.length - 1; index > 0; index -= 1) {
    const randomIndex = Math.floor(Math.random() * (index + 1));
    [pool[index], pool[randomIndex]] = [pool[randomIndex], pool[index]];
  }
  return pool.slice(0, Math.min(count, pool.length));
}

function pickDisplayItems(items, count) {
  const limitedCount = Math.min(count, items.length);
  const previousIds = new Set(state.lastShownIds);
  const freshPool = items.filter((item) => !previousIds.has(item.id));
  const basePool = freshPool.length >= limitedCount ? freshPool : items;
  const selectedItems = getRandomItems(basePool, limitedCount);

  if (selectedItems.length < limitedCount) {
    const selectedIds = new Set(selectedItems.map((item) => item.id));
    const remainder = items.filter((item) => !selectedIds.has(item.id));
    selectedItems.push(...getRandomItems(remainder, limitedCount - selectedItems.length));
  }

  state.lastShownIds = selectedItems.map((item) => item.id);
  return selectedItems;
}

function getDisplayCount() {
  const value = Number(displayCountInput.value);
  if (!Number.isFinite(value)) {
    return 3;
  }
  return Math.max(1, Math.min(30, Math.floor(value)));
}

function getSearchTerm() {
  return searchInput.value.trim().toLowerCase();
}

function getSortMode() {
  return sortSelect.value || "random";
}

function getViewMode() {
  return PAGE_MODE;
}

function applyImageFitMode() {
  document.documentElement.dataset.imageFit = imageFitSelect.value === "contain" ? "contain" : "cover";
}

function flattenImages(pages) {
  return (pages || []).flatMap((page) =>
    (Array.isArray(page.image_items) && page.image_items.length > 0 ? page.image_items : (page.images || []).map((url) => ({ url, context: "" }))).map((imageItem, index) => ({
      id: `${page.title}-${index}-${imageItem.url}`,
      imageUrl: imageItem.url,
      pageTitle: page.title,
      pageUrl: page.url,
      previewText: page.preview_text || "",
      imageContext: imageItem.context || "",
      created: page.created || 0,
      updated: page.updated || 0,
    })),
  );
}

function buildImageProxyUrl(imageUrl) {
  const query = new URLSearchParams({
    url: imageUrl,
    sid: state.sid,
    project: state.project,
  });
  return `${API_BASE}/api/image?${query.toString()}`;
}

function renderSummary(data) {
  if (!data || !data.project) {
    summaryPanel.hidden = true;
    summaryTitle.textContent = "Results";
    summaryTimestamp.textContent = "";
    summaryStats.innerHTML = "";
    controlsMeta.textContent = PAGE_MODE === "b"
      ? "A monochrome layout with images arranged side by side."
      : "Show a random selection from the extracted images.";
    return;
  }

  summaryPanel.hidden = false;
  summaryTitle.textContent = `${data.project} ${data.tag || ""}`.trim();
  summaryTimestamp.textContent = state.fetchedAt ? `Fetched: ${formatUnixTime(Math.floor(state.fetchedAt / 1000))}` : "";
  if (PAGE_MODE === "b") {
    summaryStats.innerHTML = `
      <p class="summary-inline-text">
        Scanned ${escapeHtml(data.scanned_count || 0)} / Matched ${escapeHtml(data.page_count || 0)} / Images ${escapeHtml(data.image_count || 0)} / Failed ${escapeHtml(data.skipped_count || 0)}
      </p>
    `;
    return;
  }
  summaryStats.innerHTML = [
    { label: "Scanned", value: data.scanned_count || 0 },
    { label: "Matched", value: data.page_count || 0 },
    { label: "Images", value: data.image_count || 0 },
    { label: "Failed", value: data.skipped_count || 0 },
  ]
    .map(({ label, value }) => `
      <article class="summary-stat">
        <span class="summary-label">${escapeHtml(label)}</span>
        <strong class="summary-value">${escapeHtml(value)}</strong>
      </article>
    `)
    .join("");
}

function filterItems(items) {
  const term = getSearchTerm();
  if (!term) {
    return items;
  }

  return items.filter((item) => {
    const haystack = [item.pageTitle, item.previewText, item.imageContext].join(" ").toLowerCase();
    return haystack.includes(term);
  });
}

function sortItems(items) {
  const mode = getSortMode();
  if (mode === "updated-desc") {
    return [...items].sort((left, right) => (right.updated || 0) - (left.updated || 0));
  }
  if (mode === "created-desc") {
    return [...items].sort((left, right) => (right.created || 0) - (left.created || 0));
  }
  if (mode === "title-asc") {
    return [...items].sort((left, right) => left.pageTitle.localeCompare(right.pageTitle, "ja"));
  }
  return items;
}

function getVisibleItems(items) {
  return sortItems(filterItems(items));
}

function updateControlsMeta(visibleItems, totalItems) {
  const sortMode = getSortMode();
  const sortLabel = {
    random: "Random",
    "updated-desc": "Updated",
    "created-desc": "Created",
    "title-asc": "Title",
  }[sortMode];
  const modeLabel = {
    a: "A",
    b: "B",
    c: "C",
  }[getViewMode()];
  controlsMeta.textContent = `${visibleItems.length} of ${totalItems} images visible. View ${modeLabel} / Sort ${sortLabel}.`;
}

function buildCardMarkup(item, extraClass = "") {
  const timestamp = formatUnixTime(item.updated || item.created);
  const cardClassName = ["image-card", extraClass].filter(Boolean).join(" ");
  const tooltipParts = [item.pageTitle, item.previewText, item.imageContext].filter(Boolean);
  const tooltipText = tooltipParts.join(" / ");
  return `
    <article class="${cardClassName}" data-item-id="${escapeHtml(item.id)}">
      <button class="image-link image-trigger" type="button" data-item-id="${escapeHtml(item.id)}" aria-label="${escapeHtml(item.pageTitle)}">
        <img class="gallery-image" src="${escapeHtml(buildImageProxyUrl(item.imageUrl))}" alt="${escapeHtml(item.pageTitle)}" loading="lazy" data-image-url="${escapeHtml(item.imageUrl)}" />
        <span class="image-tooltip">${escapeHtml(tooltipText || item.pageTitle)}</span>
      </button>
      <div class="image-caption">
        <a class="image-title" href="${escapeHtml(item.pageUrl)}" target="_blank" rel="noreferrer">${escapeHtml(item.pageTitle)}</a>
        ${timestamp ? `<p class="image-meta">${escapeHtml(timestamp)}</p>` : ""}
        ${item.imageContext ? `<p class="image-context">${escapeHtml(item.imageContext)}</p>` : ""}
      </div>
    </article>
  `;
}

function buildGalleryMarkup(items) {
  const mode = getViewMode();
  if (mode === "b") {
    return items.map((item) => buildCardMarkup(item, "image-card-list image-card-top")).join("");
  }
  if (mode === "c") {
    const [featured, ...rest] = items;
    return `
      <section class="spotlight-layout">
        ${featured ? buildCardMarkup(featured, "image-card-spotlight") : ""}
        ${rest.length > 0 ? `<div class="spotlight-grid">${rest.map((item) => buildCardMarkup(item, "image-card-compact")).join("")}</div>` : ""}
      </section>
    `;
  }
  return items.map((item) => buildCardMarkup(item)).join("");
}

function applyViewModeClass() {
  gallery.dataset.viewMode = getViewMode();
}

function renderGallery(items) {
  const visibleItems = getVisibleItems(items);
  updateControlsMeta(visibleItems, items.length);
  applyViewModeClass();

  if (!items || items.length === 0) {
    gallery.innerHTML = '<article class="empty-state panel">No images were found for this tag.</article>';
    shuffleButton.disabled = true;
    return;
  }

  if (visibleItems.length === 0) {
    gallery.innerHTML = '<article class="empty-state panel">No images match the current search.</article>';
    shuffleButton.disabled = true;
    return;
  }

  const displayCount = getDisplayCount();
  const selectedItems = getSortMode() === "random"
    ? pickDisplayItems(visibleItems, displayCount)
    : visibleItems.slice(0, Math.min(displayCount, visibleItems.length));
  shuffleButton.disabled = getSortMode() !== "random" || visibleItems.length <= displayCount;
  state.renderedItems = selectedItems;
  gallery.innerHTML = buildGalleryMarkup(selectedItems);
}

function replaceRenderedItem(itemId, cardElement) {
  const currentIndex = state.renderedItems.findIndex((item) => item.id === itemId);
  if (currentIndex < 0) {
    return;
  }

  const visibleItems = getVisibleItems(state.items);
  const renderedIds = new Set(state.renderedItems.map((item) => item.id));
  const candidatePool = visibleItems.filter((item) => !renderedIds.has(item.id));
  const fallbackPool = visibleItems.filter((item) => item.id !== itemId);
  const pool = candidatePool.length > 0 ? candidatePool : fallbackPool;

  if (pool.length === 0) {
    return;
  }

  const replacement = pool[Math.floor(Math.random() * pool.length)];
  state.renderedItems = state.renderedItems.map((item, index) => (index === currentIndex ? replacement : item));
  if (!cardElement) {
    return;
  }

  const classNames = Array.from(cardElement.classList).filter((className) => className !== "image-card");
  cardElement.outerHTML = buildCardMarkup(replacement, classNames.join(" "));
}

function saveSettings() {
  const payload = {
    url: urlInput.value.trim(),
    tag: tagInput.value.trim(),
    displayCount: getDisplayCount(),
    imageFit: imageFitSelect.value,
    sortMode: getSortMode(),
    search: searchInput.value,
  };
  window.localStorage.setItem(`${STORAGE_KEY}-${PAGE_MODE}`, JSON.stringify(payload));
}

function restoreSettings() {
  try {
    const raw = window.localStorage.getItem(`${STORAGE_KEY}-${PAGE_MODE}`);
    if (!raw) {
      return;
    }
    const saved = JSON.parse(raw);
    if (saved.url) {
      urlInput.value = saved.url;
    }
    if (saved.tag) {
      tagInput.value = saved.tag;
    }
    if (saved.displayCount) {
      displayCountInput.value = String(saved.displayCount);
    }
    if (saved.imageFit) {
      imageFitSelect.value = saved.imageFit;
    }
    if (saved.sortMode) {
      sortSelect.value = saved.sortMode;
    }
    if (saved.search) {
      searchInput.value = saved.search;
    }
  } catch (error) {
    window.localStorage.removeItem(`${STORAGE_KEY}-${PAGE_MODE}`);
  }
}

async function fetchJson(url) {
  let response;
  try {
    response = await fetch(`${API_BASE}${url}`);
  } catch (error) {
    throw new Error("Could not connect to the API. Make sure `server.py` is running at http://127.0.0.1:8011.");
  }
  const contentType = response.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) {
    const text = await response.text();
    const shortText = text.slice(0, 120).replace(/\s+/g, " ").trim();
    throw new Error(`The API returned HTML instead of JSON. Restart the local server and open the app from http://127.0.0.1:8011. (${response.status} ${shortText})`);
  }

  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || "Failed to load images.");
  }
  return data;
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();

  const scrapboxUrl = urlInput.value.trim();
  const tag = tagInput.value.trim() || "#dailyinput";
  const sid = sidInput.value.trim();
  state.sid = sid;

  if (!scrapboxUrl) {
    setStatus("Enter a Scrapbox URL.", true);
    return;
  }

  setLoading(true);
  shuffleButton.disabled = true;
  setStatus("Scanning pages and loading images...");
  gallery.innerHTML = '<article class="empty-state panel">Loading images. This may take a while for large projects.</article>';

  try {
    const query = new URLSearchParams({
      url: scrapboxUrl,
      tag,
      sid,
    });
    const data = await fetchJson(`/api/tagged-images?${query.toString()}`);
    saveSettings();
    updateSummary(data);
    state.items = flattenImages(data.pages || []);
    state.lastShownIds = [];
    renderGallery(state.items);
    setStatus(`Scanned ${data.scanned_count || 0} pages and found ${data.image_count} images across ${data.page_count} pages.${data.skipped_count ? ` ${data.skipped_count} pages failed.` : ""}`);
  } catch (error) {
    state.items = [];
    state.lastShownIds = [];
    updateSummary({});
    gallery.innerHTML = '<article class="empty-state panel">Failed to load images.</article>';
    shuffleButton.disabled = true;
    setStatus(error.message, true);
  } finally {
    setLoading(false);
  }
});

shuffleButton.addEventListener("click", () => {
  renderGallery(state.items);
});

displayCountInput.addEventListener("change", () => {
  displayCountInput.value = String(getDisplayCount());
  saveSettings();
  if (state.items.length > 0) {
    renderGallery(state.items);
  }
});

imageFitSelect.addEventListener("change", () => {
  saveSettings();
  applyImageFitMode();
  renderGallery(state.items);
});

searchInput.addEventListener("input", () => {
  saveSettings();
  state.lastShownIds = [];
  renderGallery(state.items);
});

sortSelect.addEventListener("change", () => {
  saveSettings();
  state.lastShownIds = [];
  renderGallery(state.items);
});

urlInput.addEventListener("change", saveSettings);
tagInput.addEventListener("change", saveSettings);

gallery.addEventListener("error", (event) => {
  const image = event.target.closest(".gallery-image");
  if (!image) {
    return;
  }

  image.replaceWith(document.createRange().createContextualFragment(`
    <div class="image-fallback">
      <p>Could not display this image.</p>
      <a href="${escapeHtml(image.dataset.imageUrl || "")}" target="_blank" rel="noreferrer">Open image URL</a>
      <a href="${escapeHtml(image.currentSrc || image.src || "")}" target="_blank" rel="noreferrer">Open proxy URL</a>
    </div>
  `));
}, true);

gallery.addEventListener("click", (event) => {
  const trigger = event.target.closest(".image-trigger");
  if (!trigger) {
    return;
  }

  replaceRenderedItem(trigger.dataset.itemId || "", trigger.closest(".image-card"));
});

restoreSettings();
applyImageFitMode();
renderSummary(null);
applyViewModeClass();
