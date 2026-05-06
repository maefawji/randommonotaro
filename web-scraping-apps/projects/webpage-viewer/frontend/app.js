// 画面上で使う入力欄・ボタン・表示エリアを最初に取得する。
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
const layoutModeSelect = document.getElementById("layout-mode-select");
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
const PAGE_KEY = document.body.dataset.pageKey || PAGE_MODE;
const VIEW_MODE = document.body.dataset.viewMode || PAGE_MODE;
const LAYOUT_MODE = document.body.dataset.layoutMode || "";

// 画面の現在状態をまとめて持つオブジェクト。
// API で取った画像、表示済み履歴、Summary 用の情報などを入れている。
const state = {
  items: [],
  sid: "",
  project: "",
  tag: "",
  lastShownIds: [],
  renderedItems: [],
  cycleSeenIds: [],
  cycleSignature: "",
  fetchedAt: 0,
  lastResponse: null,
};

// 上部ステータス文言の更新。
function setStatus(message, isError = false) {
  statusText.textContent = message;
  statusText.style.color = isError ? "#8d2f2f" : "";
}

// 読み込み中のボタン状態切替。
function setLoading(isLoading) {
  submitButton.disabled = isLoading;
  submitButton.textContent = isLoading ? "Loading..." : "Load Images";
}

// HTML に文字列を安全に差し込むためのエスケープ処理。
function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

// Unix time を画面用の日時文字列に変換する。
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

// API から返ってきた集計情報を state に保存し、Summary を描画する。
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

// 表示枚数の入力値を安全な範囲に補正して返す。
function getDisplayCount() {
  const value = Number(displayCountInput.value);
  if (!Number.isFinite(value)) {
    return 3;
  }
  return Math.max(1, Math.min(30, Math.floor(value)));
}

// 検索欄の現在値を取得する。
function getSearchTerm() {
  return searchInput.value.trim().toLowerCase();
}

// 並び順セレクトの現在値を取得する。
function getSortMode() {
  return sortSelect.value || "random";
}

// ページごとの表示モードを返す。
function getViewMode() {
  return VIEW_MODE;
}

// レイアウト切替が固定のページでは、その固定値を優先する。
function getLayoutMode() {
  if (LAYOUT_MODE) {
    return LAYOUT_MODE;
  }
  if (!layoutModeSelect) {
    return "drift";
  }
  return layoutModeSelect.value || "drift";
}

// Crop / Contain の選択結果を CSS に伝える。
function applyImageFitMode() {
  document.documentElement.dataset.imageFit = imageFitSelect.value === "contain" ? "contain" : "cover";
}

// API の pages 配列を、フロントで扱いやすい画像単位の配列に変換する。
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

// 実画像URLをバックエンドの画像プロキシURLへ変換する。
function buildImageProxyUrl(imageUrl) {
  const query = new URLSearchParams({
    url: imageUrl,
    sid: state.sid,
    project: state.project,
  });
  return `${API_BASE}/api/image?${query.toString()}`;
}

// 同じ画像に対して毎回同じ配置乱数を作るための簡易ハッシュ。
function hashString(value) {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return Math.abs(hash >>> 0);
}

// A/D ページの配置で使う、位置ずれや回転量を作る。
function buildLayoutStyle(item, index) {
  const seed = hashString(`${item.id}:${index}`);
  const driftX = ((seed % 17) - 8) * 3;
  const driftY = (((Math.floor(seed / 17)) % 13) - 6) * 3;
  const rotate = (((Math.floor(seed / 221)) % 11) - 5) * 0.7;
  const colSpan = (seed % 5 === 0) ? 2 : 1;
  const rowSpan = (seed % 7 === 0) ? 2 : 1;
  const lift = (seed % 9) * -10;
  const scale = 1 + ((seed % 6) * 0.035);
  return [
    `--drift-x:${driftX}px`,
    `--drift-y:${driftY}px`,
    `--tilt:${rotate}deg`,
    `--col-span:${colSpan}`,
    `--row-span:${rowSpan}`,
    `--lift:${lift}px`,
    `--scale:${scale.toFixed(3)}`,
  ].join(";");
}

// 列を基準にして、Y方向へランダムに散らすレイアウトを作る。
function buildAFreeformMarkup(items) {
  const columnCount = Math.max(2, Math.min(5, Math.ceil(Math.sqrt(items.length))));
  const columnBuckets = Array.from({ length: columnCount }, () => []);

  items.forEach((item, index) => {
    const seed = hashString(`${item.id}:freeform:${index}`);
    const targetColumn = seed % columnCount;
    columnBuckets[targetColumn].push({ item, index, seed });
  });

  const placements = [];
  const estimatedHeight = Math.max(720, Math.ceil(items.length / columnCount) * 180 + 180);

  columnBuckets.forEach((bucket, columnIndex) => {
    const sortedBucket = [...bucket].sort((left, right) => left.seed - right.seed);
    const count = Math.max(sortedBucket.length, 1);
    sortedBucket.forEach((entry, localIndex) => {
      const yBase = count === 1 ? 120 : 80 + (localIndex / (count - 1)) * (estimatedHeight - 200);
      const yJitter = (((Math.floor(entry.seed / 13)) % 25) - 12) * 8;
      const xJitter = (((Math.floor(entry.seed / 317)) % 11) - 5) * 10;
      const rotate = (((Math.floor(entry.seed / 97)) % 9) - 4) * 0.9;
      const scale = 0.88 + ((entry.seed % 7) * 0.045);
      const left = ((columnIndex + 0.5) / columnCount) * 100;
      const style = [
        `left:${left}%`,
        `top:${Math.max(40, yBase + yJitter)}px`,
        `--free-x:${xJitter}px`,
        `--free-rotate:${rotate}deg`,
        `--free-scale:${scale.toFixed(3)}`,
      ].join(";");
      placements.push(buildCardMarkup(entry.item, "image-card-top image-card-a image-card-freeform", entry.index, style));
    });
  });

  return `
    <section class="freeform-wall" style="height:${estimatedHeight}px">
      ${placements.join("")}
    </section>
  `;
}

// D ページ用の 5x5 グリッドプレビューを作る。
function buildGridPickMarkup(items) {
  return `
    <section class="gridpick-frame">
      ${Array.from({ length: 25 }, (_, index) => {
        const col = (index % 5) + 1;
        const row = Math.floor(index / 5) + 1;
        const style = `grid-column:${col};grid-row:${row};`;
        return `<div class="gridpick-cell" style="${style}"><span>${index + 1}</span></div>`;
      }).join("")}
    </section>
  `;
}

// Summary の表示内容をページ別に組み立てる。
function renderSummary(data) {
  if (!data || !data.project) {
    summaryPanel.hidden = true;
    summaryTitle.textContent = "Results";
    summaryTimestamp.textContent = "";
    summaryStats.innerHTML = "";
    controlsMeta.textContent = PAGE_KEY === "d"
      ? "A dedicated 5x5 pick layout page."
      : PAGE_MODE === "b"
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

// 検索欄の文字列に一致する画像だけを残す。
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

// セレクトの値に応じて画像配列を並べ替える。
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

// 最終的に画面へ出す候補を作る。
function getVisibleItems(items) {
  return sortItems(filterItems(items));
}

// 候補集合が変わったかを判定するための文字列。
function buildVisibilitySignature(items) {
  return items.map((item) => item.id).join("|");
}

// 候補が変わったら、重複回避の履歴を初期化する。
function ensureCycleState(items) {
  const nextSignature = buildVisibilitySignature(items);
  if (state.cycleSignature === nextSignature) {
    return;
  }

  state.cycleSignature = nextSignature;
  state.cycleSeenIds = [];
}

// Display セクションの説明文を更新する。
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

// 画像カード 1 枚分の HTML を作る共通関数。
function buildCardMarkup(item, extraClass = "", index = 0, overrideStyle = "") {
  const timestamp = formatUnixTime(item.updated || item.created);
  const cardClassName = ["image-card", extraClass].filter(Boolean).join(" ");
  const tooltipParts = [item.pageTitle, item.previewText, item.imageContext].filter(Boolean);
  const tooltipText = tooltipParts.join(" / ");
  const computedStyle = overrideStyle || (getViewMode() === "a" ? buildLayoutStyle(item, index) : "");
  const inlineStyle = computedStyle ? ` style="${computedStyle}"` : "";
  return `
    <article class="${cardClassName}" data-item-id="${escapeHtml(item.id)}"${inlineStyle}>
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

// ページやレイアウトモードに応じて、一覧全体の作り方を切り替える。
function buildGalleryMarkup(items) {
  const mode = getViewMode();
  if (mode === "a") {
    if (getLayoutMode() === "gridpick") {
      return buildGridPickMarkup(items);
    }
    if (getLayoutMode() === "yshuffle") {
      return buildAFreeformMarkup(items);
    }
    return items.map((item, index) => buildCardMarkup(item, "image-card-top image-card-a", index)).join("");
  }
  if (mode === "b") {
    return items.map((item, index) => buildCardMarkup(item, "image-card-list image-card-top", index)).join("");
  }
  if (mode === "c") {
    const [featured, ...rest] = items;
    return `
      <section class="spotlight-layout">
        ${featured ? buildCardMarkup(featured, "image-card-spotlight", 0) : ""}
        ${rest.length > 0 ? `<div class="spotlight-grid">${rest.map((item, index) => buildCardMarkup(item, "image-card-compact", index + 1)).join("")}</div>` : ""}
      </section>
    `;
  }
  return items.map((item, index) => buildCardMarkup(item, "", index)).join("");
}

// CSS 側で見た目を切り替えるための data 属性を付ける。
function applyViewModeClass() {
  gallery.dataset.viewMode = getViewMode();
  gallery.dataset.layoutMode = getLayoutMode();
}

// D ページで、画像がなくても 5x5 グリッドだけを出す。
function renderGridPickPreview() {
  applyViewModeClass();
  gallery.innerHTML = buildGridPickMarkup([]);
}

// ギャラリー全体の描画を担当する中心処理。
function renderGallery(items) {
  const visibleItems = getVisibleItems(items);
  ensureCycleState(visibleItems);
  updateControlsMeta(visibleItems, items.length);
  applyViewModeClass();

  if (!items || items.length === 0) {
    if (PAGE_KEY === "d" || (getViewMode() === "a" && getLayoutMode() === "gridpick")) {
      renderGridPickPreview();
    } else {
      gallery.innerHTML = '<article class="empty-state panel">No images were found for this tag.</article>';
    }
    shuffleButton.disabled = true;
    return;
  }

  if (visibleItems.length === 0) {
    gallery.innerHTML = '<article class="empty-state panel">No images match the current search.</article>';
    shuffleButton.disabled = true;
    return;
  }

  const displayCount = getDisplayCount();
  const effectiveDisplayCount = getViewMode() === "a" && getLayoutMode() === "gridpick"
    ? Math.min(3, visibleItems.length)
    : displayCount;
  const selectedItems = getSortMode() === "random"
    ? pickCycleItems(visibleItems, effectiveDisplayCount)
    : visibleItems.slice(0, Math.min(effectiveDisplayCount, visibleItems.length));
  shuffleButton.disabled = getSortMode() !== "random" || visibleItems.length <= displayCount;
  state.renderedItems = selectedItems;
  gallery.innerHTML = buildGalleryMarkup(selectedItems);
}

// 「まだ表示していない画像」を優先して選ぶ抽選処理。
function pickCycleItems(items, count, excludeIds = new Set()) {
  const selectedItems = [];
  let localSeenSet = new Set(state.cycleSeenIds);
  const targetCount = Math.min(count, items.length);

  while (selectedItems.length < targetCount) {
    const selectedIds = new Set(selectedItems.map((item) => item.id));
    let pool = items.filter((item) => !localSeenSet.has(item.id) && !excludeIds.has(item.id) && !selectedIds.has(item.id));

    if (pool.length === 0) {
      localSeenSet = new Set(excludeIds);
      pool = items.filter((item) => !localSeenSet.has(item.id) && !excludeIds.has(item.id) && !selectedIds.has(item.id));
    }

    if (pool.length === 0) {
      pool = items.filter((item) => !excludeIds.has(item.id) && !selectedIds.has(item.id));
    }

    if (pool.length === 0) {
      break;
    }

    const nextItem = pool[Math.floor(Math.random() * pool.length)];
    selectedItems.push(nextItem);
    localSeenSet.add(nextItem.id);
  }

  state.cycleSeenIds = [...localSeenSet];
  return selectedItems;
}

// クリックした画像1枚だけを別画像に差し替える。
function replaceRenderedItem(itemId, cardElement) {
  const currentIndex = state.renderedItems.findIndex((item) => item.id === itemId);
  if (currentIndex < 0) {
    return;
  }

  const visibleItems = getVisibleItems(state.items);
  ensureCycleState(visibleItems);
  const renderedIds = new Set(state.renderedItems.map((item) => item.id));
  const replacements = pickCycleItems(visibleItems, 1, renderedIds);
  const replacement = replacements[0];
  if (!replacement) {
    return;
  }

  state.renderedItems = state.renderedItems.map((item, index) => (index === currentIndex ? replacement : item));
  if (!cardElement) {
    return;
  }

  const classNames = Array.from(cardElement.classList).filter((className) => className !== "image-card");
  cardElement.outerHTML = buildCardMarkup(replacement, classNames.join(" "));
}

// 入力値や表示設定をブラウザ保存する。
function saveSettings() {
  const payload = {
    url: urlInput.value.trim(),
    tag: tagInput.value.trim(),
    displayCount: getDisplayCount(),
    imageFit: imageFitSelect.value,
    layoutMode: getLayoutMode(),
    sortMode: getSortMode(),
    search: searchInput.value,
  };
  window.localStorage.setItem(`${STORAGE_KEY}-${PAGE_KEY}`, JSON.stringify(payload));
}

// 保存しておいた入力値や表示設定を復元する。
function restoreSettings() {
  try {
    const raw = window.localStorage.getItem(`${STORAGE_KEY}-${PAGE_KEY}`);
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
    if (saved.layoutMode && layoutModeSelect) {
      layoutModeSelect.value = saved.layoutMode;
    }
    if (saved.sortMode) {
      sortSelect.value = saved.sortMode;
    }
    if (saved.search) {
      searchInput.value = saved.search;
    }
  } catch (error) {
    window.localStorage.removeItem(`${STORAGE_KEY}-${PAGE_KEY}`);
  }
}

// バックエンド API から JSON を取得する共通処理。
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

// フォーム送信時の本体。
// URL や Tag を送って画像一覧を取得し、画面を更新する。
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
    state.cycleSeenIds = [];
    state.cycleSignature = "";
    renderGallery(state.items);
    setStatus(`Scanned ${data.scanned_count || 0} pages and found ${data.image_count} images across ${data.page_count} pages.${data.skipped_count ? ` ${data.skipped_count} pages failed.` : ""}`);
  } catch (error) {
    state.items = [];
    state.lastShownIds = [];
    state.cycleSeenIds = [];
    state.cycleSignature = "";
    updateSummary({});
    gallery.innerHTML = '<article class="empty-state panel">Failed to load images.</article>';
    shuffleButton.disabled = true;
    setStatus(error.message, true);
  } finally {
    setLoading(false);
  }
});

// Shuffle ボタンは、今の候補から再抽選する。
shuffleButton.addEventListener("click", () => {
  renderGallery(state.items);
});

// 表示枚数を変えたら、その場で再描画する。
displayCountInput.addEventListener("change", () => {
  displayCountInput.value = String(getDisplayCount());
  saveSettings();
  if (state.items.length > 0) {
    renderGallery(state.items);
  }
});

// Crop / Contain の切替を反映する。
imageFitSelect.addEventListener("change", () => {
  saveSettings();
  applyImageFitMode();
  renderGallery(state.items);
});

// レイアウト切替のあるページだけ、その変更を反映する。
if (layoutModeSelect) {
  layoutModeSelect.addEventListener("change", () => {
    saveSettings();
    renderGallery(state.items);
  });
}

// 検索欄は入力中にそのまま絞り込みへ反映する。
searchInput.addEventListener("input", () => {
  saveSettings();
  state.lastShownIds = [];
  renderGallery(state.items);
});

// 並び順変更もその場で再描画する。
sortSelect.addEventListener("change", () => {
  saveSettings();
  state.lastShownIds = [];
  renderGallery(state.items);
});

urlInput.addEventListener("change", saveSettings);
tagInput.addEventListener("change", saveSettings);

// 画像の読み込み失敗時は代替メッセージを出す。
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

// 画像クリック時はページ遷移せず、そのカードだけ差し替える。
gallery.addEventListener("click", (event) => {
  const trigger = event.target.closest(".image-trigger");
  if (!trigger) {
    return;
  }

  replaceRenderedItem(trigger.dataset.itemId || "", trigger.closest(".image-card"));
});

// 初期化処理。
// 保存済み設定を読み込み、見た目を整え、必要なら D のグリッドを出す。
restoreSettings();
applyImageFitMode();
renderSummary(null);
applyViewModeClass();
if (PAGE_KEY === "d" || (getViewMode() === "a" && getLayoutMode() === "gridpick")) {
  renderGridPickPreview();
}
