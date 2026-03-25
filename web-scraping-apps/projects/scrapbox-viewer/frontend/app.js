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
const API_BASE = window.location.protocol === "file:" ? "http://127.0.0.1:8011" : "";
const state = {
  items: [],
  sid: "",
  project: "",
  lastShownIds: [],
};

function setStatus(message, isError = false) {
  statusText.textContent = message;
  statusText.style.color = isError ? "#8d2f2f" : "";
}

function setLoading(isLoading) {
  submitButton.disabled = isLoading;
  submitButton.textContent = isLoading ? "抽出中..." : "画像を抽出";
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

function renderGallery(items) {
  if (!items || items.length === 0) {
    gallery.innerHTML = '<article class="empty-state panel">該当タグ付きページに画像が見つかりませんでした。</article>';
    shuffleButton.disabled = true;
    return;
  }

  const displayCount = getDisplayCount();
  const selectedItems = pickDisplayItems(items, displayCount);
  shuffleButton.disabled = items.length <= displayCount;

  gallery.innerHTML = selectedItems
    .map((item) => {
      return `
        <article class="image-card">
          <a class="image-link" href="${escapeHtml(item.pageUrl)}" target="_blank" rel="noreferrer" title="Scrapbox を開く">
            <img class="gallery-image" src="${escapeHtml(buildImageProxyUrl(item.imageUrl))}" alt="${escapeHtml(item.pageTitle)}" loading="lazy" data-image-url="${escapeHtml(item.imageUrl)}" />
          </a>
          <div class="image-caption">
            <a class="image-title" href="${escapeHtml(item.pageUrl)}" target="_blank" rel="noreferrer">${escapeHtml(item.pageTitle)}</a>
          </div>
        </article>
      `;
    })
    .join("");
}

async function fetchJson(url) {
  let response;
  try {
    response = await fetch(`${API_BASE}${url}`);
  } catch (error) {
    throw new Error("API に接続できませんでした。server.py を起動して http://127.0.0.1:8011 を開いているか確認してください。");
  }
  const contentType = response.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) {
    const text = await response.text();
    const shortText = text.slice(0, 120).replace(/\s+/g, " ").trim();
    throw new Error(`API が JSON ではなく HTML を返しました。ローカルサーバーを再起動して http://127.0.0.1:8011 から開いてください。 (${response.status} ${shortText})`);
  }

  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || "画像抽出に失敗しました。");
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
    setStatus("Scrapbox URL を入力してください。", true);
    return;
  }

  setLoading(true);
  shuffleButton.disabled = true;
  setStatus("該当ページを走査して画像を抽出しています...");
  gallery.innerHTML = '<article class="empty-state panel">画像を抽出しています。ページ数が多いと少し時間がかかります。</article>';

  try {
    const query = new URLSearchParams({
      url: scrapboxUrl,
      tag,
      sid,
    });
    const data = await fetchJson(`/api/tagged-images?${query.toString()}`);
    updateSummary(data);
    state.items = flattenImages(data.pages || []);
    state.lastShownIds = [];
    renderGallery(state.items);
    setStatus(`${data.scanned_count || 0} ページ走査し、${data.page_count} ページから ${data.image_count} 件の画像を抽出しました。${data.skipped_count ? ` ${data.skipped_count} ページは取得失敗です。` : ""}`);
  } catch (error) {
    state.items = [];
    state.lastShownIds = [];
    updateSummary({});
    gallery.innerHTML = '<article class="empty-state panel">画像を取得できませんでした。</article>';
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
  if (state.items.length > 0) {
    renderGallery(state.items);
  }
});

imageFitSelect.addEventListener("change", () => {
  applyImageFitMode();
});

gallery.addEventListener("error", (event) => {
  const image = event.target.closest(".gallery-image");
  if (!image) {
    return;
  }

  image.replaceWith(document.createRange().createContextualFragment(`
    <div class="image-fallback">
      <p>画像を表示できませんでした。</p>
      <a href="${escapeHtml(image.dataset.imageUrl || "")}" target="_blank" rel="noreferrer">画像URLを開く</a>
      <a href="${escapeHtml(image.currentSrc || image.src || "")}" target="_blank" rel="noreferrer">プロキシURLを開く</a>
    </div>
  `));
}, true);

applyImageFitMode();
