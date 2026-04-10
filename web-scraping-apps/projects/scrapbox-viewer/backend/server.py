#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = PROJECT_ROOT / "frontend"
IS_RENDER = os.environ.get("RENDER") == "true"
HOST = os.environ.get("HOST", "0.0.0.0" if IS_RENDER else "127.0.0.1")
PORT = int(os.environ.get("PORT", "8011"))
SCRAPBOX_API_ROOT = "https://scrapbox.io/api/pages"
DEFAULT_TIMEOUT = 20
MAX_WORKERS = 6
PAGE_LIST_LIMIT = 1000
CACHE_TTL_SECONDS = 300
IMAGE_URL_PATTERN = re.compile(r"https?://[^\s\]]+\.(?:png|jpe?g|gif|webp|svg)(?:\?[^\s\]]*)?", re.IGNORECASE)
SCRAPBOX_FILE_PATTERN = re.compile(r"https?://scrapbox\.io/files/[^\s\]>)\"']+", re.IGNORECASE)
RESULT_CACHE: dict[tuple[str, str, str], tuple[float, dict]] = {}


@dataclass(frozen=True)
class ScrapboxClient:
  project: str
  sid: str = ""
  timeout: int = DEFAULT_TIMEOUT

  def fetch_page_list(self) -> list[dict]:
    all_pages: list[dict] = []
    skip = 0

    while True:
      payload = self.fetch_page_list_batch(skip=skip, limit=PAGE_LIST_LIMIT)
      pages = payload.get("pages", [])
      if not isinstance(pages, list):
        raise ValueError("Scrapbox から不正なレスポンスが返りました。")

      all_pages.extend(pages)

      count = int(payload.get("count", len(all_pages)))
      if len(all_pages) >= count or len(pages) < PAGE_LIST_LIMIT:
        break
      skip += PAGE_LIST_LIMIT

    return all_pages

  def fetch_page_list_batch(self, skip: int, limit: int) -> dict:
    encoded_project = quote(self.project, safe="")
    return self._get_json(f"{SCRAPBOX_API_ROOT}/{encoded_project}?skip={skip}&limit={limit}")

  def fetch_page(self, title: str) -> dict:
    if not title:
      raise ValueError("ページタイトルが必要です。")
    encoded_project = quote(self.project, safe="")
    encoded_title = quote(title, safe="")
    return self._get_json(f"{SCRAPBOX_API_ROOT}/{encoded_project}/{encoded_title}")

  def _get_json(self, url: str) -> dict:
    request = Request(url, headers=self._headers(), method="GET")
    try:
      with urlopen(request, timeout=self.timeout) as response:
        charset = response.headers.get_content_charset("utf-8")
        return json.loads(response.read().decode(charset))
    except HTTPError as exc:
      detail = self._read_http_error(exc)
      if exc.code == HTTPStatus.NOT_FOUND:
        raise ValueError("project または page が見つかりません。") from exc
      if exc.code in {HTTPStatus.FORBIDDEN, HTTPStatus.UNAUTHORIZED}:
        raise ValueError("アクセス権がありません。private project の場合は connect.sid を指定してください。") from exc
      raise ValueError(f"Scrapbox API エラー: {exc.code} {detail}".strip()) from exc
    except URLError as exc:
      raise ValueError(f"Scrapbox へ接続できませんでした: {exc.reason}") from exc
    except json.JSONDecodeError as exc:
      raise ValueError("Scrapbox のレスポンスを JSON として解釈できませんでした。") from exc

  def _headers(self) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": "scrapbox-viewer-local-app/1.0",
    }
    if self.sid:
      headers["Cookie"] = f"connect.sid={self.sid}"
    return headers

  @staticmethod
  def _read_http_error(error: HTTPError) -> str:
    try:
      return error.read().decode("utf-8").strip()
    except Exception:
      return ""


class AppHandler(SimpleHTTPRequestHandler):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, directory=str(FRONTEND_DIR), **kwargs)

  def do_GET(self) -> None:
    parsed = urlparse(self.path)
    if parsed.path == "/api/tagged-images":
      self.handle_tagged_images(parsed.query)
      return
    if parsed.path == "/api/image":
      self.handle_image_proxy(parsed.query)
      return
    super().do_GET()

  def log_message(self, format: str, *args) -> None:
    sys.stderr.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), format % args))

  def handle_tagged_images(self, query: str) -> None:
    try:
      params = parse_qs(query)
      scrapbox_url = self.get_required_param(params, "url")
      sid = self.get_param(params, "sid")
      raw_tag = self.get_param(params, "tag") or "dailyinput"
      tag = normalize_tag(raw_tag)
      project = extract_project_from_url(scrapbox_url)
      client = ScrapboxClient(project=project, sid=sid)
      payload = collect_tagged_images(client, tag)
      self.respond_json(HTTPStatus.OK, payload)
    except Exception as exc:
      self.respond_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

  @staticmethod
  def get_required_param(params: dict[str, list[str]], key: str) -> str:
    value = AppHandler.get_param(params, key)
    if not value:
      raise ValueError(f"{key} を入力してください。")
    return value

  @staticmethod
  def get_param(params: dict[str, list[str]], key: str) -> str:
    values = params.get(key, [])
    if not values:
      return ""
    return str(values[0]).strip()

  def respond_json(self, status: HTTPStatus, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    self.send_response(status)
    self.send_header("Content-Type", "application/json; charset=utf-8")
    self.send_header("Content-Length", str(len(body)))
    self.send_header("Cache-Control", "no-store")
    self.end_headers()
    self.wfile.write(body)

  def handle_image_proxy(self, query: str) -> None:
    try:
      params = parse_qs(query)
      image_url = self.get_required_param(params, "url")
      sid = self.get_param(params, "sid")
      project = self.get_param(params, "project")
      content_type, body = fetch_image_bytes(image_url, sid, project)
      self.send_response(HTTPStatus.OK)
      self.send_header("Content-Type", content_type)
      self.send_header("Content-Length", str(len(body)))
      self.send_header("Cache-Control", "no-store")
      self.end_headers()
      self.wfile.write(body)
    except Exception as exc:
      self.respond_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})


def extract_project_from_url(scrapbox_url: str) -> str:
  parsed = urlparse(scrapbox_url.strip())
  if parsed.scheme not in {"http", "https"} or parsed.netloc not in {"scrapbox.io", "scrapbox.io:443"}:
    raise ValueError("Scrapbox の URL を入力してください。")
  path_parts = [part for part in parsed.path.split("/") if part]
  if not path_parts:
    raise ValueError("URL から project 名を判定できませんでした。")
  return path_parts[0]


def normalize_tag(tag: str) -> str:
  normalized = tag.strip()
  if normalized.startswith("#"):
    normalized = normalized[1:]
  if not normalized:
    raise ValueError("tag を入力してください。")
  return normalized


def collect_tagged_images(client: ScrapboxClient, tag: str) -> dict:
  cache_key = build_cache_key(client, tag)
  cached_payload = get_cached_payload(cache_key)
  if cached_payload is not None:
    return cached_payload

  pages = client.fetch_page_list()
  pages_to_scan = pages
  titles = [str(page.get("title", "")).strip() for page in pages_to_scan if str(page.get("title", "")).strip()]
  matched_pages: list[dict] = []
  skipped_pages: list[str] = []

  with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_map = {
        executor.submit(fetch_page_images_if_tagged, client, page, tag): str(page.get("title", "")).strip()
        for page in pages_to_scan
        if str(page.get("title", "")).strip()
    }
    for future in as_completed(future_map):
      title = future_map[future]
      try:
        page_payload = future.result()
      except Exception:
        skipped_pages.append(title)
        continue
      if page_payload is not None:
        matched_pages.append(page_payload)

  matched_pages.sort(key=lambda item: str(item["title"]).lower())
  image_count = sum(len(page["images"]) for page in matched_pages)
  payload = {
      "project": client.project,
      "tag": f"#{tag}",
      "page_count": len(matched_pages),
      "image_count": image_count,
      "scanned_count": len(titles),
      "total_page_count": len(pages),
      "skipped_count": len(skipped_pages),
      "pages": matched_pages,
  }
  set_cached_payload(cache_key, payload)
  return payload


def build_cache_key(client: ScrapboxClient, tag: str) -> tuple[str, str, str]:
  return (client.project, tag.lower(), client.sid)


def get_cached_payload(cache_key: tuple[str, str, str]) -> dict | None:
  cached_entry = RESULT_CACHE.get(cache_key)
  if cached_entry is None:
    return None

  expires_at, payload = cached_entry
  if expires_at <= time.time():
    RESULT_CACHE.pop(cache_key, None)
    return None
  return payload


def set_cached_payload(cache_key: tuple[str, str, str], payload: dict) -> None:
  RESULT_CACHE[cache_key] = (time.time() + CACHE_TTL_SECONDS, payload)


def fetch_page_images_if_tagged(client: ScrapboxClient, page_summary: dict, tag: str) -> dict | None:
  title = str(page_summary.get("title", "")).strip()
  page = client.fetch_page(title)
  lines = page.get("lines", [])
  texts = [line.get("text", "") for line in lines if isinstance(line, dict)]
  descriptions = coerce_string_list(page.get("descriptions")) or coerce_string_list(page_summary.get("descriptions"))
  if not page_has_tag(texts, descriptions, tag):
    return None

  image_items = collect_image_entries(page, lines)
  if not image_items:
    return None

  return {
      "title": page.get("title", title),
      "created": page.get("created"),
      "updated": page.get("updated"),
      "descriptions": descriptions,
      "preview_text": build_page_preview(lines),
      "images": [item["url"] for item in image_items],
      "image_items": image_items,
      "url": f"https://scrapbox.io/{quote(client.project, safe='')}/{quote(page.get('title', title), safe='')}",
  }


def page_has_tag(lines: list[str], descriptions: list[str], tag: str) -> bool:
  pattern = re.compile(rf"(?<!\S)#{re.escape(tag)}(?!\S)", re.IGNORECASE)
  if any(pattern.search(line) for line in lines):
    return True
  return any(description.strip().lstrip("#").lower() == tag.lower() for description in descriptions)


def collect_image_entries(page: dict, lines: list[dict]) -> list[dict]:
  seen: set[str] = set()
  image_entries: list[dict] = []

  for line in lines:
    if not isinstance(line, dict):
      continue
    context = extract_line_context(str(line.get("text", "")))
    line_candidates = collect_line_image_candidates(line)
    add_candidate_entries(image_entries, seen, line_candidates, context)

  return image_entries


def is_image_url(value: str) -> bool:
  normalized = normalize_candidate_url(value)
  if not normalized:
    return False
  parsed = urlparse(normalized)
  lower_value = normalized.lower()
  path = parsed.path.lower()
  if IMAGE_URL_PATTERN.fullmatch(normalized):
    return True
  if "scrapbox.io/files/" in lower_value:
    return True
  if any(host in parsed.netloc.lower() for host in ("gyazo.com", "i.gyazo.com", "gyazousercontent.com", "images.scrapbox.io")):
    return True
  if any(segment in path for segment in ("/api/image/", "/image/")):
    return True
  return False


def add_candidate_entries(image_entries: list[dict], seen: set[str], candidates: list[str], context: str) -> None:
  for candidate in candidates:
    normalized = normalize_candidate_url(candidate)
    if is_image_url(normalized) and normalized not in seen:
      seen.add(normalized)
      image_entries.append({
          "url": normalized,
          "context": context,
      })


def collect_line_image_candidates(line: dict) -> list[str]:
  candidates: list[str] = []

  line_image = str(line.get("image", "")).strip()
  if line_image:
    candidates.append(line_image)

  text = str(line.get("text", "")).strip()
  if text:
    candidates.extend(SCRAPBOX_FILE_PATTERN.findall(text))
    candidates.extend(IMAGE_URL_PATTERN.findall(text))

  return candidates


def normalize_candidate_url(value: str) -> str:
  if not value:
    return ""

  normalized = str(value).strip()
  normalized = normalized.strip("[]()<>\"'")
  normalized = normalized.rstrip(".,;")

  file_match = SCRAPBOX_FILE_PATTERN.search(normalized)
  if file_match:
    normalized = file_match.group(0)

  return normalized


def extract_line_context(text: str) -> str:
  if not text:
    return ""
  stripped = SCRAPBOX_FILE_PATTERN.sub("", text)
  stripped = IMAGE_URL_PATTERN.sub("", stripped)
  stripped = re.sub(r"\s+", " ", stripped).strip(" []")
  return stripped[:120]


def build_page_preview(lines: list[dict]) -> str:
  snippets: list[str] = []
  for line in lines:
    if not isinstance(line, dict):
      continue
    text = extract_line_context(str(line.get("text", "")))
    if not text or text.startswith("#"):
      continue
    snippets.append(text)
    if len(snippets) >= 3:
      break
  return " / ".join(snippets)[:240]


def fetch_image_bytes(image_url: str, sid: str, project: str) -> tuple[str, bytes]:
  normalized_url = normalize_candidate_url(image_url)
  parsed = urlparse(normalized_url)
  if parsed.scheme not in {"http", "https"}:
    raise ValueError("画像 URL が不正です。")

  referer = "https://scrapbox.io/"
  if project:
    referer = f"https://scrapbox.io/{quote(project, safe='')}"

  headers = {
      "User-Agent": "scrapbox-viewer-local-app/1.0",
      "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
      "Referer": referer,
      "Origin": "https://scrapbox.io",
      "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
  }
  if sid and "scrapbox.io" in parsed.netloc.lower():
    headers["Cookie"] = f"connect.sid={sid}"

  request = Request(normalized_url, headers=headers, method="GET")
  try:
    with urlopen(request, timeout=DEFAULT_TIMEOUT) as response:
      content_type = response.headers.get("Content-Type", "application/octet-stream")
      return content_type, response.read()
  except HTTPError as exc:
    raise ValueError(f"画像の取得に失敗しました: {exc.code}") from exc
  except URLError as exc:
    raise ValueError(f"画像へ接続できませんでした: {exc.reason}") from exc


def coerce_string_list(value: object) -> list[str]:
  if not isinstance(value, list):
    return []
  return [str(item) for item in value if str(item).strip()]


def main() -> int:
  server = ThreadingHTTPServer((HOST, PORT), AppHandler)
  print(f"Serving scrapbox-viewer at http://{HOST}:{PORT}")
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    print("\nStopping server.")
  finally:
    server.server_close()
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
