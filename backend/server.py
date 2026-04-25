#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import random
import re
import ssl
import socket
import sys
import time
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup, Tag
from html.parser import HTMLParser
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urljoin, urldefrag, urlparse
from urllib.request import Request, urlopen

try:
  import certifi
except ImportError:
  certifi = None

RNG = random.SystemRandom()


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = PROJECT_ROOT / "frontend"
IS_RENDER = os.environ.get("RENDER") == "true"
HOST = os.environ.get("HOST", "0.0.0.0" if IS_RENDER else "127.0.0.1")
PORT = int(os.environ.get("PORT", "8011"))
DEFAULT_TIMEOUT = 20
MONOTARO_LIST_META_TTL_SECONDS = 900
MONOTARO_RANDOM_WORKERS = int(os.environ.get("MONOTARO_RANDOM_WORKERS", "4"))
IMAGE_CACHE_MAX_ITEMS = int(os.environ.get("IMAGE_CACHE_MAX_ITEMS", "256"))
MAX_CRAWL_BUDGET = 1200
DEFAULT_INDEX_CRAWL_BUDGET = 8000
MAX_INDEX_CRAWL_BUDGET = 50000
MONOTARO_ITEMS_PER_PAGE = 40
IMG_EXT_PATTERN = re.compile(r"\.(?:png|jpe?g|gif|webp|bmp|svg|avif)(?:$|[?#])", re.IGNORECASE)
MONOTARO_PRODUCT_PATH_RE = re.compile(r"^/g/\d+/?$")
MONOTARO_TOTAL_ITEMS_RE = re.compile(r"([\d,]+)\s*件中\s*\d+\s*[～~]\s*\d+\s*件")
INDEX_FILE_PATH = PROJECT_ROOT / "backend" / "data" / "url_index.json"
MONOTARO_LIST_META_CACHE: dict[str, tuple[float, int, int]] = {}
MONOTARO_CATEGORY_INDEX_PATH = PROJECT_ROOT / "backend" / "data" / "monotaro_categories.deep.csv"
MONOTARO_CATEGORY_POOL_CACHE: tuple[float, list[dict[str, str | int]]] | None = None
IMAGE_BYTES_CACHE: OrderedDict[str, tuple[str, bytes]] = OrderedDict()


def build_ssl_context() -> ssl.SSLContext:
  if certifi is not None:
    try:
      return ssl.create_default_context(cafile=certifi.where())
    except Exception:
      pass
  return ssl.create_default_context()


URLLIB_SSL_CONTEXT = build_ssl_context()


class LinkImageParser(HTMLParser):
  def __init__(self) -> None:
    super().__init__()
    self.links: list[str] = []
    self.images: list[str] = []

  def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
    attrs_map = dict(attrs)
    if tag == "a":
      href = (attrs_map.get("href") or "").strip()
      if href:
        self.links.append(href)
      return

    if tag == "img":
      src = (attrs_map.get("src") or "").strip()
      if src:
        self.images.append(src)

      srcset = (attrs_map.get("srcset") or "").strip()
      if srcset:
        for candidate in srcset.split(","):
          url_part = candidate.strip().split(" ")[0]
          if url_part:
            self.images.append(url_part)


class AppHandler(SimpleHTTPRequestHandler):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, directory=str(FRONTEND_DIR), **kwargs)

  def do_GET(self) -> None:
    try:
      parsed = urlparse(self.path)
      if parsed.path == "/api/web-images":
        self.handle_web_images(parsed.query)
        return
      if parsed.path == "/api/monotaro-random-products":
        self.handle_monotaro_random_products(parsed.query)
        return
      if parsed.path == "/api/index-build":
        self.handle_index_build(parsed.query)
        return
      if parsed.path == "/api/index-status":
        self.handle_index_status()
        return
      if parsed.path == "/api/index-random":
        self.handle_index_random(parsed.query)
        return
      if parsed.path == "/api/image":
        self.handle_image_proxy(parsed.query)
        return
      super().do_GET()
    except Exception as exc:
      if is_client_disconnect_error(exc):
        return
      raise

  def log_message(self, format: str, *args) -> None:
    sys.stderr.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), format % args))

  def handle_web_images(self, query: str) -> None:
    try:
      params = parse_qs(query)
      start_url = self.get_required_param(params, "url")
      page_limit = self.get_int_param(params, "max_pages", 10, minimum=1, maximum=500)
      images_per_page = self.get_int_param(params, "images_per_page", 1, minimum=1, maximum=12)
      list_page_from = self.get_int_param(params, "list_page_from", 1, minimum=1, maximum=100000)
      list_page_to = self.get_int_param(params, "list_page_to", 500, minimum=1, maximum=100000)
      list_pages_pick = self.get_int_param(params, "list_pages_pick", 3, minimum=1, maximum=200)
      cookie = self.get_param(params, "cookie") or self.get_param(params, "sid")
      delay_ms = self.get_int_param(params, "delay_ms", 0, minimum=0, maximum=5000)
      force_refresh = self.get_bool_param(params, "force_refresh", default=False)

      payload = collect_monotaro_images_from_list_pages(
          start_url=start_url,
          page_limit=page_limit,
          images_per_page=images_per_page,
          list_page_from=list_page_from,
          list_page_to=list_page_to,
          list_pages_pick=list_pages_pick,
          cookie=cookie,
          delay_ms=delay_ms,
          force_refresh=force_refresh,
      )
      self.respond_json(HTTPStatus.OK, payload)
    except Exception as exc:
      if is_client_disconnect_error(exc):
        return
      self.respond_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

  def handle_monotaro_random_products(self, query: str) -> None:
    try:
      params = parse_qs(query)
      count = self.get_int_param(params, "count", 20, minimum=1, maximum=200)
      cookie = self.get_param(params, "cookie") or self.get_param(params, "sid")
      delay_ms = self.get_int_param(params, "delay_ms", 0, minimum=0, maximum=5000)

      payload = collect_monotaro_random_products_from_categories(
          count=count,
          cookie=cookie,
          delay_ms=delay_ms,
      )
      self.respond_json(HTTPStatus.OK, payload)
    except Exception as exc:
      if is_client_disconnect_error(exc):
        return
      self.respond_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

  def handle_image_proxy(self, query: str) -> None:
    try:
      params = parse_qs(query)
      image_url = self.get_required_param(params, "url")
      cookie = self.get_param(params, "cookie") or self.get_param(params, "sid")
      content_type, body = fetch_image_bytes(image_url, cookie)
      self.send_response(HTTPStatus.OK)
      self.send_header("Content-Type", content_type)
      self.send_header("Content-Length", str(len(body)))
      self.send_header("Cache-Control", "private, max-age=86400")
      self.end_headers()
      self.wfile.write(body)
    except Exception as exc:
      if is_client_disconnect_error(exc):
        return
      self.respond_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

  def handle_index_build(self, query: str) -> None:
    try:
      params = parse_qs(query)
      start_url = self.get_required_param(params, "url")
      target_depth = self.get_int_param(params, "max_depth", 2, minimum=0, maximum=6)
      follow_scope = self.get_param(params, "follow_scope") or "same-domain"
      path_prefix = self.get_param(params, "path_prefix")
      path_regex = self.get_param(params, "path_regex")
      cookie = self.get_param(params, "cookie") or self.get_param(params, "sid")
      delay_ms = self.get_int_param(params, "delay_ms", 0, minimum=0, maximum=5000)
      crawl_budget = self.get_int_param(
          params,
          "crawl_budget",
          DEFAULT_INDEX_CRAWL_BUDGET,
          minimum=100,
          maximum=MAX_INDEX_CRAWL_BUDGET,
      )

      payload = build_and_save_url_index(
          start_url=start_url,
          target_depth=target_depth,
          crawl_budget=crawl_budget,
          follow_scope=follow_scope,
          path_prefix=path_prefix,
          path_regex=path_regex,
          cookie=cookie,
          delay_ms=delay_ms,
      )
      self.respond_json(HTTPStatus.OK, payload)
    except Exception as exc:
      if is_client_disconnect_error(exc):
        return
      self.respond_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

  def handle_index_status(self) -> None:
    try:
      payload = load_url_index()
      if payload is None:
        self.respond_json(
            HTTPStatus.OK,
            {
                "exists": False,
                "index_file": str(INDEX_FILE_PATH),
                "message": "No saved URL index yet.",
            },
        )
        return
      urls = payload.get("urls", [])
      self.respond_json(
          HTTPStatus.OK,
          {
              "exists": True,
              "index_file": str(INDEX_FILE_PATH),
              "built_at": payload.get("built_at", 0),
              "start_url": payload.get("start_url", ""),
              "target_depth": payload.get("target_depth", 0),
              "follow_scope": payload.get("follow_scope", ""),
              "path_prefix": payload.get("path_prefix", ""),
              "path_regex": payload.get("path_regex", ""),
              "visited_url_count": payload.get("visited_url_count", 0),
              "url_count": len(urls),
              "sample_urls": urls[:50],
          },
      )
    except Exception as exc:
      if is_client_disconnect_error(exc):
        return
      self.respond_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

  def handle_index_random(self, query: str) -> None:
    try:
      params = parse_qs(query)
      count = self.get_int_param(params, "count", 10, minimum=1, maximum=500)
      payload = load_url_index()
      if payload is None:
        raise ValueError("No saved URL index. Run /api/index-build first.")

      urls = payload.get("urls", [])
      if not urls:
        raise ValueError("Saved URL index is empty.")

      picked_count = min(count, len(urls))
      random_urls = RNG.sample(urls, picked_count)
      self.respond_json(
          HTTPStatus.OK,
          {
              "index_file": str(INDEX_FILE_PATH),
              "built_at": payload.get("built_at", 0),
              "source_url_count": len(urls),
              "picked_count": picked_count,
              "urls": random_urls,
          },
      )
    except Exception as exc:
      if is_client_disconnect_error(exc):
        return
      self.respond_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

  @staticmethod
  def get_required_param(params: dict[str, list[str]], key: str) -> str:
    value = AppHandler.get_param(params, key)
    if not value:
      raise ValueError(f"{key} is required.")
    return value

  @staticmethod
  def get_param(params: dict[str, list[str]], key: str) -> str:
    values = params.get(key, [])
    if not values:
      return ""
    return str(values[0]).strip()

  @staticmethod
  def get_int_param(
      params: dict[str, list[str]],
      key: str,
      default: int,
      *,
      minimum: int,
      maximum: int,
  ) -> int:
    raw = AppHandler.get_param(params, key)
    if not raw:
      return default
    try:
      value = int(raw)
    except ValueError as exc:
      raise ValueError(f"{key} must be an integer.") from exc
    return max(minimum, min(maximum, value))

  @staticmethod
  def get_bool_param(params: dict[str, list[str]], key: str, *, default: bool) -> bool:
    raw = AppHandler.get_param(params, key)
    if not raw:
      return default
    return raw.lower() in {"1", "true", "yes", "on"}

  def respond_json(self, status: HTTPStatus, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    try:
      self.send_response(status)
      self.send_header("Content-Type", "application/json; charset=utf-8")
      self.send_header("Content-Length", str(len(body)))
      self.send_header("Cache-Control", "no-store")
      self.end_headers()
      self.wfile.write(body)
    except Exception as exc:
      if is_client_disconnect_error(exc):
        return
      raise


def is_client_disconnect_error(exc: Exception) -> bool:
  if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, socket.timeout)):
    return True
  if isinstance(exc, OSError):
    if getattr(exc, "winerror", None) in {10053, 10054}:
      return True
    if getattr(exc, "errno", None) in {32, 104}:
      return True
  return False


def collect_web_images(
    *,
    start_url: str,
    target_depth: int,
    page_limit: int,
    images_per_page: int,
    follow_scope: str,
    path_prefix: str,
    path_regex: str,
    cookie: str,
    delay_ms: int,
) -> dict:
  normalized_start = normalize_page_url(start_url)
  start_parsed = urlparse(normalized_start)
  if start_parsed.scheme not in {"http", "https"} or not start_parsed.netloc:
    raise ValueError("Enter a valid webpage URL.")

  crawl_budget = min(MAX_CRAWL_BUDGET, max(page_limit * 30, 120))
  pages_by_depth, visited_count = crawl_pages(
      start_url=normalized_start,
      target_depth=target_depth,
      crawl_budget=crawl_budget,
      follow_scope=follow_scope,
      path_prefix=path_prefix.strip(),
      cookie=cookie,
      delay_ms=delay_ms,
  )

  depth_candidates = pages_by_depth.get(target_depth, [])
  if not depth_candidates:
    for depth in range(target_depth - 1, -1, -1):
      candidates = pages_by_depth.get(depth, [])
      if candidates:
        depth_candidates = candidates
        break

  depth_candidates = filter_urls_by_path_regex(depth_candidates, path_regex.strip())
  if len(depth_candidates) > page_limit:
    selected_urls = RNG.sample(depth_candidates, page_limit)
  else:
    selected_urls = list(depth_candidates)

  result_pages: list[dict] = []
  image_count = 0
  for page_url in selected_urls:
    html = fetch_html(page_url, cookie)
    image_urls = extract_image_urls(html, page_url)
    if not image_urls:
      continue
    if len(image_urls) > images_per_page:
      picked = RNG.sample(image_urls, images_per_page)
    else:
      picked = list(image_urls)
    if not picked:
      continue

    image_count += len(picked)
    result_pages.append(
        {
            "title": build_page_title(page_url),
            "url": page_url,
            "created": 0,
            "updated": 0,
            "preview_text": "",
            "images": picked,
            "image_items": [{"url": item, "context": ""} for item in picked],
        }
    )

  payload = {
      "start_url": normalized_start,
      "target_depth": target_depth,
      "candidate_url_count": len(depth_candidates),
      "url_count": len(selected_urls),
      "visited_url_count": visited_count,
      "page_count": len(result_pages),
      "image_count": image_count,
      "pages": result_pages,
  }
  return payload


def load_monotaro_category_pool() -> list[dict[str, str | int]]:
  global MONOTARO_CATEGORY_POOL_CACHE

  if not MONOTARO_CATEGORY_INDEX_PATH.exists():
    raise ValueError(f"MonotaRO category file not found: {MONOTARO_CATEGORY_INDEX_PATH}")

  mtime = MONOTARO_CATEGORY_INDEX_PATH.stat().st_mtime
  if MONOTARO_CATEGORY_POOL_CACHE is not None:
    cached_mtime, cached_rows = MONOTARO_CATEGORY_POOL_CACHE
    if cached_mtime == mtime:
      return cached_rows

  rows: list[dict[str, str | int]] = []
  with MONOTARO_CATEGORY_INDEX_PATH.open(encoding="utf-8", newline="") as handle:
    reader = csv.DictReader(handle)
    for row in reader:
      is_leaf = str(row.get("is_leaf", "")).strip().lower() == "true"
      product_count_raw = str(row.get("product_count", "")).strip()
      url = str(row.get("url", "")).strip()
      if not is_leaf or not product_count_raw or not url:
        continue
      try:
        product_count = int(product_count_raw.replace(",", ""))
      except ValueError:
        continue
      if product_count <= 0:
        continue
      rows.append(
          {
              "category_id": str(row.get("category_id", "")).strip(),
              "name": str(row.get("name", "")).strip(),
              "url": url,
              "path": str(row.get("path", "")).strip(),
              "depth": int(str(row.get("depth", "0")).strip() or 0),
              "product_count": product_count,
          }
      )

  if not rows:
    raise ValueError("No leaf categories with product_count were found in the copied MonotaRO category file.")

  MONOTARO_CATEGORY_POOL_CACHE = (mtime, rows)
  return rows


def build_monotaro_category_page_url(category_url: str, page_num: int) -> str:
  normalized = normalize_page_url(category_url)
  parsed = urlparse(normalized)
  path = parsed.path or "/"
  if page_num <= 1:
    final_path = path if path.endswith("/") else f"{path}/"
  else:
    base_path = path if path.endswith("/") else f"{path}/"
    final_path = f"{base_path}page-{page_num}/"
  return parsed._replace(path=final_path, query="", fragment="").geturl()


def get_monotaro_list_meta_for_url(list_url: str, *, cookie: str) -> tuple[int, int, str | None]:
  normalized_url = build_monotaro_category_page_url(list_url, 1)
  cache_key = f"{normalized_url}|{cookie or '__no_cookie__'}"
  cached = MONOTARO_LIST_META_CACHE.get(cache_key)
  now = time.time()
  if cached is not None:
    expires_at, total_items, total_pages = cached
    if expires_at > now:
      return total_items, total_pages, None

  html = fetch_html(normalized_url, cookie)
  total_items = extract_monotaro_total_items(html)
  total_pages = max(1, (total_items + MONOTARO_ITEMS_PER_PAGE - 1) // MONOTARO_ITEMS_PER_PAGE)
  MONOTARO_LIST_META_CACHE[cache_key] = (now + MONOTARO_LIST_META_TTL_SECONDS, total_items, total_pages)
  return total_items, total_pages, html


def pick_random_monotaro_product_from_category(*, category: dict[str, str | int], cookie: str) -> dict | None:
  category_url = str(category["url"])
  category_product_count = int(category["product_count"])
  total_pages = max(1, (category_product_count + MONOTARO_ITEMS_PER_PAGE - 1) // MONOTARO_ITEMS_PER_PAGE)
  page_numbers = list(range(1, total_pages + 1))
  RNG.shuffle(page_numbers)
  if 1 not in page_numbers:
    page_numbers.append(1)

  for page_num in page_numbers[: min(len(page_numbers), 4)]:
    list_page_url = build_monotaro_category_page_url(category_url, page_num)
    try:
      html = fetch_html(list_page_url, cookie)
    except Exception:
      continue

    product_infos = extract_monotaro_product_urls(html, list_page_url)
    if not product_infos:
      continue

    product_info = RNG.choice(product_infos)
    product_url = str(product_info["url"])
    try:
      product_html = fetch_html(product_url, cookie)
      product_title, main_image_url = find_monotaro_main_image(product_html, product_url)
    except Exception:
      continue

    if not main_image_url:
      continue

    return {
        "title": product_title or build_page_title(product_url),
        "url": product_url,
        "created": 0,
        "updated": 0,
        "preview_text": "",
        "images": [main_image_url],
        "image_items": [{"url": main_image_url, "context": ""}],
        "list_page_no": page_num,
        "list_page_url": list_page_url,
        "row_of_page": int(product_info["row_of_page"]),
        "category_id": str(category["category_id"]),
        "category_name": str(category["name"]),
        "category_path": str(category["path"]),
        "category_url": category_url,
        "category_product_count": category_product_count,
    }

  return None


def collect_monotaro_random_products_from_categories(
    *,
    count: int,
    cookie: str,
    delay_ms: int,
) -> dict:
  categories = load_monotaro_category_pool()
  result_pages: list[dict] = []
  seen_product_urls: set[str] = set()
  attempts = 0
  max_attempts = max(count * 12, 24)
  worker_count = max(1, min(MONOTARO_RANDOM_WORKERS, count, 16))

  def pick_one() -> dict | None:
    category = RNG.choice(categories)
    page_payload = pick_random_monotaro_product_from_category(category=category, cookie=cookie)
    if delay_ms > 0:
      time.sleep(delay_ms / 1000.0)
    return page_payload

  executor = ThreadPoolExecutor(max_workers=worker_count)
  pending = set()
  try:
    while len(result_pages) < count and (attempts < max_attempts or pending):
      while len(pending) < worker_count and attempts < max_attempts:
        pending.add(executor.submit(pick_one))
        attempts += 1

      if not pending:
        break

      for future in as_completed(pending):
        pending.remove(future)
        try:
          page_payload = future.result()
        except Exception:
          page_payload = None
        if page_payload is not None and page_payload["url"] not in seen_product_urls:
          seen_product_urls.add(str(page_payload["url"]))
          result_pages.append(page_payload)
        break
  finally:
    for future in pending:
      future.cancel()
    executor.shutdown(wait=False, cancel_futures=True)

  return {
      "mode": "monotaro-random-products",
      "category_source_file": str(MONOTARO_CATEGORY_INDEX_PATH),
      "category_pool_count": len(categories),
      "worker_count": worker_count,
      "attempt_count": attempts,
      "url_count": len(result_pages),
      "page_count": len(result_pages),
      "image_count": len(result_pages),
      "pages": result_pages,
  }


def collect_monotaro_images_from_list_pages(
    *,
    start_url: str,
    page_limit: int,
    images_per_page: int,
    list_page_from: int,
    list_page_to: int,
    list_pages_pick: int,
    cookie: str,
    delay_ms: int,
    force_refresh: bool = False,
) -> dict:
  normalized_start = normalize_page_url(start_url)
  parsed_start = urlparse(normalized_start)
  if parsed_start.scheme not in {"http", "https"} or not parsed_start.netloc:
    raise ValueError("Enter a valid webpage URL.")
  if "monotaro.com" not in parsed_start.netloc.lower():
    raise ValueError("This mode expects a monotaro.com list URL.")

  list_root_url = "https://www.monotaro.com/s/"
  total_items, total_pages, first_html = get_monotaro_list_meta(cookie=cookie)

  lower_page = max(1, min(list_page_from, list_page_to))
  upper_page = min(total_pages, max(list_page_from, list_page_to))
  if lower_page > upper_page:
    lower_page = 1
    upper_page = total_pages

  page_numbers = list(range(lower_page, upper_page + 1))
  selected_page_count = min(list_pages_pick, len(page_numbers))
  list_pages_scanned: list[str] = []
  product_sources: dict[str, dict[str, str | int]] = {}
  seen_list_signatures: set[tuple[str, ...]] = set()
  remaining_page_numbers = list(page_numbers)
  attempts = 0
  max_attempts = max(selected_page_count * 8, 16)

  while remaining_page_numbers and len(seen_list_signatures) < selected_page_count and attempts < max_attempts:
    page_num = RNG.choice(remaining_page_numbers)
    remaining_page_numbers.remove(page_num)
    attempts += 1
    list_page_url = build_monotaro_list_page_url(page_num)
    try:
      html = first_html if (page_num == 1 and first_html is not None) else fetch_html(list_page_url, cookie)
      product_infos = extract_monotaro_product_urls(html, list_page_url)
      list_signature = tuple(item["url"] for item in product_infos)
      if not list_signature or list_signature in seen_list_signatures:
        continue
      seen_list_signatures.add(list_signature)
      list_pages_scanned.append(list_page_url)
      for product_info in product_infos:
        product_url = str(product_info["url"])
        if product_url not in product_sources:
          product_sources[product_url] = {
              "list_page_no": page_num,
              "list_page_url": list_page_url,
              "row_of_page": int(product_info["row_of_page"]),
          }
    except Exception:
      continue
    if delay_ms > 0:
      time.sleep(delay_ms / 1000.0)

  unique_product_urls = list(product_sources.keys())
  if len(unique_product_urls) > page_limit:
    selected_product_urls = RNG.sample(unique_product_urls, page_limit)
  else:
    selected_product_urls = list(unique_product_urls)

  result_pages: list[dict] = []
  image_count = 0
  for page_url in selected_product_urls:
    source_info = product_sources.get(page_url, {})
    try:
      html = fetch_html(page_url, cookie)
      product_title, main_image_url = find_monotaro_main_image(html, page_url)
    except Exception:
      continue
    if not main_image_url:
      continue
    picked = [main_image_url]
    image_count += 1
    result_pages.append(
        {
            "title": product_title or build_page_title(page_url),
            "url": page_url,
            "created": 0,
            "updated": 0,
            "preview_text": "",
            "images": picked,
            "image_items": [{"url": item, "context": ""} for item in picked],
            "list_page_no": source_info.get("list_page_no", 0),
            "list_page_url": source_info.get("list_page_url", ""),
            "row_of_page": source_info.get("row_of_page", 0),
        }
    )
    if delay_ms > 0:
      time.sleep(delay_ms / 1000.0)

  payload = {
      "start_url": normalized_start,
      "list_root_url": list_root_url,
      "total_items": total_items,
      "total_pages": total_pages,
      "list_page_from": lower_page,
      "list_page_to": upper_page,
      "list_pages_pick": len(list_pages_scanned),
      "list_pages_scanned": list_pages_scanned,
      "candidate_url_count": len(unique_product_urls),
      "url_count": len(selected_product_urls),
      "visited_url_count": len(list_pages_scanned),
      "page_count": len(result_pages),
      "image_count": image_count,
      "pages": result_pages,
  }
  return payload


def build_and_save_url_index(
    *,
    start_url: str,
    target_depth: int,
    crawl_budget: int,
    follow_scope: str,
    path_prefix: str,
    path_regex: str,
    cookie: str,
    delay_ms: int,
) -> dict:
  normalized_start = normalize_page_url(start_url)
  parsed = urlparse(normalized_start)
  if parsed.scheme not in {"http", "https"} or not parsed.netloc:
    raise ValueError("Enter a valid webpage URL.")

  pages_by_depth, visited_count = crawl_pages(
      start_url=normalized_start,
      target_depth=target_depth,
      crawl_budget=crawl_budget,
      follow_scope=follow_scope,
      path_prefix=path_prefix.strip(),
      cookie=cookie,
      delay_ms=delay_ms,
  )

  candidates = gather_urls_upto_depth(pages_by_depth, target_depth)
  candidates = filter_urls_by_path_regex(candidates, path_regex.strip())
  unique_urls = dedupe_keep_order(candidates)

  payload = {
      "built_at": int(time.time()),
      "start_url": normalized_start,
      "target_depth": target_depth,
      "crawl_budget": crawl_budget,
      "follow_scope": follow_scope,
      "path_prefix": path_prefix.strip(),
      "path_regex": path_regex.strip(),
      "visited_url_count": visited_count,
      "urls": unique_urls,
  }
  save_url_index(payload)
  return {
      "saved": True,
      "index_file": str(INDEX_FILE_PATH),
      "built_at": payload["built_at"],
      "start_url": normalized_start,
      "target_depth": target_depth,
      "visited_url_count": visited_count,
      "url_count": len(unique_urls),
      "sample_urls": unique_urls[:50],
  }


def gather_urls_upto_depth(pages_by_depth: dict[int, list[str]], max_depth: int) -> list[str]:
  urls: list[str] = []
  for depth in range(0, max_depth + 1):
    urls.extend(pages_by_depth.get(depth, []))
  return urls


def dedupe_keep_order(urls: list[str]) -> list[str]:
  seen: set[str] = set()
  result: list[str] = []
  for url in urls:
    if url in seen:
      continue
    seen.add(url)
    result.append(url)
  return result


def load_url_index() -> dict | None:
  if not INDEX_FILE_PATH.exists():
    return None
  try:
    raw = INDEX_FILE_PATH.read_text(encoding="utf-8")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
      return None
    urls = payload.get("urls", [])
    if not isinstance(urls, list):
      payload["urls"] = []
    return payload
  except (OSError, json.JSONDecodeError):
    return None


def save_url_index(payload: dict) -> None:
  INDEX_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
  temp_path = INDEX_FILE_PATH.with_suffix(".tmp")
  temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
  temp_path.replace(INDEX_FILE_PATH)


def crawl_pages(
    *,
    start_url: str,
    target_depth: int,
    crawl_budget: int,
    follow_scope: str,
    path_prefix: str,
    cookie: str,
    delay_ms: int,
) -> tuple[dict[int, list[str]], int]:
  parsed_start = urlparse(start_url)
  start_netloc = parsed_start.netloc.lower()
  base_prefix = path_prefix or parsed_start.path.rstrip("/") or "/"

  visited: set[str] = set()
  queue: deque[tuple[str, int]] = deque([(start_url, 0)])
  pages_by_depth: dict[int, list[str]] = {}

  while queue and len(visited) < crawl_budget:
    current_url, depth = queue.popleft()
    if current_url in visited:
      continue
    visited.add(current_url)
    pages_by_depth.setdefault(depth, []).append(current_url)

    if depth >= target_depth:
      continue

    if delay_ms > 0:
      time.sleep(delay_ms / 1000.0)

    try:
      html = fetch_html(current_url, cookie)
    except Exception:
      continue

    for link in extract_links(html, current_url):
      if link in visited:
        continue
      if not should_follow(
          candidate_url=link,
          start_netloc=start_netloc,
          follow_scope=follow_scope,
          path_prefix=base_prefix,
      ):
        continue
      queue.append((link, depth + 1))

  return pages_by_depth, len(visited)


def fetch_html(url: str, cookie: str) -> str:
  request = Request(
      url,
      headers=build_headers(url, cookie, accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
      method="GET",
  )
  try:
    with urlopen(request, timeout=DEFAULT_TIMEOUT, context=URLLIB_SSL_CONTEXT) as response:
      content_type = response.headers.get("Content-Type", "")
      if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
        raise ValueError(f"Not an HTML page: {url}")
      charset = response.headers.get_content_charset("utf-8")
      return response.read().decode(charset, errors="replace")
  except HTTPError as exc:
    raise ValueError(f"Failed to fetch page ({exc.code}): {url}") from exc
  except URLError as exc:
    raise ValueError(f"Could not connect to page: {exc.reason}") from exc


def fetch_image_bytes(image_url: str, cookie: str) -> tuple[str, bytes]:
  parsed = urlparse(image_url)
  if parsed.scheme not in {"http", "https"}:
    raise ValueError("Invalid image URL.")

  cache_key = f"{image_url}|{cookie or '__no_cookie__'}"
  cached = IMAGE_BYTES_CACHE.get(cache_key)
  if cached is not None:
    IMAGE_BYTES_CACHE.move_to_end(cache_key)
    return cached

  request = Request(
      image_url,
      headers=build_headers(image_url, cookie, accept="image/avif,image/webp,image/apng,image/*,*/*;q=0.8"),
      method="GET",
  )
  try:
    with urlopen(request, timeout=DEFAULT_TIMEOUT, context=URLLIB_SSL_CONTEXT) as response:
      content_type = response.headers.get("Content-Type", "application/octet-stream")
      body = response.read()
      IMAGE_BYTES_CACHE[cache_key] = (content_type, body)
      IMAGE_BYTES_CACHE.move_to_end(cache_key)
      while len(IMAGE_BYTES_CACHE) > IMAGE_CACHE_MAX_ITEMS:
        IMAGE_BYTES_CACHE.popitem(last=False)
      return content_type, body
  except HTTPError as exc:
    raise ValueError(f"Failed to fetch image ({exc.code}).") from exc
  except URLError as exc:
    raise ValueError(f"Could not connect to image host: {exc.reason}") from exc


def build_headers(url: str, cookie: str, *, accept: str) -> dict[str, str]:
  parsed = urlparse(url)
  origin = f"{parsed.scheme}://{parsed.netloc}"
  headers = {
      "User-Agent": "webpage-viewer-local-app/1.0",
      "Accept": accept,
      "Referer": origin + "/",
      "Origin": origin,
      "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
  }
  if cookie:
    headers["Cookie"] = cookie
  return headers


def extract_links(html: str, base_url: str) -> list[str]:
  parser = LinkImageParser()
  parser.feed(html)
  normalized: list[str] = []
  seen: set[str] = set()
  for href in parser.links:
    absolute = normalize_page_url(urljoin(base_url, href))
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
      continue
    if absolute not in seen:
      seen.add(absolute)
      normalized.append(absolute)
  return normalized


def extract_monotaro_product_urls(html: str, base_url: str) -> list[dict[str, str | int]]:
  parser = LinkImageParser()
  parser.feed(html)
  urls: list[dict[str, str | int]] = []
  seen: set[str] = set()
  row_of_page = 0
  for href in parser.links:
    absolute = normalize_page_url(urljoin(base_url, href))
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
      continue
    if parsed.netloc.lower() != "www.monotaro.com":
      continue
    if parsed.query:
      continue
    path = parsed.path or "/"
    if not MONOTARO_PRODUCT_PATH_RE.match(path):
      continue
    normalized = f"{parsed.scheme}://{parsed.netloc}{path.rstrip('/')}"
    if normalized in seen:
      continue
    seen.add(normalized)
    row_of_page += 1
    urls.append({"url": normalized, "row_of_page": row_of_page})
  return urls


def extract_monotaro_total_items(html: str) -> int:
  text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
  match = MONOTARO_TOTAL_ITEMS_RE.search(text)
  if not match:
    raise ValueError("Could not read total item count from list page.")
  return int(match.group(1).replace(",", ""))


def get_monotaro_list_meta(*, cookie: str) -> tuple[int, int, str | None]:
  return get_monotaro_list_meta_for_url("https://www.monotaro.com/s/", cookie=cookie)


def build_monotaro_list_page_url(page_num: int) -> str:
  if page_num <= 1:
    return "https://www.monotaro.com/s/"
  return f"https://www.monotaro.com/s/page-{page_num}/"


def normalize_img_src_tag(img: Tag, base_url: str) -> str | None:
  candidates = [
      img.get("src"),
      img.get("data-src"),
      img.get("data-lazy"),
      img.get("data-original"),
  ]
  for src in candidates:
    if not src:
      continue
    raw = str(src).strip()
    if not raw or raw.startswith("data:"):
      continue
    absolute = urljoin(base_url, raw)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
      continue
    return absolute
  return None


def is_noise_image_tag(img: Tag, product_title: str) -> bool:
  src_text = " ".join(
      [
          str(img.get("src") or ""),
          str(img.get("data-src") or ""),
          str(img.get("class") or ""),
          str(img.get("id") or ""),
      ]
  ).lower()
  alt_text = str(img.get("alt") or "")
  combined_text = f"{src_text} {alt_text.lower()}"

  noise_keywords = [
      "logo",
      "header",
      "footer",
      "icon",
      "sprite",
      "banner",
      "recommend",
      "review",
      "sns",
      "facebook",
      "line",
      "campaign",
      "app",
      "新着",
      "おすすめ",
      "レビュー",
      "キャンペーン",
      "アプリ",
      "ヘッダー",
      "フッター",
      "ロゴ",
  ]

  if any(keyword in combined_text for keyword in noise_keywords):
    if product_title and product_title in alt_text:
      pass
    else:
      return True

  width = img.get("width")
  height = img.get("height")
  try:
    if width is not None and height is not None and int(width) <= 80 and int(height) <= 80:
      return True
  except (TypeError, ValueError):
    pass

  return False


def find_monotaro_main_image(product_html: str, base_url: str) -> tuple[str | None, str | None]:
  soup = BeautifulSoup(product_html, "html.parser")
  h1 = soup.find("h1")
  product_title = h1.get_text(" ", strip=True) if h1 else None

  if h1:
    near_imgs: list[Tag] = []
    for index, node in enumerate(h1.next_elements):
      if index >= 120:
        break
      if isinstance(node, Tag) and node.name == "img":
        near_imgs.append(node)

    if product_title:
      for img in near_imgs:
        alt_text = str(img.get("alt") or "")
        if product_title in alt_text:
          src = normalize_img_src_tag(img, base_url)
          if src and not is_noise_image_tag(img, product_title):
            return product_title, src

    for img in near_imgs:
      src = normalize_img_src_tag(img, base_url)
      if src and not is_noise_image_tag(img, product_title or ""):
        return product_title, src

  if product_title:
    for img in soup.find_all("img"):
      alt_text = str(img.get("alt") or "")
      if product_title not in alt_text:
        continue
      src = normalize_img_src_tag(img, base_url)
      if src and not is_noise_image_tag(img, product_title):
        return product_title, src

  all_imgs = soup.find_all("img")
  for img in all_imgs[:30]:
    src = normalize_img_src_tag(img, base_url)
    if src and not is_noise_image_tag(img, product_title or ""):
      return product_title, src

  return product_title, None


def extract_image_urls(html: str, base_url: str) -> list[str]:
  parser = LinkImageParser()
  parser.feed(html)
  image_urls: list[str] = []
  seen: set[str] = set()
  for src in parser.images:
    absolute = urljoin(base_url, src)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
      continue
    if not is_probable_image_url(absolute):
      continue
    if absolute not in seen:
      seen.add(absolute)
      image_urls.append(absolute)
  return image_urls


def is_probable_image_url(value: str) -> bool:
  lower = value.lower()
  if IMG_EXT_PATTERN.search(lower):
    return True
  return any(token in lower for token in ("/image", "/img", "image=", "img="))


def normalize_page_url(url: str) -> str:
  normalized, _ = urldefrag(url.strip())
  parsed = urlparse(normalized)
  if parsed.scheme in {"http", "https"} and parsed.netloc:
    path = parsed.path or "/"
    host = parsed.netloc.lower()
    if host.endswith("monotaro.com") and path in {"/s", "/s/"}:
      parsed = parsed._replace(path="/s/")
      return parsed.geturl()
  return normalized.rstrip("/")


def should_follow(*, candidate_url: str, start_netloc: str, follow_scope: str, path_prefix: str) -> bool:
  parsed = urlparse(candidate_url)
  candidate_host = parsed.netloc.lower()

  if follow_scope == "any-link":
    return True

  if follow_scope in {"same-domain", "subpath"} and candidate_host != start_netloc:
    return False

  if follow_scope == "subpath":
    candidate_path = parsed.path or "/"
    return candidate_path.startswith(path_prefix)

  return True


def build_page_title(page_url: str) -> str:
  parsed = urlparse(page_url)
  path = parsed.path.strip("/")
  if not path:
    return parsed.netloc
  return path.split("/")[-1] or parsed.netloc


def filter_urls_by_path_regex(urls: list[str], path_regex: str) -> list[str]:
  if not path_regex:
    return urls
  try:
    pattern = re.compile(path_regex)
  except re.error as exc:
    raise ValueError(f"Invalid path_regex: {exc}") from exc
  filtered: list[str] = []
  for url in urls:
    path = urlparse(url).path or "/"
    if pattern.search(path):
      filtered.append(url)
  return filtered


def main() -> int:
  server = ThreadingHTTPServer((HOST, PORT), AppHandler)
  print(f"Serving webpage-viewer at http://{HOST}:{PORT}")
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    print("\nStopping server.")
  finally:
    server.server_close()
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
