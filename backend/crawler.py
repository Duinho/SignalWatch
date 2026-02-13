import os
import random
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, Tag


class NaverStockNewsCrawler:
    """
    Naver finance item-news crawler.
    This source can return empty results depending on page state.
    """

    def __init__(self):
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }

    def get_stock_news(self, stock_code: str, max_count: int = 10) -> List[Dict[str, str]]:
        try:
            print(f"[크롤링 시작] 종목코드: {stock_code}")

            url = f"https://finance.naver.com/item/news_news.nhn?code={stock_code}&page=1"
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            response.encoding = "euc-kr"

            soup = BeautifulSoup(response.text, "lxml")
            news_list: List[Dict[str, str]] = []

            news_table = soup.find("table", {"class": "type5"})
            if not news_table:
                print(f"[경고] {stock_code}: 뉴스 테이블을 찾을 수 없습니다.")
                return []

            rows = news_table.find_all("tr")
            for idx, row in enumerate(rows):
                if len(news_list) >= max_count:
                    break

                title_cell = row.find("td", {"class": "title"})
                if not title_cell:
                    continue

                title_link = title_cell.find("a")
                if not title_link:
                    continue

                date_cell = row.find("td", {"class": "date"})
                date_text = date_cell.get_text(strip=True) if date_cell else "시간 정보 없음"

                info_cell = row.find("td", {"class": "info"})
                source_text = info_cell.get_text(strip=True) if info_cell else "출처 정보 없음"

                title = title_link.get_text(strip=True)
                link = title_link.get("href", "")
                if link.startswith("/"):
                    link = "https://finance.naver.com" + link

                news_item = {
                    "id": f"{stock_code}_{idx}_{int(time.time())}",
                    "title": title,
                    "link": link,
                    "source": source_text,
                    "published_date": date_text,
                    "stock_code": stock_code,
                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                news_list.append(news_item)

            print(f"[크롤링 완료] {stock_code}: {len(news_list)}개 뉴스 수집")
            return news_list

        except requests.RequestException as e:
            print(f"[네트워크 에러] {stock_code}: {e}")
            return []
        except Exception as e:
            print(f"[크롤링 에러] {stock_code}: {e}")
            return []

    def get_multiple_stocks_news(self, stock_codes: List[str], max_each: int = 5) -> Dict[str, List[Dict[str, str]]]:
        results: Dict[str, List[Dict[str, str]]] = {}
        for stock_code in stock_codes:
            results[stock_code] = self.get_stock_news(stock_code, max_each)
            time.sleep(1.0)
        return results


class NaverNewsSearchCrawler:
    """
    Naver news-search crawler with retry/backoff + cache for stability.
    """

    def __init__(self):
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
            "Referer": "https://search.naver.com/",
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

        self.max_retries = 3
        self.base_backoff = 0.8
        self.min_request_interval_sec = float(os.getenv("NAVER_MIN_REQUEST_INTERVAL_SEC", "0.9"))
        self.cache_ttl_sec = int(os.getenv("NEWS_CACHE_TTL_SEC", "180"))

        self._last_request_ts = 0.0
        self._cache: Dict[str, Dict[str, object]] = {}
        self._last_result_meta: Dict[str, Dict[str, object]] = {}
        self._runtime_stats: Dict[str, int] = {
            "requests_total": 0,
            "request_failures": 0,
            "network_fetches": 0,
            "cache_hits": 0,
            "stale_cache_hits": 0,
            "empty_results": 0,
            "exceptions": 0,
        }

    def get_last_result_meta(self, keyword: str) -> Dict[str, object]:
        """
        Example:
        {
          "source": "network|cache|stale_cache|empty",
          "age_sec": 1.2,
          "fetched_at": "YYYY-mm-dd HH:MM:SS",
          "cache_ttl_sec": 180
        }
        """
        return dict(self._last_result_meta.get(keyword, {}))

    def get_runtime_metrics(self) -> Dict[str, object]:
        now = time.time()
        cache_entries = len(self._cache)
        valid_cache_entries = 0
        stale_cache_entries = 0

        for entry in self._cache.values():
            fetched_at = float(entry.get("fetched_at", 0.0))
            if fetched_at <= 0:
                stale_cache_entries += 1
                continue
            if now - fetched_at <= self.cache_ttl_sec:
                valid_cache_entries += 1
            else:
                stale_cache_entries += 1

        last_request_age_sec = None
        if self._last_request_ts > 0:
            last_request_age_sec = round(max(0.0, now - self._last_request_ts), 2)

        return {
            "cache_ttl_sec": self.cache_ttl_sec,
            "min_request_interval_sec": self.min_request_interval_sec,
            "cache_entries": cache_entries,
            "valid_cache_entries": valid_cache_entries,
            "stale_cache_entries": stale_cache_entries,
            "tracked_keywords": len(self._last_result_meta),
            "last_request_age_sec": last_request_age_sec,
            "stats": dict(self._runtime_stats),
        }

    def _cache_key(self, keyword: str, max_count: int) -> str:
        return f"{keyword}::{max_count}"

    def _set_last_meta(self, keyword: str, source: str, fetched_at: float):
        age_sec = max(0.0, time.time() - fetched_at)
        self._last_result_meta[keyword] = {
            "source": source,
            "age_sec": round(age_sec, 2),
            "fetched_at": datetime.fromtimestamp(fetched_at).strftime("%Y-%m-%d %H:%M:%S"),
            "cache_ttl_sec": self.cache_ttl_sec,
        }

    def _get_cached_news(self, keyword: str, max_count: int) -> Optional[Tuple[List[Dict[str, str]], float]]:
        key = self._cache_key(keyword, max_count)
        entry = self._cache.get(key)
        if not entry:
            return None

        news = entry.get("data", [])
        fetched_at = float(entry.get("fetched_at", 0.0))
        if not isinstance(news, list) or fetched_at <= 0:
            return None

        age_sec = time.time() - fetched_at
        if age_sec > self.cache_ttl_sec:
            return None

        return ([dict(item) for item in news], fetched_at)

    def _get_stale_cached_news(self, keyword: str, max_count: int) -> Optional[Tuple[List[Dict[str, str]], float]]:
        key = self._cache_key(keyword, max_count)
        entry = self._cache.get(key)
        if not entry:
            return None

        news = entry.get("data", [])
        fetched_at = float(entry.get("fetched_at", 0.0))
        if not isinstance(news, list) or fetched_at <= 0:
            return None

        return ([dict(item) for item in news], fetched_at)

    def _set_cache(self, keyword: str, max_count: int, news_list: List[Dict[str, str]]):
        key = self._cache_key(keyword, max_count)
        self._cache[key] = {
            "fetched_at": time.time(),
            "data": [dict(item) for item in news_list],
        }

    def _throttle(self):
        now = time.time()
        elapsed = now - self._last_request_ts
        if elapsed < self.min_request_interval_sec:
            time.sleep(self.min_request_interval_sec - elapsed)
        self._last_request_ts = time.time()

    def _request_search(self, keyword: str) -> Optional[requests.Response]:
        url = "https://search.naver.com/search.naver"
        params = {"where": "news", "sm": "tab_opt", "query": keyword}

        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                self._throttle()
                self._runtime_stats["requests_total"] += 1
                response = self.session.get(url, params=params, timeout=15)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                self._runtime_stats["request_failures"] += 1
                last_error = e
                status_code = getattr(getattr(e, "response", None), "status_code", None)
                should_retry = status_code in (403, 429, 500, 502, 503, 504) or status_code is None

                if attempt < self.max_retries and should_retry:
                    sleep_sec = self.base_backoff * attempt + random.uniform(0.1, 0.5)
                    print(f"[검색 재시도] {keyword}: attempt={attempt}, sleep={sleep_sec:.2f}s, status={status_code}")
                    time.sleep(sleep_sec)
                    continue
                break

        print(f"[검색 요청 실패] {keyword}: {last_error}")
        return None

    def get_news_by_keyword(self, keyword: str, max_count: int = 10) -> List[Dict[str, str]]:
        try:
            print(f"[검색 크롤링 시작] 키워드: {keyword}")

            cached = self._get_cached_news(keyword, max_count)
            if cached is not None:
                cached_news, fetched_at = cached
                self._set_last_meta(keyword, source="cache", fetched_at=fetched_at)
                self._runtime_stats["cache_hits"] += 1
                print(f"[검색 캐시 사용] {keyword}: {len(cached_news)}개")
                return cached_news

            response = self._request_search(keyword)
            if response is None:
                stale = self._get_stale_cached_news(keyword, max_count)
                if stale is not None:
                    stale_news, fetched_at = stale
                    self._set_last_meta(keyword, source="stale_cache", fetched_at=fetched_at)
                    self._runtime_stats["stale_cache_hits"] += 1
                    print(f"[검색 stale 캐시 fallback] {keyword}: {len(stale_news)}개")
                    return stale_news

                self._set_last_meta(keyword, source="empty", fetched_at=time.time())
                self._runtime_stats["empty_results"] += 1
                return []

            soup = BeautifulSoup(response.text, "lxml")
            title_links = self._select_title_links(soup)
            news_list: List[Dict[str, str]] = []
            seen_links = set()

            for link in title_links:
                if len(news_list) >= max_count:
                    break

                href = (link.get("href") or "").strip()
                if not self._is_valid_article_link(href):
                    continue
                if href in seen_links:
                    continue

                title = self._normalize_text(link.get("title") or link.get_text(" ", strip=True))
                if not title or title == "\ub124\uc774\ubc84\ub274\uc2a4":
                    continue

                source, published_date = self._extract_source_and_date(link, href)
                news_item = {
                    "id": f"{keyword}_{len(news_list)}_{int(time.time())}",
                    "title": title,
                    "link": href,
                    "source": source,
                    "published_date": published_date,
                    "keyword": keyword,
                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                news_list.append(news_item)
                seen_links.add(href)

            fetched_at = time.time()
            self._set_cache(keyword, max_count, news_list)
            self._set_last_meta(keyword, source="network", fetched_at=fetched_at)
            self._runtime_stats["network_fetches"] += 1
            if not news_list:
                self._runtime_stats["empty_results"] += 1
            print(f"[검색 크롤링 완료] {keyword}: {len(news_list)}개 뉴스 수집")
            return news_list

        except Exception as e:
            self._runtime_stats["exceptions"] += 1
            stale = self._get_stale_cached_news(keyword, max_count)
            if stale is not None:
                stale_news, fetched_at = stale
                self._set_last_meta(keyword, source="stale_cache", fetched_at=fetched_at)
                self._runtime_stats["stale_cache_hits"] += 1
                print(f"[검색 예외 stale 캐시 fallback] {keyword}: {len(stale_news)}개")
                return stale_news

            self._set_last_meta(keyword, source="empty", fetched_at=time.time())
            self._runtime_stats["empty_results"] += 1
            print(f"[검색 크롤링 에러] {keyword}: {e}")
            return []

    def _select_title_links(self, soup: BeautifulSoup) -> List[Tag]:
        links = soup.select("div.group_news a[data-heatmap-target='.tit'][href]")
        if links:
            return links
        return soup.select("a.news_tit[href]")

    def _is_valid_article_link(self, href: str) -> bool:
        if not href:
            return False
        if href.startswith("#") or href.lower().startswith("javascript:"):
            return False
        if not href.startswith("http"):
            return False
        return True

    def _extract_source_and_date(self, title_link: Tag, href: str) -> Tuple[str, str]:
        source = self._source_from_url(href)
        published_date = "시간 정보 없음"

        # Legacy layout fallback.
        parent = title_link.parent
        if parent:
            press_elem = parent.select_one("a.info.press")
            if press_elem:
                press_text = self._normalize_text(press_elem.get_text(" ", strip=True))
                if press_text:
                    source = press_text

        profile = self._find_nearest_profile(title_link)
        if not profile:
            return source, published_date

        source_elem = profile.select_one("span.sds-comps-profile-info-title-text")
        if source_elem:
            source_text = self._normalize_text(source_elem.get_text(" ", strip=True))
            if source_text:
                source = source_text

        subtexts = profile.select("div.sds-comps-profile-info-subtexts span.sds-comps-profile-info-subtext")
        for sub in subtexts:
            text = self._normalize_text(sub.get_text(" ", strip=True))
            if not text:
                continue
            if text in ("\ub124\uc774\ubc84\ub274\uc2a4", source):
                continue
            published_date = text
            break

        return source, published_date

    def _find_nearest_profile(self, title_link: Tag) -> Optional[Tag]:
        # Prefer a direct Profile child in nearest containers first.
        for ancestor in title_link.parents:
            if not isinstance(ancestor, Tag):
                continue
            direct_profiles = ancestor.find_all("div", attrs={"data-sds-comp": "Profile"}, recursive=False)
            if len(direct_profiles) == 1:
                return direct_profiles[0]

        # Fallback to nearest profile by relative distance.
        for ancestor in title_link.parents:
            if not isinstance(ancestor, Tag):
                continue
            profiles = ancestor.select("div[data-sds-comp='Profile']")
            if not profiles:
                continue

            tags_in_ancestor = [node for node in ancestor.descendants if isinstance(node, Tag)]
            positions = {id(node): idx for idx, node in enumerate(tags_in_ancestor)}
            link_pos = positions.get(id(title_link))
            if link_pos is None:
                continue

            nearest = min(profiles, key=lambda profile: abs(positions.get(id(profile), 10**9) - link_pos))
            return nearest

        return None

    def _source_from_url(self, href: str) -> str:
        try:
            netloc = urlparse(href).netloc.lower()
            if netloc.startswith("www."):
                netloc = netloc[4:]
            return netloc if netloc else "출처 정보 없음"
        except Exception:
            return "출처 정보 없음"

    def _normalize_text(self, value: str) -> str:
        return " ".join(value.split()) if value else ""


if __name__ == "__main__":
    crawler = NaverNewsSearchCrawler()
    keyword = "\uc0bc\uc131\uc804\uc790"
    print(f"=== '{keyword}' 뉴스 검색 크롤링 테스트 ===")
    news_list = crawler.get_news_by_keyword(keyword, 10)

    for idx, news in enumerate(news_list, start=1):
        print(f"{idx}. {news['title']}")
        print(f"   출처: {news['source']} | 시간: {news['published_date']}")
        print(f"   링크: {news['link']}")
        print("-" * 80)
