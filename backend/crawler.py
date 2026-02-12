import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, Tag


class NaverStockNewsCrawler:
    def __init__(self):
        # 요청 차단을 줄이기 위한 브라우저 헤더
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
        """
        특정 종목의 최신 뉴스를 크롤링합니다.
        """
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
            for i, row in enumerate(rows):
                if len(news_list) >= max_count:
                    break

                title_cell = row.find("td", {"class": "title"})
                if not title_cell:
                    continue

                title_link = title_cell.find("a")
                if not title_link:
                    continue

                date_cell = row.find("td", {"class": "date"})
                date_text = date_cell.get_text(strip=True) if date_cell else "날짜 정보 없음"

                info_cell = row.find("td", {"class": "info"})
                source_text = info_cell.get_text(strip=True) if info_cell else "출처 정보 없음"

                title = title_link.get_text(strip=True)
                link = title_link.get("href", "")
                if link.startswith("/"):
                    link = "https://finance.naver.com" + link

                news_item = {
                    "id": f"{stock_code}_{i}_{int(time.time())}",
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
        """
        여러 종목의 뉴스를 한 번에 가져옵니다.
        """
        results: Dict[str, List[Dict[str, str]]] = {}

        for stock_code in stock_codes:
            results[stock_code] = self.get_stock_news(stock_code, max_each)
            time.sleep(1)  # 요청 간격(크롤링 예의)

        return results


class NaverNewsSearchCrawler:
    """
    네이버 뉴스 검색 기반 크롤러.
    기존 a.news_tit 셀렉터가 사라져 data-heatmap-target='.tit' 셀렉터를 사용합니다.
    """

    def __init__(self):
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        }

    def get_news_by_keyword(self, keyword: str, max_count: int = 10) -> List[Dict[str, str]]:
        """키워드로 네이버 뉴스 검색"""
        try:
            print(f"[검색 크롤링 시작] 키워드: {keyword}")

            url = "https://search.naver.com/search.naver"
            params = {"where": "news", "sm": "tab_opt", "query": keyword}

            response = requests.get(url, headers=self.headers, params=params, timeout=15)
            response.raise_for_status()

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
                if not title or title == "네이버뉴스":
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

            print(f"[검색 크롤링 완료] {keyword}: {len(news_list)}개 뉴스 수집")
            return news_list

        except Exception as e:
            print(f"[검색 크롤링 에러] {keyword}: {e}")
            return []

    def _select_title_links(self, soup: BeautifulSoup) -> List[Tag]:
        """
        현재 네이버 뉴스 검색 레이아웃 우선, 구형 레이아웃을 fallback으로 지원.
        """
        links = soup.select("div.group_news a[data-heatmap-target='.tit'][href]")
        if links:
            return links

        # 구형 마크업 fallback
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

        # 구형 레이아웃 fallback
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
            if text in ("네이버뉴스", source):
                continue
            published_date = text
            break

        return source, published_date

    def _find_nearest_profile(self, title_link: Tag) -> Optional[Tag]:
        """
        title anchor 기준으로 가장 가까운 Profile 블록을 찾아 source/date를 추출.
        네이버의 해시 클래스명에 의존하지 않기 위해 data-sds-comp 속성만 사용.
        """
        # 1) 가장 가까운 상위 블록에서 "직계 자식" Profile 1개를 찾으면 그것을 우선 사용
        #    (대형 카드/일반 카드 모두 이 규칙으로 정확하게 매칭되는 경우가 많음)
        for ancestor in title_link.parents:
            if not isinstance(ancestor, Tag):
                continue

            direct_profiles = ancestor.find_all(
                "div", attrs={"data-sds-comp": "Profile"}, recursive=False
            )
            if len(direct_profiles) == 1:
                return direct_profiles[0]

        # 2) 직계 매칭이 실패하면 기존처럼 동일 상위 블록 내 가장 가까운 Profile을 선택
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

            nearest = min(
                profiles,
                key=lambda profile: abs(positions.get(id(profile), 10**9) - link_pos),
            )
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

    keyword = "삼성전자"
    print(f"=== '{keyword}' 뉴스 검색 크롤링 테스트 ===")
    news_list = crawler.get_news_by_keyword(keyword, 10)

    for i, news in enumerate(news_list, start=1):
        print(f"{i}. {news['title']}")
        print(f"   출처: {news['source']} | 날짜: {news['published_date']}")
        print(f"   링크: {news['link']}")
        print("-" * 80)
