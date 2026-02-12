from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
from datetime import datetime
from typing import List, Optional
from crawler import NaverStockNewsCrawler, NaverNewsSearchCrawler

# FastAPI 앱 생성
app = FastAPI(
    title="kjgmacro Stock Alert API",
    description="실시간 주식 뉴스 알림 서버 - kjgmacro.com",
    version="1.0.0",
    docs_url="/docs",  # API 문서 경로
    redoc_url="/redoc"  # 대체 문서 경로
)

# CORS 설정 (모바일 앱에서 접근 가능하도록)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 개발 단계에서는 모든 도메인 허용
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 크롤러 인스턴스 생성
news_crawler = NaverStockNewsCrawler()
search_crawler = NaverNewsSearchCrawler()

# 기본 관심 종목 (나중에 DB나 사용자 설정으로 변경)
DEFAULT_WATCHLIST = [
    {"code": "005930", "name": "삼성전자"},
    {"code": "000660", "name": "SK하이닉스"},
    {"code": "035720", "name": "카카오"},
    {"code": "051910", "name": "LG화학"},
    {"code": "006400", "name": "삼성SDI"}
]

@app.get("/")
def root():
    """서버 상태 및 기본 정보"""
    return {
        "service": "kjgmacro Stock Alert Server",
        "status": "running",
        "version": "1.0.0",
        "description": "실시간 주식 뉴스 크롤링 및 알림 서비스",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "endpoints": {
            "health": "/api/health",
            "news": "/api/news/{stock_code}",
            "alerts": "/api/alerts",
            "watchlist": "/api/watchlist",
            "docs": "/docs"
        }
    }

@app.get("/api/health")
def health_check():
    """헬스 체크 (Google Cloud Run에서 사용)"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "uptime": "서버 정상 작동 중"
    }

@app.get("/api/news/{stock_code}")
def get_stock_news(
    stock_code: str,
    limit: int = Query(default=10, ge=1, le=50, description="가져올 뉴스 개수")
):
    """종목코드를 종목명으로 변환하여 뉴스 검색"""
    
    # 종목코드 → 종목명 매핑
    code_to_name = {
        "005930": "삼성전자",
        "000660": "SK하이닉스", 
        "035720": "카카오",
        "051910": "LG화학",
        "006400": "삼성SDI"
    }
    
    keyword = code_to_name.get(stock_code, stock_code)
    
    try:
        print(f"[API 요청] {stock_code} → '{keyword}' 뉴스 {limit}개 검색")
        
        # 새로운 검색 크롤러 사용
        news_list = search_crawler.get_news_by_keyword(keyword, limit)
        
        if not news_list:
            return {
                "success": False,
                "message": f"'{keyword}' 관련 뉴스를 찾을 수 없습니다.",
                "stock_code": stock_code,
                "keyword": keyword,
                "count": 0,
                "data": []
            }
        
        return {
            "success": True,
            "stock_code": stock_code,
            "keyword": keyword,
            "count": len(news_list),
            "data": news_list,
            "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"뉴스 검색 중 오류: {str(e)}"
        )

@app.get("/api/alerts")
def get_alerts(priority: Optional[str] = None, limit: int = 20):
    """
    종합 알림 목록 조회 (관심 종목들의 뉴스 요약)
    
    - **priority**: 우선순위 필터 (high/medium/low)
    - **limit**: 최대 알림 개수
    """
    try:
        alerts = []
        
        # 관심 종목들의 뉴스를 수집
        for stock in DEFAULT_WATCHLIST[:5]:  # 처음 5개만 (테스트용)
            stock_code = stock["code"]
            stock_name = stock["name"]
            
            # 각 종목당 최대 3개 뉴스
            news_list = news_crawler.get_stock_news(stock_code, 3)
            
            if news_list:
                # 알림 데이터 생성
                alert = {
                    "id": f"alert_{stock_code}_{int(datetime.now().timestamp())}",
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "title": f"{stock_name} 관련 뉴스 {len(news_list)}건 업데이트",
                    "summary": f"최근 {stock_name} 관련하여 {len(news_list)}건의 뉴스가 확인되었습니다.",
                    "article_count": len(news_list),
                    "priority": "high" if len(news_list) >= 3 else "medium",
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "latest_news": news_list[:2],  # 최신 2개만 미리보기
                    "sentiment": "neutral"  # 나중에 감성분석 추가 예정
                }
                alerts.append(alert)
        
        # 우선순위 필터링
        if priority:
            alerts = [a for a in alerts if a["priority"] == priority]
        
        return {
            "success": True,
            "count": len(alerts),
            "alerts": alerts[:limit],
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"알림 생성 중 오류가 발생했습니다: {str(e)}"
        )

@app.get("/api/watchlist")
def get_watchlist():
    """관심 종목 목록 조회"""
    return {
        "success": True,
        "watchlist": DEFAULT_WATCHLIST,
        "count": len(DEFAULT_WATCHLIST)
    }

@app.get("/api/multiple-news")
def get_multiple_news(
    codes: str = Query(..., description="종목코드들 (쉼표로 구분, 예: 005930,000660)"),
    limit_each: int = Query(default=5, description="종목당 뉴스 개수")
):
    """
    여러 종목의 뉴스를 한 번에 조회
    
    - **codes**: 종목코드들을 쉼표로 구분 (예: 005930,000660,035720)
    - **limit_each**: 종목당 가져올 뉴스 개수
    """
    try:
        stock_codes = [code.strip() for code in codes.split(",")]
        
        if len(stock_codes) > 10:
            raise HTTPException(
                status_code=400,
                detail="한 번에 최대 10개 종목까지만 조회 가능합니다."
            )
        
        # 여러 종목 뉴스 수집
        results = news_crawler.get_multiple_stocks_news(stock_codes, limit_each)
        
        return {
            "success": True,
            "requested_codes": stock_codes,
            "results": results,
            "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"다중 뉴스 조회 중 오류: {str(e)}"
        )

# 서버 실행 설정
if __name__ == "__main__":
    # Google Cloud Run은 PORT 환경변수를 사용
    port = int(os.environ.get("PORT", 8080))
    
    print(f"""
    ╔═══════════════════════════════════════════════╗
    ║           kjgmacro 주식 알림 서버            ║
    ╠═══════════════════════════════════════════════╣
    ║  🌐 서버 주소: http://localhost:{port}        ║
    ║  📚 API 문서: http://localhost:{port}/docs    ║
    ║  ⚡ 상태: 실시간 뉴스 크롤링 준비 완료      ║
    ╚═══════════════════════════════════════════════╝
    """)
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=True,  # 코드 변경 시 자동 재시작
        log_level="info"
    )
