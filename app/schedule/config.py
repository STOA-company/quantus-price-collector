"""
스케줄러 설정 관리
"""

import os
from typing import Dict, Any
from dataclasses import dataclass
from dotenv import load_dotenv
from .enums import SchedulerMode, MarketState

# .env 파일 로드
load_dotenv()


@dataclass
class SchedulerConfig:
    """스케줄러 설정"""
    
    # 기본 설정
    enabled: bool = True
    mode: SchedulerMode = SchedulerMode.TIME_BASED
    check_interval: int = 60  # 초 단위
    markets: list = None  # 지원할 시장 리스트 ["KR", "US"]
    
    # 로깅 설정
    log_state_changes: bool = True
    
    # __post_init__ 제거 - 환경변수에서만 로드


def load_scheduler_config() -> SchedulerConfig:
    """환경변수에서 스케줄러 설정 로드"""
    # 시장 운영시간은 date_utils.py에서 관리
    
    # 스케줄러 모드
    scheduler_mode = os.getenv('SCHEDULER_MODE', 'time_based')
    try:
        mode = SchedulerMode(scheduler_mode)
    except ValueError:
        mode = SchedulerMode.TIME_BASED
    
    # 지원할 시장 목록 (JSON 또는 쉼표 구분 문자열 지원)
    markets_env = os.getenv('MARKET_COUNTRIES', '["KR","US"]')
    
    try:
        # JSON 형태로 파싱 시도
        import json
        if markets_env.startswith('[') and markets_env.endswith(']'):
            markets = json.loads(markets_env)
        else:
            # 쉼표 구분 문자열로 파싱 (기존 방식 호환)
            markets = [market.strip().upper() for market in markets_env.split(',')]
    except (json.JSONDecodeError, ValueError):
        # 파싱 실패 시 기본값 사용
        markets = ["KR", "US"]
    
    # 설정 생성
    config = SchedulerConfig(
        enabled=os.getenv('ENABLE_MARKET_SCHEDULER', 'true').lower() == 'true',
        mode=mode,
        check_interval=int(os.getenv('SCHEDULER_CHECK_INTERVAL', '60')),
        markets=markets,
        log_state_changes=os.getenv('LOG_STATE_CHANGES', 'true').lower() == 'true'
    )
    
    return config


def get_default_broker_actions() -> Dict[MarketState, str]:
    """시장 상태별 기본 브로커 동작 설정"""
    return {
        MarketState.PRE_MARKET: "connect",      # 장 시작 전: 연결 준비
        MarketState.REGULAR_HOURS: "connect",   # 정규시간: 모든 세션 활성화
        MarketState.AFTER_HOURS: "minimize",    # 장 마감 후: 최소 세션 유지
        MarketState.CLOSED: "pause"             # 휴장: 일시정지 또는 연결 해제
    }


def get_market_broker_mapping() -> Dict[str, str]:
    """시장별 브로커 매개변수 매핑 (MarketType만)"""
    return {
        "KR": "DOMESTIC",
        "US": "FOREIGN"
    }