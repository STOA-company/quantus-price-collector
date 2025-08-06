"""
스케줄러 관련 열거형 정의
"""

from enum import Enum


class MarketState(Enum):
    """시장 상태 열거형"""
    PRE_MARKET = "pre_market"          # 장 시작 전 (08:00-09:00)
    REGULAR_HOURS = "regular_hours"    # 정규거래시간 (09:00-15:30)
    AFTER_HOURS = "after_hours"        # 장 마감 후 (15:30-18:00)
    CLOSED = "closed"                  # 휴장 시간
    
    def __str__(self):
        return self.value
    
    @property
    def description(self):
        """상태 설명 반환"""
        descriptions = {
            self.PRE_MARKET: "장시작전",
            self.REGULAR_HOURS: "정규거래시간",
            self.AFTER_HOURS: "장마감후",
            self.CLOSED: "휴장"
        }
        return descriptions.get(self, "알수없음")


class SchedulerMode(Enum):
    """스케줄러 동작 모드"""
    TIME_BASED = "time_based"      # 시간 기반 스케줄링
    ALWAYS_ON = "always_on"        # 항상 활성화
    MANUAL = "manual"              # 수동 제어
    
    def __str__(self):
        return self.value


class BrokerAction(Enum):
    """브로커 동작 타입"""
    CONNECT = "connect"            # 브로커 연결
    DISCONNECT = "disconnect"      # 브로커 연결 해제
    PAUSE = "pause"               # 브로커 일시정지
    RESUME = "resume"             # 브로커 재시작
    MINIMIZE = "minimize"         # 최소 연결 유지
    
    def __str__(self):
        return self.value