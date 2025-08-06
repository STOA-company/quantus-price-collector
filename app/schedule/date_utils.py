import pytz
import logging
from datetime import datetime, date, timedelta
from typing import Literal
import exchange_calendars as ecals

# 타임존 설정 (quantus_backend 의존성 제거)
korea_tz = pytz.timezone('Asia/Seoul')
utc_tz = pytz.timezone('UTC')
us_eastern_tz = pytz.timezone('US/Eastern')  # 서머타임 자동 처리

# 임시 휴일 설정 (필요시 환경변수에서 로드 가능)
TEMPORARY_HOLIDAYS_KR = []
TEMPORARY_HOLIDAYS_US = []

# 로거 설정
logger = logging.getLogger(__name__)

def get_exceptions(e, **kwargs):
    """get_exceptions 함수 대체"""
    action = kwargs.get('action', 'unknown')
    logger.error(f"Exception in {action}: {e}")
    if kwargs.get('_traceback', False):
        import traceback
        logger.error(traceback.format_exc())


class D:
    def __init__(self, *args):
        self.utc_now = datetime.utcnow()
        self.timedelta = 0

    @classmethod
    def datetime(cls, diff: int=0) -> datetime:
        return cls().utc_now + timedelta(hours=diff) if diff > 0 else cls().utc_now + timedelta(hours=diff)

    @classmethod
    def date(cls, diff: int=0) -> date:
        return cls.datetime(diff=diff).date()

    @classmethod
    def date_num(cls, diff: int=0) -> int:
        return int(cls.date(diff=diff).strftime('%Y%m%d'))
    
def now_kr(is_date: bool = False):
    now = datetime.now(korea_tz)
    if is_date:
        return datetime(now.year, now.month, now.day)
    else:
        return datetime(now.year, now.month, now.day, hour=now.hour, minute=now.minute, second=now.second)

def now_utc(is_date: bool = False):
    now = datetime.now(utc_tz)
    if is_date:
        return datetime(now.year, now.month, now.day)
    else:
        return datetime(now.year, now.month, now.day, hour=now.hour, minute=now.minute, second=now.second)

def now_us_eastern(is_date: bool = False):
    """미국 동부 시간 (서머타임 자동 적용)"""
    now = datetime.now(us_eastern_tz)
    if is_date:
        return datetime(now.year, now.month, now.day)
    else:
        return datetime(now.year, now.month, now.day, hour=now.hour, minute=now.minute, second=now.second)
    
def get_session_checker(country: Literal["KR", "US"], start_date: datetime | str):
    if country == "KR":
        calender = "XKRX"
    elif country == "US":
        calender = "XNYS"
        
    if isinstance(start_date, datetime):
        start_date = start_date.strftime("%Y-%m-%d")
    return ecals.get_calendar(calender, start=start_date)

def check_session(
    dt: datetime, 
    country: Literal["KR", "US"],
    session_checker=None,
):
    if session_checker is None:
        start_date = dt - timedelta(days=10)
        session_checker = get_session_checker(country=country, start_date=start_date)
    # 해당 날짜가 영업일인지 확인
    _dt = dt.strftime("%Y-%m-%d")
    if session_checker.is_session(_dt):
        if (country == "US" and _dt not in TEMPORARY_HOLIDAYS_US) or (country == "KR" and _dt not in TEMPORARY_HOLIDAYS_KR):
            return True
    return False

def get_business_days(
    country: Literal["KR", "US", "JP", "HK"],
    start_date: datetime,
    end_date: datetime
) -> list[datetime]:
    """
    주어진 국가와 기간에 대한 영업일 목록을 반환합니다.

    Args:
        country (Literal["KR", "US", "JP", "HK"]): 국가 코드
        start_date (datetime): 시작 날짜
        end_date (datetime): 종료 날짜

    Returns:
        list[datetime]: 영업일 목록
    """
    calendar_map = {
        "KR": "XKRX",  # 한국 거래소
        "US": "XNYS",  # 뉴욕 증권거래소
        "JP": "XTKS",  # 도쿄 증권거래소
        "HK": "XHKG",  # 홍콩 증권거래소
    }

    calendar = ecals.get_calendar(calendar_map[country])
    schedule = calendar.sessions_in_range(start_date, end_date)
    
    return schedule.tolist()
        
def get_business_days_future(
    start_date: datetime, 
    num_days: int, 
    country: Literal["KR", "US"],
    **kwargs
):
    """start_date 포함한 num_days의 미래 영업일 리스트 반환"""
    business_days = []
    try:
        current_date = start_date
        start_date = start_date - timedelta(days=10)
        session_checker = get_session_checker(country=country, start_date=start_date)
        while len(business_days) < num_days:
            if check_session(dt=current_date, country=country, session_checker=session_checker):
                business_days.append(current_date)
            # 미래 날짜로 이동
            current_date += timedelta(days=1)
    except Exception as e:
        kwargs.update(dict(business_days=business_days))
        get_exceptions(e, action="get_business_days_future", _traceback=True, logging=True, **kwargs)
    return business_days

def get_business_days_past(
    start_date: datetime, 
    num_days: int, 
    country: Literal["KR", "US"],
    **kwargs
    ):
    """start_date 포함한 num_days의 과거 영업일 리스트 반환"""
    try:
        current_date = start_date
        if num_days <= 2:
            days = 15
        else:
            days = num_days * 5
        start_date = start_date - timedelta(days=days)
        session_checker = get_session_checker(country=country, start_date=start_date)
        
        business_days = []
        while len(business_days) < num_days:
            if check_session(dt=current_date, country=country, session_checker=session_checker):
                business_days.append(current_date)
            # 과거 날짜로 이동
            current_date -= timedelta(days=1)
        return business_days
    except Exception as e:
        get_exceptions(e, action="get_business_days_past", _traceback=True, logging=True, **kwargs)
        return [current_date]
    
def check_global_variable(var_name):
    if var_name in globals():
        True
    else:
        False
        
def is_dst():
    eastern = pytz.timezone('US/Eastern')
    naive_datetime = datetime.now()
    localized_datetime = eastern.localize(naive_datetime, is_dst=None)
    return localized_datetime.dst() != timedelta(0)

def is_market_open(country: Literal["KR", "US"] = "KR", market_hours=None) -> dict:
    """시장 운영 상태 체크
    
    Args:
        country (Literal["KR", "US"]): 국가 코드
        
    Returns:
        dict: 시장 상태 정보
    """
    if country == "KR":
        now = now_kr()
    elif country == "US":
        now = now_us_eastern()  # 서머타임 자동 적용
    else:
        now = now_utc()
    
    # 영업일 체크
    is_business_day = check_session(dt=now, country=country)
    
    if country == "KR":
        # 시장 시간 설정 (기본값 또는 전달받은 설정 사용)
        if market_hours:
            pre_start_time = market_hours.pre_market_start.split(':')
            open_time = market_hours.market_open.split(':')
            close_time = market_hours.market_close.split(':')
            after_end_time = market_hours.after_market_end.split(':')
            
            pre_start_hour, pre_start_min = int(pre_start_time[0]), int(pre_start_time[1])
            open_hour, open_min = int(open_time[0]), int(open_time[1])
            close_hour, close_min = int(close_time[0]), int(close_time[1])
            after_end_hour, after_end_min = int(after_end_time[0]), int(after_end_time[1])
        else:
            # 시장 시간 설정이 없으면 오류 발생
            raise ValueError("시장 시간 설정이 필요합니다. market_hours 매개변수를 전달해주세요.")
        
        # 현재 시간을 분 단위로 변환
        current_minutes = now.hour * 60 + now.minute
        pre_start_minutes = pre_start_hour * 60 + pre_start_min
        open_minutes = open_hour * 60 + open_min
        close_minutes = close_hour * 60 + close_min
        after_end_minutes = after_end_hour * 60 + after_end_min
        
        is_trading_hours = open_minutes <= current_minutes < close_minutes
        is_pre_market = pre_start_minutes <= current_minutes < open_minutes
        is_after_market = close_minutes <= current_minutes < after_end_minutes
    elif country == "US":
        # 미국 시장: 09:30 - 16:00 EST (썸머타임 고려)
        market_open_hour = 9
        market_open_minute = 30
        market_close_hour = 16
        
        current_time_minutes = now.hour * 60 + now.minute
        open_time_minutes = market_open_hour * 60 + market_open_minute
        close_time_minutes = market_close_hour * 60
        
        is_trading_hours = open_time_minutes <= current_time_minutes < close_time_minutes
        is_pre_market = (8 * 60) <= current_time_minutes < open_time_minutes  # 08:00-09:30
        is_after_market = close_time_minutes <= current_time_minutes < (20 * 60)  # 16:00-20:00
    else:
        is_trading_hours = False
        is_pre_market = False
        is_after_market = False
    
    return {
        "country": country,
        "current_time": now,
        "is_business_day": is_business_day,
        "is_trading_hours": is_trading_hours and is_business_day,
        "is_pre_market": is_pre_market and is_business_day,
        "is_after_market": is_after_market and is_business_day,
        "is_closed": not is_business_day or not (is_trading_hours or is_pre_market or is_after_market)
    }

def get_market_state_description(market_info: dict) -> str:
    """시장 상태를 문자열로 반환
    
    Args:
        market_info (dict): is_market_open 함수의 반환값
        
    Returns:
        str: 시장 상태 설명
    """
    if market_info["is_trading_hours"]:
        return "정규거래시간"
    elif market_info["is_pre_market"]:
        return "장시작전"
    elif market_info["is_after_market"]:
        return "장마감후"
    else:
        return "휴장"

def get_next_market_open_time(country: Literal["KR", "US"] = "KR") -> datetime:
    """다음 장 시작 시간 반환
    
    Args:
        country (Literal["KR", "US"]): 국가 코드
        
    Returns:
        datetime: 다음 장 시작 시간
    """
    if country == "KR":
        now = now_kr()
    elif country == "US":
        now = now_us_eastern()  # 서머타임 자동 적용
    else:
        now = now_utc()
    
    if country == "KR":
        market_open_hour = 9
        market_open_minute = 0
    else:
        market_open_hour = 9
        market_open_minute = 30
    
    # 오늘 장 시작 시간
    today_market_open = now.replace(hour=market_open_hour, minute=market_open_minute, second=0, microsecond=0)
    
    # 현재 시간이 오늘 장 시작 시간 이전이면 오늘, 이후면 다음 영업일
    if now < today_market_open:
        # 오늘이 영업일인지 확인
        if check_session(dt=now, country=country):
            return today_market_open
    
    # 다음 영업일 찾기
    next_business_days = get_business_days_future(
        start_date=now + timedelta(days=1), 
        num_days=1, 
        country=country
    )
    
    if next_business_days:
        next_business_day = next_business_days[0]
        return next_business_day.replace(hour=market_open_hour, minute=market_open_minute, second=0, microsecond=0)
    
    # 다음 영업일을 찾지 못한 경우 내일로 설정
    tomorrow = now + timedelta(days=1)
    return tomorrow.replace(hour=market_open_hour, minute=market_open_minute, second=0, microsecond=0)