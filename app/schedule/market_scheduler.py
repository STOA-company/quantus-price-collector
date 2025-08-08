"""
시장 스케줄러 구현

시장 운영시간에 따라 브로커 데몬의 동작을 제어합니다.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Callable, Optional, Any

from .date_utils import is_market_open, get_market_state_description, get_next_market_open_time
from .enums import MarketState, SchedulerMode, BrokerAction
from .config import SchedulerConfig, load_scheduler_config, get_default_broker_actions, get_market_broker_mapping


class MarketScheduler:
    """시장 스케줄러 클래스
    
    시장 운영시간에 따라 브로커의 연결 상태를 자동으로 관리합니다.
    """
    
    def __init__(self, config: Optional[SchedulerConfig] = None):
        self.config = config or load_scheduler_config()
        self.logger = logging.getLogger(__name__)
        
        # 현재 상태
        self.current_state: MarketState = MarketState.CLOSED
        self.is_running: bool = False
        self.last_state_change: Optional[datetime] = None
        
        # 콜백 함수들
        self.state_change_callbacks: Dict[MarketState, list] = {
            state: [] for state in MarketState
        }
        self.general_callbacks: list = []
        
        # 브로커 동작 설정
        self.broker_actions: Dict[MarketState, str] = get_default_broker_actions()
        
        # 시장별 브로커 매핑
        self.market_broker_mapping = get_market_broker_mapping()
        
        # 통계
        self.stats = {
            'state_changes': 0,
            'last_check_time': None,
            'uptime_start': None,
            'errors': 0
        }
        
        self.logger.info(f"MarketScheduler 초기화됨 - 모드: {self.config.mode}, 시장: {self.config.markets}")

    def get_current_market_state(self) -> MarketState:
        """현재 시장 상태 반환 (여러 시장 통합)"""
        if self.config.mode == SchedulerMode.ALWAYS_ON:
            return MarketState.REGULAR_HOURS
        elif self.config.mode == SchedulerMode.MANUAL:
            return self.current_state
        
        # TIME_BASED 모드 - 여러 시장 체크
        markets_info = {}
        for market in self.config.markets:
            markets_info[market] = is_market_open(market)
        
        # 하나라도 정규시간이면 REGULAR_HOURS
        if any(info["is_trading_hours"] for info in markets_info.values()):
            return MarketState.REGULAR_HOURS
        
        # 하나라도 장시작전이면 PRE_MARKET
        elif any(info["is_pre_market"] for info in markets_info.values()):
            return MarketState.PRE_MARKET
            
        # 하나라도 장마감후면 AFTER_HOURS
        elif any(info["is_after_market"] for info in markets_info.values()):
            return MarketState.AFTER_HOURS
            
        # 모든 시장이 휴장이면 CLOSED
        else:
            return MarketState.CLOSED

    def register_state_callback(self, state: MarketState, callback: Callable):
        """특정 상태 진입 시 호출될 콜백 함수 등록"""
        if not callable(callback):
            raise ValueError("콜백은 호출 가능한 함수여야 합니다")
            
        self.state_change_callbacks[state].append(callback)
        self.logger.debug(f"{state.description} 상태 콜백 등록됨")

    def register_general_callback(self, callback: Callable):
        """모든 상태 변경 시 호출될 콜백 함수 등록"""
        if not callable(callback):
            raise ValueError("콜백은 호출 가능한 함수여야 합니다")
            
        self.general_callbacks.append(callback)
        self.logger.debug("일반 상태 변경 콜백 등록됨")

    def set_broker_action(self, state: MarketState, action: str):
        """시장 상태별 브로커 동작 설정"""
        self.broker_actions[state] = action
        self.logger.debug(f"{state.description} 상태의 브로커 동작을 {action}으로 설정")
    
    def get_broker_params_for_market(self, market: str) -> str:
        """특정 시장의 브로커 매개변수 반환 (MarketType)"""
        return self.market_broker_mapping.get(market, "DOMESTIC")
    
    def get_active_markets_info(self) -> Dict[str, Dict[str, Any]]:
        """현재 활성화된 시장들의 상세 정보 반환"""
        active_markets = {}
        for market in self.config.markets:
            if self.config.mode == SchedulerMode.ALWAYS_ON:
                # ALWAYS_ON 모드에서는 모든 시장을 활성화
                market_info = {
                    'is_trading_hours': True,
                    'is_pre_market': False,
                    'is_after_market': False,
                    'is_closed': False
                }
            else:
                # 실제 시장 시간 체크
                if market == "KR":
                    market_info = is_market_open(market, self.config.market_hours)
                else:
                    market_info = is_market_open(market)
            
            market_type = self.get_broker_params_for_market(market)
            
            active_markets[market] = {
                'market_info': market_info,
                'market_type': market_type,
                'is_active': market_info.get('is_trading_hours', False) or 
                        market_info.get('is_pre_market', False) or 
                        market_info.get('is_after_market', False)
            }
            
        return active_markets

    async def _handle_state_change(self, old_state: MarketState, new_state: MarketState):
        """상태 변경 처리"""
        self.last_state_change = datetime.now()
        self.stats['state_changes'] += 1
        
        if self.config.log_state_changes:
            self.logger.info(f"시장 상태 변경: {old_state.description} → {new_state.description}")
        
        try:
            # 특정 상태 콜백 실행
            for callback in self.state_change_callbacks[new_state]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(old_state, new_state)
                    else:
                        callback(old_state, new_state)
                except Exception as e:
                    self.logger.error(f"상태 콜백 실행 오류 ({new_state.description}): {e}")
                    self.stats['errors'] += 1
            
            # 일반 콜백 실행
            for callback in self.general_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(old_state, new_state)
                    else:
                        callback(old_state, new_state)
                except Exception as e:
                    self.logger.error(f"일반 콜백 실행 오류: {e}")
                    self.stats['errors'] += 1
                    
        except Exception as e:
            self.logger.error(f"상태 변경 처리 중 오류: {e}")
            self.stats['errors'] += 1

    async def start(self):
        """스케줄러 시작"""
        if not self.config.enabled:
            self.logger.info("스케줄러가 비활성화되어 있습니다")
            return
            
        if self.is_running:
            self.logger.warning("스케줄러가 이미 실행 중입니다")
            return
            
        self.is_running = True
        self.stats['uptime_start'] = datetime.now()
        
        # 초기 상태 설정
        initial_state = self.get_current_market_state()
        if initial_state != self.current_state:
            await self._handle_state_change(self.current_state, initial_state)
            self.current_state = initial_state
        
        self.logger.info(f"MarketScheduler 시작됨 - 초기 상태: {self.current_state.description}")
        
        try:
            await self._monitor_loop()
        except Exception as e:
            self.logger.error(f"스케줄러 실행 중 오류: {e}")
            self.stats['errors'] += 1
        finally:
            self.is_running = False

    async def stop(self):
        """스케줄러 정지"""
        self.logger.info("MarketScheduler 정지 중...")
        self.is_running = False

    async def _monitor_loop(self):
        """시장 상태 모니터링 루프"""
        while self.is_running:
            try:
                self.stats['last_check_time'] = datetime.now()
                
                # 현재 시장 상태 확인
                new_state = self.get_current_market_state()
                
                # 상태 변경 감지
                if new_state != self.current_state:
                    await self._handle_state_change(self.current_state, new_state)
                    self.current_state = new_state
                
                # 설정된 간격만큼 대기
                await asyncio.sleep(self.config.check_interval)
                
            except asyncio.CancelledError:
                self.logger.info("스케줄러 모니터링 루프가 취소되었습니다")
                break
            except Exception as e:
                self.logger.error(f"모니터링 루프 오류: {e}")
                self.stats['errors'] += 1
                # 오류 발생 시 짧은 대기 후 재시도
                await asyncio.sleep(min(self.config.check_interval, 30))

    def get_status(self) -> Dict[str, Any]:
        """스케줄러 상태 정보 반환"""
        # 모든 시장 정보 수집
        markets_info = {}
        for market in self.config.markets:
            if market == "KR":
                markets_info[market] = is_market_open(market, self.config.market_hours)
            else:
                markets_info[market] = is_market_open(market)
        
        uptime = None
        if self.stats['uptime_start']:
            uptime = datetime.now() - self.stats['uptime_start']
        
        return {
            'scheduler': {
                'enabled': self.config.enabled,
                'running': self.is_running,
                'mode': self.config.mode.value,
                'check_interval': self.config.check_interval,
                'uptime': str(uptime) if uptime else None,
                'last_check': self.stats['last_check_time']
            },
            'markets': {
                'supported_markets': self.config.markets,
                'current_state': self.current_state.value,
                'current_state_description': self.current_state.description,
                'last_state_change': self.last_state_change,
                'markets_info': markets_info,
                'active_markets_info': self.get_active_markets_info(),
                'broker_mapping': self.market_broker_mapping
            },
            'stats': self.stats.copy(),
            'broker_actions': {state.value: action for state, action in self.broker_actions.items()}
        }

    def set_manual_state(self, state: MarketState):
        """수동 모드에서 시장 상태 설정"""
        if self.config.mode != SchedulerMode.MANUAL:
            raise ValueError("수동 모드에서만 상태를 직접 설정할 수 있습니다")
            
        if state != self.current_state:
            old_state = self.current_state
            self.current_state = state
            
            # 비동기 콜백 실행을 위한 태스크 생성
            if self.state_change_callbacks[state] or self.general_callbacks:
                asyncio.create_task(self._handle_state_change(old_state, state))
            
            self.logger.info(f"수동으로 시장 상태 변경: {old_state.description} → {state.description}")

    def get_time_until_next_state_change(self) -> Optional[timedelta]:
        """다음 상태 변경까지 남은 시간 반환 (가장 가까운 시장 기준)"""
        if self.config.mode != SchedulerMode.TIME_BASED:
            return None
            
        try:
            # 모든 지원 시장의 다음 오픈 시간을 구해서 가장 가까운 것 선택
            next_opens = []
            for market in self.config.markets:
                try:
                    next_open = get_next_market_open_time(market)
                    next_opens.append(next_open)
                except Exception as e:
                    self.logger.warning(f"{market} 시장의 다음 오픈 시간 계산 실패: {e}")
            
            if not next_opens:
                return None
                
            # 가장 가까운 시간 선택
            closest_open = min(next_opens)
            current_time = datetime.now()
            
            if current_time < closest_open:
                return closest_open - current_time
            else:
                # 다음날 장 시작까지의 시간
                next_day_open = closest_open + timedelta(days=1)
                return next_day_open - current_time
                
        except Exception as e:
            self.logger.error(f"다음 상태 변경 시간 계산 오류: {e}")
            return None