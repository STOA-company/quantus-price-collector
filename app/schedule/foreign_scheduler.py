import logging
import asyncio
from datetime import datetime
from typing import Dict, Callable, Optional, List

from .date_utils import is_market_open, now_utc
from .enums import MarketState
from ..daemon.broker_daemon import BrokerDaemon
from ..brokers.base import MarketType

logger = logging.getLogger(__name__)


class ForeignScheduler:
    """해외 시장 전용 스케줄러 (US 등 해외 시장)"""
    
    def __init__(self, target_markets: List[str] = None, check_interval: int = 60):
        self.target_markets = target_markets or ["US"]
        self.check_interval = check_interval
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
        
        # 🔥 해외 전용 브로커 데몬
        self.broker_daemon = BrokerDaemon(market_type=MarketType.FOREIGN)
        self.daemon_task = None
        
        # 스케줄러 콜백 설정
        self._setup_scheduler_callbacks()
        
        # 통계
        self.stats = {
            'state_changes': 0,
            'last_check_time': None,
            'uptime_start': None,
            'errors': 0
        }
        
        self.logger.info(f"🌍 ForeignScheduler 초기화됨 - 대상: {self.target_markets}")

    def _setup_scheduler_callbacks(self):
        """시장 상태별 콜백 설정"""
        self.register_state_callback(MarketState.REGULAR_HOURS, self._on_market_open)
        self.register_state_callback(MarketState.CLOSED, self._on_market_closed)
        
        # 일반 상태 변경 로깅
        self.register_general_callback(self._on_market_state_change)

    def get_current_market_state(self) -> MarketState:
        """해외 시장들의 통합 상태 반환"""
        try:
            markets_info = {}
            for market in self.target_markets:
                markets_info[market] = is_market_open(market)
            
            # 하나라도 거래시간이면 REGULAR_HOURS
            if any(info["is_trading_hours"] for info in markets_info.values()):
                return MarketState.REGULAR_HOURS
            # 그 외에는 CLOSED
            else:
                return MarketState.CLOSED
                
        except Exception as e:
            self.logger.error(f"해외 시장 상태 확인 중 오류: {e}")
            return MarketState.CLOSED

    def register_state_callback(self, state: MarketState, callback: Callable):
        """특정 상태 진입 시 호출될 콜백 함수 등록"""
        if not callable(callback):
            raise ValueError("콜백은 호출 가능한 함수여야 합니다")
            
        self.state_change_callbacks[state].append(callback)
        self.logger.debug(f"[해외] {state.description} 상태 콜백 등록됨")

    def register_general_callback(self, callback: Callable):
        """모든 상태 변경 시 호출될 콜백 함수 등록"""
        if not callable(callback):
            raise ValueError("콜백은 호출 가능한 함수여야 합니다")
            
        self.general_callbacks.append(callback)
        self.logger.debug("[해외] 일반 상태 변경 콜백 등록됨")

    async def _on_market_open(self, old_state: MarketState, new_state: MarketState):
        """거래시간 시작 - 해외 브로커 데몬 시작"""
        logger.info("🟢 [해외] 거래시간 시작 - 브로커 데몬 시작")
        await self._start_broker_daemon()
    
    async def _on_market_closed(self, old_state: MarketState, new_state: MarketState):
        """휴장 - 해외 브로커 데몬 정지"""
        logger.info("🔴 [해외] 휴장 시간 - 브로커 데몬 정지")
        await self._stop_broker_daemon()
    
    async def _on_market_state_change(self, old_state: MarketState, new_state: MarketState):
        """시장 상태 변경 로깅"""
        logger.info(f"📊 [해외] 시장 상태 변경: {old_state.description} → {new_state.description}")

    async def _start_broker_daemon(self):
        """해외 브로커 데몬 시작"""
        try:
            if self.daemon_task and not self.daemon_task.done():
                logger.warning("[해외] 브로커 데몬이 이미 실행 중입니다")
                return
            
            logger.info("🌍 [해외] 브로커 데몬 시작 (해외 전용 AppKey 사용)")
            
            # 브로커 데몬을 별도 태스크로 실행
            self.daemon_task = asyncio.create_task(self.broker_daemon.start())
            logger.info("✅ [해외] 브로커 데몬 시작됨")
            
        except Exception as e:
            logger.error(f"[해외] 브로커 데몬 시작 실패: {e}")
    
    async def _stop_broker_daemon(self):
        """해외 브로커 데몬 정지"""
        try:
            if self.broker_daemon:
                await self.broker_daemon.stop()
                logger.info("✅ [해외] 브로커 데몬 정지됨")
            
            if self.daemon_task and not self.daemon_task.done():
                self.daemon_task.cancel()
                try:
                    await self.daemon_task
                except asyncio.CancelledError:
                    logger.debug("[해외] 브로커 데몬 태스크 취소됨")
            
            self.daemon_task = None
            
        except Exception as e:
            logger.error(f"[해외] 브로커 데몬 정지 실패: {e}")

    async def _handle_state_change(self, old_state: MarketState, new_state: MarketState):
        """시장 상태 변경 처리"""
        self.logger.info(f"[해외] 시장 상태 변경: {old_state.description} → {new_state.description}")
        
        self.last_state_change = datetime.now()
        self.stats['state_changes'] += 1
        
        try:
            # 일반 콜백 실행
            for callback in self.general_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(old_state, new_state)
                    else:
                        callback(old_state, new_state)
                except Exception as e:
                    self.logger.error(f"[해외] 일반 콜백 실행 오류: {e}")
            
            # 특정 상태 콜백 실행
            for callback in self.state_change_callbacks[new_state]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(old_state, new_state)
                    else:
                        callback(old_state, new_state)
                except Exception as e:
                    self.logger.error(f"[해외] {new_state.description} 콜백 실행 오류: {e}")
                    
        except Exception as e:
            self.logger.error(f"[해외] 상태 변경 처리 중 오류: {e}")
            self.stats['errors'] += 1

    async def start(self):
        """해외 시장 스케줄러 시작"""
        if self.is_running:
            self.logger.warning("[해외] 스케줄러가 이미 실행 중입니다")
            return
            
        self.is_running = True
        self.stats['uptime_start'] = datetime.now()
        
        # 초기 상태 설정
        initial_state = self.get_current_market_state()
        if initial_state != self.current_state:
            await self._handle_state_change(self.current_state, initial_state)
            self.current_state = initial_state
        
        self.logger.info(f"🌍 [해외] ForeignScheduler 시작됨 - 초기 상태: {self.current_state.description}")
        
        try:
            await self._monitor_loop()
        except Exception as e:
            self.logger.error(f"[해외] 스케줄러 실행 중 오류: {e}")
            self.stats['errors'] += 1
        finally:
            self.is_running = False

    async def stop(self):
        """해외 시장 스케줄러 정지"""
        self.logger.info("🌍 [해외] ForeignScheduler 정지 중...")
        self.is_running = False
        
        # 브로커 데몬 정지
        await self._stop_broker_daemon()

    async def _monitor_loop(self):
        """해외 시장 상태 모니터링 루프"""
        while self.is_running:
            try:
                self.stats['last_check_time'] = datetime.now()
                
                # 현재 해외 시장 상태 확인
                new_state = self.get_current_market_state()
                
                # 상태 변경 감지
                if new_state != self.current_state:
                    await self._handle_state_change(self.current_state, new_state)
                    self.current_state = new_state
                
                # 설정된 간격만큼 대기
                await asyncio.sleep(self.check_interval)
                
            except asyncio.CancelledError:
                self.logger.info("[해외] 스케줄러 모니터링 루프가 취소되었습니다")
                break
            except Exception as e:
                self.logger.error(f"[해외] 모니터링 루프 오류: {e}")
                self.stats['errors'] += 1
                # 오류 발생 시 짧은 대기 후 재시도
                await asyncio.sleep(min(self.check_interval, 30))

    def get_market_info(self) -> Dict[str, any]:
        """해외 시장 정보 반환"""
        try:
            markets_info = {}
            for market in self.target_markets:
                market_info = is_market_open(market)
                markets_info[market] = {
                    'market_info': market_info,
                    'market_type': 'FOREIGN',
                    'is_active': market_info.get('is_trading_hours', False)
                }
            return markets_info
        except Exception as e:
            self.logger.error(f"해외 시장 정보 조회 중 오류: {e}")
            return {}

    def get_status(self) -> dict:
        """스케줄러 상태 정보 반환"""
        daemon_running = self.daemon_task and not self.daemon_task.done() if self.daemon_task else False
        markets_info = self.get_market_info()
        
        uptime = None
        if self.stats['uptime_start']:
            uptime = datetime.now() - self.stats['uptime_start']
        
        return {
            'scheduler_type': 'foreign',
            'target_markets': self.target_markets,
            'scheduler': {
                'running': self.is_running,
                'check_interval': self.check_interval,
                'uptime': str(uptime) if uptime else None,
                'last_check': self.stats['last_check_time']
            },
            'markets': {
                'current_state': self.current_state.value,
                'current_state_description': self.current_state.description,
                'last_state_change': self.last_state_change,
                'markets_info': markets_info,
                'utc_time': now_utc().isoformat()
            },
            'broker_daemon': {
                'initialized': self.broker_daemon is not None,
                'running': daemon_running
            },
            'stats': self.stats.copy()
        }