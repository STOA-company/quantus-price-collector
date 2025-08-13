import logging
import asyncio
from datetime import datetime
from typing import Dict, Set, Optional

from app.services.redis_service import RedisService
from app.brokers.factory import broker_factory_manager
from app.brokers.base import BrokerWebSocketClient, MarketType, BrokerConfig
from app.utils.config import config
from app.utils.exceptions import (
    BrokerConnectionError, 
    CircuitBreakerError, 
    BrokerInitializationError,
    BrokerReconnectionError,
    ResubscriptionFailedError
)
from app.utils.exceptions import BrokerDaemonStatusNotification

logger = logging.getLogger(__name__)

class BrokerDaemon:
    def __init__(self, market_type: MarketType):
        self.market_type = market_type
        self.config = config
        self.redis_service = RedisService()
        self.broker_factory_manager = broker_factory_manager
        
        # 브로커 관리
        self.brokers: Dict[str, BrokerWebSocketClient] = {}
        self.running = False
        
        # 서킷브레이커 설정
        self.circuit_breakers: Dict[str, dict] = {}
        self.circuit_breaker_config = {
            'failure_threshold': 1,      # 3회 연속 실패 시 OPEN
            'recovery_timeout': 5,      # 60초 후 복구 시도
            'fallback_enabled': True     # REST API 폴백 활성화
        }
        
        # 재연결 설정
        self.reconnect_intervals = {'dbfi': 30}
        self.max_reconnect_attempts = 10
        
        # 구독 관리
        self.requested_symbols: Dict[str, Set[str]] = {}
        self.confirmed_symbols: Dict[str, Set[str]] = {}
        self.pending_resubscriptions: Dict[str, Set[str]] = {}
        self.resubscription_tasks: Dict[str, asyncio.Task] = {}
        
        # 재구독 설정
        self.resubscription_config = {
            'max_retries': 10,
            'base_interval': 10,
            'max_interval': 300,
            'exponential_backoff': True
        }
        
        # REST API 태스크 관리 추가
        self.rest_api_tasks: Dict[str, asyncio.Task] = {}
        
        # 통계
        self.stats = {
            'total_messages': 0,
            'error_count': 0,
            'last_update': None
        }
        
        # 시작 시간 기록 (가동시간 계산용)
        self.start_time = None
        
        # 시장별 설정 로드
        if self.market_type == MarketType.DOMESTIC:
            self.dbfi_config = config.dbfi.get_config_for_market(MarketType.DOMESTIC)
            self.watch_symbols = self.config.broker.watch_symbols_domestic
            self.market_prefix = "d"
        else:
            self.dbfi_config = config.dbfi.get_config_for_market(MarketType.FOREIGN)
            self.watch_symbols = self.config.broker.watch_symbols_foreign
            self.market_prefix = "f"

    async def start(self, active_markets_info=None):
        """데몬 시작"""
        self.start_time = datetime.now()
        logger.info(f"🚀 Broker Daemon 시작 ({self.market_type.value})")
        
        # Slack 알림: 데몬 시작
        try:
            BrokerDaemonStatusNotification.send_startup_notification(
                market_type=self.market_type.value,
                broker_count=0,  # 아직 초기화 전
                symbol_count=len(self.watch_symbols)
            )
        except Exception as e:
            logger.warning(f"Slack 시작 알림 전송 실패: {e}")
        
        self.running = True
        
        try:
            # Redis 연결
            if not self.redis_service.connect():
                raise Exception("Redis 연결 실패")
            
            # 브로커 초기화
            await self._initialize_brokers()
            
            # 브로커 태스크 시작
            broker_tasks = []
            for broker_name, broker in self.brokers.items():
                task = asyncio.create_task(self._run_broker_loop(broker_name, broker))
                broker_tasks.append(task)
                logger.info(f"✅ {broker_name} 브로커 태스크 시작")
            
            # 모니터링 태스크 시작
            monitor_task = asyncio.create_task(self._monitor_brokers())
            broker_tasks.append(monitor_task)
            
            # 모든 태스크 실행
            await asyncio.gather(*broker_tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"❌ Broker Daemon 시작 실패: {e}")
            await self.stop()
            raise

    async def stop(self):
        """데몬 정지"""
        logger.info("🛑 Broker Daemon 정지 시작...")
        
        try:
            self.running = False
            # 1. 브로커 연결 해제
            for broker_name, broker in self.brokers.items():
                # if hasattr(broker, 'set_shutdown_mode'):
                broker.set_shutdown_mode(True)  # ← 정상 종료 모드 설정
                logger.debug(f"{broker_name} 정상 종료 모드 설정")

            for broker_name, broker in self.brokers.items():
                try:
                    if broker.is_connected():
                        await broker.disconnect()
                        logger.debug(f"✅ {broker_name} 연결 해제")
                except Exception as e:
                    logger.error(f"❌ {broker_name} 연결 해제 실패: {e}")
            
            # 2. 재구독 태스크 취소
            for broker_name, task in self.resubscription_tasks.items():
                if not task.done():
                    task.cancel()
                    logger.debug(f"🔄 {broker_name} 재구독 태스크 취소")
            
            # 3. 상태 정리
            self.running = False
            self.resubscription_tasks.clear()
            self.pending_resubscriptions.clear()
            self.requested_symbols.clear()
            self.confirmed_symbols.clear()
            
            # 4. Redis 연결 해제
            if self.redis_service:
                self.redis_service.disconnect()
            
            # Slack 알림: 데몬 종료
            try:
                uptime = self._calculate_uptime()
                BrokerDaemonStatusNotification.send_shutdown_notification(
                    market_type=self.market_type.value,
                    uptime=uptime,
                    total_messages=self.stats['total_messages']
                )
            except Exception as e:
                logger.warning(f"Slack 종료 알림 전송 실패: {e}")
            
            logger.info("✅ Broker Daemon 정상 종료")
            
        except Exception as e:
            logger.error(f"💥 Broker Daemon 종료 중 오류: {e}")
            self.running = False

    def _calculate_uptime(self) -> str:
        """가동시간 계산"""
        if not self.start_time:
            return "알 수 없음"
        
        uptime = datetime.now() - self.start_time
        hours = uptime.seconds // 3600
        minutes = (uptime.seconds % 3600) // 60
        
        if hours > 0:
            return f"{hours}시간 {minutes}분"
        else:
            return f"{minutes}분"

    async def _initialize_brokers(self):
        """브로커 초기화"""
        try:
            enabled_brokers = self.config.broker.enabled_brokers
            logger.info(f"🔧 {self.market_type.value} 브로커 초기화 중... (종목: {len(self.watch_symbols)}개)")
            
            for broker_name in enabled_brokers:
                try:
                    # 필요한 세션 수 계산
                    session_count = self._calculate_required_sessions(broker_name, self.watch_symbols)
                    logger.info(f"{broker_name}: {len(self.watch_symbols)}개 종목, {session_count}개 세션 필요")
                    
                    # 세션별 브로커 생성
                    for session_id in range(session_count):
                        broker_key = f"{self.market_prefix}_{broker_name}_{session_id}" if session_count > 1 else f"{self.market_prefix}{broker_name}"
                        
                        # 브로커 설정
                        broker_config = BrokerConfig(
                            api_key=self.dbfi_config['api_key'],
                            api_secret=self.dbfi_config['api_secret'],
                            websocket_url=self.dbfi_config['websocket_url'],
                            batch_size=self.dbfi_config['batch_size'],
                            available_sessions=self.dbfi_config['available_sessions'],
                            market_type=self.market_type
                        )
                        
                        # 브로커 생성
                        broker = self.broker_factory_manager.factory.create_broker(
                            broker_name, config=broker_config, market_type=self.market_type
                        )
                        
                        if broker:
                            self.brokers[broker_key] = broker
                            
                            # 서킷브레이커 초기화
                            self.circuit_breakers[broker_key] = {
                                'state': 'CLOSED',
                                'failure_count': 0,
                                'last_failure_time': None,
                                'last_success_time': datetime.now()
                            }
                            
                            logger.info(f"✅ {broker_key} 브로커 초기화 완료")
                            
                            # Slack 알림: 브로커 초기화 성공
                            try:
                                BrokerDaemonStatusNotification.send_broker_initialization_success(
                                    broker_name=broker_key,
                                    market_type=self.market_type.value,
                                    session_count=session_count
                                )
                            except Exception as e:
                                logger.warning(f"Slack 브로커 초기화 성공 알림 전송 실패: {e}")
                        else:
                            logger.error(f"❌ {broker_key} 브로커 생성 실패")
                            
                            # Slack 알림: 브로커 초기화 실패
                            try:
                                raise BrokerInitializationError(
                                    message=f"{broker_key} 브로커 생성 실패",
                                    broker_name=broker_key,
                                    market_type=self.market_type.value,
                                    error_details="브로커 팩토리에서 None 반환"
                                )
                            except BrokerInitializationError as e:
                                logger.error(f"❌ {e}")
                            
                except Exception as e:
                    logger.error(f"❌ {broker_name} 브로커 초기화 실패: {e}")
                    
                    # Slack 알림: 브로커 초기화 실패
                    try:
                        raise BrokerInitializationError(
                            message=f"{broker_name} 브로커 초기화 실패",
                            broker_name=broker_name,
                            market_type=self.market_type.value,
                            error_details=str(e)
                        )
                    except BrokerInitializationError as e:
                        logger.error(f"❌ {e}")
                    
                    continue
            
            if not self.brokers:
                raise BrokerInitializationError(
                    message=f"사용 가능한 {self.market_type.value} 브로커가 없습니다",
                    broker_name="",
                    market_type=self.market_type.value,
                    error_details="모든 브로커 초기화 실패"
                )
                
        except Exception as e:
            logger.error(f"❌ 브로커 초기화 실패: {e}")
            raise

    def _calculate_required_sessions(self, broker_name: str, total_symbols: list) -> int:
        """필요한 세션 수 계산"""
        if not total_symbols:
            return 0
        
        max_sessions = self.config.dbfi.available_sessions
        batch_size = self.config.dbfi.batch_size
        
        import math
        required_sessions = math.ceil(len(total_symbols) / batch_size)
        actual_sessions = min(required_sessions, max_sessions)
        
        return actual_sessions

    def _get_symbols_for_session(self, broker_name: str, session_id: int, total_symbols: list) -> list:
        """세션별 할당 종목 리스트"""
        batch_size = self.config.dbfi.batch_size
        start_idx = session_id * batch_size
        end_idx = min(start_idx + batch_size, len(total_symbols))
        return total_symbols[start_idx:end_idx]

    async def _run_broker_loop(self, broker_name: str, broker: BrokerWebSocketClient):
        """브로커 메인 루프"""
        reconnect_count = 0
        subscribed_symbols = set()
        
        while self.running:
            try:
                # 서킷브레이커 상태 확인
                circuit_breaker = self.circuit_breakers.get(broker_name, {})

                if circuit_breaker.get('state') == 'DISABLED':
                    logger.info(f"🔒 {broker_name} 의도적 종료 - 브로커 루프 종료")
                    break
                
                if circuit_breaker.get('state') == 'OPEN':
                    logger.warning(f"🚨 {broker_name} 서킷브레이커 OPEN - REST API 폴백 실행")
                    # Slack 알림: 서킷브레이커 OPEN
                    try:
                        BrokerDaemonStatusNotification.send_circuit_breaker_open_notification(
                            broker_name=broker_name,
                            failure_count=circuit_breaker.get('failure_count', 0),
                            last_failure_time=circuit_breaker.get('last_failure_time')
                        )
                    except Exception as e:
                        logger.warning(f"Slack 서킷브레이커 OPEN 알림 전송 실패: {e}")
                    
                    # REST API를 백그라운드 태스크로 실행 (무한 루프 방지)
                    if broker_name not in self.rest_api_tasks or self.rest_api_tasks[broker_name].done():
                        rest_api_task = asyncio.create_task(self._execute_rest_api_fallback(broker_name))
                        self.rest_api_tasks[broker_name] = rest_api_task
                        logger.info(f"📊 {broker_name} REST API 백그라운드 태스크 시작")
                    
                    # 복구 타임아웃 대기
                    await asyncio.sleep(self.circuit_breaker_config['recovery_timeout'])
                    circuit_breaker['state'] = 'HALF_OPEN'
                    logger.info(f"🔄 {broker_name} 서킷브레이커 HALF_OPEN - 복구 시도")
                    
                    # Slack 알림: 서킷브레이커 HALF_OPEN
                    try:
                        BrokerDaemonStatusNotification.send_circuit_breaker_half_open_notification(
                            broker_name=broker_name
                        )
                    except Exception as e:
                        logger.warning(f"Slack 서킷브레이커 HALF_OPEN 알림 전송 실패: {e}")
                    
                    continue
                
                # HALF_OPEN 상태에서 연결 시도
                if circuit_breaker.get('state') == 'HALF_OPEN':
                    logger.info(f"🔌 {broker_name} HALF_OPEN 상태 - 웹소켓 재연결 시도")
                    
                    # Slack 알림: 웹소켓 재연결 시도
                    try:
                        BrokerDaemonStatusNotification.send_websocket_reconnection_attempt_notification(
                            broker_name=broker_name,
                            attempt_type="HALF_OPEN 복구"
                        )
                    except Exception as e:
                        logger.warning(f"Slack 웹소켓 재연결 시도 알림 전송 실패: {e}")
                    
                    await broker.connect()
                    
                    # Ping으로 연결 상태 확인
                    res = await broker._send_ping()
                    if res:
                        logger.info(f"✅ {broker_name} 재연결 및 Ping 성공 - 서킷브레이커 CLOSED")
                        circuit_breaker['state'] = 'CLOSED'
                        circuit_breaker['failure_count'] = 0
                        circuit_breaker['last_success_time'] = datetime.now()
                        
                        # Slack 알림: 서킷브레이커 CLOSED (복구 성공)
                        try:
                            BrokerDaemonStatusNotification.send_circuit_breaker_closed_notification(
                                broker_name=broker_name,
                                recovery_type="HALF_OPEN 복구"
                            )
                        except Exception as e:
                            logger.warning(f"Slack 서킷브레이커 CLOSED 알림 전송 실패: {e}")
                        
                        # REST API 태스크 취소
                        if hasattr(self, 'rest_api_tasks') and broker_name in self.rest_api_tasks:
                            rest_api_task = self.rest_api_tasks[broker_name]
                            if not rest_api_task.done():
                                rest_api_task.cancel()
                                logger.info(f"✅ {broker_name} REST API 태스크 취소됨")
                    else:
                        logger.warning(f"❌ {broker_name} 재연결 실패 - 서킷브레이커 다시 OPEN")
                        circuit_breaker['state'] = 'OPEN'
                        continue
                
                # CLOSED 상태에서 정상 연결 확인
                if circuit_breaker.get('state') == 'CLOSED':
                    # 브로커 연결 상태 확인
                    if not broker.is_connected():
                        logger.debug(f"🔌 {broker_name} 연결 시도...")
                        await broker.connect()
                        
                        # 연결 후 Ping으로 실제 연결 상태 확인
                        if not await broker._send_ping():
                            logger.error(f"❌ {broker_name} 연결 후 Ping 실패")
                            
                            # Slack 알림: 웹소켓 연결 실패
                            try:
                                BrokerDaemonStatusNotification.send_websocket_connection_failed_notification(
                                    broker_name=broker_name,
                                    failure_reason="Ping 실패"
                                )
                            except Exception as e:
                                logger.warning(f"Slack 웹소켓 연결 실패 알림 전송 실패: {e}")
                            
                            raise CircuitBreakerError(
                                message="연결 후 Ping 실패",
                                broker_name=broker_name,
                                failure_count=circuit_breaker.get('failure_count', 0) + 1
                            )
                        
                        logger.info(f"✅ {broker_name} 연결 및 Ping 성공")
                        
                        # Slack 알림: 웹소켓 연결 성공
                        try:
                            BrokerDaemonStatusNotification.send_websocket_connection_success_notification(
                                broker_name=broker_name
                            )
                        except Exception as e:
                            logger.warning(f"Slack 웹소켓 연결 성공 알림 전송 실패: {e}")
                
                # 재연결 성공 시 상태 초기화
                reconnect_count = 0
                self._initialize_resubscription_state(broker_name)
                
                # 구독 상태 초기화
                if broker_name in self.requested_symbols:
                    self.requested_symbols[broker_name].clear()
                if broker_name in self.confirmed_symbols:
                    self.confirmed_symbols[broker_name].clear()
                subscribed_symbols.clear()
                
                # 세션별 종목 할당
                session_id = 0
                if '_' in broker_name:
                    try:
                        session_id = int(broker_name.split('_')[-1])
                    except (IndexError, ValueError):
                        session_id = 0
                
                symbols = self._get_symbols_for_session(broker_name, session_id, self.watch_symbols)
                
                if not symbols:
                    logger.info(f"⏸️ {broker_name} 구독할 종목이 없음, 대기 모드")
                    await asyncio.sleep(30)
                    continue
                
                # 종목 구독
                logger.info(f"📡 {broker_name} 구독 시작: {len(symbols)}개 종목")
                
                if broker_name not in self.requested_symbols:
                    self.requested_symbols[broker_name] = set()
                    self.confirmed_symbols[broker_name] = set()
                
                for symbol in symbols:
                    if symbol not in subscribed_symbols and symbol not in self.requested_symbols[broker_name]:
                        self.requested_symbols[broker_name].add(symbol)
                        
                        success = await broker.subscribe_symbol(symbol)
                        if success:
                            subscribed_symbols.add(symbol)
                            logger.debug(f"✅ {broker_name}: {symbol} 구독 성공")
                        else:
                            logger.warning(f"❌ {broker_name}: {symbol} 구독 실패")
                        
                        await asyncio.sleep(0.1)  # 구독 간격
                
                # 데이터 수신 루프
                try:
                    async for data in broker.receive_data():
                        if not self.running:
                            break
                        await self._process_broker_data(broker_name, data)
                        
                except Exception as e:
                    logger.error(f"❌ {broker_name} 데이터 수신 오류: {e}")
                    raise CircuitBreakerError(
                        message=f"데이터 수신 실패: {e}",
                        broker_name=broker_name,
                        failure_count=circuit_breaker.get('failure_count', 0) + 1
                    )
            
            except CircuitBreakerError as e:
                # 서킷브레이커 실패 카운트 증가
                circuit_breaker['failure_count'] += 1
                circuit_breaker['last_failure_time'] = datetime.now()
                
                # 실패 임계값 도달 시 서킷브레이커 OPEN
                if circuit_breaker['failure_count'] >= self.circuit_breaker_config['failure_threshold']:
                    circuit_breaker['state'] = 'OPEN'
                    logger.warning(f"🚨 {broker_name} 서킷브레이커 OPEN - {circuit_breaker['failure_count']}회 연속 실패")
                    
                    # Slack 알림: 서킷브레이커 OPEN (임계값 도달)
                    try:
                        BrokerDaemonStatusNotification.send_circuit_breaker_open_notification(
                            broker_name=broker_name,
                            failure_count=circuit_breaker['failure_count'],
                            last_failure_time=circuit_breaker['last_failure_time']
                        )
                    except Exception as e:
                        logger.warning(f"Slack 서킷브레이커 OPEN 알림 전송 실패: {e}")
                    
                    # REST API 폴백 시작
                    # try:
                    #     await self._execute_rest_api_fallback(broker_name)
                    # except Exception as rest_error:
                    #     logger.error(f"❌ {broker_name} REST API 폴백 실패: {rest_error}")
            
            except BrokerConnectionError as e:
                reconnect_count += 1
                logger.error(f"🔌 {broker_name} 연결 오류 (재시도 {reconnect_count}회): {e}")
                
                if reconnect_count >= self.max_reconnect_attempts:
                    logger.error(f"🚨 {broker_name} 최대 재연결 시도 초과, 브로커 비활성화")
                    
                    # Slack 알림: 브로커 재연결 실패
                    try:
                        raise BrokerReconnectionError(
                            message=f"{broker_name} 최대 재연결 시도 초과",
                            broker_name=broker_name,
                            attempt_count=reconnect_count,
                            max_attempts=self.max_reconnect_attempts,
                            error_details=str(e)
                        )
                    except BrokerReconnectionError as e:
                        logger.error(f"❌ {e}")
                    
                    break
                
                # 재연결 전 종목 백업
                all_requested_symbols = set()
                if broker_name in self.requested_symbols:
                    all_requested_symbols = self.requested_symbols[broker_name].copy()
                
                # 재연결 대기
                base_broker_name = broker_name.split('_')[1] if '_' in broker_name else broker_name
                wait_time = min(
                    self.reconnect_intervals.get(base_broker_name, 30) * reconnect_count,
                    300
                )
                logger.info(f"⏳ {broker_name} {wait_time}초 후 재연결 시도")
                await asyncio.sleep(wait_time)
                
                # 구독 상태 초기화 및 재구독 대기 목록에 추가
                subscribed_symbols.clear()
                if broker_name in self.requested_symbols:
                    self.requested_symbols[broker_name].clear()
                if broker_name in self.confirmed_symbols:
                    self.confirmed_symbols[broker_name].clear()
                
                if all_requested_symbols:
                    self._add_to_pending_resubscriptions(broker_name, all_requested_symbols)
                    logger.info(f"🔄 {broker_name} 재연결로 인한 {len(all_requested_symbols)}개 종목 재구독 대기")
                
                # Slack 알림: 브로커 재연결 성공
                try:
                    BrokerDaemonStatusNotification.send_broker_reconnection_success(
                        broker_name=broker_name,
                        attempt_count=reconnect_count,
                        wait_time=wait_time
                    )
                except Exception as e:
                    logger.warning(f"Slack 재연결 성공 알림 전송 실패: {e}")

            except Exception as e:
                logger.error(f"💥 {broker_name} 예상치 못한 오류: {e}")
                self.stats['error_count'] += 1
                await asyncio.sleep(10)

    async def _execute_rest_api_fallback(self, broker_name: str):
        """REST API 폴백 실행"""
        try:
            logger.debug(f"📊 {broker_name} REST API 폴백 시작")
            
            # 브로커 타입에 따른 시장 구분
            market_type = MarketType.DOMESTIC if broker_name.startswith('d_') else MarketType.FOREIGN
            
            # 전체 요청 종목 가져오기
            total_symbols = []
            if broker_name in self.requested_symbols:
                total_symbols = list(self.requested_symbols[broker_name])
            elif broker_name in self.confirmed_symbols:
                total_symbols = list(self.confirmed_symbols[broker_name])
            
            if not total_symbols:
                logger.warning(f"⚠️ {broker_name} 수집할 종목이 없음")
                return
            
            logger.debug(f"📊 {broker_name} REST API로 {len(total_symbols)}개 종목 데이터 수집 시작")
            
            # REST API 인스턴스 생성 및 데이터 수집
            from app.brokers.dbfi.rest_api import DBFIRestAPI
            
            rest_api = DBFIRestAPI(market_type=market_type)
            price_data = await rest_api.get_all_symbols_prices(total_symbols, market_type)
            
            logger.debug(f"✅ {broker_name} REST API 데이터 수집 완료: {len(price_data)}개 종목")
            
        except Exception as e:
            logger.error(f"❌ {broker_name} REST API 폴백 실행 실패: {e}")

    async def _process_broker_data(self, broker_name: str, data: dict):
        """브로커 데이터 처리"""
        try:
            message_type = data.get('type', 'unknown')
            
            # 구독 응답 메시지 처리
            if message_type == 'subscribe_response':
                confirmed_symbols = set(data.get('tr_key', []))
                
                if broker_name not in self.confirmed_symbols:
                    self.confirmed_symbols[broker_name] = set()
                
                self.confirmed_symbols[broker_name].update(confirmed_symbols)
                self._remove_from_pending_resubscriptions(broker_name, confirmed_symbols)
                
                logger.debug(f"✅ {broker_name} 구독 응답: {len(confirmed_symbols)}개 종목 확인")
                
                # 누락된 종목 확인
                if broker_name in self.requested_symbols:
                    requested = self.requested_symbols[broker_name]
                    missing = requested - confirmed_symbols
                    
                    if missing:
                        logger.warning(f"⚠️ {broker_name} 누락된 종목: {list(missing)} - 재구독 대기 목록에 추가")
                        self._add_to_pending_resubscriptions(broker_name, missing)
                    else:
                        logger.info(f"🎉 {broker_name} 모든 요청 종목이 성공적으로 구독됨")
                return
            
            # 실시간 데이터 처리
            if message_type == 'realtime_data':
                if not self._validate_realtime_data(data):
                    logger.warning(f"⚠️ {broker_name} 유효하지 않은 실시간 데이터: {data}")
                    return
                
                # 데이터에 메타정보 추가
                processed_data = {
                    **data,
                    'broker': broker_name,
                    'timestamp': datetime.now().isoformat(),
                    'daemon_id': 'broker_daemon'
                }
                
                # Redis에 발행
                self.redis_service.publish_raw_data(broker_name, processed_data)
                
                # 통계 업데이트
                self.stats['total_messages'] += 1
                self.stats['last_update'] = datetime.now().isoformat()
                
                # logger.debug(f"📊 {broker_name} 실시간 데이터 처리: {data.get('symbol', 'unknown')}")
            else:
                logger.debug(f"❓ {broker_name} 알 수 없는 메시지 타입: {message_type}")
            
        except Exception as e:
            logger.error(f"❌ {broker_name} 데이터 처리 중 오류: {e}")
            self.stats['error_count'] += 1

    def _validate_realtime_data(self, data: dict) -> bool:
        """실시간 데이터 유효성 검증"""
        required_fields = ['symbol', 'current_price', 'volume']
        
        try:
            for field in required_fields:
                if field not in data:
                    return False
            
            if not isinstance(data['current_price'], (int, float)) or data['current_price'] <= 0:
                return False
                
            if not isinstance(data['volume'], (int, float)):
                return False
            
            return True
            
        except Exception:
            return False

    async def _monitor_brokers(self):
        """브로커 상태 모니터링"""
        while self.running:
            try:
                for broker_name, broker in self.brokers.items():
                    is_connected = broker.is_connected()
                    
                    # 상태 정보 수집
                    status_data = {
                        'broker': broker_name,
                        'connected': is_connected,
                        'timestamp': datetime.now().isoformat(),
                        'daemon_stats': self.stats,
                        'circuit_breaker': self.circuit_breakers.get(broker_name, {}),
                        'subscription_status': {
                            'requested_count': len(self.requested_symbols.get(broker_name, set())),
                            'confirmed_count': len(self.confirmed_symbols.get(broker_name, set())),
                            'pending_count': len(self.pending_resubscriptions.get(broker_name, set()))
                        }
                    }
                    
                    # ping 통계 추가
                    if hasattr(broker, 'get_ping_stats'):
                        status_data['ping_stats'] = broker.get_ping_stats()
                        ping_stats = status_data['ping_stats']
                        status_data['healthy'] = (
                            is_connected and 
                            ping_stats['ping_success_rate'] > 80
                        )
                    else:
                        status_data['healthy'] = is_connected
                    
                    # Redis에 상태 저장
                    self.redis_service.set_broker_status(broker_name, status_data)
                    
                    # 로깅
                    if status_data.get('healthy', False):
                        if hasattr(broker, 'ping_count') and broker.ping_count > 0:
                            success_rate = broker.pong_count / broker.ping_count * 100
                            logger.debug(f"✅ {broker_name} 상태: 정상 (ping 성공률: {success_rate:.1f}%)")
                    else:
                        logger.warning(f"⚠️ {broker_name} 상태: 비정상 (연결: {is_connected})")
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"❌ 브로커 모니터링 중 오류: {e}")
                await asyncio.sleep(10)

    # 재구독 관련 메서드들
    def _initialize_resubscription_state(self, broker_name: str):
        """재구독 상태 초기화"""
        if broker_name not in self.pending_resubscriptions:
            self.pending_resubscriptions[broker_name] = set()
        
        if broker_name in self.resubscription_tasks:
            old_task = self.resubscription_tasks[broker_name]
            if not old_task.done():
                old_task.cancel()
        
        logger.debug(f"🔄 {broker_name} 재구독 상태 초기화")

    def _add_to_pending_resubscriptions(self, broker_name: str, symbols: set):
        """재구독 대기 목록에 종목 추가"""
        if not self.running:
            return
        
        if broker_name not in self.pending_resubscriptions:
            self.pending_resubscriptions[broker_name] = set()
        
        new_symbols = symbols - self.pending_resubscriptions[broker_name]
        if new_symbols:
            self.pending_resubscriptions[broker_name].update(new_symbols)
            logger.info(f"🔄 {broker_name} 재구독 대기 목록에 추가: {list(new_symbols)}")
            self._start_resubscription_task(broker_name)

    def _remove_from_pending_resubscriptions(self, broker_name: str, symbols: set):
        """재구독 대기 목록에서 종목 제거"""
        if broker_name in self.pending_resubscriptions:
            removed = symbols & self.pending_resubscriptions[broker_name]
            if removed:
                self.pending_resubscriptions[broker_name] -= removed
                logger.debug(f"✅ {broker_name} 재구독 대기 목록에서 제거: {list(removed)}")

    def _start_resubscription_task(self, broker_name: str):
        """재구독 태스크 시작"""
        if not self.running:
            return
        
        if (broker_name in self.resubscription_tasks and 
            not self.resubscription_tasks[broker_name].done()):
            logger.debug(f"🔄 {broker_name} 재구독 태스크 이미 실행 중")
            return
        
        if (broker_name in self.pending_resubscriptions and 
            self.pending_resubscriptions[broker_name]):
            
            pending_symbols = self.pending_resubscriptions[broker_name].copy()
            task = asyncio.create_task(
                self._resubscribe_missing_symbols(broker_name, pending_symbols)
            )
            self.resubscription_tasks[broker_name] = task
            
            logger.info(f"🔄 {broker_name} 재구독 태스크 시작: {list(pending_symbols)}")

    async def _resubscribe_missing_symbols(self, broker_name: str, missing_symbols: set):
        """누락된 종목들 재구독"""
        try:
            if broker_name not in self.brokers:
                logger.error(f"❌ {broker_name} 브로커를 찾을 수 없음")
                return
            
            broker = self.brokers[broker_name]
            retry_count = 0
            max_retries = self.resubscription_config['max_retries']
            
            logger.info(f"🔄 {broker_name} 재구독 시작: {list(missing_symbols)}")

            while retry_count < max_retries and self.running:
                if broker_name not in self.pending_resubscriptions:
                    break
                
                current_pending = self.pending_resubscriptions[broker_name].copy()
                if not current_pending:
                    break

                retry_count += 1

                # 대기 시간 계산
                if retry_count == 1:
                    wait_time = 3
                else:
                    base = self.resubscription_config['base_interval']
                    max_interval = self.resubscription_config['max_interval']
                    if self.resubscription_config['exponential_backoff']:
                        wait_time = min(base * (2 ** (retry_count - 2)), max_interval)
                    else:
                        wait_time = base
                
                logger.info(f"🔄 {broker_name} 재구독 시도 #{retry_count} - {wait_time}초 후 시작")
                await asyncio.sleep(wait_time)
                
                # 브로커 연결 상태 확인
                if not broker.is_connected():
                    logger.warning(f"🔌 {broker_name} 브로커 연결 끊김 - 재구독 중단")
                    break
                
                successfully_subscribed = set()

                # 종목 재구독 시도
                for symbol in current_pending:
                    try:
                        success = await broker.subscribe_symbol(symbol)
                        if success:
                            successfully_subscribed.add(symbol)
                            logger.info(f"✅ {broker_name} {symbol} 재구독 성공 (시도 #{retry_count})")
                        else:
                            logger.debug(f"❌ {broker_name} {symbol} 재구독 실패 (시도 #{retry_count})")
                        
                        await asyncio.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"💥 {broker_name} {symbol} 재구독 중 오류: {e}")
                
                # 성공한 종목들 제거
                if successfully_subscribed:
                    self._remove_from_pending_resubscriptions(broker_name, successfully_subscribed)
                    logger.info(f"🎉 {broker_name} {len(successfully_subscribed)}개 종목 재구독 성공")
                    retry_count = max(0, retry_count - 2)
            
        except asyncio.CancelledError:
            logger.info(f"🔄 {broker_name} 재구독 태스크 취소됨")
        except Exception as e:
            logger.error(f"💥 {broker_name} 재구독 중 치명적 오류: {e}")
        finally:
            self._cleanup_resubscription_task(broker_name)
            
            remaining = self.pending_resubscriptions.get(broker_name, set())
            if remaining:
                if retry_count >= max_retries:
                    logger.error(f"🚨 {broker_name} 최대 재시도 도달 - 실패 종목: {list(remaining)}")
                    try:
                        raise ResubscriptionFailedError(
                            message=f"{broker_name} 최대 재시도 도달 - 실패 종목: {list(remaining)}",
                            broker_name=broker_name,
                            failed_symbols=list(remaining),
                            max_retries=max_retries,
                            error_details=f"최대 재시도 횟수({max_retries}회) 도달"
                        )
                    except ResubscriptionFailedError as e:
                        logger.error(f"❌ {e}")
                else:
                    logger.warning(f"⚠️ {broker_name} 재구독 중단됨 - 실패 종목: {list(remaining)}")

    def _cleanup_resubscription_task(self, broker_name: str):
        """재구독 태스크 정리"""
        if broker_name in self.resubscription_tasks:
            task = self.resubscription_tasks[broker_name]
            if task.done():
                del self.resubscription_tasks[broker_name]
                logger.debug(f"🔄 {broker_name} 재구독 태스크 정리됨")

    async def get_stats(self) -> dict:
        """데몬 통계 정보 반환"""
        broker_stats = {}
        
        for broker_name, broker in self.brokers.items():
            broker_stat = {
                'connected': broker.is_connected(),
                'error_count': getattr(broker, 'error_count', 0),
                'circuit_breaker': self.circuit_breakers.get(broker_name, {})
            }
            
            if hasattr(broker, 'get_ping_stats'):
                broker_stat['ping_stats'] = broker.get_ping_stats()
                ping_stats = broker_stat['ping_stats']
                broker_stat['healthy'] = (
                    broker_stat['connected'] and 
                    ping_stats['ping_success_rate'] > 80
                )
            else:
                broker_stat['healthy'] = broker_stat['connected']
            
            broker_stats[broker_name] = broker_stat
        
        return {
            'daemon_stats': self.stats,
            'broker_stats': broker_stats,
            'active_brokers': list(self.brokers.keys()),
            'system_info': {
                'auto_ping_enabled': True,
                'ping_interval': '5초',
                'health_threshold': '80% ping 성공률'
            }
        }