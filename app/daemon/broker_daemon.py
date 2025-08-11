import logging
import asyncio

from datetime import datetime
from typing import Dict

from app.services.redis_service import RedisService
from app.brokers.factory import broker_factory_manager
from app.brokers.base import BrokerWebSocketClient, MarketType
from app.utils.config import config
from ..brokers.base import BrokerConfig

logger = logging.getLogger(__name__)

class BrokerConnectionError(Exception):
    """브로커 연결 관련 예외"""
    pass

class BrokerDaemon:
    def __init__(self, market_type: MarketType):
        self.market_type = market_type
        self.config = config
        self.redis_service = RedisService()
        self.broker_factory_manager = broker_factory_manager

        self.brokers: Dict[str, BrokerWebSocketClient] = {}
        # self.dbfi_session_managers = {}  # market별 DBFI 세션 매니저
        
        # 시장별 설정 로드
        if self.market_type == MarketType.DOMESTIC:
            self.dbfi_config = config.dbfi.get_config_for_market(MarketType.DOMESTIC)
            # logger.debug("🔑 [국내] DBFI 설정 로드:")
        else:
            self.dbfi_config = config.dbfi.get_config_for_market(MarketType.FOREIGN)
            # logger.debug("🔑 [해외] DBFI 설정 로드:")
            
        # logger.debug(f"   API Key: {self.dbfi_config['api_key'][:10]}..." if self.dbfi_config['api_key'] else "   API Key: 설정되지 않음")
        # logger.debug(f"   API Secret: {'설정됨' if self.dbfi_config['api_secret'] else '설정되지 않음'}")
        # logger.debug(f"   WebSocket URL: {self.dbfi_config['websocket_url']}")
        
        self.running = False

        self.reconnect_intervals = {
            'dbfi': 30
        }
        
        self.max_reconnect_attempts = 10  # 최대 재연결 시도 횟수
        
        # 구독 관리용 변수들
        self.requested_symbols: Dict[str, set] = {}  # 브로커별 요청한 종목들
        self.confirmed_symbols: Dict[str, set] = {}  # 브로커별 확인된 종목들

        # 🔥 지속적 재구독을 위한 새로운 변수들
        self.pending_resubscriptions: Dict[str, set] = {}  # 브로커별 재구독 대기 종목
        self.resubscription_tasks: Dict[str, asyncio.Task] = {}  # 브로커별 재구독 태스크

        self.resubscription_config = {
            'max_retries': 10,           # 최대 10번 재시도
            'base_interval': 10,         # 기본 30초 간격
            'max_interval': 300,         # 최대 5분 간격
            'exponential_backoff': True  # 지수 백오프 사용
        }

        self.stats = {
            'total_messages': 0,
            'error_count': 0,
            'last_update': None
        }

    async def start(self, active_markets_info=None):
        """데몬 시작"""
        logger.debug(f"Broker Daemon 시작... ({self.market_type.value})")
        self.running = True

        try:
            # Redis 연결 확인
            if not self.redis_service.connect():
                raise Exception("Redis 연결 실패")

            # 브로커 초기화 (시장 정보 전달)
            await self._initialize_brokers()

            # 각 증권사별 데이터 수집 태스크 시작
            tasks = []
            for broker_name, broker in self.brokers.items():
                task = asyncio.create_task(
                    self._run_broker_loop(broker_name, broker)
                )
                tasks.append(task)
                logger.debug(f"{broker_name} 브로커 태스크 시작됨")

            # 브로커 상태 모니터링 태스크 시작
            monitor_task = asyncio.create_task(self._monitor_brokers())
            tasks.append(monitor_task)
            logger.debug("브로커 상태 모니터링 태스크 시작됨")

            # 모든 태스크 실행
            await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"Broker Daemon 시작 실패: {e}")
            await self.stop()
            raise

    async def stop(self):
        """데몬 정지"""
        logger.info("🛑 Broker Daemon 정지 시작...")
        
        try:
            # 🔥 1단계: 먼저 모든 브로커 연결 해제 (running=True 상태에서)
            logger.debug("1단계: 브로커 연결 해제 중...")
            for broker_name, broker in self.brokers.items():
                try:
                    if broker.is_connected():
                        await broker.disconnect()
                        logger.debug(f"✅ [{broker_name}] 브로커 연결 해제됨")
                except Exception as e:
                    logger.error(f"❌ [{broker_name}] 브로커 연결 해제 실패: {e}")
            
            # 🔥 2단계: 재구독 태스크들 취소
            logger.debug("2단계: 재구독 태스크 취소 중...")
            cancel_tasks = []
            for broker_name, task in self.resubscription_tasks.items():
                if not task.done():
                    logger.debug(f"🔄 [{broker_name}] 재구독 태스크 취소 중...")
                    task.cancel()
                    cancel_tasks.append(task)
            
            # 취소된 태스크들 완료 대기 (짧은 타임아웃)
            if cancel_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*cancel_tasks, return_exceptions=True),
                        timeout=5.0
                    )
                    logger.debug("✅ 모든 재구독 태스크 취소 완료")
                except asyncio.TimeoutError:
                    logger.warning("⚠️ 일부 재구독 태스크 취소 타임아웃")
            
            # 🔥 3단계: 이제 running을 False로 설정 (브로커 루프들 종료)
            logger.debug("3단계: 브로커 루프 종료 신호 전송...")
            self.running = False
            
            # 🔥 4단계: 상태 정리
            logger.debug("4단계: 상태 정리 중...")
            self.resubscription_tasks.clear()
            self.pending_resubscriptions.clear()
            self.requested_symbols.clear()
            self.confirmed_symbols.clear()
            
            # 🔥 5단계: Redis 연결 해제
            logger.debug("5단계: Redis 연결 해제 중...")
            if self.redis_service:
                self.redis_service.disconnect()
                logger.debug("✅ Redis 연결 해제됨")
            
            logger.info("✅ Broker Daemon 정상 종료 완료")
            
        except Exception as e:
            logger.error(f"💥 Broker Daemon 종료 중 오류: {e}")
            # 강제 종료
            self.running = False
            raise

    async def _initialize_brokers(self):
        """활성화된 증권사 브로커들 초기화"""
        try:
            # 🔥 시장 정보 없이 현재 market_type 사용
            enabled_brokers = self.config.broker.enabled_brokers
            
            # 시장 타입에 따른 종목 리스트 선택
            if self.market_type == MarketType.DOMESTIC:
                total_symbols = self.config.broker.watch_symbols_domestic
                market_name = "국내"
                prefix="d"
            else:
                total_symbols = self.config.broker.watch_symbols_foreign
                market_name = "해외"
                prefix="f"
            
            logger.info(f"🔧 {market_name} 브로커 초기화 중... (종목: {len(total_symbols)}개)")
            
            for broker_name in enabled_brokers:
                try:
                    actual_session_count = self._calculate_required_sessions(broker_name, total_symbols)
                    
                    logger.info(f"{broker_name}: 총 {len(total_symbols)}개 종목, {actual_session_count}개 세션 필요")
                    
                    # 필요한 세션만 생성
                    for session_id in range(actual_session_count):
                        broker_key = f"{prefix}_{broker_name}_{session_id}" if actual_session_count > 1 else f"{prefix}{broker_name}"
                        
                        # 시장별 설정으로 브로커 설정 생성
                        broker_config = BrokerConfig(
                            api_key=self.dbfi_config['api_key'],
                            api_secret=self.dbfi_config['api_secret'],
                            websocket_url=self.dbfi_config['websocket_url'],
                            batch_size=self.dbfi_config['batch_size'],
                            available_sessions=self.dbfi_config['available_sessions'],
                            market_type=self.market_type
                        )
                        
                        # 브로커 생성 시 config와 market_type 전달
                        broker = self.broker_factory_manager.factory.create_broker(
                            broker_name, 
                            config=broker_config,
                            market_type=self.market_type
                        )
                        
                        if broker:
                            self.brokers[broker_key] = broker
                            logger.info(f"{broker_key} 브로커 초기화 완료 (세션 {session_id + 1}/{actual_session_count}, {self.market_type.value})")
                        else:
                            logger.error(f"{broker_key} 브로커 생성 실패")
                    
                except Exception as e:
                    logger.error(f"{broker_name} 브로커 초기화 실패: {e}")
                    # 다른 브로커는 계속 진행
                    continue
            
            if not self.brokers:
                raise BrokerConnectionError(f"사용 가능한 {market_name} 브로커가 없습니다")
                
        except Exception as e:
            logger.error(f"브로커 초기화 실패: {e}")
            raise
    
    async def _initialize_default_brokers(self):
        """기본 설정으로 브로커 초기화 (기존 방식)"""
        try:
            # 설정에서 활성화된 브로커 목록 가져오기 (기본값: dbfi)
            enabled_brokers = self.config.broker.enabled_brokers
            
            for broker_name in enabled_brokers:
                try:
                    # 실제 필요한 세션 수 계산 (기본 DOMESTIC)
                    total_symbols = self.config.broker.watch_symbols_domestic
                    actual_session_count = self._calculate_required_sessions(broker_name, total_symbols)
                    
                    logger.info(f"{broker_name}: 총 {len(total_symbols)}개 종목, {actual_session_count}개 세션 필요")
                    
                    # 필요한 세션만 생성 (기본 DOMESTIC)
                    for session_id in range(actual_session_count):
                        broker_key = f"{broker_name}_{session_id}" if actual_session_count > 1 else broker_name
                        
                        broker = self.broker_factory_manager.factory.create_broker(broker_name, market_type=self.market_type)
                        if broker:
                            self.brokers[broker_key] = broker
                            logger.info(f"{broker_key} 브로커 초기화 완료 (세션 {session_id + 1}/{actual_session_count})")
                        else:
                            logger.error(f"{broker_key} 브로커 생성 실패")
                    
                except Exception as e:
                    logger.error(f"{broker_name} 브로커 초기화 실패: {e}")
                    # 다른 브로커는 계속 진행
                    continue
                    
        except Exception as e:
            logger.error(f"기본 브로커 초기화 실패: {e}")
            raise

    def _get_broker_session_count(self, broker_name: str) -> int:
        """브로커별 지원 세션 수 반환"""
        if broker_name == 'dbfi':
            return self.config.dbfi.available_sessions
        else:
            return 1

    def _calculate_required_sessions(self, broker_name: str, total_symbols: list) -> int:
        """종목 수에 따라 실제 필요한 세션 수 계산"""
        if not total_symbols:
            return 0  # 종목이 없으면 세션 불필요
        
        max_sessions = self._get_broker_session_count(broker_name)
        batch_size = self._get_broker_batch_size(broker_name)
        
        # 필요한 세션 수 = ceil(총 종목 수 / 배치 크기)
        import math
        required_sessions = math.ceil(len(total_symbols) / batch_size)
        
        # 최대 세션 수를 초과하지 않도록 제한
        actual_sessions = min(required_sessions, max_sessions)
        
        logger.debug(f"{broker_name}: 종목 {len(total_symbols)}개, 배치크기 {batch_size}, 필요세션 {required_sessions}, 실제세션 {actual_sessions}")
        
        return actual_sessions

    def _get_broker_batch_size(self, broker_name: str) -> int:
        if 'dbfi' in broker_name:  # ddbfi, fdbfi, d_dbfi_0 모두 포함
            return self.config.dbfi.batch_size
        else:
            return 20  # 기본값

    def _get_symbols_for_session(self, broker_name: str, session_id: int, total_symbols: list) -> list:
        """특정 브로커 세션이 구독해야 할 종목 리스트 반환"""
        batch_size = self._get_broker_batch_size(broker_name)
        
        # 세션별 종목 분할
        start_idx = session_id * batch_size
        end_idx = min(start_idx + batch_size, len(total_symbols))
        
        assigned_symbols = total_symbols[start_idx:end_idx]
        
        logger.debug(f"{broker_name} 세션 {session_id}: {start_idx}~{end_idx-1} 인덱스, {len(assigned_symbols)}개 종목 할당")
        
        return assigned_symbols

    async def _run_broker_loop(self, broker_name: str, broker: BrokerWebSocketClient):
        """개별 브로커의 데이터 수집 루프"""
        reconnect_count = 0
        subscribed_symbols = set()  # 이미 구독한 종목 추적

        while self.running:
            try:
                logger.debug(f"{broker_name} 브로커 연결 시도...")

                await broker.connect()
                
                self._initialize_resubscription_state(broker_name)

                reconnect_count = 0

                # 웹소켓 연결 성공 시 구독 상태 초기화 (새로운 연결에서는 이전 구독이 없음)
                if broker_name in self.requested_symbols:
                    self.requested_symbols[broker_name].clear()
                if broker_name in self.confirmed_symbols:
                    self.confirmed_symbols[broker_name].clear()
                subscribed_symbols.clear()
                logger.debug(f"{broker_name} 연결 성공 - 구독 상태 초기화")

                # 세션별 종목 분할 및 구독
                # 브로커의 시장 타입에 따라 종목 리스트 결정
                broker_market_type = getattr(broker, 'market_type', MarketType.DOMESTIC)
                if broker_market_type == MarketType.DOMESTIC:
                    all_symbols = self.config.broker.watch_symbols_domestic
                else:
                    all_symbols = self.config.broker.watch_symbols_foreign
                
                session_id = 0
                if '_' in broker_name:
                    try:
                        session_id = int(broker_name.split('_')[-1])  # 'd_dbfi_1' -> '1' -> 1
                    except (IndexError, ValueError):
                        session_id = 0
                                
                # 이 세션이 구독해야 할 종목들
                symbols = self._get_symbols_for_session(broker_name, session_id, all_symbols)
                
                # 구독할 종목이 없으면 대기 모드로 전환
                if not symbols:
                    logger.info(f"{broker_name} 구독할 종목이 없음, 대기 모드로 전환")
                    # 연결은 유지하되 구독하지 않고 대기
                    await asyncio.sleep(30)  # 30초마다 체크
                    continue
                
                # 브로커별 구독 관리 초기화
                if broker_name not in self.requested_symbols:
                    self.requested_symbols[broker_name] = set()
                    self.confirmed_symbols[broker_name] = set()
                
                logger.info(f"{broker_name} 구독 대상 종목: {symbols} ({len(symbols)}개)")
                
                for i, symbol in enumerate(symbols):
                    # 세션별 고유 종목이므로 이미 구독한 종목은 건너뛰기
                    if symbol not in subscribed_symbols and symbol not in self.requested_symbols[broker_name]:
                        success = await broker.subscribe_symbol(symbol)
                        self.requested_symbols[broker_name].add(symbol)
                        if success:
                            subscribed_symbols.add(symbol)
                            logger.debug(f"{broker_name}: {symbol} 구독 요청 (총 요청: {len(self.requested_symbols[broker_name])}개)")
                            
                            # 구독 요청 간격 (마지막 종목 제외)
                            if i < len(symbols) - 1:
                                logger.debug(f"{broker_name}: 다음 구독 전 1초 대기...")
                                await asyncio.sleep(1)
                        else:
                            logger.warning(f"{broker_name}: {symbol} 구독 실패")
                    else:
                        logger.debug(f"{broker_name}: {symbol} 이미 요청된 종목, 건너뛰기")

                # 데이터 수신 루프
                try:
                    async for data in broker.receive_data():
                        if not self.running:
                            break
                        
                        await self._process_broker_data(broker_name, data)
                            
                except Exception as e:
                    logger.error(f"{broker_name} 데이터 수신 중 오류: {e}")
                    # receive_data에서 예외가 발생하면 연결을 재시도
                    raise BrokerConnectionError(f"데이터 수신 실패: {e}")

            except BrokerConnectionError as e:
                reconnect_count += 1
                logger.error(f"{broker_name} 연결 오류 (재시도 {reconnect_count}회): {e}")
                
                # 최대 재연결 시도 횟수 확인
                if reconnect_count >= self.max_reconnect_attempts:
                    logger.error(f"{broker_name} 최대 재연결 시도 횟수 초과, 브로커 비활성화")
                    break
                
                # 🔥 재연결 전에 먼저 기존 요청 종목들을 백업
                all_requested_symbols = set()
                if broker_name in self.requested_symbols:
                    all_requested_symbols = self.requested_symbols[broker_name].copy()
                
                # 재연결 대기 (지수 백오프)
                base_broker_name = broker_name.split('_')[1] if '_' in broker_name else broker_name  # d_dbfi_0 -> dbfi
                wait_time = min(
                    self.reconnect_intervals.get(base_broker_name, 30) * reconnect_count,
                    300  # 최대 5분
                )
                logger.info(f"🔌 [{broker_name}] {wait_time}초 후 재연결 시도")
                await asyncio.sleep(wait_time)
                
                # 🔥 재연결 시 구독 상태 초기화 (웹소켓 연결 끊어지면 서버에서도 구독 해제됨)
                subscribed_symbols.clear()
                if broker_name in self.requested_symbols:
                    self.requested_symbols[broker_name].clear()
                if broker_name in self.confirmed_symbols:
                    self.confirmed_symbols[broker_name].clear()
                
                # 🔥 백업한 종목들을 재구독 대기 목록에 추가
                if all_requested_symbols:
                    self._add_to_pending_resubscriptions(broker_name, all_requested_symbols)
                    logger.info(f"🔄 [{broker_name}] 재연결로 인한 {len(all_requested_symbols)}개 종목 재구독 대기")
                
                logger.debug(f"🔄 [{broker_name}] 재연결로 인한 모든 구독 상태 초기화")
                
            except Exception as e:
                logger.error(f"{broker_name} 예상치 못한 오류: {e}")
                self.stats['error_count'] += 1
                await asyncio.sleep(10)  # 10초 대기 후 재시도

    async def _process_broker_data(self, broker_name: str, data: dict):
        """브로커에서 받은 데이터 처리"""
        try:
            # 메시지 타입 확인
            message_type = data.get('type', 'unknown')
            
            # 구독 응답 메시지 처리
            if message_type == 'subscribe_response':
                confirmed_symbols = set(data.get('tr_key', []))
                confirmed_clean = confirmed_symbols.copy()

                if broker_name not in self.confirmed_symbols:
                    self.confirmed_symbols[broker_name] = set()

                self.confirmed_symbols[broker_name].update(confirmed_clean)
                
                logger.debug(f"{broker_name} 구독 응답: {data.get('rsp_msg', '')} - 확인된 종목: {list(confirmed_clean)} ({len(confirmed_clean)}개)")
                
                # 확인된 종목 제거
                self._remove_from_pending_resubscriptions(broker_name, confirmed_clean)

                # 요청한 종목과 확인된 종목 비교
                if broker_name in self.requested_symbols:
                    requested = self.requested_symbols[broker_name]
                    missing = requested - confirmed_clean
                    
                    if missing:
                        logger.warning(f"{broker_name} 누락된 종목: {list(missing)} - 즉시 재구독 시도")
                        # ================================slack 알림추가 필요===============================
                        # 누락된 종목 재구독 - 
                        # asyncio.create_task(self._resubscribe_missing_symbols(broker_name, missing))
                        self._add_to_pending_resubscriptions(broker_name, missing)
                    else:
                        logger.info(f"{broker_name} 모든 요청 종목이 성공적으로 구독됨")
                return  # 구독 응답은 별도 처리하지 않음
            
            # 실시간 데이터만 검증 및 처리
            if message_type == 'realtime_data':
                # 실시간 데이터 검증
                if not self._validate_realtime_data(data):
                    logger.warning(f"{broker_name}에서 받은 실시간 데이터가 유효하지 않음: {data}")
                    return
                # logger.debug(f"{broker_name} 실시간 데이터 처리 시작: {data}")
                
                # 데이터에 메타정보 추가
                processed_data = {
                    **data,
                    'broker': broker_name,
                    'timestamp': datetime.now().isoformat(),
                    'daemon_id': 'broker_daemon'
                }
                
                self.redis_service.publish_raw_data(broker_name, processed_data)
                
                # 통계 업데이트
                self.stats['total_messages'] += 1
                self.stats['last_update'] = datetime.now().isoformat()
                
                # logger.debug(f"{broker_name} 실시간 데이터 처리 완료: {data.get('symbol', 'unknown')}")
            else:
                # 알 수 없는 메시지 타입은 로그만 남기고 무시
                logger.debug(f"{broker_name} 알 수 없는 메시지 타입: {message_type}")
            
        except Exception as e:
            logger.error(f"데이터 처리 중 오류: {e}")
            self.stats['error_count'] += 1
    
    def _validate_realtime_data(self, data: dict) -> bool:
        """실시간 데이터 유효성 검증"""
        required_fields = ['symbol', 'current_price', 'volume']
        
        try:
            # 필수 필드 확인
            for field in required_fields:
                if field not in data:
                    return False
            
            # 데이터 타입 확인
            if not isinstance(data['current_price'], (int, float)):
                return False
                
            if not isinstance(data['volume'], (int, float)):
                return False
            
            # 가격이 양수인지 확인
            if data['current_price'] <= 0:
                return False
            
            return True
            
        except Exception:
            return False
    
    async def _monitor_brokers(self):
        """브로커 상태 모니터링 및 Redis 저장"""
        while self.running:
            try:
                # 브로커별 연결 상태 및 ping 통계 수집
                for broker_name, broker in self.brokers.items():
                    is_connected = broker.is_connected()
                    
                    # 기본 상태 정보
                    status_data = {
                        'broker': broker_name,
                        'connected': is_connected,
                        'timestamp': datetime.now().isoformat(),
                        'daemon_stats': self.stats
                    }

                    pending_symbols = self.pending_resubscriptions.get(broker_name, set())
                    confirmed_symbols = self.confirmed_symbols.get(broker_name, set())
                    resubscription_task_running = (
                        broker_name in self.resubscription_tasks and 
                        not self.resubscription_tasks[broker_name].done()
                    )
                    
                    status_data['resubscription_status'] = {
                        'pending_count': len(pending_symbols),
                        'pending_symbols': list(pending_symbols),
                        'confirmed_count': len(confirmed_symbols),
                        'task_running': resubscription_task_running,
                        'subscription_rate': (
                            f"{len(confirmed_symbols)}/{len(confirmed_symbols) + len(pending_symbols)}"
                            if (len(confirmed_symbols) + len(pending_symbols)) > 0 else "0/0"
                        )
                    }
                    
                    # ping 통계 추가 (base 클래스에서 자동 관리)
                    if hasattr(broker, 'get_ping_stats'):
                        status_data['ping_stats'] = broker.get_ping_stats()
                        
                        # ping 기반 헬스 판정
                        ping_stats = status_data['ping_stats']
                        status_data['healthy'] = (
                            is_connected and 
                            ping_stats['ping_success_rate'] > 80  # 80% 이상 성공률
                        )
                    else:
                        # ping 통계가 없으면 단순 연결 상태만
                        status_data['healthy'] = is_connected
                    
                    self.redis_service.set_broker_status(broker_name, status_data)
                    
                    # 로깅 (정상일 때는 간결하게)
                    if status_data.get('healthy', False):
                        if hasattr(broker, 'ping_count') and broker.ping_count > 0:
                            success_rate = broker.pong_count / broker.ping_count * 100
                            logger.debug(f"{broker_name} 상태: 정상 (ping 성공률: {success_rate:.1f}%)")
                    else:
                        logger.warning(f"{broker_name} 상태: 비정상 (연결: {is_connected})")
                
                # 30초마다 상태 수집 및 저장
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"브로커 모니터링 중 오류: {e}")
                await asyncio.sleep(10)
    
    async def get_stats(self) -> dict:
        """데몬 통계 정보 반환"""
        broker_stats = {}
        
        for broker_name, broker in self.brokers.items():
            broker_stat = {
                'connected': broker.is_connected(),
                'error_count': getattr(broker, 'error_count', 0)
            }
            
            # ping 통계가 있다면 추가
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
    
    async def _resubscribe_missing_symbols(self, broker_name: str, missing_symbols: set):
        """누락된 종목들을 재구독"""
        try:
            if broker_name not in self.brokers:
                logger.error(f"{broker_name} 브로커를 찾을 수 없음")
                return
            
            broker = self.brokers[broker_name]
            retry_count = 0
            max_retries = self.resubscription_config['max_retries']
            logger.info(f"{broker_name} 재구독 시작: {list(missing_symbols)}")

            try:
                while retry_count < max_retries and self.running:
                    if broker_name not in self.pending_resubscriptions:
                        logger.debug(f"{broker_name} 재구독 대기 목록에 없음, 종료")
                        break
                    current_pending = self.pending_resubscriptions[broker_name].copy()
                    if not current_pending:
                        logger.debug(f"{broker_name} 모든 종목 구독 완료, 종료")
                        break

                    retry_count += 1

                    if retry_count == 1:
                        wait_time = 3  # 첫 시도는 3초 후
                    else:
                        base = self.resubscription_config['base_interval']
                        max_interval = self.resubscription_config['max_interval']
                        if self.resubscription_config['exponential_backoff']:
                            wait_time = min(base * (2 ** (retry_count - 2)), max_interval)
                        else:
                            wait_time = base
                    
                    logger.info(f"🔄 [{broker_name}] 재구독 시도 #{retry_count} - {wait_time}초 후 시작")
                    await asyncio.sleep(wait_time)
                    
                    # 브로커 연결 상태 확인
                    if not broker.is_connected():
                        logger.warning(f"🔌 [{broker_name}] 브로커 연결 끊김 - 재구독 중단")
                        break
                    
                    successfully_subscribed = set()

                     # 현재 대기 중인 종목들 재구독 시도
                    for symbol in current_pending:
                        try:
                            success = await broker.subscribe_symbol(symbol)
                            if success:
                                successfully_subscribed.add(symbol)
                                logger.info(f"✅ [{broker_name}] {symbol} 재구독 성공 (시도 #{retry_count})")
                            else:
                                logger.debug(f"❌ [{broker_name}] {symbol} 재구독 실패 (시도 #{retry_count})")
                            
                            await asyncio.sleep(1)  # 구독 간격
                            
                        except Exception as e:
                            logger.error(f"💥 [{broker_name}] {symbol} 재구독 중 오류: {e}")
                    
                    # 🔥 성공한 종목들은 대기 목록에서 제거
                    if successfully_subscribed:
                        self._remove_from_pending_resubscriptions(broker_name, successfully_subscribed)
                        logger.info(f"🎉 [{broker_name}] {len(successfully_subscribed)}개 종목 재구독 성공")
                        
                        # 부분 성공 시 재시도 카운트 완화
                        retry_count = max(0, retry_count - 2)
            
            except asyncio.CancelledError:
                logger.info(f"🔄 [{broker_name}] 재구독 태스크 취소됨")
            except Exception as e:
                logger.error(f"💥 [{broker_name}] 재구독 중 치명적 오류: {e}")
            finally:
                # 🔥 태스크 완료 시 정리
                self._cleanup_resubscription_task(broker_name)
                
                # 남은 종목이 있으면 로깅
                remaining = self.pending_resubscriptions.get(broker_name, set())
                if remaining:
                    if retry_count >= max_retries:
                        logger.error(f"🚨 [{broker_name}] 최대 재시도 도달 - 실패 종목: {list(remaining)}")
                    else:
                        logger.warning(f"⚠️ [{broker_name}] 재구독 중단됨 - 실패 종목: {list(remaining)}")


                
        #         # 재구독 대기 목록에서 종목 제거
        #         self._remove_from_pending_resubscriptions(broker_name, current_pending)
                
            
        #     # 재구독 전 잠시 대기 (서버 안정화)
        #     await asyncio.sleep(3)
            
        #     for symbol in missing_symbols:
        #         try:
        #             success = await broker.subscribe_symbol(symbol)
        #             if success:
        #                 # logger.info(f"{broker_name}: {symbol} 재구독 성공")
        #                 pass
        #             else:
        #                 logger.warning(f"{broker_name}: {symbol} 재구독 실패")
                    
        #             # 재구독 간격
        #             await asyncio.sleep(1)
                    
        #         except Exception as e:
        #             logger.error(f"{broker_name}: {symbol} 재구독 중 오류: {e}")
                    
        except Exception as e:
            logger.error(f"{broker_name} 재구독 처리 중 오류: {e}")

    def _initialize_resubscription_state(self, broker_name: str):
        """브로커별 재구독 상태 초기화"""
        if broker_name not in self.pending_resubscriptions:
            self.pending_resubscriptions[broker_name] = set()
        
        # 기존 재구독 태스크가 있으면 취소
        if broker_name in self.resubscription_tasks:
            old_task = self.resubscription_tasks[broker_name]
            if not old_task.done():
                old_task.cancel()
        
        logger.debug(f"🔄 [{broker_name}] 재구독 상태 초기화")    

    def _add_to_pending_resubscriptions(self, broker_name: str, symbols: set):
        """재구독 대기 목록에 종목 추가"""
        if not self.running:
            logger.debug(f"🔄 [{broker_name}] 데몬 중지 중 - 재구독 대기 목록에 추가 건너뜀")
            return
        
        if broker_name not in self.pending_resubscriptions:
            self.pending_resubscriptions[broker_name] = set()
        
        # 새로운 종목들 추가
        new_symbols = symbols - self.pending_resubscriptions[broker_name]
        if new_symbols:
            self.pending_resubscriptions[broker_name].update(new_symbols)
            logger.info(f"🔄 [{broker_name}] 재구독 대기 목록에 추가: {list(new_symbols)}")
            
            # 재구독 태스크 시작
            self._start_resubscription_task(broker_name)

    def _remove_from_pending_resubscriptions(self, broker_name: str, symbols: set):
        """재구독 대기 목록에서 종목 제거 (성공한 종목들)"""
        if broker_name in self.pending_resubscriptions:
            removed = symbols & self.pending_resubscriptions[broker_name]
            if removed:
                self.pending_resubscriptions[broker_name] -= removed
                logger.debug(f"✅ [{broker_name}] 재구독 대기 목록에서 제거: {list(removed)}")
    
    def _start_resubscription_task(self, broker_name: str):
        """재구독 태스크 시작 (이미 실행 중이면 스킵)"""
        if not self.running:
            logger.debug(f"🔄 [{broker_name}] 데몬 중지 중 - 재구독 대기 목록에 추가 건너뜀")
            return
        # 이미 실행 중인 태스크가 있으면 스킵
        if (broker_name in self.resubscription_tasks and 
            not self.resubscription_tasks[broker_name].done()):
            logger.debug(f"🔄 [{broker_name}] 재구독 태스크 이미 실행 중")
            return
        
        # 재구독할 종목이 있을 때만 태스크 시작
        if (broker_name in self.pending_resubscriptions and 
            self.pending_resubscriptions[broker_name]):
            
            pending_symbols = self.pending_resubscriptions[broker_name].copy()
            task = asyncio.create_task(
                self._resubscribe_missing_symbols(broker_name, pending_symbols)
            )
            self.resubscription_tasks[broker_name] = task
            
            logger.info(f"🔄 [{broker_name}] 재구독 태스크 시작: {list(pending_symbols)}")

    def _cleanup_resubscription_task(self, broker_name: str):
        """완료된 재구독 태스크 정리"""
        if broker_name in self.resubscription_tasks:
            task = self.resubscription_tasks[broker_name]
            if task.done():
                del self.resubscription_tasks[broker_name]
                logger.debug(f"🔄 [{broker_name}] 재구독 태스크 정리됨")
    
    # async def run_dbfi_subscribe(self, broker_market_key: str, symbols: list):
    #     """DBFI 세션 매니저를 통한 종목 구독"""
    #     if broker_market_key not in self.dbfi_session_managers:
    #         logger.error(f"{broker_market_key} DBFI 세션 매니저가 없습니다.")
    #         return
    #     session_manager = self.dbfi_session_managers[broker_market_key]
    #     await session_manager.subscribe_symbols(symbols)