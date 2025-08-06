import logging
import asyncio

from datetime import datetime
from typing import Dict

from app.services.redis_service import RedisService
from app.brokers.factory import broker_factory_manager
from app.brokers.base import BrokerWebSocketClient, MarketType
from app.utils.config import config

# 브로커 자동 등록을 위한 import
import app.brokers.dbfi


logger = logging.getLogger(__name__)

class BrokerConnectionError(Exception):
    """브로커 연결 관련 예외"""
    pass

class BrokerDaemon:
    def __init__(self):
        self.config = config
        self.redis_service = RedisService()
        self.broker_factory_manager = broker_factory_manager

        self.brokers: Dict[str, BrokerWebSocketClient] = {}
        
        self.running = False

        self.reconnect_intervals = {
            'dbfi': 5
        }
        
        self.max_reconnect_attempts = 10  # 최대 재연결 시도 횟수
        
        # 구독 관리용 변수들
        self.requested_symbols: Dict[str, set] = {}  # 브로커별 요청한 종목들
        self.confirmed_symbols: Dict[str, set] = {}  # 브로커별 확인된 종목들

        self.stats = {
            'total_messages': 0,
            'error_count': 0,
            'last_update': None
        }

    async def start(self, active_markets_info=None):
        """데몬 시작"""
        logger.debug("Broker Daemon 시작...")
        
        # 활성화된 시장 정보 저장
        self.active_markets_info = active_markets_info or {}
        
        if self.active_markets_info:
            logger.info("활성화된 시장 정보:")
            for market, info in self.active_markets_info.items():
                if info.get('is_active'):
                    market_type = info.get('market_type', 'DOMESTIC')
                    logger.info(f"  {market}: {market_type}")

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
        logger.debug("Broker Daemon 정지 중...")
        
        self.running = False
        
        try:
            # 모든 브로커 연결 해제
            for broker_name, broker in self.brokers.items():
                try:
                    await broker.disconnect()
                    logger.debug(f"{broker_name} 브로커 연결 해제됨")
                except Exception as e:
                    logger.error(f"{broker_name} 브로커 연결 해제 실패: {e}")
            
            # Redis 연결 해제
            self.redis_service.disconnect()
            
            logger.debug("Broker Daemon 정상 종료됨")
            
        except Exception as e:
            logger.error(f"Broker Daemon 종료 중 오류: {e}")

    async def _initialize_brokers(self):
        """활성화된 증권사 브로커들 초기화"""
        try:
            # 활성화된 시장이 있으면 시장별로 브로커 생성
            if hasattr(self, 'active_markets_info') and self.active_markets_info:
                active_markets = [market for market, info in self.active_markets_info.items() if info.get('is_active')]
                
                if not active_markets:
                    logger.warning("활성화된 시장이 없습니다. 기본 설정으로 진행합니다.")
                    return await self._initialize_default_brokers()
                
                # 활성화된 시장별로 브로커 생성
                enabled_brokers = self.config.broker.enabled_brokers
                
                for market in active_markets:
                    market_info = self.active_markets_info[market]
                    market_type_str = market_info.get('market_type', 'DOMESTIC')
                    market_type = MarketType.DOMESTIC if market_type_str == 'DOMESTIC' else MarketType.FOREIGN
                    
                    logger.info(f"{market} 시장 ({market_type.value}) 브로커 초기화 중...")
                    
                    for broker_name in enabled_brokers:
                        try:
                            # 실제 필요한 세션 수 계산
                            if market_type == MarketType.DOMESTIC:
                                total_symbols = self.config.broker.watch_symbols_domestic
                            else:
                                total_symbols = self.config.broker.watch_symbols_foreign
                            
                            actual_session_count = self._calculate_required_sessions(broker_name, total_symbols)
                            
                            logger.info(f"{broker_name}-{market}: 총 {len(total_symbols)}개 종목, {actual_session_count}개 세션 필요")
                            
                            # 필요한 세션만 생성
                            for session_id in range(actual_session_count):
                                broker_key = f"{broker_name}_{market}_{session_id}" if actual_session_count > 1 else f"{broker_name}_{market}"
                                
                                # MarketType을 전달하여 브로커 생성
                                broker = self.broker_factory_manager.factory.create_broker(broker_name, market_type=market_type)
                                if broker:
                                    self.brokers[broker_key] = broker
                                    logger.info(f"{broker_key} 브로커 초기화 완료 (세션 {session_id + 1}/{actual_session_count}, {market_type.value})")
                                else:
                                    logger.error(f"{broker_key} 브로커 생성 실패")
                            
                        except Exception as e:
                            logger.error(f"{broker_name}-{market} 브로커 초기화 실패: {e}")
                            # 다른 브로커는 계속 진행
                            continue
            else:
                # 시장 정보가 없으면 기본 설정으로 초기화
                return await self._initialize_default_brokers()
            
            if not self.brokers:
                raise BrokerConnectionError("사용 가능한 브로커가 없습니다")
                
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
                        
                        broker = self.broker_factory_manager.factory.create_broker(broker_name, market_type=MarketType.DOMESTIC)
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
        """브로커별 배치 크기 반환"""
        base_name = broker_name.split('_')[0]  # dbfi_0 -> dbfi
        if base_name == 'dbfi':
            return self.config.dbfi.batch_size
        else:
            return 20  # 기본값

    def _get_symbols_for_session(self, broker_name: str, session_id: int, total_symbols: list) -> list:
        """특정 브로커 세션이 구독해야 할 종목 리스트 반환"""
        base_name = broker_name.split('_')[0]
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
                reconnect_count = 0

                # 세션별 종목 분할 및 구독
                # 브로커의 시장 타입에 따라 종목 리스트 결정
                broker_market_type = getattr(broker, 'market_type', MarketType.DOMESTIC)
                if broker_market_type == MarketType.DOMESTIC:
                    all_symbols = self.config.broker.watch_symbols_domestic
                else:
                    all_symbols = self.config.broker.watch_symbols_foreign
                
                # 세션 ID 추출 (dbfi_0 -> 0, dbfi -> 0)
                session_id = 0
                if '_' in broker_name:
                    session_id = int(broker_name.split('_')[1])
                
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
                        if success:
                            subscribed_symbols.add(symbol)
                            self.requested_symbols[broker_name].add(symbol)
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
                logger.error(
                    f"{broker_name} 연결 오류 (재시도 {reconnect_count}회): {e}"
                )
                
                # 최대 재연결 시도 횟수 확인
                if reconnect_count >= self.max_reconnect_attempts:
                    logger.error(f"{broker_name} 최대 재연결 시도 횟수 초과, 브로커 비활성화")
                    break
                
                # 재연결 대기 (브로커 타입 추출)
                base_broker_name = broker_name.split('_')[0]  # dbfi_0 -> dbfi
                wait_time = min(
                    self.reconnect_intervals.get(base_broker_name, 30) * reconnect_count,
                    300  # 최대 5분
                )
                logger.debug(f"{broker_name} {wait_time}초 후 재연결 시도")
                await asyncio.sleep(wait_time)
                
                # 재연결 시 로컬 구독 상태만 초기화 (requested_symbols는 유지)
                subscribed_symbols.clear()
                logger.debug(f"{broker_name} 재연결로 인한 로컬 구독 상태 초기화")
                
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
                # J 프리픽스 제거하여 비교
                confirmed_clean = {s.replace('J ', '') for s in confirmed_symbols}
                
                logger.debug(f"{broker_name} 구독 응답: {data.get('rsp_msg', '')} - 확인된 종목: {list(confirmed_clean)} ({len(confirmed_clean)}개)")
                
                # 요청한 종목과 확인된 종목 비교
                if broker_name in self.requested_symbols:
                    requested = self.requested_symbols[broker_name]
                    missing = requested - confirmed_clean
                    
                    if missing:
                        logger.warning(f"{broker_name} 누락된 종목: {list(missing)} - 즉시 재구독 시도")
                        # 누락된 종목 재구독 (비동기로 실행)
                        asyncio.create_task(self._resubscribe_missing_symbols(broker_name, missing))
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
            logger.info(f"{broker_name} 재구독 시작: {list(missing_symbols)}")
            
            # 재구독 전 잠시 대기 (서버 안정화)
            await asyncio.sleep(3)
            
            for symbol in missing_symbols:
                try:
                    success = await broker.subscribe_symbol(symbol)
                    if success:
                        # logger.info(f"{broker_name}: {symbol} 재구독 성공")
                        pass
                    else:
                        logger.warning(f"{broker_name}: {symbol} 재구독 실패")
                    
                    # 재구독 간격
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"{broker_name}: {symbol} 재구독 중 오류: {e}")
                    
        except Exception as e:
            logger.error(f"{broker_name} 재구독 처리 중 오류: {e}")