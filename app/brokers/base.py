import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, AsyncGenerator, List
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from app.utils.exceptions import BrokerConnectionError

class MarketType(Enum):
    """시장 타입 열거형"""
    DOMESTIC = "DOMESTIC"
    FOREIGN = "FOREIGN"


@dataclass
class BrokerConfig:
    """브로커 설정 데이터 클래스"""
    api_key: str
    api_secret: str
    websocket_url: str
    batch_size: int = 100
    available_sessions: int = 1
    market_type: MarketType = MarketType.DOMESTIC


class BrokerMessage(ABC):
    """브로커 메시지 추상 클래스"""
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """메시지를 딕셔너리로 변환"""
        pass


class BrokerMessageBuilder(ABC):
    """브로커 메시지 빌더 추상 클래스"""
    
    @abstractmethod
    def build_subscribe_message(self, symbol: str, market_type: MarketType, token: str) -> Dict[str, Any]:
        """구독 메시지 생성"""
        pass
    
    @abstractmethod
    def build_unsubscribe_message(self, symbol: str, market_type: MarketType, token: str) -> Dict[str, Any]:
        """구독 해제 메시지 생성"""
        pass


class BrokerMessageParser(ABC):
    """브로커 메시지 파서 추상 클래스"""
    
    @abstractmethod
    def parse_message(self, raw_message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """메시지 파싱"""
        pass


class BrokerOAuth(ABC):
    """브로커 OAuth 추상 클래스"""
    
    @abstractmethod
    def get_token(self) -> str:
        """액세스 토큰 발급"""
        pass
    
    @abstractmethod
    def is_token_valid(self) -> bool:
        """토큰 유효성 확인"""
        pass
    
    @abstractmethod
    def revoke_token(self) -> Dict[str, Any]:
        """토큰 해지"""
        pass


class BrokerWebSocketClient(ABC):
    """브로커 웹소켓 클라이언트 추상 클래스"""
    
    def __init__(self, config: BrokerConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.websocket = None
        self.access_token = None
        self.message_builder: Optional[BrokerMessageBuilder] = None
        self.message_parser: Optional[BrokerMessageParser] = None
        self.oauth: Optional[BrokerOAuth] = None
        
        # ping/pong 관련 설정
        self.ping_interval = 1.0  # 5초마다 ping
        self.ping_timeout = 10.0  # 10초 ping 타임아웃
        self._running = False
        self._ping_task = None
        self.last_ping_time = None
        self.last_pong_time = None
        self.ping_count = 0
        self.pong_count = 0
        
        # 🔥 연결 실패 감지를 위한 플래그 추가
        self._connection_failed = False
        self._connection_error = None
    
    @abstractmethod
    async def _connect_websocket(self):
        """웹소켓 연결 구현"""
        pass
    
    @abstractmethod
    async def disconnect(self):
        """웹소켓 연결 해제"""
        pass
    
    @abstractmethod
    async def _send_ping(self) -> bool:
        """ping 전송 (브로커별 구현)"""
        pass
    
    async def connect(self):
        """웹소켓 연결 (공통 구현)"""
        try:
            if self.is_connected():
                self.logger.debug("이미 연결되어 있습니다")
                return
            
            # 🔥 1단계: 기존 ping 루프 완전 정리
            await self.stop_ping_loop()
            
            # 🔥 2단계: 연결 상태 플래그 완전 초기화
            self._connection_failed = False
            self._connection_error = None
            self._running = False  # 기존 루프들 정지 신호
            
            # 🔥 3단계: 잠시 대기 (기존 태스크들이 정리될 시간)
            await asyncio.sleep(0.1)
            
            # 🔥 4단계: 액세스 토큰 발급
            self.access_token = self._get_access_token()
            
            # 🔥 5단계: 웹소켓 연결
            await self._connect_websocket()
            
            # 🔥 6단계: 새로운 ping 루프 시작
            self._running = True
            self._ping_task = asyncio.create_task(self._ping_loop())
            
            self.logger.info("웹소켓 연결 성공")
            
        except Exception as e:
            self.logger.error(f"웹소켓 연결 실패: {e}")
            raise
    
    @abstractmethod
    async def _subscribe_symbol_impl(self, symbol: str) -> bool:
        """종목 구독 구현"""
        pass
    
    @abstractmethod
    async def _unsubscribe_symbol_impl(self, symbol: str) -> bool:
        """종목 구독 해제 구현"""
        pass
    
    @abstractmethod
    async def _receive_message(self) -> Optional[Dict[str, Any]]:
        """메시지 수신"""
        pass
    
    @abstractmethod
    def _get_access_token(self) -> str:
        """액세스 토큰 발급"""
        pass
    
    @abstractmethod
    def _build_websocket_url(self) -> str:
        """웹소켓 URL 구성"""
        pass
    
    async def subscribe_symbol(self, symbol: str) -> bool:
        """종목 구독 (공통 구현)"""
        try:
            return await self._subscribe_symbol_impl(symbol)
        except Exception as e:
            self.logger.error(f"종목 구독 실패: {symbol}, {e}")
            return False
    
    async def unsubscribe_symbol(self, symbol: str) -> bool:
        """종목 구독 해제 (공통 구현)"""
        try:
            return await self._unsubscribe_symbol_impl(symbol)
        except Exception as e:
            self.logger.error(f"종목 구독 해제 실패: {symbol}, {e}")
            return False
    
    async def receive_data(self) -> AsyncGenerator[Dict[str, Any], None]:
        """데이터 수신 제너레이터 (공통 구현)"""
        try:
            while self.is_connected() and not self._connection_failed:
                raw_message = await self._receive_message()
                
                if raw_message is None:
                    continue
                
                if self.message_parser:
                    parsed_message = self.message_parser.parse_message(raw_message)
                    if parsed_message:
                        yield parsed_message
                else:
                    yield raw_message
            
            # 🔥 루프 종료 후 연결 실패 확인
            if self._connection_failed:
                error_msg = str(self._connection_error) if self._connection_error else "연결 실패 감지됨"
                raise BrokerConnectionError(f"ping 루프 연결 실패: {error_msg}")
                    
        except BrokerConnectionError:
            # 🔥 BrokerConnectionError는 그대로 전파
            raise
        except Exception as e:
            self.logger.error(f"데이터 수신 실패: {e}")
            # 🔥 ping 루프 연결 실패는 BrokerConnectionError로 변환
            if self._connection_failed:
                raise BrokerConnectionError(f"ping 루프 연결 실패: {self._connection_error}")
            # 🔥 기타 예외도 BrokerConnectionError로 변환 (연결 관련 문제로 간주)
            raise BrokerConnectionError(f"데이터 수신 실패: {e}")
    
    
    async def test_connection(self) -> bool:
        """웹소켓 연결 테스트 (공통 구현)"""
        try:
            self.logger.debug("웹소켓 연결 테스트 시작")
            
            await self._connect_websocket()
            
            # 테스트용 구독 메시지 전송
            if self.message_builder:
                test_symbol = "000660"  # 테스트용 종목
                subscribe_message = self.message_builder.build_subscribe_message(
                    test_symbol, MarketType.DOMESTIC, self.access_token
                )
                await self.websocket.send(json.dumps(subscribe_message))
                self.logger.debug("테스트 구독 메시지 전송 완료")
            
            # 응답 대기 (5초 타임아웃)
            try:
                response = await asyncio.wait_for(self.websocket.recv(), timeout=5.0)
                self.logger.debug(f"서버 응답: {response}")
            except asyncio.TimeoutError:
                self.logger.warning("서버 응답 타임아웃 (정상적인 경우일 수 있음)")
            
            await self.disconnect()
            self.logger.debug("웹소켓 연결 테스트 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"웹소켓 연결 테스트 실패: {e}")
            return False
    
    def is_connected(self) -> bool:
        """연결 상태 확인 (공통 구현)"""
        return self.websocket is not None and self.websocket.open
    
    def get_broker_name(self) -> str:
        """브로커 이름 반환 (하위 클래스에서 오버라이드)"""
        return self.__class__.__name__.lower()
    
    async def _ping_loop(self):
        """ping 루프 - 정기적으로 ping 전송"""
        try:
            while self._running and self.is_connected():
                try:
                    current_time = datetime.now()
                    self.last_ping_time = current_time
                    self.ping_count += 1
                    
                    # 브로커별 ping 전송
                    success = await self._send_ping()
                    
                    if success:
                        self.last_pong_time = datetime.now()
                        self.pong_count += 1
                        response_time = (self.last_pong_time - self.last_ping_time).total_seconds() * 1000
                        self.logger.debug(f"ping 성공 - 응답시간: {response_time:.1f}ms, ping/pong 카운트: {self.ping_count}/{self.pong_count}")
                    else:
                        self.logger.warning(f"ping 실패 - ping 카운트: {self.ping_count}, pong 카운트: {self.pong_count}")
                        raise BrokerConnectionError(f"ping 실패 - 응답 없음")
                    await asyncio.sleep(self.ping_interval)
                except BrokerConnectionError as e:
                    self.logger.error(f"ping 루프 중 연결 오류: {e}")
                    # 🔥 연결 실패 플래그 설정
                    self._connection_failed = True
                    self._connection_error = e
                    break  # ping 루프 종료
                except Exception as e:
                    self.logger.error(f"ping 루프 중 오류: {e}")
                    await asyncio.sleep(self.ping_interval)
                    
        except asyncio.CancelledError:
            self.logger.debug("ping 루프 취소됨")
        except Exception as e:
            self.logger.error(f"ping 루프 치명적 오류: {e}")
            # 🔥 예상치 못한 오류도 연결 실패로 처리
            self._connection_failed = True
            self._connection_error = e

    async def stop_ping_loop(self):
        """ping 루프 중지"""
        try:
            self._running = False
            
            if self._ping_task:
                self._ping_task.cancel()
                try:
                    await self._ping_task
                except asyncio.CancelledError:
                    pass
                self._ping_task = None
                
        except Exception as e:
            self.logger.error(f"ping 루프 중지 실패: {e}")
    
    def get_ping_stats(self) -> Dict[str, Any]:
        """ping/pong 통계 반환"""
        return {
            'ping_count': self.ping_count,
            'pong_count': self.pong_count,
            'ping_success_rate': (self.pong_count / self.ping_count * 100) if self.ping_count > 0 else 0,
            'last_ping_time': self.last_ping_time.isoformat() if self.last_ping_time else None,
            'last_pong_time': self.last_pong_time.isoformat() if self.last_pong_time else None,
            'ping_interval': self.ping_interval,
            'ping_timeout': self.ping_timeout
        }


class BrokerManager:
    """브로커 관리자 클래스"""
    
    def __init__(self):
        self.brokers: Dict[str, BrokerWebSocketClient] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def register_broker(self, broker: BrokerWebSocketClient):
        """브로커 등록"""
        broker_name = broker.get_broker_name()
        self.brokers[broker_name] = broker
        self.logger.info(f"브로커 등록: {broker_name}")
    
    def get_broker(self, broker_name: str) -> Optional[BrokerWebSocketClient]:
        """브로커 조회"""
        return self.brokers.get(broker_name)
    
    def get_all_brokers(self) -> Dict[str, BrokerWebSocketClient]:
        """모든 브로커 조회"""
        return self.brokers.copy()
    
    async def connect_all_brokers(self):
        """모든 브로커 연결"""
        for broker_name, broker in self.brokers.items():
            try:
                await broker._connect_websocket()
                self.logger.info(f"브로커 연결 성공: {broker_name}")
            except Exception as e:
                self.logger.error(f"브로커 연결 실패: {broker_name}, {e}")
    
    async def disconnect_all_brokers(self):
        """모든 브로커 연결 해제"""
        for broker_name, broker in self.brokers.items():
            try:
                await broker.disconnect()
                self.logger.info(f"브로커 연결 해제: {broker_name}")
            except Exception as e:
                self.logger.error(f"브로커 연결 해제 실패: {broker_name}, {e}")
    
    async def subscribe_symbol_all_brokers(self, symbol: str):
        """모든 브로커에서 종목 구독"""
        for broker_name, broker in self.brokers.items():
            try:
                success = await broker.subscribe_symbol(symbol)
                if success:
                    self.logger.info(f"종목 구독 성공: {broker_name} - {symbol}")
                else:
                    self.logger.warning(f"종목 구독 실패: {broker_name} - {symbol}")
            except Exception as e:
                self.logger.error(f"종목 구독 중 오류: {broker_name} - {symbol}, {e}")
    
    async def unsubscribe_symbol_all_brokers(self, symbol: str):
        """모든 브로커에서 종목 구독 해제"""
        for broker_name, broker in self.brokers.items():
            try:
                success = await broker.unsubscribe_symbol(symbol)
                if success:
                    self.logger.info(f"종목 구독 해제 성공: {broker_name} - {symbol}")
                else:
                    self.logger.warning(f"종목 구독 해제 실패: {broker_name} - {symbol}")
            except Exception as e:
                self.logger.error(f"종목 구독 해제 중 오류: {broker_name} - {symbol}, {e}")
