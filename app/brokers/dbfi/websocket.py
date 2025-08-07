import asyncio
import websockets
import json
import logging
from datetime import datetime

from typing import Dict, Optional, AsyncGenerator, Any

from app.brokers.base import BrokerWebSocketClient, BrokerConfig, MarketType
from app.brokers.dbfi.oauth import DBFIOAuth
from app.brokers.dbfi.schemas import DBFIMessageBuilder, DBFIMessageParser, DBFIMarketType
from app.utils.config import config


logger = logging.getLogger(__name__)

class DBFIWebSocketClient(BrokerWebSocketClient):
    """DBFI 웹소켓 클라이언트"""
    
    def __init__(self, broker_config: BrokerConfig = None, market_type: MarketType = MarketType.DOMESTIC):
        # broker_config이 None이면 DBFI 전용 설정 사용
        if broker_config is None:
            broker_config = BrokerConfig(
                api_key=config.dbfi.api_key,
                api_secret=config.dbfi.api_secret,
                websocket_url=config.dbfi.websocket_url,
                batch_size=config.dbfi.batch_size,
                available_sessions=config.dbfi.available_sessions,
                market_type=market_type
            )
        
        super().__init__(broker_config)
        
        # MarketType 저장
        self.market_type = broker_config.market_type
        
        # DBFI 설정 (config에서 직접 가져옴)
        self.api_key = config.dbfi.api_key
        self.api_secret = config.dbfi.api_secret
        self.websocket_url = config.dbfi.websocket_url
        self.batch_size = config.dbfi.batch_size
        self.available_sessions = config.dbfi.available_sessions
        self.heartbeat_timeout = config.dbfi.heartbeat_timeout
        self.reconnect_delay = config.dbfi.reconnect_delay

        # message 처리
        self.message_builder = DBFIMessageBuilder()
        self.message_parser = DBFIMessageParser()
        self.oauth = DBFIOAuth(self.api_key, self.api_secret)

    async def _connect_websocket(self):
        """DBFI 웹소켓 연결"""
        try:
            # 액세스 토큰 발급
            if not self.access_token:
                self.access_token = self._get_access_token()

            # 웹소켓 URL 구성
            ws_url = self._build_websocket_url()

            # ping을 1초마다 날리고 10초 타임아웃 설정
            self.websocket = await websockets.connect(
                ws_url,
                ping_interval=1,  # 1초마다 ping
                ping_timeout=10,  # 10초 ping 타임아웃
                close_timeout=5   # 5초 연결 종료 타임아웃
            )
            logger.debug(f"DBFI 웹소켓 연결 성공 (ping: 1초간격): {ws_url}")
            return self.websocket

        except Exception as e:
            logger.error(f"DBFI 웹소켓 연결 실패: {e}")
            raise

    async def disconnect(self):
        """웹소켓 연결 해제"""
        try:
            # ping 루프 중지
            await self.stop_ping_loop()
            
            if self.websocket:
                await self.websocket.close()
                self.websocket = None
                logger.debug("DBFI 웹소켓 연결 해제됨")
        except Exception as e:
            logger.error(f"DBFI 웹소켓 연결 해제 실패: {e}")

    async def _subscribe_symbol_impl(self, symbol: str) -> bool:
        """종목 구독 구현"""
        try:
            if not self.is_connected():
                logger.error("웹소켓이 연결되지 않았습니다")
                return False
            
            # 구독 메시지 생성 (설정된 MarketType 사용)
            subscribe_message = self.message_builder.build_subscribe_message(symbol, self.market_type, self.access_token)
            
            # 메시지 전송
            await self.websocket.send(json.dumps(subscribe_message))
            
            logger.debug(f"DBFI 종목 구독 요청: {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"DBFI 종목 구독 실패: {symbol}, {e}")
            return False

    async def _unsubscribe_symbol_impl(self, symbol: str) -> bool:
        """종목 구독 해제 구현"""
        try:
            if not self.is_connected():
                logger.error("웹소켓이 연결되지 않았습니다")
                return False
            
            # 구독 해제 메시지 생성 (설정된 MarketType 사용)
            unsubscribe_message = self.message_builder.build_unsubscribe_message(symbol, self.market_type, self.access_token)
            
            # 메시지 전송
            await self.websocket.send(json.dumps(unsubscribe_message))
            
            logger.debug(f"DBFI 종목 구독 해제 요청: {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"DBFI 종목 구독 해제 실패: {symbol}, {e}")
            return False

    async def _receive_message(self) -> Optional[Dict[str, Any]]:
        """메시지 수신"""
        try:
            if not self.is_connected():
                return None
            
            # 메시지 수신
            message = await self.websocket.recv()
            
            # JSON 파싱
            data = json.loads(message)
            
            # 전체 메시지 로깅 (개발/디버깅용)
            # logger.debug(f"DBFI 원본 메시지: {message}")
            # logger.debug(f"DBFI 파싱된 데이터: {json.dumps(data, ensure_ascii=False, indent=2)}")
            
            return data
            
        except websockets.exceptions.ConnectionClosed:
            logger.warning("DBFI 웹소켓 연결이 종료되었습니다")
            self.websocket = None  # 연결 종료 시 websocket 객체 정리
            return None
        except json.JSONDecodeError as e:
            logger.error(f"DBFI 메시지 JSON 파싱 실패: {e}")
            return None
        except Exception as e:
            logger.error(f"DBFI 메시지 수신 실패: {e}")
            return None

    def _get_access_token(self) -> str:
        """액세스 토큰 발급"""
        try:
            token = self.oauth.get_token()
            logger.debug("액세스 토큰 발급 성공")
            return token
        except Exception as e:
            logger.error(f"액세스 토큰 발급 실패: {e}")
            raise

    def _build_websocket_url(self) -> str:
        """웹소켓 URL 구성"""
        if not self.websocket_url:
            raise ValueError("웹소켓 URL이 설정되지 않았습니다")
        return f"{self.websocket_url}/websocket"

    def is_connected(self) -> bool:
        """연결 상태 확인 (websockets 호환성 개선)"""
        if self.websocket is None:
            return False
        
        # websockets 11.x 이상에서는 closed 속성 사용
        try:
            return not self.websocket.closed
        except AttributeError:
            # 이전 버전 호환성을 위해 open 속성 시도
            try:
                return self.websocket.open
            except AttributeError:
                # 둘 다 없으면 websocket 존재 여부로만 판단
                return True

    def get_broker_name(self) -> str:
        """브로커 이름 반환"""
        return "dbfi"
    
    async def _send_ping(self) -> bool:
        """ping 전송 구현 (websockets 라이브러리 사용)"""
        try:
            if not self.is_connected():
                return False
            
            # websockets 라이브러리의 ping 메서드 사용
            pong_waiter = await self.websocket.ping()
            
            # pong 응답 대기 (ping_timeout 사용)
            await asyncio.wait_for(pong_waiter, timeout=self.ping_timeout)
            
            return True
            
        except asyncio.TimeoutError:
            logger.warning("DBFI ping 타임아웃 - pong 응답 없음")
            return False
        except Exception as e:
            logger.error(f"DBFI ping 전송 실패: {e}")
            return False