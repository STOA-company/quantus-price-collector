import asyncio
import websockets
import json
import logging
from datetime import datetime

from typing import Dict, Optional, AsyncGenerator, Any

from app.brokers.base import BrokerWebSocketClient, BrokerConfig, MarketType
from app.brokers.dbfi.oauth import DBFIOAuth, TokenRateLimitError
from app.brokers.dbfi.schemas import DBFIMessageBuilder, DBFIMessageParser, DBFIMarketType
from app.utils.config import config
from app.utils.exceptions import BrokerConnectionError


logger = logging.getLogger(__name__)

class DBFIWebSocketClient(BrokerWebSocketClient):
    """DBFI 웹소켓 클라이언트"""
    
    def __init__(self, broker_config: BrokerConfig = None, market_type: MarketType = MarketType.DOMESTIC):
        # broker_config이 None이면 DBFI 전용 설정 사용
        if broker_config is None:
            # 시장별 설정 가져오기
            dbfi_config = config.dbfi.get_config_for_market(market_type)  # 👈 여기가 핵심
            broker_config = BrokerConfig(
                api_key=dbfi_config['api_key'],
                api_secret=dbfi_config['api_secret'],
                websocket_url=dbfi_config['websocket_url'],
                batch_size=dbfi_config['batch_size'],
                available_sessions=dbfi_config['available_sessions'],
                market_type=market_type
            )
        
        super().__init__(broker_config)

        # MarketType 저장
        self.market_type = broker_config.market_type

        # 시장별 설정 가져오기
        dbfi_market_config = config.dbfi.get_config_for_market(self.market_type)

        logger.info(f"🔑 DBFIWebSocketClient 초기화 ({self.market_type.value}):")
        logger.info(f"   API Key: {broker_config.api_key[:10]}..." if broker_config.api_key else "   API Key: 설정되지 않음")
        logger.info(f"   API Secret: {'설정됨' if broker_config.api_secret else '설정되지 않음'}")
        logger.info(f"   WebSocket URL: {broker_config.websocket_url}")
        logger.info(f"   Account No: {dbfi_market_config.get('account_no', '')[:4]}***" if dbfi_market_config.get('account_no') else "   Account No: 설정되지 않음")

        # DBFI 설정 (config에서 직접 가져옴)
        self.api_key = broker_config.api_key
        self.api_secret = broker_config.api_secret
        self.account_no = dbfi_market_config.get('account_no', '')
        self.websocket_url = broker_config.websocket_url
        self.batch_size = broker_config.batch_size
        self.available_sessions = broker_config.available_sessions
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
            if not self.access_token or self.oauth.is_token_valid():
                self.access_token = self._get_access_token()

            # 🔥 웹소켓 연결 전 기존 세션 초기화
            if self.account_no:
                try:
                    logger.info(f"🧹 [{self.market_type.value}] 기존 웹소켓 세션 초기화 시도...")
                    result = self.oauth.disconnect_session(self.account_no)
                    logger.info(f"✅ [{self.market_type.value}] 세션 초기화 완료: {result.get('result', result)}")
                except Exception as e:
                    logger.warning(f"⚠️  [{self.market_type.value}] 세션 초기화 실패 (무시하고 진행): {e}")

            # 웹소켓 URL 구성
            ws_url = self._build_websocket_url()

            # 연결 전 현재 설정 출력
            logger.info(f"🔑 웹소켓 연결 시도 ({self.market_type.value}):")
            logger.info(f"   API Key: {self.api_key[:10]}...")
            logger.info(f"   Access Token: {self.access_token[:30]}...")

            # 웹소켓 연결
            self.websocket = await websockets.connect(
                ws_url,
                ping_interval=1,
                ping_timeout=10,
            )
            
            # 연결 후 서버 초기 응답 대기 (최대 3초)
            try:
                initial_response = await asyncio.wait_for(
                    self.websocket.recv(), 
                    timeout=3.0
                )
                logger.debug(f"DBFI 초기 응답 수신: {initial_response}")
            except asyncio.TimeoutError:
                logger.debug("DBFI 초기 응답 없음 (정상)")
            except Exception as e:
                logger.warning(f"DBFI 초기 응답 처리 중 오류: {e}")
            
            logger.debug(f"DBFI 웹소켓 연결 성공 (ping: 5초간격): {ws_url}")
            return self.websocket

        except Exception as e:
            logger.error(f"DBFI 웹소켓 연결 실패: {e}")
            raise

    # async def _connect_websocket(self):
    #     """DBFI 웹소켓 연결"""
    #     try:
    #         # 액세스 토큰 발급
    #         if not self.access_token:
    #             self.access_token = self._get_access_token()

    #         # 웹소켓 URL 구성
    #         ws_url = self._build_websocket_url()

    #         # ping을 1초마다 날리고 10초 타임아웃 설정
    #         self.websocket = await websockets.connect(
    #             ws_url,
    #             ping_interval=5,  # 1초 → 5초로 증가
    #             ping_timeout=15,  # 10초 → 15초로 증가
    #             # ping_interval=1,  # 1초마다 ping
    #             # ping_timeout=10,  # 10초 ping 타임아웃
    #             # close_timeout=5   # 5초 연결 종료 타임아웃
    #         )
    #         logger.debug(f"DBFI 웹소켓 연결 성공 (ping: 1초간격): {ws_url}")
    #         return self.websocket

    #     except Exception as e:
    #         logger.error(f"DBFI 웹소켓 연결 실패: {e}")
    #         raise

    async def disconnect(self):
        """웹소켓 연결 해제"""
        try:
            # ping 루프 중지
            await self.stop_ping_loop()

            if self.websocket:
                await self.websocket.close()
                self.websocket = None
                logger.debug("DBFI 웹소켓 연결 해제됨")

            # 🔥 토큰 해지 (서버 리소스 정리)
            try:
                if self.oauth and self.access_token:
                    logger.info(f"🔓 [{self.market_type.value}] 액세스 토큰 해지 시도...")
                    result = self.oauth.revoke_token()
                    logger.info(f"✅ [{self.market_type.value}] 토큰 해지 완료: {result.get('message', result)}")
            except Exception as e:
                logger.warning(f"⚠️  [{self.market_type.value}] 토큰 해지 실패 (무시): {e}")

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
            
            # 구독 응답 대기 (최대 3초)
            try:
                response = await asyncio.wait_for(
                    self.websocket.recv(), 
                    timeout=10.0
                )
                response_data = json.loads(response)
                
                # 응답 헤더 체크
                header = response_data.get('header', {})
                rsp_cd = header.get('rsp_cd', '')
                rsp_msg = header.get('rsp_msg', '')
                
                if rsp_cd == '00000':  # 정상처리
                    logger.debug(f"DBFI 구독 성공: {symbol} - {rsp_msg}")
                    return True
                elif rsp_cd == '10017':
                    logger.error(f"🚨 DBFI 구독 실패 - 종목코드를 찾을 수 없습니다: {symbol}")
                    logger.error(f"   응답: {rsp_msg}")
                    logger.error(f"   해결방안: 종목코드를 찾을 수 없습니다")
                    return False
                elif rsp_cd == '10011':  # 계좌별 허용 세션 수 초과
                    logger.error(f"🚨 DBFI 구독 실패 - 세션 수 초과: {symbol}")
                    logger.error(f"   응답: {rsp_msg}")
                    logger.error(f"   해결방안: 다른 세션을 종료하거나 세션 수를 줄여주세요")
                    return False
                elif rsp_cd == 'IGW00203':  # 분당 접속 횟수 초과 (6TPM)
                    logger.error(f"🚨 DBFI 구독 실패 - 접속 빈도 초과: {symbol}")
                    logger.error(f"   응답: {rsp_msg}")
                    logger.error(f"   해결방안: 1분 후 재시도하거나 재연결 간격을 늘려주세요")
                    return False
                else:  # 기타 에러
                    logger.warning(f"DBFI 구독 응답 에러: {symbol}")
                    logger.warning(f"   에러코드: {rsp_cd}")
                    logger.warning(f"   에러메시지: {rsp_msg}")
                    return False
                    
            except asyncio.TimeoutError:
                logger.warning(f"DBFI 구독 응답 타임아웃: {symbol} (3초)")
                return False
            except json.JSONDecodeError as e:
                logger.error(f"DBFI 구독 응답 파싱 실패: {symbol}, {e}")
                return False
                
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"DBFI 종목 구독 중 연결 종료: {symbol}, {e}")
            self.websocket = None  # 연결 상태 정리
            return False
        except Exception as e:
            logger.error(f"DBFI 종목 구독 실패: {symbol}, {e}")
            return False

    # async def _subscribe_symbol_impl(self, symbol: str) -> bool:
    #     """종목 구독 구현"""
    #     try:
    #         if not self.is_connected():
    #             logger.error("웹소켓이 연결되지 않았습니다")
    #             return False
            
    #         # 구독 메시지 생성 (설정된 MarketType 사용)
    #         subscribe_message = self.message_builder.build_subscribe_message(symbol, self.market_type, self.access_token)
            
    #         # 메시지 전송
    #         await self.websocket.send(json.dumps(subscribe_message))
            
    #         logger.debug(f"DBFI 종목 구독 요청: {symbol}")
    #         return True
            
    #     except Exception as e:
    #         logger.error(f"DBFI 종목 구독 실패: {symbol}, {e}")
    #         return False

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
            
            return data
            
        except websockets.exceptions.ConnectionClosed:
            if self.is_shutting_down:  # ← base.py에서 제공하는 플래그 사용
                # 정상 종료 시
                logger.debug("DBFI 웹소켓 정상 종료됨")
                self.websocket = None
                return None
            else:
                # 비정상 종료 시
                logger.warning("DBFI 웹소켓 연결이 비정상으로 종료되었습니다")
                self.websocket = None
                raise BrokerConnectionError("웹소켓 연결이 비정상으로 종료되었습니다")
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
        except TokenRateLimitError as e:
            logger.error(f"🚨 토큰 발급 횟수 제한 초과: {e}")
            logger.error(f"   재연결이 너무 빈번합니다. 근본 원인을 확인하세요.")
            raise
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
            logger.debug("웹소켓 연결 상태 확인: 연결되지 않음")
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
    
    # async def _send_ping(self) -> bool:
    #     """ping 전송 구현 (websockets 라이브러리 사용)"""
    #     try:
    #         if not self.is_connected():
    #             return False
    #         # 기존 ping 로직
    #         pong_waiter = await self.websocket.ping()
    #         await asyncio.wait_for(pong_waiter, timeout=self.ping_timeout)
    #         return True
            
    #     except asyncio.TimeoutError:
    #         logger.warning("DBFI ping 타임아웃 - pong 응답 없음")
    #         return False
    #     except Exception as e:
    #         logger.error(f"DBFI ping 전송 실패: {e}")
    #         return False

    async def _send_ping(self) -> bool:
        try:
            if not self.is_connected():
                return False
            
            # # 🔍 테스트: 30초 후 연결 종료
            # if not hasattr(self, '_test_done'):
            #     self._test_done = False
            #     logger.info(" 테스트 플래그 초기화")
            
            # if not self._test_done:
            #     current_time = asyncio.get_event_loop().time()
            #     if not hasattr(self, '_start_time'):
            #         self._start_time = current_time
            #         logger.info(f" 테스트 시작 시간 설정: {self._start_time}")
                
            #     elapsed = current_time - self._start_time
            #     logger.debug(f" 테스트 경과 시간: {elapsed:.1f}초")
                
            #     if elapsed > 30:
            #         logger.info(" 테스트: 30초 경과로 연결 강제 종료")
            #         await self.disconnect()
            #         self._test_done = True
                    
            #         raise BrokerConnectionError("테스트용 연결 종료")
            
            # 기존 ping 로직
            pong_waiter = await self.websocket.ping()
            await asyncio.wait_for(pong_waiter, timeout=self.ping_timeout)
            return True
            
        except Exception as e:
            logger.error(f"DBFI ping 전송 실패: {e}")
            return False
