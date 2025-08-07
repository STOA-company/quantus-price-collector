import asyncio
import logging
from typing import Dict, List, Optional
from app.brokers.dbfi.websocket import DBFIWebSocketClient
from app.brokers.base import MarketType
from app.utils.config import config

logger = logging.getLogger(__name__)

class DBFISessionManager:
    """
    DBFI 토큰 1개당 2개 세션을 관리하는 세션 매니저
    - 각 세션은 동일한 websocket url 사용
    - 10초 이내에 ping/pong 또는 메시지 전송으로 keepalive 보장
    - 세션별 연결/재연결/상태 추적
    """
    def __init__(self, token: str, market_type: MarketType = MarketType.DOMESTIC):
        self.token = token
        self.market_type = market_type
        self.max_sessions = 2  # DBFI 정책: 토큰당 2개 세션
        self.sessions: List[DBFIWebSocketClient] = []
        self.session_tasks: List[asyncio.Task] = []
        self.keepalive_interval = 8  # 8초마다 ping (10초 제한 대비 여유)
        self.running = False

    async def start(self):
        self.running = True
        # 세션 2개 생성 및 연결
        for i in range(self.max_sessions):
            ws_client = DBFIWebSocketClient(market_type=self.market_type)
            self.sessions.append(ws_client)
            task = asyncio.create_task(self._session_worker(i, ws_client))
            self.session_tasks.append(task)
        logger.info(f"DBFI 세션 매니저: {self.max_sessions}개 세션 시작")

    async def stop(self):
        self.running = False
        for ws in self.sessions:
            await ws.disconnect()
        for task in self.session_tasks:
            task.cancel()
        logger.info("DBFI 세션 매니저: 모든 세션 정지")

    async def _session_worker(self, idx: int, ws_client: DBFIWebSocketClient):
        reconnect_delay = 3
        while self.running:
            try:
                await ws_client.connect()
                logger.info(f"[DBFI 세션 {idx}] 연결 성공")
                # keepalive 루프
                while self.running and ws_client.is_connected():
                    await asyncio.sleep(self.keepalive_interval)
                    try:
                        await ws_client._send_ping()
                        logger.debug(f"[DBFI 세션 {idx}] ping 전송")
                    except Exception as e:
                        logger.warning(f"[DBFI 세션 {idx}] ping 실패: {e}")
                logger.warning(f"[DBFI 세션 {idx}] 연결 끊김, 재연결 시도")
            except Exception as e:
                logger.error(f"[DBFI 세션 {idx}] 연결 실패: {e}")
            await asyncio.sleep(reconnect_delay)

    def get_sessions(self) -> List[DBFIWebSocketClient]:
        return self.sessions

    def get_connected_sessions(self) -> List[DBFIWebSocketClient]:
        return [s for s in self.sessions if s.is_connected()]

    async def subscribe_symbols(self, symbols: List[str], delay: float = 0.5):
        """
        종목 리스트를 세션별로 분할하여,
        세션0의 모든 구독이 끝난 후 세션1의 구독을 시작 (동시에 여러 세션에 요청이 가지 않음)
        delay: 각 구독 요청 사이 대기 시간(초)
        """
        if not self.sessions:
            logger.warning("세션이 초기화되지 않았습니다. start()를 먼저 호출하세요.")
            return
        n_sessions = len(self.sessions)
        # 세션별로 종목 분할
        session_symbols = [[] for _ in range(n_sessions)]
        for idx, symbol in enumerate(symbols):
            session_idx = idx % n_sessions
            session_symbols[session_idx].append(symbol)
        # 세션별로 순차 구독
        for session_idx, ws in enumerate(self.sessions):
            for symbol in session_symbols[session_idx]:
                try:
                    success = await ws.subscribe_symbol(symbol)
                    if success:
                        logger.info(f"[DBFI 세션 {session_idx}] {symbol} 구독 성공")
                    else:
                        logger.warning(f"[DBFI 세션 {session_idx}] {symbol} 구독 실패")
                except Exception as e:
                    logger.error(f"[DBFI 세션 {session_idx}] {symbol} 구독 중 오류: {e}")
                await asyncio.sleep(delay)