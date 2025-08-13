import logging
import sys
import signal
import time
import asyncio
import os
from typing import Dict, Any

from .utils.config import config
from .services.redis_service import redis_service
from .brokers.dbfi.websocket import DBFIWebSocketClient
from .daemon.broker_daemon import BrokerDaemon
from .schedule import MarketScheduler, MarketState
from .schedule.domestic_scheduler import DomesticScheduler
from .schedule.foreign_scheduler import ForeignScheduler


# 환경변수 기반 로깅 설정
def setup_logging():
    """환경변수를 기반으로 로깅 설정"""
    # 기본 로그 레벨
    log_level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper())
    log_format = os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_file = os.getenv('LOG_FILE', 'app.log')
    log_to_console = os.getenv('LOG_TO_CONSOLE', 'true').lower() == 'true'
    log_to_file = os.getenv('LOG_TO_FILE', 'true').lower() == 'true'
    
    # 핸들러 설정
    handlers = []
    if log_to_console:
        handlers.append(logging.StreamHandler(sys.stdout))
    if log_to_file:
        handlers.append(logging.FileHandler(log_file))
    
    # 기본 로깅 설정
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers
    )
    
    # 모듈별 로그 레벨 설정
    app_level = getattr(logging, os.getenv('APP_LOG_LEVEL', 'DEBUG').upper())
    brokers_level = getattr(logging, os.getenv('BROKERS_LOG_LEVEL', 'DEBUG').upper())
    daemon_level = getattr(logging, os.getenv('DAEMON_LOG_LEVEL', 'DEBUG').upper())
    scheduler_level = getattr(logging, os.getenv('SCHEDULER_LOG_LEVEL', 'INFO').upper())
    
    logging.getLogger('app').setLevel(app_level)
    logging.getLogger('app.brokers').setLevel(brokers_level)
    logging.getLogger('app.daemon').setLevel(daemon_level)
    logging.getLogger('app.schedule').setLevel(scheduler_level)

# 로깅 설정 적용
setup_logging()

logger = logging.getLogger(__name__)


# class ScheduledBrokerController:
#     """시장 스케줄러를 활용한 브로커 데몬 제어 클래스"""
    
#     def __init__(self):
#         self.running = False
#         self.broker_daemon = None
#         self.market_scheduler = MarketScheduler()
#         self.daemon_task = None
        
#         # 스케줄러 콜백 설정
#         self._setup_scheduler_callbacks()
        
#         logger.info("ScheduledBrokerController 초기화 완료")
    
#     def _setup_scheduler_callbacks(self):
#         """시장 상태별 콜백 설정"""
#         self.market_scheduler.register_state_callback(MarketState.PRE_MARKET, self._on_pre_market)
#         self.market_scheduler.register_state_callback(MarketState.REGULAR_HOURS, self._on_market_open)
#         self.market_scheduler.register_state_callback(MarketState.AFTER_HOURS, self._on_after_market)
#         self.market_scheduler.register_state_callback(MarketState.CLOSED, self._on_market_closed)
        
#         # 일반 상태 변경 로깅
#         self.market_scheduler.register_general_callback(self._on_market_state_change)

#     async def _on_pre_market(self, old_state: MarketState, new_state: MarketState):
#         """장시작전 - 브로커 데몬 준비"""
#         logger.info("🟡 장시작전 - 브로커 데몬 준비")

#     async def _on_after_market(self, old_state: MarketState, new_state: MarketState):
#         """장마감후 - 브로커 데몬 최소화"""
#         logger.info("🟠 장마감후 - 브로커 데몬 최소화")
    
#     async def _on_market_open(self, old_state: MarketState, new_state: MarketState):
#         """거래시간 시작 - 브로커 데몬 시작"""
#         logger.info("🟢 거래시간 시작 - 브로커 데몬 시작")
#         if not self.broker_daemon:
#             await self._prepare_broker_daemon()
#         await self._start_broker_daemon()
    
#     async def _on_market_closed(self, old_state: MarketState, new_state: MarketState):
#         """휴장 - 브로커 데몬 정지"""
#         logger.info("🔴 휴장 시간 - 브로커 데몬 정지")
#         await self._stop_broker_daemon()
    
#     async def _on_market_state_change(self, old_state: MarketState, new_state: MarketState):
#         """시장 상태 변경 로깅"""
#         logger.info(f"📊 시장 상태 변경: {old_state.description} → {new_state.description}")
    
#     async def _prepare_broker_daemon(self):
#         """브로커 데몬 준비 (초기화만)"""
#         if not self.broker_daemon:
#             self.broker_daemon = BrokerDaemon()
#             logger.info("브로커 데몬 인스턴스 생성 완료")
    
#     async def _start_broker_daemon(self):
#         """브로커 데몬 시작"""
#         try:
#             if not self.broker_daemon:
#                 await self._prepare_broker_daemon()
            
#             if self.daemon_task and not self.daemon_task.done():
#                 logger.warning("브로커 데몬이 이미 실행 중입니다")
#                 return
            
#             # 현재 활성 시장 정보 가져오기
#             active_markets_info = self.market_scheduler.get_active_markets_info()
            
#             # 브로커 데몬을 별도 태스크로 실행 (시장 정보 전달)
#             self.daemon_task = asyncio.create_task(
#                 self.broker_daemon.start(active_markets_info=active_markets_info)
#             )
#             logger.info("✅ 브로커 데몬 시작됨")
            
#         except Exception as e:
#             logger.error(f"브로커 데몬 시작 실패: {e}")
    
#     async def _stop_broker_daemon(self):
#         """브로커 데몬 정지"""
#         try:
#             if self.broker_daemon:
#                 await self.broker_daemon.stop()
#                 logger.info("✅ 브로커 데몬 정지됨")
            
#             if self.daemon_task and not self.daemon_task.done():
#                 self.daemon_task.cancel()
#                 try:
#                     await self.daemon_task
#                 except asyncio.CancelledError:
#                     logger.debug("브로커 데몬 태스크 취소됨")
            
#             self.daemon_task = None
            
#         except Exception as e:
#             logger.error(f"브로커 데몬 정지 실패: {e}")
    
#     async def start(self):
#         """스케줄 브로커 컨트롤러 시작"""
#         logger.info("🚀 ScheduledBrokerController 시작")
#         self.running = True
        
#         try:
#             # 스케줄러 시작
#             await self.market_scheduler.start()
            
#         except Exception as e:
#             logger.error(f"스케줄 브로커 컨트롤러 시작 실패: {e}")
#             await self.stop()
#             raise
    
#     async def stop(self):
#         """스케줄드 브로커 컨트롤러 정지"""
#         logger.info("🛑 ScheduledBrokerController 정지 중...")
#         self.running = False
        
#         try:
#             # 브로커 데몬 정지
#             await self._stop_broker_daemon()
            
#             # 스케줄러 정지
#             if self.market_scheduler:
#                 await self.market_scheduler.stop()
            
#             logger.info("✅ ScheduledBrokerController 정상 종료됨")
            
#         except Exception as e:
#             logger.error(f"ScheduledBrokerController 종료 중 오류: {e}")
    
#     def get_status(self) -> dict:
#         """컨트롤러 상태 정보 반환"""
#         daemon_running = self.daemon_task and not self.daemon_task.done() if self.daemon_task else False
        
#         return {
#             'controller': {
#                 'running': self.running,
#                 'broker_daemon_initialized': self.broker_daemon is not None,
#                 'broker_daemon_running': daemon_running
#             },
#             'market_scheduler': self.market_scheduler.get_status() if self.market_scheduler else None
#         }

class PriceCollector:
    """PriceCollector 애플리케이션 메인 클래스"""
    
    def __init__(self):
        self.running = False
        self.redis_service = redis_service
        self.websocket_client = DBFIWebSocketClient()
        # self.scheduled_controller = ScheduledBrokerController()
        # MarketScheduler에서 시장 정보 가져오기
        # markets = self.scheduled_controller.market_scheduler.config.markets
        markets = ['KR', 'US']
        
        # 시장별 스케줄러 조건부 초기화
        self.domestic_scheduler = None
        self.foreign_scheduler = None
        
        if 'KR' in markets:
            self.domestic_scheduler = DomesticScheduler()
            logger.info("DomesticScheduler 초기화됨")
        
        if 'US' in markets:
            self.foreign_scheduler = ForeignScheduler()
            logger.info("ForeignScheduler 초기화됨")
        
        logger.info(f"설정된 시장: {markets}")
        
    def start(self):
        """애플리케이션 시작"""
        logger.info(f"{config.app_name} 애플리케이션 시작")
        
        # Redis 연결
        if not self.redis_service.connect():
            logger.error("Redis 연결 실패. 애플리케이션을 종료합니다.")
            return False
        
        self.running = True
        logger.info("애플리케이션이 성공적으로 시작되었습니다.")
        
        # 시그널 핸들러 설정
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        return True
    
    def stop(self):
        """애플리케이션 종료"""
        logger.info("애플리케이션 종료 중...")
        self.running = False
        
        # Redis 연결 해제
        self.redis_service.disconnect()
        
        logger.info("애플리케이션이 종료되었습니다.")
    
    def _signal_handler(self, signum, frame):
        """시그널 핸들러"""
        logger.info(f"시그널 {signum} 수신. 애플리케이션을 종료합니다.")
        self.stop()
        sys.exit(0)
    
    # async def test_websocket_connection(self):
    #     """DBFI 웹소켓 연결 테스트"""
    #     logger.info("=== DBFI 웹소켓 연결 테스트 ===")
        
    #     try:
    #         # 설정 확인
    #         logger.info(f"API Key: {'설정됨' if config.dbfi.api_key else '설정되지 않음'}")
    #         logger.info(f"API Secret: {'설정됨' if config.dbfi.api_secret else '설정되지 않음'}")
    #         logger.info(f"WebSocket URL: {config.dbfi.websocket_url}")
            
    #         if not config.dbfi.api_key or not config.dbfi.api_secret:
    #             logger.error("DBFI API 키가 설정되지 않았습니다.")
    #             return False
            
    #         if not config.dbfi.websocket_url:
    #             logger.error("DBFI 웹소켓 URL이 설정되지 않았습니다.")
    #             return False
            
    #         # 웹소켓 연결 테스트
    #         success = await self.websocket_client.test_connection()
            
    #         if success:
    #             logger.info("DBFI 웹소켓 연결 테스트 성공")
    #         else:
    #             logger.error("DBFI 웹소켓 연결 테스트 실패")
            
    #         return success
            
    #     except Exception as e:
    #         logger.error(f"DBFI 웹소켓 연결 테스트 중 오류 발생: {e}")
    #         return False

    # async def run_broker_daemon(self):
    #     """브로커 데몬 실행 (기존 방식)"""
    #     logger.info("=== 브로커 데몬 시작 ===")
        
    #     try:
    #         # 브로커 데몬 시작
    #         await self.broker_daemon.start()
            
    #     except KeyboardInterrupt:
    #         logger.info("키보드 인터럽트로 브로커 데몬 종료")
    #     except Exception as e:
    #         logger.error(f"브로커 데몬 실행 중 오류 발생: {e}")
    #     finally:
    #         # 브로커 데몬 정지
    #         await self.broker_daemon.stop()
    
    # async def run_scheduled_broker_daemon(self):
    #     """스케줄드 브로커 데몬 실행 (시장 시간 기반)"""
    #     logger.info("=== 스케줄드 브로커 데몬 시작 ===")
        
    #     try:
    #         # 스케줄드 컨트롤러 시작
    #         await self.scheduled_controller.start()
            
    #     except KeyboardInterrupt:
    #         logger.info("키보드 인터럽트로 스케줄드 브로커 데몬 종료")
    #     except Exception as e:
    #         logger.error(f"스케줄드 브로커 데몬 실행 중 오류 발생: {e}")
    #     finally:
    #         # 스케줄드 컨트롤러 정지
    #         await self.scheduled_controller.stop()

    def run_health_check(self):
        """Redis 연결 상태 확인"""
        if self.redis_service.is_connected():
            logger.debug("Redis 연결 상태: 정상")
            return True
        else:
            logger.error("Redis 연결 상태: 비정상")
            return False

    async def run_multi_market_daemon(self):
        """🔥 NEW: 국내/해외 분리 브로커 데몬 실행"""
        logger.info("=== 국내/해외 분리 브로커 데몬 시작 ===")
        
        try:
            # Redis 헬스체크 태스크 시작
            health_check_task = asyncio.create_task(self._run_existing_health_check())
            
            # 활성화된 스케줄러만 실행
            scheduler_tasks = [health_check_task]
            
            if self.domestic_scheduler:
                scheduler_tasks.append(self.domestic_scheduler.start())
            
            if self.foreign_scheduler:
                scheduler_tasks.append(self.foreign_scheduler.start())
            
            # 활성화된 스케줄러들 병렬 실행
            await asyncio.gather(*scheduler_tasks)
            
        except KeyboardInterrupt:
            logger.info("키보드 인터럽트로 분리 브로커 데몬 종료")
        except Exception as e:
            logger.error(f"분리 브로커 데몬 실행 중 오류 발생: {e}")
        finally:
            # 스케줄러들 정지
            await self._stop_all_schedulers()

    async def _run_existing_health_check(self):
        """기존 run_loop() 로직을 비동기로 실행"""
        while self.running:
            if not self.run_health_check():
                logger.error("Redis 연결이 끊어졌습니다. 재연결을 시도합니다.")
                if not self.redis_service.connect():
                    logger.error("Redis 재연결 실패")
                    break
            
            await asyncio.sleep(5)  # 기존 로직 그대로, 비동기로만 수정
        
    async def _stop_all_schedulers(self):
        """모든 스케줄러 정지"""
        try:
            await asyncio.gather(
                self.domestic_scheduler.stop(),
                self.foreign_scheduler.stop(),
                return_exceptions=True
            )
            logger.info("✅ 모든 스케줄러 정지 완료")
        except Exception as e:
            logger.error(f"스케줄러 정지 중 오류: {e}")



def main():
    """메인 함수""" 
    app = PriceCollector()
    
    if app.start():
        # 웹소켓 연결 테스트 실행
        # asyncio.run(app.test_websocket_connection())
        
        # 🔥 스케줄드 브로커 데몬 실행 (시장 시간 기반) - 새로운 방식
        asyncio.run(app.run_multi_market_daemon())
        
        # 기존 브로커 데몬 실행 (항상 실행) - 기존 방식
        # asyncio.run(app.run_broker_daemon())
        
        # 데모 실행 (선택사항)
        # app.run_demo()
        
        # 메인 루프 실행 (선택사항)
        # app.run_loop()
    else:
        logger.error("애플리케이션 시작 실패")
        sys.exit(1)


if __name__ == "__main__":
    main()
