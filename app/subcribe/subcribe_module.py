import asyncio
import json
import logging
import redis
from datetime import datetime
from typing import Dict, List, Optional

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PriceSubscriber:
    """Redis 실시간 가격 구독 클라이언트"""
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
        self.symbols: List[str] = []
        self.brokers: List[str] = []
        self.latest_prices: Dict[str, Dict] = {}
        self.running = False
        
        # 하드코딩된 설정
        self._load_config()
        
    def _load_config(self):
        """하드코딩된 설정 로드"""
        try:
            # Redis 설정
            self.redis_host = 'localhost'
            self.redis_port = 6379
            self.redis_db = 0
            self.redis_password = ''
            
            # 브로커 및 종목 설정
            self.brokers = ["dbfi"]
            self.symbols = ["FADIA", "FASSO", "FASPXL", "FAQLD", "FNTQQQ", "FNSOXX", "FAGLD", "FAGLDM", "FNTLT", "FAUVIX", "FAUSO"]
            WATCH_SYMBOLS_FOREIGN=["FADIA", "FASSO", "FASPXL", "FAQLD", "FNTQQQ", "FNSOXX", "FAGLD", "FAGLDM", "FNTLT", "FAUVIX", "FAUSO"]
            
            logger.info(f"설정 로드 완료 - 브로커: {self.brokers}, 종목: {self.symbols}")
            
        except Exception as e:
            logger.error(f"설정 로드 실패: {e}")
            raise
    
    def connect_redis(self) -> bool:
        """Redis 연결"""
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password if self.redis_password else None,
                decode_responses=True
            )
            
            # 연결 테스트
            self.redis_client.ping()
            logger.info(f"Redis 연결 성공: {self.redis_host}:{self.redis_port}")
            
            # Pub/Sub 클라이언트 생성
            self.pubsub = self.redis_client.pubsub()
            return True
            
        except Exception as e:
            logger.error(f"Redis 연결 실패: {e}")
            return False
    
    def subscribe_channels(self):
        """채널 구독"""
        try:
            channels = []
            
            # 브로커별, 종목별 채널 구독
            for broker in self.brokers:
                for symbol in self.symbols:
                    channel = f"{symbol}:raw_data"  # cltest에서 broker명 제외한 형태
                    channels.append(channel)
                    self.pubsub.subscribe(channel)
                    logger.info(f"구독 시작: {channel}")
            
            logger.info(f"총 {len(channels)}개 채널 구독 완료")
            
        except Exception as e:
            logger.error(f"채널 구독 실패: {e}")
            raise
    
    def process_message(self, message):
        """메시지 처리"""
        try:
            if message['type'] != 'message':
                return
            
            # JSON 파싱
            data = json.loads(message['data'])
            symbol = data.get('symbol', 'unknown')
            current_price = data.get('current_price', 0)
            price_change = data.get('price_change', 0)
            price_change_rate = data.get('price_change_rate', 0)
            volume = data.get('volume', 0)
            timestamp = data.get('timestamp', '')
            
            # 최신 가격 저장
            self.latest_prices[symbol] = {
                'current_price': current_price,
                'price_change': price_change,
                'price_change_rate': price_change_rate,
                'volume': volume,
                'timestamp': timestamp
            }
            
            # 가격 변동 방향 표시
            change_sign = "+" if price_change > 0 else ""
            change_color = "📈" if price_change > 0 else "📉" if price_change < 0 else "➡️"
            
            # 실시간 출력
            print(f"\n{change_color} [{symbol}] {current_price:,}원 ({change_sign}{price_change:,}, {price_change_rate:+.2f}%) | 거래량: {volume:,}")
            print(f"   시간: {timestamp}")
            
        except Exception as e:
            logger.error(f"메시지 처리 오류: {e}")
    
    def print_summary(self):
        """현재 구독 중인 종목 요약 출력"""
        print("\n" + "="*80)
        print("📊 실시간 주식 가격 모니터링")
        print("="*80)
        print(f"구독 브로커: {', '.join(self.brokers)}")
        print(f"구독 종목: {', '.join(self.symbols)}")
        print(f"Redis 서버: {self.redis_host}:{self.redis_port}")
        print("="*80)
        print("실시간 데이터를 기다리는 중... (Ctrl+C로 종료)")
        print("="*80)
    
    async def run(self):
        """메인 실행 함수"""
        try:
            # Redis 연결
            if not self.connect_redis():
                return
            
            # 채널 구독
            self.subscribe_channels()
            
            # 요약 정보 출력
            self.print_summary()
            
            self.running = True
            
            # 메시지 수신 루프
            while self.running:
                try:
                    message = self.pubsub.get_message(timeout=1.0)
                    if message:
                        self.process_message(message)
                    
                    # 비동기 처리를 위한 짧은 대기
                    await asyncio.sleep(0.01)
                    
                except KeyboardInterrupt:
                    logger.info("사용자 요청으로 종료합니다...")
                    break
                except Exception as e:
                    logger.error(f"메시지 수신 오류: {e}")
                    await asyncio.sleep(1)
        
        except Exception as e:
            logger.error(f"실행 오류: {e}")
        
        finally:
            self.cleanup()
    
    def cleanup(self):
        """리소스 정리"""
        try:
            self.running = False
            
            if self.pubsub:
                self.pubsub.close()
                logger.info("Pub/Sub 연결 종료")
            
            if self.redis_client:
                self.redis_client.close()
                logger.info("Redis 연결 종료")
                
            print("\n" + "="*80)
            print("📊 최종 가격 요약")
            print("="*80)
            
            if self.latest_prices:
                for symbol, data in self.latest_prices.items():
                    change_sign = "+" if data['price_change'] > 0 else ""
                    print(f"{symbol}: {data['current_price']:,}원 ({change_sign}{data['price_change']:,}, {data['price_change_rate']:+.2f}%)")
            else:
                print("수신된 데이터가 없습니다.")
            
            print("="*80)
            print("프로그램을 종료합니다.")
            
        except Exception as e:
            logger.error(f"정리 중 오류: {e}")


async def main():
    """메인 함수"""
    subscriber = PriceSubscriber()
    
    try:
        await subscriber.run()
    except KeyboardInterrupt:
        logger.info("프로그램 종료")
    except Exception as e:
        logger.error(f"프로그램 실행 오류: {e}")


if __name__ == "__main__":
    asyncio.run(main())
