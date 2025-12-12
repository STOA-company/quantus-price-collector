import json
import logging
import requests
import asyncio
import time
from typing import List, Dict, Optional
from datetime import datetime

from app.brokers.base import BrokerConfig, MarketType
from app.brokers.dbfi.oauth import DBFIOAuth
from app.brokers.dbfi.schemas import DBFIMessageBuilder, DBFIMessageParser, DBFIMarketType
from app.utils.config import config

logger = logging.getLogger(__name__)

class DBFIRestAPI:
    def __init__(self, broker_config: Optional[BrokerConfig] = None, market_type: MarketType = MarketType.DOMESTIC):
        self._token_rate_limits = {}

        # broker_config이 None이면 DBFI 전용 설정 사용
        self.broker_config = broker_config
        self.base_url = "https://openapi.dbsec.co.kr:8443"
        self.access_token = None
        self.market_type = market_type
        self._setup_dbfi_oauth()

        # Redis 서비스 추가
        from app.services.redis_service import RedisService
        self.redis_service = RedisService()
        if not self.redis_service.connect():
            logger.warning("Redis 연결 실패 - 데이터 발행 불가")

        # 토큰별 요청 제한 초기화 (더 보수적으로 설정)
        if self.access_token and self.access_token not in self._token_rate_limits:
            self._token_rate_limits[self.access_token] = {
                'request_count': 0,
                'last_request_time': time.time(),
                'rate_limit': 4  # 1초당 4개로 줄임 (더 보수적)
            }
            logger.debug(f"✅ API 제한 설정 완료: 1초당 {self._token_rate_limits[self.access_token]['rate_limit']}개 요청")
            logger.debug(f"🔍 초기화된 제한 정보: {self._token_rate_limits}")
        else:
            logger.warning(f"⚠️ 토큰 제한 초기화 실패: access_token={bool(self.access_token)}, already_exists={self.access_token in self._token_rate_limits if self.access_token else False}")

    def _setup_dbfi_oauth(self):
        """DBFI 전용 OAuth 설정"""
        try:
            # config에서 api_key, api_secret 가져오기
            from app.utils.config import config
            
            if self.market_type == MarketType.DOMESTIC:
                dbfi_config = config.dbfi.get_config_for_market(MarketType.DOMESTIC)
            else:
                dbfi_config = config.dbfi.get_config_for_market(MarketType.FOREIGN)
            
            # OAuth 인스턴스 생성 시 api_key, api_secret 전달
            oauth = DBFIOAuth(
                appkey=dbfi_config['api_key'],
                appsecretkey=dbfi_config['api_secret']
            )
            
            self.access_token = oauth.get_token()  # get_access_token() → get_token()
            logger.info("DBFI OAuth 인증 완료")
            
        except Exception as e:
            logger.error(f"DBFI OAuth 인증 실패: {e}")
            raise

    async def _check_rate_limit(self):
        """토큰별 요청 제한 확인"""
        
        if not self.access_token or self.access_token not in self._token_rate_limits:
            return  # 토큰이 없으면 제한 확인 건너뛰기
        
        current_time = time.time()
        token_limit = self._token_rate_limits[self.access_token]
        
        # 1초가 지났으면 카운트 리셋
        if current_time - token_limit['last_request_time'] >= 1.0:
            token_limit['request_count'] = 0
            token_limit['last_request_time'] = current_time
        
        # 제한에 도달했으면 대기
        if token_limit['request_count'] >= token_limit['rate_limit']:
            wait_time = 1.0 - (current_time - token_limit['last_request_time'])
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                token_limit['request_count'] = 0
                token_limit['last_request_time'] = time.time()
        
        token_limit['request_count'] += 1

    async def get_all_symbols_prices(self, symbols: List[str], market_type: MarketType) -> Dict[str, Dict]:
        """전체 종목 가격 정보 조회 (서킷브레이커 발동 시 사용)"""
        logger.info(f"전체 종목 가격 조회 시작: {len(symbols)}개 종목, 시장: {market_type}")
        
        results = {}
        
        # 무한 루프로 계속 가격 조회 (서킷브레이커 정상화될 때까지)
        while True:  # 🔄 무한 루프
            try:
                logger.debug(f"REST API 가격 조회 사이클 시작 ({len(symbols)}개 종목)")
                
                for i, symbol in enumerate(symbols):
                    try:
                        logger.debug(f"🔍 {symbol} 처리 시작 ({i+1}/{len(symbols)})")
                        
                        # API 제한 확인 및 대기
                        await self._check_rate_limit()
                        
                        if market_type == MarketType.DOMESTIC:
                            price_data = await self.get_domestic_price(symbol)
                        else:
                            price_data = await self.get_foreign_price(symbol)
                        
                        if price_data:
                            results[symbol] = price_data
                            # logger.info(f"✅ {symbol} 가격 조회 성공")
                        
                        # 추가 안전장치: 4개마다 1.0초 대기 (더 보수적)
                        if (i + 1) % 4 == 0:
                            logger.debug(f"API 제한 준수를 위해 1.5초 대기... ({i+1}/{len(symbols)})")
                            await asyncio.sleep(1.0)
                            
                        await asyncio.sleep(0.1)
                    except Exception as e:
                        logger.error(f"❌ {symbol} 가격 조회 실패: {e}")
                        # 오류 발생 시에도 API 제한 준수
                        await asyncio.sleep(0.2)
                
                # 한 사이클 완료 후 잠시 대기
                # logger.info(f"🔄 REST API 사이클 완료, 10초 후 재시작...")
                # await asyncio.sleep(10)  # 10초 후 다시 시작
                
            except Exception as e:
                logger.error(f"❌ REST API 사이클 오류: {e}")
                await asyncio.sleep(5)  # 오류 시 5초 대기

    async def get_domestic_price(self, symbol: str):
        """국내 주식 가격 조회"""
        if not self.access_token:
            logger.error("Access token이 설정되지 않음")
            return None
            
        PATH = "api/v1/quote/kr-stock/inquiry/price"
        URL = f"{self.base_url}/{PATH}"
        request_symbol = symbol.split(" ")
        mrktDivCode = request_symbol[0]
        iscd = request_symbol[1]

        headers = {  
            "content-type": "application/json; charset=utf-8", 
            "authorization": f"Bearer {self.access_token}",
            "cont_yn": "",
            "cont_key": "",
        }

        body = {
            "In": {
                "InputCondMrktDivCode": mrktDivCode,
                "InputIscd1": iscd
            }
        }

        try:
            response = requests.post(URL, headers=headers, json=body, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return self.parse_domestic_price_response(data, symbol)
            else:
                logger.error(f"API 호출 실패: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"가격 조회 중 오류: {e}")
            return None

    async def get_foreign_price(self, symbol: str):
        """해외 주식 가격 조회"""
        if not self.access_token:
            logger.error("Access token이 설정되지 않음")
            return None
        
        PATH = "api/v1/quote/overseas-stock/inquiry/price"
        URL = f"{self.base_url}/{PATH}"
        mrktDivCode = symbol[:2]
        iscd = symbol[2:]

        headers = {  
            "content-type": "application/json; charset=utf-8", 
            "authorization": f"Bearer {self.access_token}",
            "cont_yn": "",
            "cont_key": "",
        }

        body = {
            "In": {
                "InputCondMrktDivCode": mrktDivCode,
                "InputIscd1": iscd
            }
        }

        try:
            response = requests.post(URL, headers=headers, json=body, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return self.parse_foreign_price_response(data, symbol)
            else:
                logger.error(f"API 호출 실패: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"가격 조회 중 오류: {e}")
            return None

    def parse_domestic_price_response(self, response_data: Dict, original_symbol: str = "") -> Dict:
        """국내 주식 가격 응답 파싱 (웹소켓과 동일한 형식)"""
        try:
            if response_data.get("rsp_cd") == "00000":
                out_data = response_data.get("Out", {})
                
                # 안전한 숫자 변환 함수
                def safe_int(value, default=0):
                    if value == '' or value is None:
                        return default
                    try:
                        return int(value)
                    except (ValueError, TypeError):
                        return default
                
                def safe_float(value, default=0.0):
                    if value == '' or value is None:
                        return default
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return default
                
                # 원래 요청한 symbol에서 종목코드만 추출 (예: "E 005930" -> "005930")
                clean_symbol = original_symbol.split()[-1] if original_symbol else ""
                
                # 웹소켓과 동일한 형식으로 변환
                parsed_data = {
                    "type": "realtime_data",
                    "broker": "dbfi",
                    "symbol": clean_symbol,  # 원래 symbol에서 종목코드만 사용
                    "date": datetime.now().strftime("%Y%m%d"),
                    "time": datetime.now().strftime("%H%M%S"),
                    "current_price": safe_int(out_data.get("Prpr")),
                    "price_change": safe_int(out_data.get("PrdyVrss")),
                    "price_change_rate": safe_float(out_data.get("PrdyCtrt")),
                    "open_price": safe_int(out_data.get("Oprc")),
                    "high_price": safe_int(out_data.get("Hprc")),
                    "low_price": safe_int(out_data.get("Lprc")),
                    "volume": safe_int(out_data.get("PrdyVol")),
                    "accumulated_volume": safe_int(out_data.get("AcmlVol")),
                    "ask_price": safe_int(out_data.get("Askp1")),
                    "bid_price": safe_int(out_data.get("Bidp1")),
                    "ask_quantity": 0,  # REST API에는 없음
                    "bid_quantity": 0,  # REST API에는 없음
                    "price_color": "-" if safe_int(out_data.get("Prpr")) >= safe_int(out_data.get("Sdpr")) else "+",
                    "change_color": "-" if safe_int(out_data.get("PrdyVrss")) >= 0 else "+",
                    "rate_color": "-" if safe_float(out_data.get("PrdyCtrt")) >= 0 else "+",
                    "raw_data": response_data,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Redis에 발행
                self._publish_to_redis(parsed_data)
                
                return parsed_data
            else:
                return {
                    "status": "error",
                    "message": response_data.get("rsp_msg", "알 수 없는 오류")
                }
        except Exception as e:
            logger.error(f"응답 파싱 오류: {e}")
            return {"status": "error", "message": str(e)}

    def parse_foreign_price_response(self, response_data: Dict, symbol: str = "") -> Dict:
        """해외 주식 가격 응답 파싱 (웹소켓과 동일한 형식)"""
        try:
            if response_data.get("rsp_cd") == "00000":
                out_data = response_data.get("Out", {})
                
                # 안전한 숫자 변환 함수
                def safe_int(value, default=0):
                    if value == '' or value is None:
                        return default
                    try:
                        return int(float(value)) if value != "0.0000" else default
                    except (ValueError, TypeError):
                        return default
                
                def safe_float(value, default=0.0):
                    if value == '' or value is None:
                        return default
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return default
                
                # 원래 요청한 symbol에서 종목코드만 추출
                
                # 웹소켓과 동일한 형식으로 변환
                parsed_data = {
                    "type": "realtime_data",
                    "broker": "dbfi",
                    "symbol": symbol,
                    "date": datetime.now().strftime("%Y%m%d"),
                    "time": datetime.now().strftime("%H%M%S"),
                    "current_price": safe_float(out_data.get("Prpr")),
                    "price_change": safe_float(out_data.get("PrdyVrss")),
                    "price_change_rate": safe_float(out_data.get("PrdyCtrt")),
                    "open_price": safe_float(out_data.get("Oprc")),
                    "high_price": safe_float(out_data.get("Hprc")),
                    "low_price": safe_float(out_data.get("Lprc")),
                    "volume": safe_int(out_data.get("prdyVol")),
                    "accumulated_volume": safe_int(out_data.get("AcmlVol")),
                    "ask_price": safe_float(out_data.get("askp1")),
                    "bid_price": safe_float(out_data.get("bidp1")),
                    "ask_quantity": 0,  # REST API에는 없음
                    "bid_quantity": 0,  # REST API에는 없음
                    "price_color": "-" if safe_float(out_data.get("Prpr")) >= safe_float(out_data.get("Sdpr")) else "+",
                    "change_color": "-" if safe_float(out_data.get("PrdyVrss")) >= 0 else "+",
                    "rate_color": "-" if safe_float(out_data.get("PrdyCtrt")) >= 0 else "+",
                    "raw_data": response_data,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Redis에 발행
                self._publish_to_redis(parsed_data)
                
                return parsed_data
            else:
                return {
                    "status": "error",
                    "message": response_data.get("rsp_msg", "알 수 없는 오류")
                }
        except Exception as e:
            logger.error(f"응답 파싱 오류: {e}")
            return {"status": "error", "message": str(e)}

    def _publish_to_redis(self, data: Dict):
        """Redis에 실시간 데이터 발행 (웹소켓과 동일한 형식)"""
        try:
            if hasattr(self, 'redis_service') and self.redis_service:
                symbol = data.get('symbol', '')
                if symbol:
                    # 웹소켓과 동일한 형식으로 메타정보 추가
                    processed_data = {
                        **data,  # REST API 원본 데이터
                        'broker': 'dbfi',
                        'timestamp': datetime.now().isoformat(),
                        'daemon_id': 'rest_api_fallback'
                    }
                    
                    # 웹소켓과 동일한 방식으로 Redis 발행
                    self.redis_service.publish_raw_data("dbfi", processed_data)
                    
                    logger.debug(f"💾 REST API Redis 발행 완료: {symbol}")
        except Exception as e:
            logger.error(f"Redis 발행 실패: {e}")



    # def update_access_token(self, new_token: str):
    #     """Access token 업데이트"""
    #     self.access_token = new_token
    #     logger.info("Access token 업데이트 완료")

    # def is_authenticated(self) -> bool:
    #     """인증 상태 확인"""
    #     return self.access_token is not None