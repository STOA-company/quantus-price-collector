import logging
import json
from typing import Any, Optional, Union, Dict, List
from redis import Redis, ConnectionPool, RedisError
from redis.exceptions import ConnectionError, TimeoutError
import time

from ..utils.config import config


logger = logging.getLogger(__name__)


class RedisService:
    """Redis 연결 및 관리 서비스"""
    
    def __init__(self):
        self._redis_client: Optional[Redis] = None
        self._connection_pool: Optional[ConnectionPool] = None
        self._is_connected = False
        self._last_publish_time: Dict[str, float] = {}  # 종목별 마지막 발송 시간
        self._throttle_interval = 0.5  # 0.5초 간격 (더 빠른 업데이트)
        
    def connect(self) -> bool:
        """Redis에 연결"""
        try:
            # 연결 풀 생성
            self._connection_pool = ConnectionPool(
                host=config.redis.redis_host,
                port=config.redis.redis_port,
                db=config.redis.redis_db,
                password=config.redis.redis_password,
                max_connections=config.redis.redis_max_connections,
                retry_on_timeout=config.redis.redis_retry_on_timeout,
                socket_connect_timeout=config.redis.redis_socket_connect_timeout,
                socket_timeout=config.redis.redis_socket_timeout,
                decode_responses=True
            )
            
            # Redis 클라이언트 생성
            self._redis_client = Redis(connection_pool=self._connection_pool)
            
            # 연결 테스트
            self._redis_client.ping()
            self._is_connected = True
            
            logger.info(f"Redis 연결 성공: {config.redis.redis_host}:{config.redis.redis_port}")
            return True
            
        except (ConnectionError, TimeoutError, RedisError) as e:
            logger.error(f"Redis 연결 실패: {e}")
            self._is_connected = False
            return False
    
    def disconnect(self):
        """Redis 연결 해제"""
        if self._redis_client:
            self._redis_client.close()
            self._redis_client = None
        
        if self._connection_pool:
            self._connection_pool.disconnect()
            self._connection_pool = None
        
        self._is_connected = False
        logger.info("Redis 연결 해제됨")
    
    def is_connected(self) -> bool:
        """연결 상태 확인"""
        if not self._redis_client or not self._is_connected:
            return False
        
        try:
            self._redis_client.ping()
            return True
        except (ConnectionError, TimeoutError, RedisError):
            self._is_connected = False
            return False
    
    def get_client(self) -> Optional[Redis]:
        """Redis 클라이언트 반환"""
        if not self.is_connected():
            if not self.connect():
                return None
        return self._redis_client
    
    def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """키-값 설정"""
        try:
            client = self.get_client()
            if not client:
                return False
            
            # 값이 딕셔너리나 리스트인 경우 JSON으로 직렬화
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            
            result = client.set(key, value, ex=ex)
            return result
        except Exception as e:
            logger.error(f"Redis SET 오류 - 키: {key}, 오류: {e}")
            return False
    
    def get(self, key: str) -> Optional[Any]:
        """키로 값 조회"""
        try:
            client = self.get_client()
            if not client:
                return None
            
            value = client.get(key)
            if value is None:
                return None
            
            # JSON으로 직렬화된 값인지 확인
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
                
        except Exception as e:
            logger.error(f"Redis GET 오류 - 키: {key}, 오류: {e}")
            return None
    
    def delete(self, key: str) -> bool:
        """키 삭제"""
        try:
            client = self.get_client()
            if not client:
                return False
            
            result = client.delete(key)
            return result > 0
        except Exception as e:
            logger.error(f"Redis DELETE 오류 - 키: {key}, 오류: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """키 존재 여부 확인"""
        try:
            client = self.get_client()
            if not client:
                return False
            
            return client.exists(key) > 0
        except Exception as e:
            logger.error(f"Redis EXISTS 오류 - 키: {key}, 오류: {e}")
            return False
    
    def expire(self, key: str, seconds: int) -> bool:
        """키 만료 시간 설정"""
        try:
            client = self.get_client()
            if not client:
                return False
            
            return client.expire(key, seconds)
        except Exception as e:
            logger.error(f"Redis EXPIRE 오류 - 키: {key}, 오류: {e}")
            return False
    
    def ttl(self, key: str) -> int:
        """키의 남은 만료 시간 조회 (초)"""
        try:
            client = self.get_client()
            if not client:
                return -1
            
            return client.ttl(key)
        except Exception as e:
            logger.error(f"Redis TTL 오류 - 키: {key}, 오류: {e}")
            return -1
    
    def hset(self, name: str, key: str, value: Any) -> bool:
        """해시 설정"""
        try:
            client = self.get_client()
            if not client:
                return False
            
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            
            result = client.hset(name, key, value)
            return result >= 0
        except Exception as e:
            logger.error(f"Redis HSET 오류 - 해시: {name}, 키: {key}, 오류: {e}")
            return False
    
    def hget(self, name: str, key: str) -> Optional[Any]:
        """해시 값 조회"""
        try:
            client = self.get_client()
            if not client:
                return None
            
            value = client.hget(name, key)
            if value is None:
                return None
            
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
                
        except Exception as e:
            logger.error(f"Redis HGET 오류 - 해시: {name}, 키: {key}, 오류: {e}")
            return None
    
    def hgetall(self, name: str) -> Dict[str, Any]:
        """해시 전체 조회"""
        try:
            client = self.get_client()
            if not client:
                return {}
            
            result = client.hgetall(name)
            if not result:
                return {}
            
            # JSON으로 직렬화된 값들 복원
            parsed_result = {}
            for key, value in result.items():
                try:
                    parsed_result[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    parsed_result[key] = value
            
            return parsed_result
        except Exception as e:
            logger.error(f"Redis HGETALL 오류 - 해시: {name}, 오류: {e}")
            return {}
    
    def lpush(self, name: str, *values) -> int:
        """리스트 왼쪽에 값 추가"""
        try:
            client = self.get_client()
            if not client:
                return 0
            
            # 값들을 JSON으로 직렬화
            serialized_values = []
            for value in values:
                if isinstance(value, (dict, list)):
                    serialized_values.append(json.dumps(value, ensure_ascii=False))
                else:
                    serialized_values.append(value)
            
            return client.lpush(name, *serialized_values)
        except Exception as e:
            logger.error(f"Redis LPUSH 오류 - 리스트: {name}, 오류: {e}")
            return 0
    
    def rpush(self, name: str, *values) -> int:
        """리스트 오른쪽에 값 추가"""
        try:
            client = self.get_client()
            if not client:
                return 0
            
            # 값들을 JSON으로 직렬화
            serialized_values = []
            for value in values:
                if isinstance(value, (dict, list)):
                    serialized_values.append(json.dumps(value, ensure_ascii=False))
                else:
                    serialized_values.append(value)
            
            return client.rpush(name, *serialized_values)
        except Exception as e:
            logger.error(f"Redis RPUSH 오류 - 리스트: {name}, 오류: {e}")
            return 0
    
    def lrange(self, name: str, start: int, end: int) -> List[Any]:
        """리스트 범위 조회"""
        try:
            client = self.get_client()
            if not client:
                return []
            
            result = client.lrange(name, start, end)
            if not result:
                return []
            
            # JSON으로 직렬화된 값들 복원
            parsed_result = []
            for value in result:
                try:
                    parsed_result.append(json.loads(value))
                except (json.JSONDecodeError, TypeError):
                    parsed_result.append(value)
            
            return parsed_result
        except Exception as e:
            logger.error(f"Redis LRANGE 오류 - 리스트: {name}, 오류: {e}")
            return []
    
    def info(self) -> Dict[str, Any]:
        """Redis 서버 정보 조회"""
        try:
            client = self.get_client()
            if not client:
                return {}
            
            return client.info()
        except Exception as e:
            logger.error(f"Redis INFO 오류: {e}")
            return {}

    def publish_raw_data(self, broker_name: str, data: Dict[str, Any]) -> bool:
        """원본 데이터를 Redis에 발행 (1초 throttling 적용)"""
        try:
            client = self.get_client()
            if not client:
                return False
            
            symbol = data.get('symbol', 'unknown')
            current_time = time.time()
            
            # Throttling 체크: 같은 종목에 대해 1초 이내 발송 제한
            last_time = self._last_publish_time.get(symbol, 0)
            if current_time - last_time < self._throttle_interval:
                # logger.debug(f"Throttling: {symbol} 데이터 발송 스킵 (마지막 발송: {current_time - last_time:.2f}초 전)")
                return False
            
            # 채널명 생성
            channel = f"{symbol}:raw_data"
            
            # 데이터를 JSON으로 직렬화
            message = json.dumps(data, ensure_ascii=False)
            
            # Redis Pub/Sub으로 발행
            result = client.publish(channel, message)
            
            # 발송 시간 기록
            self._last_publish_time[symbol] = current_time
            
            logger.debug(f"원본 데이터 발행: {broker_name} -> {channel}")
            return result > 0
            
        except Exception as e:
            logger.error(f"원본 데이터 발행 오류 - 브로커: {broker_name}, 오류: {e}")
            return False

    def set_broker_status(self, broker_name: str, status_data: Dict[str, Any]) -> bool:
        """브로커 상태 정보를 Redis에 저장"""
        try:
            client = self.get_client()
            if not client:
                return False
            
            # 키명 생성
            key = f"broker:{broker_name}:status"
            
            # 상태 데이터를 JSON으로 직렬화
            message = json.dumps(status_data, ensure_ascii=False)
            
            # Redis에 저장 (30초 만료)
            result = client.setex(key, 30, message)
            
            logger.debug(f"브로커 상태 저장: {broker_name}")
            return result
            
        except Exception as e:
            logger.error(f"브로커 상태 저장 오류 - 브로커: {broker_name}, 오류: {e}")
            return False

    def get_broker_status(self, broker_name: str) -> Optional[Dict[str, Any]]:
        """브로커 상태 정보를 Redis에서 조회"""
        try:
            client = self.get_client()
            if not client:
                return None
            
            # 키명 생성
            key = f"broker:{broker_name}:status"
            
            # Redis에서 조회
            value = client.get(key)
            if value is None:
                return None
            
            # JSON으로 직렬화된 값 복원
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return None
                
        except Exception as e:
            logger.error(f"브로커 상태 조회 오류 - 브로커: {broker_name}, 오류: {e}")
            return None

    def get_all_broker_status(self) -> Dict[str, Dict[str, Any]]:
        """모든 브로커 상태 정보 조회"""
        try:
            client = self.get_client()
            if not client:
                return {}
            
            # 브로커 상태 키 패턴으로 검색
            pattern = "broker:*:status"
            keys = client.keys(pattern)
            
            result = {}
            for key in keys:
                # 키에서 브로커명 추출
                broker_name = key.split(':')[1]
                
                # 상태 데이터 조회
                value = client.get(key)
                if value:
                    try:
                        status_data = json.loads(value)
                        result[broker_name] = status_data
                    except (json.JSONDecodeError, TypeError):
                        continue
            
            return result
            
        except Exception as e:
            logger.error(f"모든 브로커 상태 조회 오류: {e}")
            return {}


# 전역 Redis 서비스 인스턴스
redis_service = RedisService()
