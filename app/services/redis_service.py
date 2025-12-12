import logging
import json
import time
import os

from typing import Any, Optional, Union, Dict, List
from redis import Redis, ConnectionPool, RedisError, Sentinel
from redis.exceptions import ConnectionError, TimeoutError
from datetime import datetime

from ..utils.exceptions import RedisConnectionError, RedisOperationError
from ..utils.config import config

logger = logging.getLogger(__name__)


class RedisService:
    """Redis Sentinel을 사용한 고가용성 Redis 연결 및 관리 서비스"""
    
    def __init__(self):
        self._redis_client: Optional[Redis] = None
        self._sentinel: Optional[Sentinel] = None
        self._connection_pool: Optional[ConnectionPool] = None
        self._is_connected = False
        self._last_publish_time: Dict[str, float] = {}  # 종목별 마지막 발송 시간
        self._throttle_interval = 0.5  # 0.5초 간격 (더 빠른 업데이트)
        
        # Sentinel 설정 - config.py 사용
        self._sentinel_hosts = self._get_sentinel_hosts()
        self._master_name = config.redis.redis_master_name
        
        # 마스터 변경 감지를 위한 상태 저장
        self._last_master_info = None
        self._last_connection_status = True
        
    def _get_sentinel_hosts(self) -> List[tuple]:
        """config.py에서 Sentinel 호스트 정보를 가져옴"""
        sentinel_hosts = []
        
        # config.py에서 Sentinel 정보 가져오기
        if config.redis.redis_sentinel_enabled:
            sentinel_env = config.redis.redis_sentinel_hosts
            if sentinel_env:
                for sentinel in sentinel_env.split(','):
                    if ':' in sentinel:
                        host, port = sentinel.strip().split(':')
                        sentinel_hosts.append((host.strip(), int(port.strip())))
        
        # 기본값 설정 (docker-compose.yml의 기본 설정)
        if not sentinel_hosts:
            sentinel_hosts = [
                ('redis-sentinel-1', 26379),
                ('redis-sentinel-2', 26380),
                ('redis-sentinel-3', 26381)
            ]
        
        logger.info(f"Redis Sentinel 호스트: {sentinel_hosts}")
        return sentinel_hosts
        
    def connect(self) -> bool:
        """Redis Sentinel을 통해 Redis에 연결"""
        try:
            if not self._sentinel_hosts:
                raise RedisConnectionError(
                    message="Sentinel 호스트가 설정되지 않음",
                    operation="connect",
                    error_details="REDIS_SENTINELS 환경변수가 설정되지 않음"
                )
            
            # Sentinel 연결 생성
            self._sentinel = Sentinel(
                self._sentinel_hosts,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
                decode_responses=True
            )
            
            # 마스터 Redis 클라이언트 생성
            self._redis_client = self._sentinel.master_for(
                self._master_name,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
                decode_responses=True
            )
            
            # 연결 테스트
            self._redis_client.ping()
            self._is_connected = True
            
            logger.info(f"Redis Sentinel을 통한 연결 성공: 마스터={self._master_name}")
            return True
            
        except (ConnectionError, TimeoutError, RedisError) as e:
            self._is_connected = False
            error_msg = f"Redis Sentinel 연결 실패: {e}"
            logger.error(f"Redis Sentinel 연결 실패: {error_msg}")
            raise RedisConnectionError(
                message=error_msg,
                operation="connect",
                error_details=str(e)
            )
    
    def disconnect(self):
        """Redis 연결 해제"""
        if self._redis_client:
            self._redis_client.close()
            self._redis_client = None
        
        # if self._sentinel:
        #     self._sentinel.close()
        #     self._sentinel = None
        
        if self._connection_pool:
            self._connection_pool.disconnect()
            self._connection_pool = None
        
        self._is_connected = False
        logger.info("Redis 연결 해제됨")
    
    def is_connected(self) -> bool:
        """연결 상태 확인"""
        if not self._redis_client or not self._is_connected:
            # 연결 상태가 변경되었는지 확인
            if self._last_connection_status:
                self._last_connection_status = False
                self._send_redis_connection_lost_alert()
            return False
        
        try:
            self._redis_client.ping()
            # 연결 상태가 변경되었는지 확인
            if not self._last_connection_status:
                self._last_connection_status = True
                self._send_redis_connection_restored_alert()
            return True
        except (ConnectionError, TimeoutError, RedisError):
            self._is_connected = False
            # 연결 상태가 변경되었는지 확인
            if self._last_connection_status:
                self._last_connection_status = False
                self._send_redis_connection_lost_alert()
            return False
    
    def get_client(self) -> Optional[Redis]:
        """Redis 클라이언트 반환 (필요시 재연결)"""
        if not self.is_connected():
            try:
                if not self.connect():
                    return None
            except RedisConnectionError as e:
                logger.error(f"Redis 재연결 실패: {e}")
                return None
        return self._redis_client
    
    def get_slave_client(self) -> Optional[Redis]:
        """읽기 전용 슬레이브 Redis 클라이언트 반환"""
        try:
            if not self._sentinel:
                if not self.connect():
                    return None
            
            return self._sentinel.slave_for(
                self._master_name,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
                decode_responses=True
            )
        except Exception as e:
            logger.error(f"슬레이브 클라이언트 생성 실패: {e}")
            return None
    
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
            error_msg = f"Redis SET 오류 - 키: {key}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="set",
                key=key,
                error_details=str(e)
            )
    
    def get(self, key: str) -> Optional[Any]:
        """키로 값 조회 (슬레이브 사용)"""
        try:
            # 읽기 작업은 슬레이브 사용
            client = self.get_slave_client() or self.get_client()
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
            error_msg = f"Redis GET 오류 - 키: {key}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="get",
                key=key,
                error_details=str(e)
            )
    
    def delete(self, key: str) -> bool:
        """키 삭제 (마스터 사용)"""
        try:
            client = self.get_client()
            if not client:
                return False
            
            result = client.delete(key)
            return result > 0
        except Exception as e:
            error_msg = f"Redis DELETE 오류 - 키: {key}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="delete",
                key=key,
                error_details=str(e)
            )
    
    def exists(self, key: str) -> bool:
        """키 존재 여부 확인 (슬레이브 사용)"""
        try:
            # 읽기 작업은 슬레이브 사용
            client = self.get_slave_client() or self.get_client()
            if not client:
                return False
            
            return client.exists(key) > 0
        except Exception as e:
            error_msg = f"Redis EXISTS 오류 - 키: {key}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="exists",
                key=key,
                error_details=str(e)
            )
    
    def expire(self, key: str, seconds: int) -> bool:
        """키 만료 시간 설정 (마스터 사용)"""
        try:
            client = self.get_client()
            if not client:
                return False
            
            return client.expire(key, seconds)
        except Exception as e:
            error_msg = f"Redis EXPIRE 오류 - 키: {key}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="expire",
                key=key,
                error_details=str(e)
            )
    
    def ttl(self, key: str) -> int:
        """키의 남은 만료 시간 조회 (초) (슬레이브 사용)"""
        try:
            # 읽기 작업은 슬레이브 사용
            client = self.get_slave_client() or self.get_client()
            if not client:
                return -1
            
            return client.ttl(key)
        except Exception as e:
            error_msg = f"Redis TTL 오류 - 키: {key}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="ttl",
                key=key,
                error_details=str(e)
            )
    
    def hset(self, name: str, key: str, value: Any) -> bool:
        """해시 설정 (마스터 사용)"""
        try:
            client = self.get_client()
            if not client:
                return False
            
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            
            result = client.hset(name, key, value)
            return result >= 0
        except Exception as e:
            error_msg = f"Redis HSET 오류 - 해시: {name}, 키: {key}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="hset",
                key=f"{name}:{key}",
                error_details=str(e)
            )
    
    def hget(self, name: str, key: str) -> Optional[Any]:
        """해시 값 조회 (슬레이브 사용)"""
        try:
            # 읽기 작업은 슬레이브 사용
            client = self.get_slave_client() or self.get_client()
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
            error_msg = f"Redis HGET 오류 - 해시: {name}, 키: {key}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="hget",
                key=f"{name}:{key}",
                error_details=str(e)
            )
    
    def hgetall(self, name: str) -> Dict[str, Any]:
        """해시 전체 조회 (슬레이브 사용)"""
        try:
            # 읽기 작업은 슬레이브 사용
            client = self.get_slave_client() or self.get_client()
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
            error_msg = f"Redis HGETALL 오류 - 해시: {name}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="hgetall",
                key=name,
                error_details=str(e)
            )
    
    def lpush(self, name: str, *values) -> int:
        """리스트 왼쪽에 값 추가 (마스터 사용)"""
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
            error_msg = f"Redis LPUSH 오류 - 리스트: {name}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="lpush",
                key=name,
                error_details=str(e)
            )
    
    def rpush(self, name: str, *values) -> int:
        """리스트 오른쪽에 값 추가 (마스터 사용)"""
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
            error_msg = f"Redis RPUSH 오류 - 리스트: {name}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="rpush",
                key=name,
                error_details=str(e)
            )
    
    def lrange(self, name: str, start: int, end: int) -> List[Any]:
        """리스트 범위 조회 (슬레이브 사용)"""
        try:
            # 읽기 작업은 슬레이브 사용
            client = self.get_slave_client() or self.get_client()
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
            error_msg = f"Redis LRANGE 오류 - 리스트: {name}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="lrange",
                key=name,
                error_details=str(e)
            )
    
    def info(self) -> Dict[str, Any]:
        """Redis 서버 정보 조회 (슬레이브 사용)"""
        try:
            # 읽기 작업은 슬레이브 사용
            client = self.get_slave_client() or self.get_client()
            if not client:
                return {}
            
            return client.info()
        except Exception as e:
            error_msg = f"Redis INFO 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="info",
                key="",
                error_details=str(e)
            )

    def publish_raw_data(self, broker_name: str, data: Dict[str, Any]) -> bool:
        """원본 데이터를 Redis에 발행 (1초 throttling 적용, 마스터 사용)"""
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
            error_msg = f"원본 데이터 발행 오류 - 브로커: {broker_name}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생 (중요한 작업이므로 슬랙 알림 포함)
            raise RedisOperationError(
                message=error_msg,
                operation="publish_raw_data",
                key=f"{broker_name}:{data.get('symbol', 'unknown')}",
                error_details=str(e)
            )

    def set_broker_status(self, broker_name: str, status_data: Dict[str, Any]) -> bool:
        """브로커 상태 정보를 Redis에 저장 (마스터 사용)"""
        try:
            client = self.get_client()
            if not client:
                return False
            
            # 키명 생성
            key = f"broker:{broker_name}:status"

            # datetime 객체를 문자열로 변환
            serialized_data = self._serialize_datetime_objects(status_data)
            
            # 상태 데이터를 JSON으로 직렬화
            message = json.dumps(serialized_data, ensure_ascii=False)
            
            # Redis에 저장 (30초 만료)
            result = client.setex(key, 30, message)
            
            logger.debug(f"브로커 상태 저장: {broker_name}")
            return result
            
        except Exception as e:
            error_msg = f"브로커 상태 저장 오류 - 브로커: {broker_name}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생 (중요한 작업이므로 슬랙 알림 포함)
            raise RedisOperationError(
                message=error_msg,
                operation="set_broker_status",
                key=key,
                error_details=str(e)
            )

    def get_broker_status(self, broker_name: str) -> Optional[Dict[str, Any]]:
        """브로커 상태 정보를 Redis에서 조회 (슬레이브 사용)"""
        try:
            # 읽기 작업은 슬레이브 사용
            client = self.get_slave_client() or self.get_client()
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
            error_msg = f"브로커 상태 조회 오류 - 브로커: {broker_name}, 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="get_broker_status",
                key=key,
                error_details=str(e)
            )

    def get_all_broker_status(self) -> Dict[str, Dict[str, Any]]:
        """모든 브로커 상태 정보 조회 (슬레이브 사용)"""
        try:
            # 읽기 작업은 슬레이브 사용
            client = self.get_slave_client() or self.get_client()
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
            error_msg = f"모든 브로커 상태 조회 오류: {e}"
            logger.error(error_msg)
            
            # Redis 작업 에러 예외 발생
            raise RedisOperationError(
                message=error_msg,
                operation="get_all_broker_status",
                key="",
                error_details=str(e)
            )
    def _serialize_datetime_objects(self, obj):
        """datetime 객체를 문자열로 변환하는 헬퍼 메서드"""
        if isinstance(obj, dict):
            return {key: self._serialize_datetime_objects(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._serialize_datetime_objects(item) for item in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj

    def _send_redis_connection_lost_alert(self):
        """Redis 연결이 끊어졌음을 알립니다."""
        # Slack 알림 직접 전송하지 않고 예외 발생
        raise RedisConnectionError(
            message="Redis 연결 끊김",
            operation="is_connected",
            error_details="Redis 클라이언트 연결 상태 확인 실패"
        )

    def _send_redis_connection_restored_alert(self):
        """Redis 연결이 복원되었음을 알립니다."""
        # 연결 복원은 로그만 남기고 Slack 알림은 하지 않음
        logger.info("Redis 연결이 복원되었습니다.")

    def _check_master_change(self):
        """마스터 변경을 감지하고 Slack 알림을 보냅니다."""
        try:
            if not self._sentinel:
                return
            
            # 현재 마스터 정보 조회
            current_master_info = self._sentinel.master(self._master_name)
            
            if current_master_info and self._last_master_info:
                # 마스터 정보가 변경되었는지 확인
                if current_master_info != self._last_master_info:
                    self._send_master_change_alert(current_master_info)
            
            # 마스터 정보 업데이트
            self._last_master_info = current_master_info
            
        except Exception as e:
            logger.error(f"마스터 변경 감지 중 오류: {e}")

    def _send_master_change_alert(self, new_master_info):
        """마스터 변경 시 Slack 알림을 보냅니다."""
        # 마스터 변경은 로그만 남기고 Slack 알림은 하지 않음
        logger.warning(f"Redis 마스터 변경 감지: {new_master_info}")
        
        # 마스터 변경 시에도 예외 발생 (Slack 알림을 위해)
        raise RedisConnectionError(
            message="Redis 마스터 변경 감지",
            operation="monitor_sentinel_status",
            error_details=f"새 마스터: {new_master_info}"
        )

    def monitor_sentinel_status(self) -> bool:
        """Sentinel 상태 모니터링 및 마스터 변경 감지"""
        try:
            if not self._sentinel:
                return False
            
            # 마스터/슬레이브 상태 확인
            master_info = self._sentinel.master(self._master_name)
            slaves_info = self._sentinel.slaves(self._master_name)
            
            # 마스터 변경 감지
            self._check_master_change()
            
            # 상태 로깅
            logger.info(f"Redis Sentinel 상태 - 마스터: {master_info}, 슬레이브: {len(slaves_info)}개")
            
            return True
            
        except Exception as e:
            # Slack 알림 전송
            raise RedisConnectionError(
                message="Sentinel 상태 모니터링 실패",
                operation="monitor_sentinel_status",
                error_details=str(e)
            )


# 전역 Redis 서비스 인스턴스
redis_service = RedisService()