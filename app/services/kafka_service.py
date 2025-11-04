import logging
import json
import time
from typing import Dict, Any
from kafka import KafkaProducer
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class KafkaService:
    def __init__(self):
        self.producer = None
        self.bootstrap_servers = os.getenv('KAFKA_BROKERS', 'localhost:19092,localhost:29092,localhost:39092').split(',')
        self.topic_prefix = 'stock_prices'
        self._is_connected = False
        
        self._last_publish_time: Dict[str, float] = {}  # 종목별 마지막 발송 시간
        self._throttle_interval = 0.5  # 0.5초 간격 (Redis와 동일)
        
        # 🔥 초기 연결 시도
        self.connect()
    
    def connect(self):
        """Kafka Producer 연결"""
        try:
            if self.producer:
                self.producer.close()
            
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(self._serialize_data(v)).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                security_protocol=os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
            )
            self._is_connected = True
            logger.info(f"✅ Kafka Producer 연결 성공: {self.bootstrap_servers}")
            return True
        except Exception as e:
            logger.error(f"❌ Kafka Producer 연결 실패: {e}")
            self._is_connected = False
            return False
    
    # �� disconnect 메서드 추가
    def disconnect(self):
        """Kafka Producer 연결 해제"""
        try:
            if self.producer:
                self.producer.close()
                self.producer = None
                self._is_connected = False
                logger.info("✅ Kafka Producer 연결 해제")
        except Exception as e:
            logger.error(f"❌ Kafka Producer 연결 해제 실패: {e}")
    
    def close(self):
        """disconnect의 별칭"""
        self.disconnect()
    
    def is_connected(self) -> bool:
        """연결 상태 확인"""
        try:
            if self.producer:
                # 간단한 메타데이터 조회로 연결 상태 확인
                self.producer.metrics()
                return True
        except Exception:
            self._is_connected = False
        return self._is_connected
    
    def reconnect(self) -> bool:
        """재연결 시도"""
        try:
            if self.producer:
                self.producer.close()
            self._connect()
            return self._is_connected
        except Exception as e:
            logger.error(f"❌ Kafka 재연결 실패: {e}")
            return False

    def publish_raw_data(self, broker_name: str, data: Dict[str, Any]) -> bool:
        """원본 데이터를 Kafka에 발행 (Redis와 동일한 쓰로틀링 적용)"""
        try:
            if not self.producer:
                return False
            
            symbol = data.get('symbol', 'unknown')
            current_time = time.time()
            
            # 🔥 Redis와 동일한 Throttling 체크: 같은 종목에 대해 0.5초 이내 발송 제한
            last_time = self._last_publish_time.get(symbol, 0)
            if current_time - last_time < self._throttle_interval:
                # logger.debug(f"Throttling: {symbol} 데이터 발송 스킵 (마지막 발송: {current_time - last_time:.2f}초 전)")
                return False
            
            # 토픽명 생성
            topic = f"{symbol}_raw_data"
            
            # 키는 브로커명, 값은 데이터
            future = self.producer.send(
                topic=topic,
                key=broker_name,
                value=data
            )
            
            # 비동기 발행 결과 확인
            record_metadata = future.get(timeout=10)
            
            # �� 발송 시간 기록 (Redis와 동일)
            self._last_publish_time[symbol] = current_time
            
            logger.debug(f"📨 Kafka 발행 성공: {topic} (파티션: {record_metadata.partition}, 오프셋: {record_metadata.offset})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Kafka 발행 오류: {e}")
            return False
    
    def publish_broker_status(self, broker_name: str, status_data: Dict[str, Any]) -> bool:
        """브로커 상태를 Kafka에 발행 (Redis와 동일한 형태)"""
        try:
            if not self.is_connected():
                return False
            
            # �� Redis와 동일한 토픽명 생성
            topic = f"broker_{broker_name}_status"  # "broker:dbfi:status" 형태
            
            future = self.producer.send(
                topic=topic,
                key=broker_name,
                value=status_data
            )
            
            record_metadata = future.get(timeout=10)
            logger.debug(f"브로커 상태 발행: {broker_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 브로커 상태 발행 오류: {e}")
            return False
    def _serialize_data(self, data: Any) -> str:
        """데이터를 JSON 직렬화 가능한 형태로 변환"""
        if isinstance(data, dict):
            return {k: self._serialize_data(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._serialize_data(item) for item in data]
        elif isinstance(data, datetime):
            return data.isoformat()
        elif hasattr(data, 'isoformat'):  # date 객체 등
            return data.isoformat()
        else:
            return data