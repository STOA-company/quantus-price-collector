import logging
from typing import Dict, Type, Optional
from app.brokers.base import BrokerWebSocketClient, BrokerConfig, BrokerManager, MarketType


class BrokerFactory:
    """브로커 팩토리 클래스"""
    
    def __init__(self):
        self._broker_classes: Dict[str, Type[BrokerWebSocketClient]] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def register_broker(self, broker_name: str, broker_class: Type[BrokerWebSocketClient]):
        """브로커 클래스 등록"""
        self._broker_classes[broker_name.lower()] = broker_class
        self.logger.info(f"브로커 클래스 등록: {broker_name} -> {broker_class.__name__}")
    
    def create_broker(self, broker_name: str, config: BrokerConfig = None, market_type: MarketType = MarketType.DOMESTIC) -> Optional[BrokerWebSocketClient]:
        """브로커 인스턴스 생성"""
        broker_class = self._broker_classes.get(broker_name.lower())
        
        if broker_class is None:
            self.logger.error(f"등록되지 않은 브로커: {broker_name}")
            return None
        
        try:
            # config가 None이면 기본 config에 market_type만 설정
            if config is None:
                # 브로커별 기본 설정은 브로커 클래스에서 처리하되, market_type만 전달
                broker_instance = broker_class(None, market_type=market_type)
            else:
                # config가 있으면 market_type 업데이트
                config.market_type = market_type
                broker_instance = broker_class(config)
                
            self.logger.info(f"브로커 인스턴스 생성 성공: {broker_name} (시장: {market_type.value})")
            return broker_instance
        except Exception as e:
            self.logger.error(f"브로커 인스턴스 생성 실패: {broker_name}, {e}")
            return None
    
    def get_available_brokers(self) -> list:
        """사용 가능한 브로커 목록 반환"""
        return list(self._broker_classes.keys())
    
    def is_broker_available(self, broker_name: str) -> bool:
        """브로커 사용 가능 여부 확인"""
        return broker_name.lower() in self._broker_classes


class BrokerFactoryManager:
    """브로커 팩토리 관리자 클래스"""
    
    def __init__(self):
        self.factory = BrokerFactory()
        self.broker_manager = BrokerManager()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def register_broker_class(self, broker_name: str, broker_class: Type[BrokerWebSocketClient]):
        """브로커 클래스 등록"""
        self.factory.register_broker(broker_name, broker_class)
    
    def create_and_register_broker(self, broker_name: str, config: BrokerConfig = None, market_type: MarketType = MarketType.DOMESTIC) -> Optional[BrokerWebSocketClient]:
        """브로커 생성 및 등록"""
        broker = self.factory.create_broker(broker_name, config, market_type)
        
        if broker:
            self.broker_manager.register_broker(broker)
            self.logger.info(f"브로커 생성 및 등록 완료: {broker_name} (시장: {market_type.value})")
            return broker
        else:
            self.logger.error(f"브로커 생성 및 등록 실패: {broker_name}")
            return None
    
    def get_broker(self, broker_name: str) -> Optional[BrokerWebSocketClient]:
        """브로커 조회"""
        return self.broker_manager.get_broker(broker_name)
    
    def get_all_brokers(self):
        """모든 브로커 조회"""
        return self.broker_manager.get_all_brokers()
    
    async def connect_all_brokers(self):
        """모든 브로커 연결"""
        await self.broker_manager.connect_all_brokers()
    
    async def disconnect_all_brokers(self):
        """모든 브로커 연결 해제"""
        await self.broker_manager.disconnect_all_brokers()
    
    async def subscribe_symbol_all_brokers(self, symbol: str):
        """모든 브로커에서 종목 구독"""
        await self.broker_manager.subscribe_symbol_all_brokers(symbol)
    
    async def unsubscribe_symbol_all_brokers(self, symbol: str):
        """모든 브로커에서 종목 구독 해제"""
        await self.broker_manager.unsubscribe_symbol_all_brokers(symbol)
    
    def get_available_brokers(self) -> list:
        """사용 가능한 브로커 목록 반환"""
        return self.factory.get_available_brokers()


# 전역 팩토리 관리자 인스턴스
broker_factory_manager = BrokerFactoryManager()


def register_broker(broker_name: str, broker_class: Type[BrokerWebSocketClient]):
    """브로커 등록 헬퍼 함수"""
    broker_factory_manager.register_broker_class(broker_name, broker_class)


def create_broker(broker_name: str, config: BrokerConfig = None, market_type: MarketType = MarketType.DOMESTIC) -> Optional[BrokerWebSocketClient]:
    """브로커 생성 헬퍼 함수"""
    return broker_factory_manager.create_and_register_broker(broker_name, config, market_type)


def get_broker(broker_name: str) -> Optional[BrokerWebSocketClient]:
    """브로커 조회 헬퍼 함수"""
    return broker_factory_manager.get_broker(broker_name)


def get_all_brokers():
    """모든 브로커 조회 헬퍼 함수"""
    return broker_factory_manager.get_all_brokers()


async def connect_all_brokers():
    """모든 브로커 연결 헬퍼 함수"""
    await broker_factory_manager.connect_all_brokers()


async def disconnect_all_brokers():
    """모든 브로커 연결 해제 헬퍼 함수"""
    await broker_factory_manager.disconnect_all_brokers()


async def subscribe_symbol_all_brokers(symbol: str):
    """모든 브로커에서 종목 구독 헬퍼 함수"""
    await broker_factory_manager.subscribe_symbol_all_brokers(symbol)


async def unsubscribe_symbol_all_brokers(symbol: str):
    """모든 브로커에서 종목 구독 해제 헬퍼 함수"""
    await broker_factory_manager.unsubscribe_symbol_all_brokers(symbol)


def get_available_brokers() -> list:
    """사용 가능한 브로커 목록 반환 헬퍼 함수"""
    return broker_factory_manager.get_available_brokers()
