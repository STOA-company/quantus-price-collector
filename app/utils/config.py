import os
from typing import Optional, List
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class RedisConfig(BaseSettings):
    """Redis 연결 설정"""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )
    
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    redis_ssl: bool = False
    redis_ssl_cert_reqs: Optional[str] = None
    
    # 연결 풀 설정
    redis_max_connections: int = 10
    redis_retry_on_timeout: bool = True
    redis_socket_connect_timeout: int = 5
    redis_socket_timeout: int = 5


class DBFIConfig(BaseSettings):
    """DBFI 설정 - 국내/해외 별도 AppKey 지원"""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )
    
    # 🔥 국내 시장 전용 설정
    domestic_api_key: str = Field(default="", alias="DBFI_DOMESTIC_API_KEY")
    domestic_api_secret: str = Field(default="", alias="DBFI_DOMESTIC_API_SECRET")
    domestic_account_no: str = Field(default="", alias="DBFI_DOMESTIC_ACCOUNT_NO")
    domestic_ws_url: str = Field(default="wss://openapi.dbsec.co.kr:7070", alias="DBFI_DOMESTIC_WS_URL")
    domestic_batch_size: int = Field(default=20, alias="DBFI_DOMESTIC_BATCH_SIZE")
    domestic_available_sessions: int = Field(default=2, alias="DBFI_DOMESTIC_AVAILABLE_SESSIONS")
    
    # 🔥 해외 시장 전용 설정
    foreign_api_key: str = Field(default="", alias="DBFI_FOREIGN_API_KEY")
    foreign_api_secret: str = Field(default="", alias="DBFI_FOREIGN_API_SECRET")
    foreign_account_no: str = Field(default="", alias="DBFI_FOREIGN_ACCOUNT_NO")
    foreign_ws_url: str = Field(default="wss://openapi.dbsec.co.kr:7070", alias="DBFI_FOREIGN_WS_URL")
    foreign_batch_size: int = Field(default=20, alias="DBFI_FOREIGN_BATCH_SIZE")
    foreign_available_sessions: int = Field(default=2, alias="DBFI_FOREIGN_AVAILABLE_SESSIONS")
    
    # 기존 통합 설정 (하위 호환성)
    api_key: str = Field(default="", alias="DBFI_API_KEY")
    api_secret: str = Field(default="", alias="DBFI_API_SECRET")
    websocket_url: str = Field(default="wss://openapi.dbsec.co.kr:7070", alias="DBFI_WS_URL")
    batch_size: int = Field(default=20, alias="DBFI_BATCH_SIZE")
    available_sessions: int = Field(default=2, alias="DBFI_AVAILABLE_SESSIONS")
    
    # 공통 설정
    heartbeat_timeout: float = Field(default=10.0, alias="DBFI_HEARTBEAT_TIMEOUT")
    reconnect_delay: float = Field(default=5.0, alias="DBFI_RECONNECT_DELAY")

    def get_config_for_market(self, market_type) -> dict:
        """시장 타입에 따른 설정 반환"""
        from ..brokers.base import MarketType

        if market_type == MarketType.DOMESTIC:
            config = {
                'api_key': self.domestic_api_key or self.api_key,
                'api_secret': self.domestic_api_secret or self.api_secret,
                'account_no': self.domestic_account_no,
                'websocket_url': self.domestic_ws_url or self.websocket_url,
                'batch_size': self.domestic_batch_size or self.batch_size,
                'available_sessions': self.domestic_available_sessions or self.available_sessions
            }
        else:  # FOREIGN
            config = {
                'api_key': self.foreign_api_key or self.api_key,
                'api_secret': self.foreign_api_secret or self.api_secret,
                'account_no': self.foreign_account_no,
                'websocket_url': self.foreign_ws_url or self.websocket_url,
                'batch_size': self.foreign_batch_size or self.batch_size,
                'available_sessions': self.foreign_available_sessions or self.available_sessions
            }
        
        # 🔥 로깅 추가
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"🔑 [{market_type.value}] DBFI 설정 로드:")
        logger.info(f"   API Key: {config['api_key'][:10]}..." if config['api_key'] else "   API Key: 설정되지 않음")
        logger.info(f"   API Secret: {'설정됨' if config['api_secret'] else '설정되지 않음'}")
        logger.info(f"   WebSocket URL: {config['websocket_url']}")
        
        return config
    
    def validate_market_config(self, market_type) -> bool:
        """시장별 설정 유효성 검증"""
        config = self.get_config_for_market(market_type)
        return bool(config['api_key'] and config['api_secret'] and config['websocket_url'])


class BrokerConfig(BaseSettings):
    """관심 종목 설정"""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )

    watch_symbols_domestic: List[str] = Field(default=[], alias="WATCH_SYMBOLS_DOMESTIC")
    watch_symbols_foreign: List[str] = Field(default=[], alias="WATCH_SYMBOLS_FOREIGN")
    enabled_brokers: List[str] = Field(default=[], alias="ENABLED_BROKERS")


class AppConfig(BaseSettings):
    """애플리케이션 전체 설정"""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )
    
    app_name: str = "CLTest"
    debug: bool = False
    log_level: str = "DEBUG"
    
    # Redis 설정
    redis: RedisConfig = RedisConfig()
    
    # DBFI 설정
    dbfi: DBFIConfig = DBFIConfig()

    # broker 설정
    broker: BrokerConfig = BrokerConfig()


# 전역 설정 인스턴스
config = AppConfig()