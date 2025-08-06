import os
from typing import Optional, List
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    """DBFI 설정"""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
        env_prefix="DBFI_",
        field_mapping={
            'api_key': 'API_KEY',
            'api_secret': 'API_SECRET',
            'heartbeat_timeout': 'HEARTBEAT_TIMEOUT',
            'reconnect_delay': 'RECONNECT_DELAY',
            'websocket_url': 'WS_URL',
            'batch_size': 'BATCH_SIZE',
            'available_sessions': 'AVAILABLE_SESSIONS'
        }
    )
    
    api_key: str = ""
    api_secret: str = ""
    heartbeat_timeout: float = 10.0
    reconnect_delay: float = 5.0
    websocket_url: str = "wss://openapi.dbsec.co.kr:7070"
    batch_size: int = 20
    available_sessions: int = 2

class BrokerConfig(BaseSettings):
    """관심 종목 설정"""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
        field_mapping={
            'watch_symbols_domestic': 'WATCH_SYMBOLS_DOMESTIC',
            'watch_symbols_foreign': 'WATCH_SYMBOLS_FOREIGN',
            'enabled_brokers': 'ENABLED_BROKERS'
        }
    )

    watch_symbols_domestic: List[str] = [] 
    watch_symbols_foreign: List[str] = [] 
    enabled_brokers: List[str] = []


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
