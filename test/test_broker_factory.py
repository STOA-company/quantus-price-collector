#!/usr/bin/env python3
"""
브로커 팩토리 시스템 테스트 예제
"""

import asyncio
import logging
from app.brokers import (
    BrokerConfig,
    create_broker,
    get_broker,
    get_all_brokers,
    connect_all_brokers,
    disconnect_all_brokers,
    subscribe_symbol_all_brokers,
    unsubscribe_symbol_all_brokers,
    get_available_brokers
)


# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def test_broker_factory():
    """브로커 팩토리 시스템 테스트"""
    
    logger.info("=== 브로커 팩토리 시스템 테스트 시작 ===")
    
    # 1. 사용 가능한 브로커 확인
    available_brokers = get_available_brokers()
    logger.info(f"사용 가능한 브로커: {available_brokers}")
    
    # 2. DBFI 브로커 생성 (config 없이 - 자동으로 config.dbfi 사용)
    dbfi_broker = create_broker("dbfi")
    if dbfi_broker:
        logger.info("DBFI 브로커 생성 성공")
    else:
        logger.error("DBFI 브로커 생성 실패")
        return
    
    # 3. 브로커 조회 테스트
    broker = get_broker("dbfi")
    if broker:
        logger.info(f"브로커 조회 성공: {broker.get_broker_name()}")
    
    # 4. 모든 브로커 조회
    all_brokers = get_all_brokers()
    logger.info(f"등록된 모든 브로커: {list(all_brokers.keys())}")
    
    # 5. 브로커 연결 테스트
    logger.info("브로커 연결 테스트 시작...")
    await connect_all_brokers()
    
    # 6. 종목 구독 테스트
    test_symbol = "000660"  # 삼성전자
    logger.info(f"종목 구독 테스트: {test_symbol}")
    await subscribe_symbol_all_brokers(test_symbol)
    
    # 7. 데이터 수신 테스트 (5초간)
    logger.info("데이터 수신 테스트 시작 (5초간)...")
    try:
        async for data in broker.receive_data():
            logger.info(f"수신된 데이터: {data}")
            break  # 첫 번째 데이터만 출력하고 종료
    except Exception as e:
        logger.error(f"데이터 수신 중 오류: {e}")
    
    # 8. 종목 구독 해제
    logger.info(f"종목 구독 해제: {test_symbol}")
    await unsubscribe_symbol_all_brokers(test_symbol)
    
    # 9. 브로커 연결 해제
    logger.info("브로커 연결 해제...")
    await disconnect_all_brokers()
    
    logger.info("=== 브로커 팩토리 시스템 테스트 완료 ===")


async def test_individual_broker():
    """개별 브로커 테스트"""
    
    logger.info("=== 개별 브로커 테스트 시작 ===")
    
    # 브로커 생성 (config 없이)
    broker = create_broker("dbfi")
    if not broker:
        logger.error("브로커 생성 실패")
        return
    
    # 연결 테스트
    logger.info("브로커 연결 테스트...")
    success = await broker.test_connection()
    if success:
        logger.info("브로커 연결 테스트 성공")
    else:
        logger.error("브로커 연결 테스트 실패")
    
    logger.info("=== 개별 브로커 테스트 완료 ===")


async def test_config_override():
    """설정 오버라이드 테스트"""
    
    logger.info("=== 설정 오버라이드 테스트 시작 ===")
    
    # 커스텀 설정으로 브로커 생성
    custom_config = BrokerConfig(
        api_key="custom_api_key",
        api_secret="custom_api_secret",
        websocket_url="wss://custom.example.com",
        batch_size=50,
        available_sessions=3
    )
    
    broker = create_broker("dbfi", custom_config)
    if broker:
        logger.info("커스텀 설정으로 브로커 생성 성공")
        logger.info(f"설정 확인: batch_size={broker.config.batch_size}")
    else:
        logger.error("커스텀 설정으로 브로커 생성 실패")
    
    logger.info("=== 설정 오버라이드 테스트 완료 ===")


async def main():
    """메인 함수"""
    try:
        # 팩토리 시스템 테스트
        await test_broker_factory()
        
        # 개별 브로커 테스트
        await test_individual_broker()
        
        # 설정 오버라이드 테스트
        await test_config_override()
        
    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {e}")


if __name__ == "__main__":
    asyncio.run(main()) 