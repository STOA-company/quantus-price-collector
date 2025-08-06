#!/usr/bin/env python3
"""
시장 상태 확인 스크립트

현재 시장 상태와 스케줄러 정보를 확인합니다.
"""

import asyncio
import logging
import sys
from datetime import datetime

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def check_market_status():
    """현재 시장 상태 확인 (동기 버전)"""
    try:
        from app.schedule.date_utils import is_market_open, get_market_state_description, get_next_market_open_time
        
        print("=" * 60)
        print("📊 한국 주식시장 현재 상태")
        print("=" * 60)
        
        # 설정 로드
        from app.schedule.config import load_scheduler_config
        config = load_scheduler_config()
        
        # 현재 시장 정보
        market_info = is_market_open("KR", config.market_hours)
        state_desc = get_market_state_description(market_info)
        
        print(f"🕐 현재 시간: {market_info['current_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📈 시장 상태: {state_desc}")
        print(f"📅 영업일 여부: {'예' if market_info['is_business_day'] else '아니오'}")
        
        print("\n" + "-" * 40)
        print("세부 상태:")
        print(f"  🟢 정규거래시간: {'예' if market_info['is_trading_hours'] else '아니오'}")
        print(f"  🟡 장시작전: {'예' if market_info['is_pre_market'] else '아니오'}")
        print(f"  🟠 장마감후: {'예' if market_info['is_after_market'] else '아니오'}")
        print(f"  🔴 휴장: {'예' if market_info['is_closed'] else '아니오'}")
        
        # 다음 장 시작 시간
        next_open = get_next_market_open_time("KR")
        
        # 설정된 시장 운영시간 표시
        print(f"\n⚙️  설정된 시장 운영시간:")
        print(f"  장시작전: {config.market_hours.pre_market_start}")
        print(f"  정규시간: {config.market_hours.market_open} - {config.market_hours.market_close}")
        print(f"  장마감후: {config.market_hours.market_close} - {config.market_hours.after_market_end}")
        print(f"\n⏰ 다음 장 시작: {next_open.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 시간 차이 계산
        time_diff = next_open - market_info['current_time']
        if time_diff.total_seconds() > 0:
            hours, remainder = divmod(time_diff.total_seconds(), 3600)
            minutes, _ = divmod(remainder, 60)
            print(f"⏳ 남은 시간: {int(hours)}시간 {int(minutes)}분")
        
        print("=" * 60)
        
        return True
        
    except ImportError as e:
        print(f"❌ 모듈 import 오류: {e}")
        print("💡 필요한 패키지를 설치해주세요: pip install pytz exchange_calendars")
        return False
    except Exception as e:
        print(f"❌ 시장 상태 확인 중 오류: {e}")
        import traceback
        traceback.print_exc()
        return False

async def check_scheduler_status():
    """스케줄러 상태 확인 (비동기 버전)"""
    try:
        from app.schedule import MarketScheduler
        
        print("🔧 MarketScheduler 상태 확인")
        print("=" * 60)
        
        scheduler = MarketScheduler()
        status = scheduler.get_status()
        
        print("📊 스케줄러 정보:")
        print(f"  활성화: {status['scheduler']['enabled']}")
        print(f"  모드: {status['scheduler']['mode']}")
        print(f"  체크 간격: {status['scheduler']['check_interval']}초")
        
        print("\n🌏 시장 정보:")
        print(f"  국가: {status['market']['country']}")
        print(f"  현재 상태: {status['market']['current_state_description']}")
        print(f"  마지막 상태 변경: {status['market']['last_state_change'] or '없음'}")
        
        print("\n📈 시장 세부 정보:")
        market_info = status['market']['market_info']
        print(f"  영업일: {'예' if market_info['is_business_day'] else '아니오'}")
        print(f"  정규시간: {'예' if market_info['is_trading_hours'] else '아니오'}")
        print(f"  장시작전: {'예' if market_info['is_pre_market'] else '아니오'}")
        print(f"  장마감후: {'예' if market_info['is_after_market'] else '아니오'}")
        print(f"  휴장: {'예' if market_info['is_closed'] else '아니오'}")
        
        print("=" * 60)
        
        return True
        
    except ImportError as e:
        print(f"❌ 모듈 import 오류: {e}")
        return False
    except Exception as e:
        print(f"❌ 스케줄러 상태 확인 중 오류: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """메인 함수"""
    print("🚀 시장 상태 및 스케줄러 확인 도구")
    print("현재 시간:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print()
    
    success_count = 0
    
    # 1. 시장 상태 확인
    if check_market_status():
        success_count += 1
        print("✅ 시장 상태 확인 성공\n")
    else:
        print("❌ 시장 상태 확인 실패\n")
    
    # 2. 스케줄러 상태 확인  
    if await check_scheduler_status():
        success_count += 1
        print("✅ 스케줄러 상태 확인 성공")
    else:
        print("❌ 스케줄러 상태 확인 실패")
    
    print(f"\n📊 결과: {success_count}/2 성공")
    
    if success_count == 2:
        print("🎉 모든 확인 완료!")
        return 0
    else:
        print("⚠️  일부 확인 실패")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)