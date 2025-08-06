#!/bin/bash

# pricecollector Docker 엔트리포인트 스크립트

echo "🚀 pricecollector 애플리케이션 초기화..."

# .env 파일이 없으면 env.example에서 생성
if [ ! -f ".env" ]; then
    echo "📝 .env 파일이 없습니다. env.example에서 생성중..."
    cp env.example .env
    echo "✅ .env 파일이 생성되었습니다."
fi

# 필수 환경변수 확인
echo "🔍 환경변수 확인 중..."

# Redis 연결 확인
if [ -z "$REDIS_HOST" ]; then
    echo "⚠️  REDIS_HOST가 설정되지 않았습니다. 기본값 사용: redis"
    export REDIS_HOST=redis
fi

echo "📊 현재 설정:"
echo "  - REDIS_HOST: $REDIS_HOST"
echo "  - REDIS_PORT: ${REDIS_PORT:-6379}"
echo "  - LOG_LEVEL: ${LOG_LEVEL:-INFO}"
echo "  - MARKET_COUNTRIES: ${MARKET_COUNTRIES:-[\"KR\",\"US\"]}"

# 애플리케이션 시작
echo "🎯 pricecollector 애플리케이션 시작..."
exec python -m app.main