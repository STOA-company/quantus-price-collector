#!/bin/bash

echo "🚀 pricecollector Docker 이미지 빌드 시작..."

# 현재 디렉토리가 pricecollector 인지 확인
if [ ! -f "app/main.py" ]; then
    echo "❌ 오류: pricecollector 프로젝트 루트 디렉토리에서 실행해주세요"
    exit 1
fi

# Docker 이미지 빌드
docker build -t pricecollector:latest .

if [ $? -eq 0 ]; then
    echo "✅ Docker 이미지 빌드 완료: pricecollector:latest"
    echo ""
    echo "📋 사용 가능한 명령어:"
    echo "  docker-compose up -d    # 백그라운드 실행"
    echo "  docker-compose up       # 포그라운드 실행"
    echo "  docker-compose logs -f  # 로그 확인"
    echo "  docker-compose down     # 정지 및 제거"
else
    echo "❌ Docker 이미지 빌드 실패"
    exit 1
fi