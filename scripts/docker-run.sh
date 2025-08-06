#!/bin/bash

# pricecollector Docker 실행 스크립트

echo "🚀 pricecollector 애플리케이션 시작..."

# 현재 디렉토리가 pricecollector 인지 확인
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ 오류: pricecollector 프로젝트 루트 디렉토리에서 실행해주세요"
    exit 1
fi

# .env 파일 확인
if [ ! -f ".env" ]; then
    echo "⚠️  .env 파일이 없습니다. env.example을 복사해서 .env 파일을 생성하세요."
    echo "📝 명령어: cp env.example .env"
    echo ""
    read -p "env.example을 .env로 복사하시겠습니까? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cp env.example .env
        echo "✅ .env 파일이 생성되었습니다. DBFI API 키를 설정해주세요."
        echo "📝 편집: nano .env"
    else
        echo "❌ .env 파일 없이는 실행할 수 없습니다."
        exit 1
    fi
fi

# Docker Compose로 실행
echo "🐳 Docker Compose 실행 중..."
docker-compose up -d

if [ $? -eq 0 ]; then
    echo "✅ pricecollector 애플리케이션이 시작되었습니다."
    echo ""
    echo "📋 유용한 명령어:"
    echo "  docker-compose logs -f pricecollector  # 애플리케이션 로그 확인"
    echo "  docker-compose logs -f redis   # Redis 로그 확인"
    echo "  docker-compose ps              # 컨테이너 상태 확인"
    echo "  docker-compose down            # 애플리케이션 정지"
    echo ""
    echo "📊 로그 확인 중..."
    docker-compose logs -f pricecollector
else
    echo "❌ 애플리케이션 시작 실패"
    exit 1
fi