#!/bin/bash

set -e

# 환경 변수 설정
PROJECT_DIR="/dockerProjects/quantus-price-collector"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"
NGINX_CONF="$PROJECT_DIR/nginx.conf"
IMAGE_TAG="${1:-latest}"

echo "🚀 Starting Blue-Green deployment with image tag: $IMAGE_TAG"

# 프로젝트 디렉토리로 이동
cd $PROJECT_DIR

# 현재 활성 컨테이너 확인
CURRENT_ACTIVE=$(docker ps --filter "name=pricecollector-" --filter "status=running" --format "{{.Names}}" | grep -E "(blue|green)" | head -1)

if [[ "$CURRENT_ACTIVE" == *"blue"* ]]; then
    CURRENT="blue"
    NEW="green"
    NEW_PORT=8001
else
    CURRENT="green"
    NEW="blue"  
    NEW_PORT=8000
fi

echo "📍 Current active: $CURRENT, Deploying to: $NEW"

# 1. Green 컨테이너 시작 (또는 Blue로 전환)
echo "🟢 Starting $NEW container..."
docker compose --profile $NEW up -d pricecollector-$NEW

# 2. 헬스체크 대기
echo "🔍 Waiting for $NEW container to be healthy..."
for i in {1..30}; do
    # Docker 컨테이너 헬스체크 상태 확인
    if [ "$(docker inspect --format='{{.State.Health.Status}}' pricecollector-$NEW 2>/dev/null)" = "healthy" ]; then
        echo "✅ $NEW container is healthy"
        break
    fi
    echo "⏳ Waiting for $NEW container... ($i/30)"
    sleep 10
done

# 3. 헬스체크 실패시 롤백
if [ "$(docker inspect --format='{{.State.Health.Status}}' pricecollector-$NEW 2>/dev/null)" != "healthy" ]; then
    echo "❌ $NEW container failed health check, rolling back..."
    docker compose stop pricecollector-$NEW
    docker compose rm -f pricecollector-$NEW
    exit 1
fi

# 4. Nginx 설정 업데이트 (Blue/Green 전환)
echo "🔄 Switching nginx to $NEW..."
if [ "$NEW" == "green" ]; then
    # Blue -> Green
    sed -i 's/server pricecollector-blue:8000/server pricecollector-green:8001/' $NGINX_CONF
    sed -i 's/# server pricecollector-green:8001/server pricecollector-blue:8000 backup;/' $NGINX_CONF
else
    # Green -> Blue  
    sed -i 's/server pricecollector-green:8001/server pricecollector-blue:8000/' $NGINX_CONF
    sed -i 's/server pricecollector-blue:8000 backup;/# server pricecollector-green:8001/' $NGINX_CONF
fi

# 5. Nginx 리로드
echo "♻️ Reloading nginx..."
docker exec pricecollector-nginx nginx -s reload

# 6. 최종 헬스체크
echo "🔍 Final health check..."
sleep 5
if curl -f http://localhost/health > /dev/null 2>&1; then
    echo "✅ Deployment successful!"
    
    # 7. 이전 컨테이너 정리
    echo "🧹 Cleaning up old $CURRENT container..."
    docker compose stop pricecollector-$CURRENT
    docker compose rm -f pricecollector-$CURRENT
    
    echo "🎉 Blue-Green deployment completed successfully!"
else
    echo "❌ Final health check failed, manual intervention required"
    exit 1
fi