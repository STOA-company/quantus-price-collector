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

# 최신 이미지 강제 pull
echo "📥 Pulling latest image with tag: $IMAGE_TAG"
export GITHUB_REPOSITORY_OWNER=${GITHUB_REPOSITORY_OWNER:-stoa-company}
export IMAGE_TAG=${IMAGE_TAG}
docker pull ghcr.io/${GITHUB_REPOSITORY_OWNER}/quantus-price-collector:${IMAGE_TAG}

# 기본 서비스들 먼저 시작 (redis 클러스터, nginx)
echo "🔧 Starting base services (redis cluster, nginx)..."
docker compose up -d redis-master redis-slave redis-sentinel-1 redis-sentinel-2 redis-sentinel-3 nginx

# Redis 서비스들이 healthy 상태가 될 때까지 대기
echo "⏳ Waiting for Redis services to be healthy..."
for i in {1..60}; do
    if docker compose ps --services --filter "status=running" | grep -q redis-master && \
       docker compose ps --services --filter "status=running" | grep -q redis-slave; then
        echo "✅ Redis services are running"
        break
    fi
    echo "⏳ Waiting for Redis services... ($i/60)"
    sleep 5
done

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

# 1. 새로운 컨테이너 시작
echo "🟢 Starting $NEW container..."
if [ "$NEW" == "green" ]; then
    docker compose --profile green up -d pricecollector-green
else
    docker compose up -d pricecollector-blue
fi

# 2. 컨테이너 실행 확인 대기
echo "🔍 Waiting for $NEW container to be running..."
for i in {1..30}; do
    # 컨테이너가 실행 중인지 확인
    if [ "$(docker inspect --format='{{.State.Status}}' pricecollector-$NEW 2>/dev/null)" = "running" ]; then
        echo "✅ $NEW container is running"
        break
    fi
    echo "⏳ Waiting for $NEW container... ($i/30)"
    sleep 10
done

# 3. 컨테이너 실행 확인 실패시 롤백
if [ "$(docker inspect --format='{{.State.Status}}' pricecollector-$NEW 2>/dev/null)" != "running" ]; then
    echo "❌ $NEW container is not running, rolling back..."
    docker compose stop pricecollector-$NEW
    docker compose rm -f pricecollector-$NEW
    exit 1
fi

# 4. 애플리케이션 헬스체크 (포트로 직접 확인)
echo "🔍 Health checking $NEW container on port $NEW_PORT..."
for i in {1..30}; do
    if curl -f http://localhost:$NEW_PORT/health > /dev/null 2>&1; then
        echo "✅ $NEW container health check passed"
        break
    fi
    echo "⏳ Waiting for $NEW container health check... ($i/30)"
    sleep 5
done

# 5. 애플리케이션 헬스체크 실패시 롤백
if ! curl -f http://localhost:$NEW_PORT/health > /dev/null 2>&1; then
    echo "❌ $NEW container health check failed, rolling back..."
    docker compose stop pricecollector-$NEW
    docker compose rm -f pricecollector-$NEW
    exit 1
fi

# 6. Nginx 설정 업데이트 (Blue/Green 전환)
echo "🔄 Switching nginx to $NEW..."
if [ "$NEW" == "green" ]; then
    # Blue -> Green
    sed -i 's/server pricecollector-blue:8000 max_fails=3 fail_timeout=30s;/server pricecollector-green:8001 max_fails=3 fail_timeout=30s;/' $NGINX_CONF
    sed -i 's/# server pricecollector-green:8001 max_fails=3 fail_timeout=30s backup;/# server pricecollector-blue:8000 max_fails=3 fail_timeout=30s backup;/' $NGINX_CONF
else
    # Green -> Blue  
    sed -i 's/server pricecollector-green:8001 max_fails=3 fail_timeout=30s;/server pricecollector-blue:8000 max_fails=3 fail_timeout=30s;/' $NGINX_CONF
    sed -i 's/# server pricecollector-blue:8000 max_fails=3 fail_timeout=30s backup;/# server pricecollector-green:8001 max_fails=3 fail_timeout=30s backup;/' $NGINX_CONF
fi

# 7. Nginx 설정 검증
echo "🔍 Testing nginx configuration..."
if ! docker compose exec nginx nginx -t; then
    echo "❌ Nginx configuration test failed, rolling back..."
    # Nginx 설정 되돌리기
    if [ "$NEW" == "green" ]; then
        sed -i 's/server pricecollector-green:8001 max_fails=3 fail_timeout=30s;/server pricecollector-blue:8000 max_fails=3 fail_timeout=30s;/' $NGINX_CONF
        sed -i 's/# server pricecollector-blue:8000 max_fails=3 fail_timeout=30s backup;/# server pricecollector-green:8001 max_fails=3 fail_timeout=30s backup;/' $NGINX_CONF
    else
        sed -i 's/server pricecollector-blue:8000 max_fails=3 fail_timeout=30s;/server pricecollector-green:8001 max_fails=3 fail_timeout=30s;/' $NGINX_CONF
        sed -i 's/# server pricecollector-green:8001 max_fails=3 fail_timeout=30s backup;/# server pricecollector-blue:8000 max_fails=3 fail_timeout=30s backup;/' $NGINX_CONF
    fi
    docker compose stop pricecollector-$NEW
    docker compose rm -f pricecollector-$NEW
    exit 1
fi

# 8. Nginx 재시작 (설정 변경 적용)
echo "♻️ Restarting nginx to apply new configuration..."
docker compose restart nginx

# 9. 최종 헬스체크 (nginx를 통한 확인)
echo "🔍 Final health check through nginx..."
sleep 10
for i in {1..10}; do
    if curl -f http://localhost/health > /dev/null 2>&1; then
        echo "✅ Deployment successful!"
        DEPLOYMENT_SUCCESS=true
        break
    fi
    echo "⏳ Final health check... ($i/10)"
    sleep 5
done

if [ "$DEPLOYMENT_SUCCESS" = true ]; then
    # 10. 이전 컨테이너 정리
    echo "🧹 Cleaning up old $CURRENT container..."
    if [ -n "$CURRENT_ACTIVE" ]; then
        docker compose stop pricecollector-$CURRENT
        docker compose rm -f pricecollector-$CURRENT
    fi
    
    echo "🎉 Blue-Green deployment completed successfully!"
else
    echo "❌ Final health check failed, rolling back..."
    # Nginx 설정 되돌리기
    if [ "$NEW" == "green" ]; then
        sed -i 's/server pricecollector-green:8001 max_fails=3 fail_timeout=30s;/server pricecollector-blue:8000 max_fails=3 fail_timeout=30s;/' $NGINX_CONF
        sed -i 's/# server pricecollector-blue:8000 max_fails=3 fail_timeout=30s backup;/# server pricecollector-green:8001 max_fails=3 fail_timeout=30s backup;/' $NGINX_CONF
    else
        sed -i 's/server pricecollector-blue:8000 max_fails=3 fail_timeout=30s;/server pricecollector-green:8001 max_fails=3 fail_timeout=30s;/' $NGINX_CONF
        sed -i 's/# server pricecollector-green:8001 max_fails=3 fail_timeout=30s backup;/# server pricecollector-blue:8000 max_fails=3 fail_timeout=30s backup;/' $NGINX_CONF
    fi
    docker compose restart nginx
    docker compose stop pricecollector-$NEW
    docker compose rm -f pricecollector-$NEW
    exit 1
fi