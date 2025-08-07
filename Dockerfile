# pricecollector - 실시간 주식 데이터 수집기
# Multi-stage build for Python application

# Build Stage
FROM python:3.11-slim as builder

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# 가상환경 설정
ENV VENV_PATH=/opt/venv
ENV PATH="$VENV_PATH/bin:$PATH"

# 시스템 의존성 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python 가상환경 생성
RUN python -m venv $VENV_PATH

# Python 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


# Runtime Stage  
FROM python:3.11-slim as runtime

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# 가상환경 경로 설정
ENV VENV_PATH=/opt/venv
ENV PATH="$VENV_PATH/bin:$PATH"

# 런타임 의존성 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 애플리케이션 사용자 생성
RUN groupadd -r pricecollector && useradd -r -g pricecollector pricecollector

# 가상환경 복사
COPY --from=builder $VENV_PATH $VENV_PATH

# 작업 디렉토리 설정
WORKDIR /app

# 애플리케이션 코드 복사
COPY . .

# 권한 설정
RUN chown -R pricecollector:pricecollector /app
USER pricecollector


# 포트 노출 (필요시)
# EXPOSE 8080

# 실행 명령
CMD ["python", "-m", "app.main"]
