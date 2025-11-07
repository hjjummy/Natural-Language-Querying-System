FROM python:3.11

# ────────────────────────────────
# 1. 환경 변수 및 타임존 설정
# ────────────────────────────────
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Seoul

WORKDIR /app

# ────────────────────────────────
# 2. 의존성 설치
# ────────────────────────────────
COPY requirements.txt /app/
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir --default-timeout=200 -r requirements.txt -i https://mirror.kakao.com/pypi/simple

# ────────────────────────────────
# 3. 소스 코드 및 환경 파일 복사
# ────────────────────────────────
COPY . /app/

# ────────────────────────────────
# 4. 디렉토리 보장 및 권한 부여
# ────────────────────────────────
RUN mkdir -p /app/data /app/workspace/cache /app/workspace/sessions /app/logs && \
    chmod -R 755 /app

# ────────────────────────────────
# 5. 포트 및 실행 명령
# ────────────────────────────────
EXPOSE 8501

CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.enableCORS=false", \
     "--server.headless=true"]
