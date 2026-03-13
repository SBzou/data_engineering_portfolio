FROM python:3.10-slim

# ---------- 1. Java for Spark ----------
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ---------- 2. Working directory ----------
WORKDIR /app

# ---------- 3. Install Python Dependencies ----------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------- 4. Code copy ----------
COPY . .
RUN mkdir -p data logs

# ---------- 5. Setup Spark variable for Windows ----------
ENV SPARK_LOCAL_HOSTNAME=localhost

# ---------- 6. Pipeline run ----------
CMD ["python", "-m", "src.scripts.main"]