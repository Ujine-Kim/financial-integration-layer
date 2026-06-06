FROM apache/airflow:2.9.2-python3.11

# Устанавливаем Java (нужна для PySpark)
USER root
RUN rm -f /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Определяем JAVA_HOME динамически — путь зависит от архитектуры (amd64/arm64)
RUN JAVA_PATH=$(dirname $(dirname $(readlink -f $(which java)))) && \
    ln -sfn $JAVA_PATH /usr/local/java && \
    echo "Detected JAVA_HOME: $JAVA_PATH"

ENV JAVA_HOME=/usr/local/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Устанавливаем Python зависимости от имени пользователя airflow
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
