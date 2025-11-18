FROM apache/airflow:3.1.1

USER root

RUN apt-get update || true \
 && apt-get install -y --no-install-recommends ca-certificates gnupg wget curl \
 && update-ca-certificates
 
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    wget unzip curl gnupg \
    fonts-liberation libnss3 libnspr4 libxss1 libappindicator3-1 \
    libatk1.0-0 libcups2 libxcomposite1 libxrandr2 libgtk-3-0 libasound2 xdg-utils \
 && rm -rf /var/lib/apt/lists/*

RUN chromium --version

USER airflow

RUN pip install --no-cache-dir selenium webdriver-manager boto3 requests
