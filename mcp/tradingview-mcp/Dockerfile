FROM docker.io/python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*
RUN git clone --depth=1 https://github.com/bidouilles/mcp-tradingview-server.git . && \
    pip install --no-cache-dir -e .
EXPOSE 8000
CMD ["python", "/app/tradingview_server.py", "--transport", "http", "--host", "0.0.0.0", "--port", "8000"]
