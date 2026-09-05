FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    fastapi==0.104.1 \
    uvicorn==0.24.0 \
    httpx==0.25.2 \
    pydantic==2.5.0

# Copy server
COPY server.py .

# Health check
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8080/health', timeout=2)"

# Run server
CMD ["python", "server.py"]
