# Test-only OIDC provider for stage S4. Never published, never deployed.
FROM python:3.13-alpine
RUN pip install --no-cache-dir "cryptography>=42"
COPY oidc.py /oidc.py
EXPOSE 8080
CMD ["python", "-u", "/oidc.py"]
