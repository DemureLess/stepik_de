FROM python:3.9-slim

WORKDIR /app
COPY infra/start_panel/static/index.html /app/static/
COPY infra/start_panel/static_server.py /app/

EXPOSE 8000

CMD ["python", "static_server.py"]