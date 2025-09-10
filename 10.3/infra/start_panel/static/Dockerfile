FROM python:3.9-slim

WORKDIR /app
COPY static/ /app/static/
COPY static_server.py /app/

EXPOSE 8000

CMD ["python", "static_server.py"]
