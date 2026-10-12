FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir "pymodbus==3.16.1"

WORKDIR /app
COPY mock_events_modbus.py run_server.py ./

EXPOSE 5020
CMD ["python", "run_server.py"]
