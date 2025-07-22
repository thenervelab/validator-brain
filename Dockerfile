FROM python:3.11-slim

WORKDIR /ipfs-service-validator

ENV PYTHONPATH=/ipfs-service-validator

COPY pyproject.toml ./
COPY . .
RUN pip install -e .[dev]

CMD ["python", "-c", "print('No command specified')"]