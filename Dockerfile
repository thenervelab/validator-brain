FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Using volume mount for development
# COPY . .

CMD ["python", "main.py"]