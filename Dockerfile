# Use python 3.10-slim (Explicitly using 3.10 is safer)
FROM python:3.10-slim

WORKDIR /app

# Removed software-properties-common as it's not available in Debian 13 (Trixie)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy everything else
COPY . .

# Create the data folder structure inside the container
RUN mkdir -p data/bronze

EXPOSE 7860

CMD ["streamlit", "run", "app.py", "--server.port", "7860", "--server.address", "0.0.0.0"]