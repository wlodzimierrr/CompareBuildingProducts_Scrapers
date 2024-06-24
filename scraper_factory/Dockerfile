FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    # any other dependencies you might need
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /scraper_factory

# Copy the requirements file
COPY ./requirements.txt /scraper_factory/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . /scraper_factory
COPY ./src/.env /scraper_factory/src/.env

EXPOSE 5000         

CMD ["python", "-u", "/scraper_factory/src/main.py"]


