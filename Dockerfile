FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-client \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy Streamlit requirements
COPY requirements-streamlit.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements-streamlit.txt

# Copy project files
COPY . .

# Run Streamlit
EXPOSE 8501

HEALTHCHECK CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health').read()"

CMD sh -c "streamlit run streamlit_app.py --server.port=${PORT:-8501} --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false"