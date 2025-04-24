# Base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy pyproject.toml
COPY pyproject.toml .

# Install uv and use it to install dependencies
RUN pip install uv && \
    uv pip install --system -e .

# Copy the rest of the application
COPY . .

# Command to run when the container starts
CMD ["python", "main.py"]