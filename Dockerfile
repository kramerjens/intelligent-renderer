# Start from Google's official Python 3.12 base image.
FROM python:3.12-slim

# Set the working directory inside the container.
WORKDIR /app

# Copy the requirements file into the container.
COPY requirements.txt .

# Install the Python packages specified in requirements.txt.
RUN pip install --no-cache-dir -r requirements.txt

# --- THIS IS THE CRITICAL STEP ---
# Run the Playwright command to install both the browsers AND their system dependencies.
RUN playwright install chromium --with-deps

# Copy the rest of your application code (main.py) into the container.
COPY . .

# Set the command that will run when the container starts.
# This tells Google's Functions Framework to start the web server.
CMD ["functions-framework", "--target=intelligent_renderer"]
