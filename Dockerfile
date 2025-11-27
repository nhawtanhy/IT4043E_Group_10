# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 9092 available to the world outside this container (Kafka)
EXPOSE 9092

# Run main.py when the container launches
CMD ["python", "main.py"]
