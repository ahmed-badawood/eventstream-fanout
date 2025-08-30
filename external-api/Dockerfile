FROM python:3.11-slim
RUN pip install --no-cache-dir flask gunicorn
WORKDIR /app
COPY app.py /app/app.py
ENV PORT=8080
CMD ["gunicorn","-b",":8080","-w","1","app:app"]
