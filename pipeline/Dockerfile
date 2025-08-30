FROM bitnami/spark:3.5
USER root
RUN pip install --no-cache-dir redis clickhouse-connect requests pandas
COPY app.py /app/app.py
CMD ["/opt/bitnami/spark/bin/spark-submit","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3","/app/app.py"]
