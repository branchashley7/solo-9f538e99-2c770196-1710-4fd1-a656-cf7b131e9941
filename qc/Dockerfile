FROM python:3.11-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV QC_DATA=/data QC_PORT=9100
VOLUME /data
EXPOSE 9100
CMD ["python", "qc_app.py"]
