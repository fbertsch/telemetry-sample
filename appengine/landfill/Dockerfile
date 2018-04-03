FROM python:3.6
WORKDIR /app
RUN groupadd --gid 1001 app && useradd -m -g app --uid 1001 --shell /usr/sbin/nologin app
RUN mkdir -p /var/log/app_engine && chown app /var/log/app_engine && chmod 777 /var/log
RUN apt update && \
    apt install -y lsb-release && \
    echo "deb http://packages.cloud.google.com/apt cloud-sdk-$(lsb_release -c -s) main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    apt remove -y lsb-release && \
    curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt update && \
    apt install -y google-cloud-sdk
ADD requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt
USER app
ENV FLASK_APP=app.py
ADD app.py /app
ADD landfill.py /app
CMD ["python", "/app/landfill.py"]
