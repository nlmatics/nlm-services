# syntax=docker/dockerfile:experimental
FROM python:3.10.4-buster

RUN mkdir -p -m 0600 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts

RUN mkdir -p /app
WORKDIR /app
COPY . /app

# patch security packages
RUN apt-get update; apt-get -s dist-upgrade | grep "^Inst" | grep -i securi | awk -F " " {'print $2'} | xargs apt-get install \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

# install Java
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y \
    openjdk-11-jre-headless \
    libxml2-dev libxmlsec1-dev libxmlsec1-openssl \
    libmagic-dev \
    tesseract-ocr \
    unzip \
    lsb-release \
    && echo "deb https://notesalexp.org/tesseract-ocr5/$(lsb_release -cs)/ $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/notesalexp.list > /dev/null \
    && apt-get update -oAcquire::AllowInsecureRepositories=true \
    && apt-get install notesalexp-keyring -oAcquire::AllowInsecureRepositories=true -y --allow-unauthenticated \
    && apt-get update \
    && apt-get install -y \
    tesseract-ocr libtesseract-dev \
    && pip install --upgrade pip setuptools\
    && pip install --no-cache-dir -r requirements.txt \
    && python -m nltk.downloader stopwords \
    && python -m nltk.downloader punkt \
    && wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb -O /app/google-chrome-stable_current_amd64.deb \
    && wget https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/115.0.5790.110/linux64/chromedriver-linux64.zip -O /app/chromedriver_linux64.zip \
    && wget -P /usr/share/tesseract-ocr/5/tessdata/ https://github.com/tesseract-ocr/tessdata/raw/main/eng.traineddata \
    && apt install -y /app/google-chrome-stable_current_amd64.deb \
    && unzip /app/chromedriver_linux64.zip -d /app/ \
    && cp /app/chromedriver-linux64/chromedriver /app/ \
    && wget https://github.com/nlmatics/nlm-ingestor/raw/main/jars/tika-server-standard-nlm-modified-2.4.1_v6.jar -O /app/tika-server.jar \
    && wget https://github.com/nlmatics/nlm-ingestor/raw/main/jars/tika-server-config-default.xml -O /app/tika-server-config.xml \
    && ln -s /app/tika-server.jar /tmp/tika-server.jar \
    && touch /tmp/tika-server.jar.md5 \
    && apt purge -y build-essential\
    && apt-get clean autoclean \
    && apt-get autoremove -y \
    && rm /app/google-chrome-stable_current_amd64.deb /app/chromedriver_linux64.zip \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/ \
    && rm -rf /app/chromedriver-linux64


# we will copy the tika file to source folder in CI workflow
ENV TIKA_SERVER_JAR /app/tika-server.jar
ENV TIKA_SERVER_ENDPOINT http://localhost:9998
ENV TIKA_CONFIG_FILE /app/tika-server-config.xml
ENV PATH $PATH:/app

EXPOSE 5000

# uncomment below to use embedded tika without rabbitmq
# CMD gunicorn -b 0.0.0.0:5000 --config server/tika_daemon_check.py --name nlm-services-v2 --workers 8 --log-level=info --timeout 3600 server.__main__:app
CMD gunicorn -b 0.0.0.0:5000 --name nlm-services --workers 8 --log-level=info --timeout 3600 server.__main__:app
