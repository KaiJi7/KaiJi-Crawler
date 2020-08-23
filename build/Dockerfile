ARG APP_PROJECT=kaiji-crawler

FROM python:3

WORKDIR /app/$APP_PROJECT

COPY ./$APP_PROJECT .
RUN pip3 install --requirement ./requirements.txt

CMD ["python3", "main.py"]
