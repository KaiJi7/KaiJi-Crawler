ARG APP_PROJECT=sports_data_crawler

FROM python:3

WORKDIR /app/sports_data_crawler

COPY ./$APP_PROJECT .
RUN pip3 install --requirement ./requirements.txt

CMD ["python3", "main.py", "update_db", "-k"]
