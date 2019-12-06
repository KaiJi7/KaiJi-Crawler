ARG APP_PROJECT=gambling_simulation

FROM python:3

WORKDIR /app/gambling_simulation

COPY ./$APP_PROJECT .
RUN pip3 install --requirement ./requirements.txt

CMD ["python3", "main.py", "update_db", "-k"]
