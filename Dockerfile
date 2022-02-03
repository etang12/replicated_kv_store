FROM python:3.8
RUN pip3 install --upgrade pip
WORKDIR /app
COPY . /app
RUN pip3 install -r requirements.txt
EXPOSE 8085
ENTRYPOINT ["python3"]
CMD ["app.py" ]