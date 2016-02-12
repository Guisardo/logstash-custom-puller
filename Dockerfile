FROM python

RUN pip install --upgrade pip && pip install decorator

COPY puller.py /

CMD ["python", "-u", "/puller.py"]