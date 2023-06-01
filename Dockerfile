FROM python

COPY ./python ./python
#WORKDIR /python
RUN pip install requests
RUN pip install pyyaml
RUN pip install py7zr
RUN pip install psycopg2
RUN pip install pandas
RUN pip install chardet
RUN pip install flask
RUN pip install sqlalchemy
RUN pip install redis
RUN pip install pymongo


#EXPOSE 80
CMD ["python", "python/main.py"]