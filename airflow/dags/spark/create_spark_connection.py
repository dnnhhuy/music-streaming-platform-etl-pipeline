from airflow import settings
from airflow.models import Connection
conn = Connection(
        conn_id="sparkLocal",
        conn_type="Spark",
        host="spark://spark-master",
        port="7077"
) #create a connection object
session = settings.Session() # get the session
session.add(conn)
session.commit() # it will insert the connection object programmatically.