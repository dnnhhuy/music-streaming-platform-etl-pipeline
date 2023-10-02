# **Music Event Data Pipeline**
The main purpose of the project is building a data pipeline which is using various tools for processing data such as Kafka, Spark, Airflow, Hadoop, etc. 

## **Overview**


## **Data**
[Eventsim](https://github.com/Interana/eventsim) is the main data sources being used to generates events that simulates from a music streaming platform. The generated data based on user interaction on pages, user authentication and user listening events. 

## **Architecture**
Lamda architecture is used to handle for both real time events visualization and data analysis from batch data events. Docker containerize all the services which are available in this project to make them organizable and manageable. The architecture diagram is show below:

![architecture](images/architecture.jpg)

### **ETL flow**:
The generated data is sent to Kafka. Lamda architecture is used to handle for both streaming process and batch process.
#### **Streaming Process**: 
Data is extracted, transformed following specific purposes of visualization and stored in Cassandra for every minute. Presto is an intermediate distributed query engine for superset to interact with Cassandra.

#### **Batch Process**: 
Data is extracted, stored in Hadoop HDFS, transformed to **Star Schema**  and stored in Hive dataware house for Data Analysis to answer following questions:
* "How many users that logged in?" in different granularity such as minute, second, hour, day, week, month, year.
* "What is the top songs, artists?" in different granularity: minute, second, hour, day, week, month, year.
* "What is the most location that listen on the platform the most?" in different granularity.
* How many songs that has been played? in differnt granularity: minute, second, hour, day, week, month, year.
* What is distribution of users based on genders, level?
* What is average number of songs listened by users?
* etc.

#### Star Schema:
The schema includes:
* Fact tables:
    * fact_listen
    * fact_auth
    * fact_page_view
* Dimension tables:
    * dim_time
    * dim_date
    * dim_user
    * dim_song
    * dim_location

All dimension tables are conformed dimension which are used for multiple fact tables.

The data model diagram is shown as below:

![star-schema](images/star_schema.jpg)

Orchestration:
Airflow is used to schedule, trigger 2 DAGs in this project:
* Full load DAG is scheduled in the first run of the project.
* Incremental load DAG is scheduled new data to the data warehouse every midnight.

## **Dashboard**

Example:

![dashboard](images/dashboard.jpg)

## **Setup & Deployment**

