# Web Stack Overview
This is the web package of the final project. For this component I decided to break my web-application into multiple components. First, there is a "frontend" application the the user will use to issue new data requests and second, there is "backend" application that will fulfill twitter and stock data requests. This backend application will also run a job which periodically fetches the lastest data from twitter and the stock markets on a daily basis.

# Big Data Architecture Considerations
The web architecture for this project takes advantage of a distributed architecture. That is, from a frontend application, we're like to take advantage of the client's compute by using reactive applications to reduce web request load on the system. We would also like to have horizontal scalability by having backend machines that can be quickly spun up by autoscaling applications.

For the most part, users making requests to our big data system is ideally not going to grow very large and the operations that we would like to perform on these records are highly transactional so a SQL database is a good candidate for storing and serving these kinds of requests.

**Write about Batch to Kafka**

# Technologies Used
* Frontend (ES6):
    * Nodejs (web application server)
    * Vuejs (reactive web framework)
    * Vuex (state management)
    * Axios (web request management)
    * Bulma (CSS)
* Backend (Python):
    * Flask (web application server)
    * SQLAlchemy (database ORM)
    * Tweepy (twitter API wrapper)
    * iexfinance (stock data)
    * Kafka Python (Kafka API wrapper)
* Database:
    * AWS RDS (PostgreSQL)

# Backend Installation & Deployment
Make sure that you have python3 and pip3 on your deployment environment. Locate the `requirements.txt` and run `pip3 install -r requirements.txt`. This will download all the python dependencies and libraries needed the execute the application.

If you plan to properly deploy the system into a production environment, you'll need to provision a database (ie. AWS RDS) and configure the `config.py` settings to connect with your database. The SQLite database is just meant for development.

Once you provision a database run the following commands:

```
python manage.py db init
python manage.py db migrate
python manage.py db upgrade
```

The backend system allows the frontend client to manage requests to the big data system by keeping track of twitter users and stock history jobs that are already being processed.

Depending on your Kafka settings you may need to create the following kafka topics in order for the system to function correctly. Use the following commands:

```
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic huarngpa_ingest_batch_twitter
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic huarngpa_ingest_batch_stock
```

When ready, you can use the `manage.py` utility to start the backend application by running `python manage.py runserver`.

# Frontend Installation & Deployment
