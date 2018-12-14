# Twitter Stock Big Data Project
This is Patrick Huarng's submission for the final project for MPCS 53013 Big Data. This project demonstrates the lambda architecture applied on big data being generated by the twitter firehose and dailying/historical stock information.

This project makes use of a number of big data technologies (Kafka, Spark, Hive, HBase, etc.) and the details on how these are applied are contained in their relevant sub-project folder of this submission.

## Project URLs
Please visit: [http://35.225.120.103:11182/#/](http://35.225.120.103:11182/#/). Note that you may need to wait for a number of seconds since the response time may be a bit slow. I did not have time to build loading features on the application, but it works.

## Breakdown of Project Stucture
Please visit the sub-project folders for more information. They each contain their own `README.md` on architecture considerations/decisions as well as installation and deployment information.
```
project root
|
|
|_ big-data
|  |_ batch-ingest (spark, kafka, scala)
|  |_ batch-processing (spark, hive)
|  |_ speed-layer (kafka, hbase, python)
|
|
|_ scripts
|  |_ batch-to-serving (hql, manual workaround)
|  |_ misc-stuff
|
|
|_ web
   |_ backend-web-app (flask, kafka, hbase, python)
   |_ frontend-web-app (client interface, vuejs)
```

## Project Status
* Batch Ingest  -- completed
* Batch Processing  -- completed
    * Individual views -- completed
    * Join views -- completed, not turned on
    * ML-Sentiment analysis -- completed
    * ML-LinRegression -- not completed
* Batch to Serving  -- manual workaround (see scripts)
* Serving Layer --completed
* Speed Layer -- completed
* Backend Web -- completed
* Frontend Web -- completed

## Other Project Thoughts
The final project was a very worthwhile learning experience. I definitely struggled through it, but learned a lot about Scala and Spark, especially discovering how programming on a single computer versus a cluster (or distributed environment) requires a very different approach to programming.

Thank you for the lectures and the experience! Hopefully we cross paths again one day!

Sincerely, Patrick
