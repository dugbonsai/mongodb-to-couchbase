# mongodb-to-couchbase
Code to assist with one-time migration of data from MongoDB to Couchbase and used in the following blog articles:
[Migrating Data from MongoDB to Couchbase](https://blog.couchbase.com/migrating-data-from-mongodb-to-couchbase/)

[Migrating Data from MongoDB to Couchbase, Part 2](https://blog.couchbase.com/migrating-from-m…couchbase-part-2/)

**couchbaseexample**
Full Java console application that demonstrates how to connect to a Couchbase cluster, perform key/value operations, and execute secondary lookups via N1QL queries using the Couchbase Java SDK.

**mongoexample**
Full Java console application that demonstrates how to connect to a MongoDB cluster, perform key/value operations, and execute secondary lookups using the MongoDB Java SDK.

**transform.js**
Couchbase Eventing function to transform MongoDB export data in real time as it is imported into Couchbase.

The function includes log() statements to log the original document, transformed document, and any errors. Feel free to change these as necessary. The Eventing log file can be found in the @eventing application log. See [this link](https://docs.couchbase.com/server/6.0/manage/manage-logging/manage-logging.html#logging_overview) for the location of this log file.

You can easily extend the capability of this function to perform other transformations by adding the necessary code in the transformValues() function. If you need to make any changes to the function, you must undeploy it, edit the JavaScript, and then deploy it again.
