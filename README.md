# BulkWriteExample

This is a simple implementation of BulkWrite API in MongoDB. It is maven project, uses Java 8 and MongoDB 3.10 Driver.

Before running this program, generate sample customer collection using mgeneratejs. For test I generated 2.6 Million Customers. Mutiple instances can be run. 

```javascript
mgeneratejs -n 1000000 customertemplate.json | mongoimport --host MongoServer:27017 --ssl --username <username> --password <Password> --authenticationDatabase admin --db test --collection customers --type json
```
### App.java

Update MongoDB Atlas or Local Cluster connection String
```java
String uri = "mongodb+srv://<username>:<password>@<Atlas Cluster>/test?retryWrites=true&w=majority";
```


