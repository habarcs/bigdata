# bigdata
Big data project for UNITN


Supplier, manufacturier, etc... sending data to central system. With this data we do predictions and dashboard. 

Decide on format of data: tabular data, relational data. It is going to be synthetic data based on real data sets. This way we decide what variables could be used. 

The simpler the project the better. In Martons head:

Entities send data with a msg quue system to a central data warehouse. 
Then processing stuff that runs in dask.
Frontend system with dashboard. 

Technologies we used:

Overall Architecture Diagram
Data Sources →
Data Ingestion (API Clients, Kafka, Pub/Sub) →
Data Preparation (Knime, PySpark) →
Data Storage (HDFS, Data Warehouse, Data Lake, NoSQL) →
Data Processing (Dask, PySpark, Spark Streaming) →
Data Retrieval (SQL, MongoDB, SPARQL) →
Deployment (Docker, Kubernetes) →
Analytics (Predictive Models, Visualization Tools)

##### Problem statement
We are a company that provides predictive insights to suppliers by forecasting the optimal quantity of raw materials they should produce. Our solution leverages data from intermediate manufacturing processes and retail performance to deliver precise, data-driven recommendations, helping suppliers align their production plans with actual market demand, minimize waste, and optimize inventory management.




