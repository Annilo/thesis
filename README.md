# Exploring SQL-Based Near Real-Time Conformance Checking Using Stream Processing Engines

Conformance checking is used to validate the accurate completion of business
 processes against an existing process model. Effective conformance checking using event
 logs has been a vital strategy in ensuring the operational integrity and compliance of a
 business. With the development of streaming platforms, which offer near real-time data
 processing, non-conformance can be identified quickly and accurately. Streaming engines
 like Kafka or Spark are designed to process data in near real-time, in addition they are
 optimised for SQL. This thesis explores the possible application of conformance checking
 using SQL with Kafka and ksql or Spark Structured Streaming in the conformance
 checking of near real-time data streams.

 This repository includes two datasets: BPI_2012_1k_sample.xes from which the trie can be created and BPI_2012_Sim_2k_random_0.95.xes with which the suggested solutions can be tested. The file streaming_tables.py includes the code that was used for simple testing of a ksql solution. The Final Demo Notebook.ipynb includes the suggested solutions using Spark Structured Streaming with the additional code to generate dummy data.

There are additional notebooks: Dummy_Data_Notebook_*.ipynb where the outputs of using a dummy model and dummy data can be seen, additionally Scala_UDF_separate.html where the Needleman-Wunsch algorithm implementation can be seen in work separately.
