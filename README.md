# Web Data Processing Systems Project

## Part 1: Python Wrapper For Spark
### The basic operation of the PySpark_Parser, is to 
* extract text data from compressed WARC files 
* Pre-process and sanitize the data for further analysis
* Extract any active entities 
* Perform Entity Linking/Disambiguation with a Knowledge Base(Freebase)
* Resort only on the baseline ranking system of the Freebase(No time for self-ranking methods)
* Parallelize the main procedure function by performing SPark map-reduce operations
_______________________________________________________________________________________________

## Part 2: Perform Entity Extraction and Analysis based on New York Times Articles
### The basic operation of the Entity Analysis - New York Times.py, is to 
* Perform data extraction from New York Times API 
* Recognise any of the unique entities and their frequency per document
* Recognise any relational patterns between entities(Co-occurances)
* Perform Sentiment Analysis on Entity content
* Perform basic statistics and measure any Influence points among the entities
* Depict the results in a form of bar chart 
