# Cloud_computing

This is programming based on Hadoop, in the context of information retrieval. 
Information retrieval (IR) is concerned with finding material (e.g., documents) of an unstructured nature (usually text) in response to an information need (e.g., a query) from large collections. One approach to identify relevant documents is to compute scores based on the matches between terms in the query and terms in the documents. For example, a document with words such as ball, team, score, championship is likely to be about sports. It is helpful to define a weight for each term in a document that can be meaningful for computing such a score. I have used various information retrieval metrics such as term frequency, inverse document frequency, and their product, term frequency-inverse document frequency (TF-IDF), in order to define weights for terms. Under the Information Retrieval folder, you will find following Java files:

1. DocWordCount.java
  It is a WordCount program which outputs the wordcount for each distinct word in each file.
 
 2. TermFrequency.java
   It computes the logarithmic Term Frequency
   
 3. TFIDF.java
    It runs two mapreduce jobs, one after another. The first mapreduce job computes the Term Frequency as described above. The second job takes the output files of the first job as input and computes TF-IDF values. 
    
 4. Search.java
    Developed a job which implements a simple batch mode search engine. The job (Search.java) accepts as input a user query and outputs a list of documents with scores that best matches the query (a.k.a search hits).
