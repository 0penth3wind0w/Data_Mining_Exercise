# Big Data Mining - Homework #3  

**Note:**  

- In this question set, Reuters-21578 Text Categorization Collection Data Set is used, a copy of the data can be downloaded from [here](https://archive.ics.uci.edu/ml/datasets/reuters-21578+text+categorization+collection). Make sure to have the archive file.  
- Only .sgml file are needed (21 files)  
  
**Data description:**  
The file is xml-like, this homework only deal with news contents inside ```<body> </body>``` tags.  
  
**Question:**  

1. Given the Reuters-21578 dataset, please calculate all k-shingles and output the set representation of the text dataset as a matrix.  
  
2. Given the set representation, compute the minhash signatures of all documents using MapReduce.  

3. Implement the LSH algorithm by MapReduce and output the resulting candidate pairs of similar documents.  
