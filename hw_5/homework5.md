# Big Data Mining - Homework #5  
  
**Note:**  
  
- In this question set, Google web graph Data Set is used, a copy of the data can be downloaded from [here](http://snap.stanford.edu/data/web-Google.html). Make sure to have the txt file in the archive file so as to use the code.  

**Data description:**  
Each line is a directed edge representing hyperlinks between nodes (web pages)  
```  
<FromNodeId>	<ToNodeId>
```  
  
**Question:**  
  
1. Given the Google web graph dataset, please output the list of web pages with the number of outlinks, sorted in descending order of the out-degrees.  
  
2. Please output the inlink distribution of the top linked web pages, sorted in descending order of the in-degrees.  
  
3. Design an algorithm that maintains the connectivity of two nodes in an efficient way. Given a node v, please output the list of nodes that v points to, and the list of nodes that points to v.  