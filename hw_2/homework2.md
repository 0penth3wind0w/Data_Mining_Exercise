# Big Data Mining - Homework #2  

**Note:**  

- In this question set, News Popularity in Multiple Social Media Platforms Data Set  is used, a copy of the data can be downloaded from [here](https://archive.ics.uci.edu/ml/datasets/News+Popularity+in+Multiple+Social+Media+Platforms). Make sure to have all the csv file.  
  
**Data description:**  
The news data file News_Final.csv contains 11 attributes.
```  
-------0,------1,---------2,-------3,------4,  
"IDLink","Title","Headline","Source","Topic",  
------------5,---------------6,------------------7,---------8,-----------9,  
"PublishDate","SentimentTitle","SentimentHeadline","Facebook","GooglePlus",  
--------10  
"LinkedIn"  
```  
- IDLink: Unique identifier of news items  
- Title: Title of the news item according to the official media sources  
- Headline: Headline of the news item according to the official media sources  
- Source: Original news outlet that published the news item 
- Topic: Query topic used to obtain the items in the official media sources  
- PublishDate: Date and time of the news items' publication 
- SentimentTitle: Sentiment score of the text in the news items' title  
- SentimentHeadline: Sentiment score of the text in the news items' headline  
- Facebook: Final value of the news items' popularity according to the social media source Facebook  
- GooglePlus: Final value of the news items' popularity according to the social media source Google+  
- LinkedIn: Final value of the news items' popularity according to the social media source LinkedIn  

Social feedback data (4 topics on 3 platforms)  
```  
-------0,----1,----2,...,----144
"IDLink","TS1","TS2",...,"TS144"
```  
- IDLink: Unique identifier of news items  
- TS1: Level of popularity in time slice 1 (0-20 minutes upon publication)  
- TS2: Level of popularity in time slice 2 (20-40 minutes upon publication)  
...   
- TS144 (numeric): Final level of popularity after 2 days upon publication  

**Question:**  

1. In news data, count the words in two fields: **'Title'** and **'Headline'** respectively, and list the most frequent words according to the term frequency in descending order, in total, per day, and per topic, respectively.  
  
2. In social feedback data, calculate the average popularity of each news by hour, and by day, respectively (for each platform).  

3. In news data, calculate the sum and average sentiment score of each topic, respectively.  

4. From subtask (i), for the top-100 frequent words per topic in titles and headlines, calculate their co-occurrence matrices (100x100), respectively. Each entry in the matrix will contain the co-occurrence frequency in all news titles and headlines, respectively.  