# Big Data Mining - Bonus #1  
  
**Note:**  
  
- In this question set, Chicago crime data is used, a copy of the data can be downloaded from [here](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2). Make sure to have the csv file so as to use the code.  
- In order to practice manipulating different data types, Spark RDD is used in question 1 and Spark DataFrame is used in question 2, 3, and 4.  
  
**Data description:**  
The file contains 22 attributes.  
```  
-0,----------1,---2,----3,---4,  
ID,Case Number,Date,Block,IUCR,  
-----------5,----------6,-------------------7,-----8,-------9,  
Primary Type,Description,Location Description,Arrest,Domestic,  
--10,------11,--12,------------13,------14,  
Beat,District,Ward,Community Area,FBI Code,  
----------15,----------16,--17,--------18,------19,  
X Coordinate,Y Coordinate,Year,Updated On,Latitude,  
-------20,------21  
Longitude,Location  
```  
  
**Question:**  
  
1. For the attributes  **'Primary type'** and **'Location description'**, output the list of each value and the corresponding frequency count, sorted in descending order of the count, respectively.  
  
2. Output the most frequently occurred **'Primary type'** for each possible value of **'Location description'**, sorted in descending order of the frequency count.  
  
3. Output the most frequently occurred street name in the attribute **'Block'** for each **'Primary type'**, sorted in descending order of the frequency count. (You should remove the numbers in the 'Block' address of a street/avenue/boulevard)  
  
4. From the attribute **'Date'**, extract the time in hours and output the most frequently occurred hour for each **'Primary type'** and ‘Location description', sorted in descending order of the frequency count, respectively.  