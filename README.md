

 ETL (Extract, Transform, Load) process,
 data is first extracted from an API which was in  XML format. 
 the extracted XML data is transformed into a structured format, 
 typically  converting into csv file. The transformation process involves parsing the csv content and structuring it appropriately. 
 Subsequently, data cleaning steps are applied to the DataFrame using pyspark, addressing tasks such as handling null values ,removing unwanted data or managing missing data. 
 Finally, the cleaned data is loaded into a CSV file using PySpark's DataFrame write method, 
 with the resulting CSV file serving as the refined output. 
 This seamless ETL workflow ensures that data from the XML API is efficiently processed, transformed, and stored in a CSV format, ready for further analysis or utilization.
