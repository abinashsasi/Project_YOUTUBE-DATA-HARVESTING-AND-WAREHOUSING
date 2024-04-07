## YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit-GUVI Capstone Project
This project aims to develop a user-friendly Streamlit application that utilizes the Google 
API to extract information on a YouTube channel, stores it in a MongoDB database, migrates 
it to a SQL data warehouse, and displaying the data in the Streamlit app.



Introduction

    This project aims to develop a user-friendly Streamlit application that utilizes the Google 
    API to extract information on a YouTube channel, stores it in a MongoDB database, migrates 
    it to a SQL data warehouse, and displaying the data in the Streamlit app.
    
Features

    Retrieve data from the YouTube API, including channel information, playlists, videos, and comments.
    Store the retrieved data in a MongoDB database.
    Migrate the data to a SQL data warehouse.
    Analyze and visualize data using Streamlit and Plotly.
    Perform queries on the SQL data warehouse.
    Gain insights into channel performance, video metrics, and more.

Workflow

   Data Collection:

      1.The first step is to collect data from the YouTube using the YouTube Data API. 
      2.The API and the Channel ID (Extracted from the Channel Page) is used to retrieve channel details, 
        videos details and comment details. 
      3.I have used the Google API client library for Python to make requests to the API and the responses 
        are Collected as a Dictionary (Data Collection)
    
   Loading(Storing) the Collected Data to MongoDB:

      1.Once the Data Collection is done, store it in MongoDB, which is a NoSQL Database great choice for 
      handling unstructured and semi-structured data.
  
   Data Migration to SQL:

      1.After Loading all the data into MongoDB, it is then migrated to a structured MySQL data warehouse.
      2.Then used SQL queries to join the tables and retrieve data for specific channels.
  
   Data Analysis:

      1.With the help of SQL query, I have got many interesting insights about the youtube channels.
      2.Finally, the data retrieved from the SQL is displayed using the Streamlit Web app.
      3.Streamlit is used to create dashboard that allows users to visualize and analyze the data. 

*Tools and Technologies used*

         • Python • MongoDB • MySQL • YouTube Data API • Streamlit • Pandas 

conclusion 

    Overall, this approach involves building a simple UI with Streamlit, retrieving data from the 
    YouTube API, storing it in a MongoDB datalake, migrating it to a SQL data warehouse, querying 
    the data warehouse with SQL, and displaying the data in the Streamlit app. This approach can be 
    used to identify trends, make predictions, and improve decision-making.

Thank you

    
