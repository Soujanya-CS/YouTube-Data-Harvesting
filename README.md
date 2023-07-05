# YouTube-Data-Harvesting
Project to fetch YouTube channel details using the YouTube API, save the data in a MongoDB database, migrate it to a MySQL server, and then retrieve the query results and perform queries in a Streamlit application.

Tools:

MongoDB: A NoSQL database used for storing the YouTube channel details.

MySQL: A relational database management system used for migrating the data from MongoDB and performing queries.

Streamlit: A Python library used for building interactive web applications.

Packages:

googleapiclient: A Python library that allows you to interact with the YouTube API.

pandas: A powerful data analysis and manipulation library in Python.

streamlit: A Python library for creating interactive web applications.

pymongo: Python package that provides tools and functionalities for working with MongoDB databases.


set up the necessary dependencies, including installing MongoDB, MySQL, and the required Python packages. Once we have everything set up, we can proceed with the following steps:

Fetching YouTube Channel Details:

Use the googleapiclient library to interact with the YouTube API and fetch the desired channel details. You may need to set up an API key to authenticate your requests.
Process the fetched data and store it in a suitable format (e.g., pandas DataFrame).

Saving Data to MongoDB:

Connect to your MongoDB database using the appropriate driver or ORM (Object-Relational Mapping) library.
Create a collection (similar to a table in SQL) to store the YouTube channel details.
Insert the processed data into the MongoDB collection.

Migrating Data to MySQL:

Connect to your MySQL server using the pymysql library.
Create a table in the MySQL database to match the structure of the MongoDB collection.
Query the data from the MongoDB collection and insert it into the MySQL table.

Retrieving Query Results and Performing Queries in Streamlit:

Use the pymysql library to establish a connection to your MySQL database.
Write SQL queries to retrieve the desired information from the MySQL table.
Utilize the streamlit library to create a web application.

Display the query results in the Streamlit application and enable users to perform additional queries or interact with the data.

Here is a snippet from streamlit Application:

Youtube Data Analysis page where it takes channel id as input and perfrom the insertion of channel details to mongodb and later migrate to sql.
![image](https://github.com/Soujanya-CS/YouTube-Data-Harvesting/assets/136436804/04e15c42-2d67-42bd-8b99-e790e925161d)

![image](https://github.com/Soujanya-CS/YouTube-Data-Harvesting/assets/136436804/5d5362ce-288c-4590-b64f-2280a3ca4985)

Preloaded sql queries:

![image](https://github.com/Soujanya-CS/YouTube-Data-Harvesting/assets/136436804/ca2ab4b8-b8c6-4e47-a66a-2d9d8d959b9c)

user query:

![image](https://github.com/Soujanya-CS/YouTube-Data-Harvesting/assets/136436804/62a01802-436a-41bc-98af-78b3a84ec31e)

![image](https://github.com/Soujanya-CS/YouTube-Data-Harvesting/assets/136436804/925e62e6-eb25-4231-a242-00f1aa2b5349)


json formatted:

![image](https://github.com/Soujanya-CS/YouTube-Data-Harvesting/assets/136436804/75eeac38-1c62-41fd-b523-a754824d6099)

