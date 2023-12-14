# Assignment-1
Youtube Data Harvesting using MongoBD, SQL and Streamlit

YouTube Data Harvesting and Warehousing is a project that aims to allow users to access and analyze data from various YouTube channels. The program should facilitate storing the data in a MongoDB database and allow users to collect data from YouTube channels, migrate the collected data to MySQL for further analysis

## Technologies Used
1. Python
2. MySQL
3. MongoDB
4. Google Client Library

## Approach
1. Establish a connection to the YouTube API, which allows me to retrieve channel and video data by utilizing the Google API client library for Python. 
2. Store the retrieved data in MongoDB, as it is a suitable choice for handling unstructured data.
3. Transferring the collected data from multiple channels namely the channels, videos, and comments to a SQL data warehouse
4. The retrieved data is displayed within the Streamlit application, leveraging Streamlit's data visualization capabilities
