# YouTube-Data-Harvesting-and-Warehousing
#### This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information from YouTube channel, store it in  #### MongoDB &amp; migrate it to MYSQL data warehouse and enable users to search channel details in Streamlit app.
# Domain Name: Social Media
# Used Technologies : Python, SQL, MongoDB, pymongo,Streamlit and Youtube API
# Project Approach:
1. Set up a Streamlit app: Streamlit is a great choice for building data
visualization and analysis tools quickly and easily. This application has a simple UI
where users can enter a YouTube channel ID, view the channel details, and select channels
to migrate to the data warehouse.

2. Connect to the YouTube API: I have used the YouTube API to retrieve
channel and video data. To create youtube api I have used Google developer console.

3. Store data in a MongoDB data lake: After retrieving the data from the
YouTube API, I have stored it in a MongoDB Database. MongoDB is a great
choice for storing real world data because it can handle unstructured and semi-structured
data easily.

4. Migrate data to a SQL data warehouse: After collecting data from each channel,
   I have migrated it to a MYSQL data warehouse.
   
5. Query the SQL data warehouse: I have used SQL queries to join the tables
   (channel, playlist, video, comment) in the MYSQL data warehouse and retrieve
   data for specific channels based on user input.
   
6. Display data in the Streamlit app: Finally, I have displayed the retrieved data
in the Streamlit app. I have used Streamlit's data visualization features to
create charts like pie chart, bar chart to help users analyze the data.

# Challenges that I have faced during making project
1. Fatching necessary data from multiple youtube api.
2. Storing it to mongodb database
3. Fatching details of particular channel from mongodb
4. Writing MYSQL query to create tables so that they can join on specific field and to insert data in the table.
5. Writing Mysql query for user selected operations
6. visualize all project elements on web application using streamlit.

# Handled all challenges:
I have done my project with my hard working & curiocity of learning new things. 
