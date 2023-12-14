#importing the necessary librariesd
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build

from pymongo import MongoClient
from bson import ObjectId
# import mysql.connector
import mysql.connector as sql 
from datetime import datetime
import time
from googleapiclient.errors import HttpError
import pymongo
myclient=pymongo.MongoClient("mongodb://localhost:27017/")
import mysql.connector

api_service_name = "youtube"
api_version = "v3"
api_key='AIzaSyD-2UHs1wj37Miz8ppTWO5QdFN1gCfvon8'

#Getting Channle Details
youtube =build(
        api_service_name, api_version, developerKey=api_key)

def get_channel_info(channel_id):
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
    response=request.execute()

    for item in response["items"]:
        data = dict(
                    Channel_Name = item["snippet"]["title"],
                    Channel_Id = item["id"],
                    Subscription_Count= item["statistics"]["subscriberCount"],
                    Views = item["statistics"]["viewCount"],
                    Total_Videos = item["statistics"]["videoCount"],
                    Channel_Description = item["snippet"]["description"],
                    Playlist_Id = item["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
    return data


#Getting Video IDs

def get_videos_ids(Channel_Id):
    video_Ids=[]
    request = youtube.channels().list(
                id=Channel_Id,
                part='contentDetails'
            )
    response = request.execute()
    
    Playlist_Id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    nxt_pg_tkn=None
    
    while True:
        request1 = youtube.playlistItems().list(part='snippet',
                                               playlistId=Playlist_Id,
                                               maxResults=50,
                                               pageToken=nxt_pg_tkn)
        response1 = request1.execute()
        
        for i in range(len(response1['items'])):
            video_Ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        nxt_pg_tkn=response1.get('nextPageToken')
    
        if nxt_pg_tkn is None:
            break
    return video_Ids


#Get Video Info

def get_video_info(video_id):
    video_data=[]
    for video_id in video_id:
            request=youtube.videos().list(
                part='snippet,ContentDetails,statistics',
                id=video_id
            )
            response=request.execute()
            for item in response['items']:
                data=dict(
                    Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Video_Name=item['snippet']['title'],
                    Video_Description=item['snippet'].get('description'),
                    PublishedAt=item['snippet']['publishedAt'],
                    View_Count=item['statistics'].get('viewCount'),
                    Like_Count=item['statistics'].get('likeCount'),
                    Comment_Count=item['statistics'].get('commentCount'),
                    Fav_Count=item['statistics']['favoriteCount'],
                    Duration=item['contentDetails']['duration'],
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Caption_Status=item['contentDetails']['caption']
                )
                video_data.append(data)
    return video_data


#Getting Comments Info

def get_comment_info(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for comment in response['items']:
                data = {
                    'Video_Id': comment['snippet']['videoId'],
                    'Comment_Id': comment['snippet']['topLevelComment']['id'],
                    'Comment_Text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_Author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_PublishedAt': comment['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                comment_data.append(data)
    except Exception as e:
        print("An error occurred:", e)

    return comment_data


#Copy data to MongoDB

db = myclient['youtube']

def channel_details(Channel_Id):
    ch_details=get_channel_info(Channel_Id)
    vi_ids=get_videos_ids(Channel_Id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    combined_data = {
        "Channel_Information": ch_details,
        "Video_Information": vi_details,
        "Comment_Information": com_details
    }

    coll1 = db['channel_details']
    coll1.insert_one(combined_data)  # Insert the combined data as a single document

    return 'Upload completed successfully'


#Table Creation in MySQL

def channels_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="0000"
    )
    mycursor = mydb.cursor()
    mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube")
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="0000",
        database="youtube"
    )
    mycursor=mydb.cursor()
    
    drop_query='''drop table if exists channel'''
    mycursor.execute(drop_query)
    mydb.commit()
    
    try:
        query='''create table channels(Channel_Name varchar(225),
                                     Channel_id varchar(225) primary key,
                                     Subscription_Count bigint,
                                     Views bigint,
                                     Total_Videos int,
                                     Channel_Description text)'''
        mycursor.execute(query)
    except:
        print('Channel tables already created')

    db = myclient['youtube']
    coll1=db['channel_details']
    Channel_List=[]
    for ch_data in coll1.find({},{'_id':0,'Channel_Information':1}):
        Channel_List.append(ch_data['Channel_Information'])
    df=pd.DataFrame(Channel_List)
    
    for index,row in df.iterrows():
        insert='''insert into channels(Channel_Name,
                                       Channel_Id,
                                       Subscription_Count,
                                       Views,
                                       Total_Videos,
                                       Channel_Description)
                                       values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
               row['Channel_Id'],
               row['Subscription_Count'],
               row['Views'],
               row['Total_Videos'],
               row['Channel_Description'])
    
        try:
            mycursor.execute(insert,values)
            mydb.commit()
        except:
            print('Values were already inserted')


#SQL table for video detils

def video_table():
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="0000",
        database="youtube"
    )
    mycursor=mydb.cursor()
    
    drop_query='''drop table if exists video_details'''
    mycursor.execute(drop_query)
    mydb.commit()
    
    
    query = '''CREATE TABLE video_details (
            Channel_Name varchar(225),
            Channel_Id varchar(50),
            Video_Id varchar(50),
            Video_Name varchar(225),
            Video_Description text,
            PublishedAt varchar(50),
            View_Count int,
            Like_Count int,
            Comment_Count int,
            Duration varchar(50),
            Thumbnail varchar(225),
            Caption_Status varchar(50))'''
    mycursor.execute(query)
    mydb.commit()
    
    db = myclient['youtube']
    coll1 = db['channel_details']
    video_list = [] 
    for vi_data in coll1.find({}, {'_id': 0, 'Video_Information': 1}):
        for i in range(len(vi_data['Video_Information'])):
            video_list.append(vi_data['Video_Information'][i])
    
    df1 = pd.DataFrame(video_list)
    
    # Assuming video_list is a DataFrame with columns matching the table schema
    # Replace this assumption with the correct data transformation if needed
    # Insert data into the MySQL table
    try:
        # Your existing code for iterating through the DataFrame and inserting data into MySQL
        for index, row in df1.iterrows():
            sql = '''
                INSERT INTO video_details 
                (Channel_Name, Channel_Id, Video_Id, Video_Name, Video_Description, PublishedAt, View_Count, Like_Count, Comment_Count, Duration, Thumbnail, Caption_Status) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            values = (
                row['Channel_Name'], row['Channel_Id'], row['Video_Id'], row['Video_Name'], row['Video_Description'],
                row['PublishedAt'], row['View_Count'], row['Like_Count'], row['Comment_Count'],
                row['Duration'], row['Thumbnail'], row['Caption_Status']
            )
            mycursor.execute(sql, values)
            mydb.commit()
    
        print("Data insertion to MySQL successful!")
    
    except mysql.connector.Error as error:
        print("Error inserting data into MySQL:", error)
    
    finally:
        # Close the cursor and the database connection
        if mycursor:
            mycursor.close()
        if mydb:
            mydb.close()


#SQL table for comment detils

def comment_table():
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="0000",
        database="youtube"
    )
    mycursor=mydb.cursor()
    
    drop_query='''drop table if exists comment_details'''
    mycursor.execute(drop_query)
    mydb.commit()
    
    
    query = '''CREATE TABLE comment_details (
            Video_Id varchar(50),
            Comment_Id varchar(50),
            Comment_Text text,
            Comment_Author varchar(50),
            Comment_PublishedAt varchar(50))'''
    mycursor.execute(query)
    mydb.commit()
    
    db = myclient['youtube']
    coll1 = db['channel_details']
    comment_list = [] 
    for com_data in coll1.find({}, {'_id': 0, 'Comment_Information': 1}):
        for i in range(len(com_data['Comment_Information'])):
            comment_list.append(com_data['Comment_Information'][i])
    
    df2 = pd.DataFrame(comment_list)
    
    
    try:
        for index, row in df2.iterrows():
            sql = '''
                INSERT INTO comment_details 
                (Video_Id, Comment_Id, Comment_Text, Comment_Author, Comment_PublishedAt) 
                VALUES (%s, %s, %s, %s, %s)
            '''
            values = (
                row['Video_Id'], row['Comment_Id'], row['Comment_Text'],
                row['Comment_Author'], row['Comment_PublishedAt']
            )
            mycursor.execute(sql, values)
            mydb.commit()
    
        print("Data insertion to MySQL successful!")
    
    except mysql.connector.Error as error:
        print("Error inserting data into MySQL:", error)
    
    finally:
        if mycursor:
            mycursor.close()
        if mydb:
            mydb.close()


def sql_tables():
    channels_table()
    video_table()
    comment_table()
    return 'Tables were created successfully'


def show_ch_tab():
    db = myclient['youtube']
    coll1=db['channel_details']
    Channel_List=[]
    for ch_data in coll1.find({},{'_id':0,'Channel_Information':1}):
        Channel_List.append(ch_data['Channel_Information'])
    df=st.dataframe(Channel_List)
    return df

def show_vi_tab():
    db = myclient['youtube']
    coll1 = db['channel_details']
    video_list = [] 
    for vi_data in coll1.find({}, {'_id': 0, 'Video_Information': 1}):
        for i in range(len(vi_data['Video_Information'])):
            video_list.append(vi_data['Video_Information'][i])    
    df1 = st.dataframe(video_list)
    return df1

def show_com_tab():
    db = myclient['youtube']
    coll1 = db['channel_details']
    comment_list = [] 
    for com_data in coll1.find({}, {'_id': 0, 'Comment_Information': 1}):
        for i in range(len(com_data['Comment_Information'])):
            comment_list.append(com_data['Comment_Information'][i])    
    df2 = st.dataframe(comment_list)
    return df2


#Streamlit

st.title(':blue[YouTube Data Harvesting and Warehousing using MongoDB, SQL and Streamlit]')
with st.sidebar:
    st.markdown('[GitHub](https://github.com/leninsivasankaran/Assignment-1)')
    st.markdown('---')
    st.markdown('**Skill Takeaway**')
    st.markdown('>*Python Scripting*')
    st.markdown('>*Data Collection*')
    st.markdown('>*MongoDB*')
    st.markdown('>*API Integration*')
    st.markdown('>*Data Management using MongoDB and MySQL*')
    st.markdown('---')

st.markdown('---')
channel_id=st.text_input('Enter the Channel ID')

if st.button('Collect & Store data'):
    ch_ids=[]
    db=myclient['youtube']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'Channel_Information':1}):
        ch_ids.append(ch_data['Channel_Information']['Channel_Id'])

    if channel_id in ch_ids:
        st.success('Already exisits!')
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button('Migrate to MySQL'):
    Table=sql_tables()
    st.success(Table)

show_table=st.radio('Select the Table for View',('Channels','Videos','Comments'))

if show_table=='Channels':
    show_ch_tab()

elif show_table=='Videos':
    show_vi_tab()

elif show_table=='Comments':
    show_com_tab()


#SQL Connection
mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="0000",
        database="youtube"
    )
mycursor=mydb.cursor()

question=st.selectbox('Select your question',('1. What are the names of all the videos and their corresponding channels?',
                                             '2. Which channels have the most number of videos, and how many videos do they have?',
                                             '3. What are the top 10 most viewed videos and their respective channels?',
                                             '4. How many comments were made on each video, and what are their corresponding video names?',
                                             '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                             '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                             '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                             '8. What are the names of all the channels that have published videos in the year 2022?',
                                             '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                             '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))


if question=='1. What are the names of all the videos and their corresponding channels?':
    query1='''select video_name as video_name, channel_name as channelname from Video_details'''
    mycursor.execute(query1)
    t1=mycursor.fetchall()
    df1=pd.DataFrame(t1,columns=['Video Title','Channel Name'])
    mydb.commit()
    st.write(df1)

elif question=='2. Which channels have the most number of videos, and how many videos do they have?':
    query2='''select channel_name as channelname, total_videos as no_of_videos from channels
                order by total_videos desc'''
    mycursor.execute(query2)
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=['Channel Name','No. of Videos'])
    mydb.commit()
    st.write(df2)

elif question=='3. What are the top 10 most viewed videos and their respective channels?':
    query3='''select view_count as views, channel_name as channelname, video_name as videoname from video_details
                where view_count is not null order by views desc limit 10'''
    mycursor.execute(query3)
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=['Views','Channel Name','Video Title'])
    mydb.commit()
    st.write(df3)

elif question=='4. How many comments were made on each video, and what are their corresponding video names?':
    query4='''select comment_count as CommentNum, video_name as title from video_details where comment_count is not null'''
    mycursor.execute(query4)
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=['Comment Count','Video Name'])
    mydb.commit()
    st.write(df4)

elif question=='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5='''select video_name as title,channel_name as chname, like_count as likes from video_details
                where like_count is not null order by like_count desc'''
    mycursor.execute(query5)
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=['Video Name','Channel Name','Like Count'])
    mydb.commit()
    st.write(df5)

elif question=='6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6='''select video_name as title, like_count as likes from video_details'''
    mycursor.execute(query6)
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=['Video Name','Like Count'])
    mydb.commit()
    st.write(df6)

elif question=='7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query7='''select channel_name as channel, views as view from channels'''
    mycursor.execute(query7)
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=['Channel Name','Like Count'])
    mydb.commit()
    st.write(df7)

elif question=='8. What are the names of all the channels that have published videos in the year 2022?':
    query8='''select channel_name as channel, video_name as title, publishedat as date from video_details
                where extract(year from publishedat)=2022'''
    mycursor.execute(query8)
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=['Channel Name','Video Name','PublishedAt'])
    mydb.commit()
    st.write(df8)

elif question=='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 = '''SELECT channel_name AS channel, AVG(duration) AS average FROM video_details GROUP BY channel_name'''
    mycursor.execute(query9)
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=['Channel Name','Average Duration of Videos'])
    mydb.commit()
    
    t9=[]
    for index,row in df9.iterrows():
        channel_title=row['Channel Name']
        average_duration=row['Average Duration of Videos']
        average_duration_str=str(average_duration)
        t9.append(dict(title=channel_title,avgdur=average_duration_str))
    df0=pd.DataFrame(t9)
    st.write(df0)

elif question=='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10 = '''SELECT video_name AS title, channel_name AS channel, comment_count AS comments FROM video_details 
                where comment_count is not null order by comment_count desc'''
    mycursor.execute(query10)
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=['Video Name','Channel Name','Comment Count'])
    mydb.commit()
    st.write(df10)