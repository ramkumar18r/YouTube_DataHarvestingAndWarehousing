import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
import pymongo
import psycopg2
import isodate
import numpy as np


# Create Streamlit UI
st.set_page_config(
    page_title="Youtube Data Harvesting",
    layout="wide",
    initial_sidebar_state="expanded")
st.title('Youtube - DATA HARVESTING & WAREHOUSING')
channel_input = st.sidebar.text_input('Enter the channel name: ')
button1 = st.sidebar.button('show channel details')
channel_id = st.sidebar.text_input('Copy & Paste the Channel Id here: ')
button2 = st.sidebar.button('Upload in Mongodb')
button3 = st.sidebar.button('Transfer data to SQL')
button4 = st.sidebar.button('Query Results')

# Create object for youtube API
api_key = "Your API Key"
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey=api_key)

# Create MongoDb Connection
client = pymongo.MongoClient("Mongo DB server address")
db = client['Data_Harvesting']
channel_col = db['channel_collection']


# Get Youtube channel ID through channel name:
def channel_name(channel_input):    
    channel_list = []
    
    request = youtube.search().list(
        part="snippet",
        maxResults=1,
        q=channel_input
    )
    response = request.execute()

    data = dict(channelid = response['items'][0]['snippet']['channelId'],
                channeltitle = response['items'][0]['snippet']['channelTitle'],
                description = response['items'][0]['snippet']['description']  
                )
    channel_list.append(data)
    df = pd.DataFrame(channel_list) 
    return df

# Get Youtube Highlevel channels details using channel ID:
def channel_data(channel_id):
    try:
        request = youtube.channels().list(
                                    part="snippet,contentDetails,statistics",
                                    id=channel_id
                                    )
        response = request.execute()

        data = dict(
            channel_id = response['items'][0]['id'],
            title = response['items'][0]['snippet']['title'],    
            playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            viewCount = response['items'][0]['statistics']['viewCount'],
            subscriberCount = response['items'][0]['statistics']['subscriberCount'],
            videoCount = response['items'][0]['statistics']['videoCount']
            )
        return data
    except:
        pass  

# Get Youtube Videos ID using playlist ID:
def get_videoid(playlist_id):
    nextPageToken = None
    data = []
    try:
        while True:
            request = youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId = playlist_id,
                                            maxResults=50,
                                            pageToken=nextPageToken
                                            )
            response = request.execute()    
            
            for i in range(len(response['items'])):
                data.append(response['items'][i]['snippet']['resourceId']['videoId'])    
            nextPageToken = response.get('nextPageToken')

            if nextPageToken is None:
                break
    except:
        pass
    return(data)

# Get Youtube Videos details using video ID:
def video_data(video_id):
    vi_data=[]
    for ID in video_id:
        request = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=ID)
        response = request.execute()
       
        if 'channelId' in response['items'][0]['snippet']:
            channeId = response['items'][0]['snippet']['channelId']
        else:
            channeId = ''
        if 'channelTitle' in response['items'][0]['snippet']:
            channel_name = response['items'][0]['snippet']['channelTitle']
        else:
            channel_name = ''
        if 'id' in response['items'][0]:
            videoId = response['items'][0]['id']
        else:
            videoId = ''
        if 'title' in response['items'][0]['snippet']:
            videoTitle = response['items'][0]['snippet']['title']
        else:
            videoTitle = ''        
        if 'publishedAt' in response['items'][0]['snippet']:
            publishedDate = response['items'][0]['snippet']['publishedAt']
        else:
            publishedDate = ''   
        if 'duration' in response['items'][0]['contentDetails']:
            Duration = response['items'][0]['contentDetails']['duration']
            videoDuration = isodate.parse_duration(Duration).seconds
        else:
            videoDuration = ''
        if 'viewCount' in response['items'][0]['statistics']:
            video_viewCount =  response['items'][0]['statistics']['viewCount']
        else:
           video_viewCount = ''
        if 'likeCount' in response['items'][0]['statistics']:
            video_likeCount =  response['items'][0]['statistics']['likeCount']
        else:
            video_likeCount = ''
        if 'favoriteCount' in response['items'][0]['statistics']:
            video_favoriteCount =  response['items'][0]['statistics']['favoriteCount']
        else:
            video_favoriteCount = ''
        if 'commentCount' in response['items'][0]['statistics']:
            video_commentCount =  response['items'][0]['statistics']['commentCount']
        else:
            video_commentCount = ''   
        
        data = dict(channeId = channeId,
                    channel_name = channel_name,
                    videoId = videoId,
                    videoTitle = videoTitle,
                    publishedDate = publishedDate,
                    videoDuration = videoDuration,
                    video_viewCount =  video_viewCount,
                    video_likeCount =  video_likeCount,
                    video_favoriteCount =  video_favoriteCount,
                    video_commentCount =  video_commentCount
                    )
        vi_data.append(data)
          
    return(vi_data)

# Get Comment details using video ID:
def comment_data(video_id):
    cmt_data=[]
    nextPageToken = None
    try:
        while True:
            for vid in video_id:
                request = youtube.commentThreads().list(
                                part="snippet,replies",
                                videoId=vid, 
                                maxResults=10,
                                #pageToken=nextPageToken
                            )
                response = request.execute()

                for i in range(len(response['items'])):                
                    data = dict(channeId = response['items'][i]['snippet']['channelId'],
                        videoId = response['items'][i]['snippet']['videoId'],
                        comment_ID = response['items'][i]['id'],
                        comment_Text = response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_Author = response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_Date = response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']        
                        )
                    cmt_data.append(data)
                    

                nextPageToken = response.get('nextPageToken')
                if nextPageToken is None:
                    break
    except:
        pass
    return(cmt_data)

# Get all channel details & consolidate in dictionary.
def channels(channel_id):                
    channelData = channel_data(channel_id)           
    playlist_id = channelData['playlist_id']
    if playlist_id is not None:
            channel_details = channelData
            video_id = get_videoid(playlist_id)
            video_details = video_data(video_id)
            comment_details = comment_data(video_id)
            data = {'channel_docs':channelData,
                    'video_docs':video_details,
                    'comment_docs':comment_details        
                    }
    if playlist_id is None:
            exit
    return data

# Upload channel details in Mongodb using streamlit application
def upload_channel_to_mongodb(channel_id):
    try:
        channel_collections = channels(channel_id)     
        if channel_id != "":                                   
            channel_col.insert_one(channel_collections)
            st.write('Channel details are uploaded in MongoDB successfully')      
        
    except Exception as error:
        st.write('Error occurred while uploading channel details in MongoDB:', error)


def create_table():
    conn = psycopg2.connect(
        database='youtube',
        user="postgres",
        password="****",
        host="127.0.0.1",
        port="SQL Port"
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS channels (
            _id SERIAL PRIMARY KEY,
            channel_id VARCHAR(255) UNIQUE,
            title VARCHAR(255),
            playlist_id VARCHAR(255),
            viewCount VARCHAR,
            subscriberCount INT,
            videoCount INT);'''
        )

    cur.execute(
        '''CREATE TABLE IF NOT EXISTS videos (
            _id SERIAL PRIMARY KEY,
            channel_id VARCHAR(255),
            channel_name VARCHAR(255),
            videoId VARCHAR(255) UNIQUE,
            videoTitle VARCHAR(255),
            publishedDate TIMESTAMP,
            videoDuration INT,
            video_viewCount INT,
            video_likeCount INT,
            video_favoriteCount INT,
            video_commentCount INT);'''
        )

    cur.execute(
        '''CREATE TABLE IF NOT EXISTS comments (
            _id SERIAL PRIMARY KEY,
            channel_id VARCHAR(255),
            videoId VARCHAR(255),
            comment_ID VARCHAR(255) UNIQUE,
            comment_Text TEXT,
            comment_Author VARCHAR(255),
            comment_Date TIMESTAMP);'''
        )

    cur.close()
    conn.close()

def data_to_sql(channel_col):
    conn = psycopg2.connect(
        database='youtube',
        user="postgres",
        password="****",
        host="127.0.0.1",
        port="SQL Port"
    )

    conn.autocommit = True
    cursor = conn.cursor()
    channel_details = channel_col.find()

    vi_data = {}
    cmt_data = {}

    for channels in channel_details:
        cursor.execute("""
            INSERT INTO channels (channel_id, title, playlist_id, viewCount, subscriberCount, 
                                     videoCount)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (channel_id) DO NOTHING
        """, (
            channels['channel_docs']['channel_id'], channels['channel_docs']['title'], channels['channel_docs']['playlist_id'], channels['channel_docs']['viewCount'],
            channels['channel_docs']['subscriberCount'],channels['channel_docs']['videoCount']
        ))   
    
        Vdata = channels['video_docs']
        print(Vdata)        
        for i in Vdata:
            vi_data.update(i)            
            cursor.execute("""
                INSERT INTO videos(channel_id, channel_name, videoid, videotitle, publisheddate, videoduration, 
                                        video_viewcount, video_likecount, video_favoritecount, video_commentcount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (videoid) DO NOTHING
            """, (
                vi_data['channeId'], vi_data['channel_name'], vi_data['videoId'], 
                vi_data['videoTitle'], vi_data['publishedDate'],vi_data['videoDuration'],
                vi_data['video_viewCount'], vi_data['video_likeCount'], vi_data['video_favoriteCount'],
                vi_data['video_commentCount']
            ))     
        
        Cdata = channels['comment_docs']
        print(Cdata)        
        for i in Cdata:
            cmt_data.update(i)

            cursor.execute("""
            INSERT INTO comments (channel_id, videoId,comment_id,comment_Text, comment_Author, comment_Date)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (comment_id) DO NOTHING
        """, (
            cmt_data['channeId'],cmt_data['videoId'],cmt_data['comment_ID'],cmt_data['comment_Text'],
            cmt_data['comment_Author'], cmt_data['comment_Date']
        ))
            
    conn.commit()
    cursor.close()
    conn.close()

# Youtube Query:
selectbox = st.selectbox('Select the following Queries and click "Query Results" button',
                        ('1. What are the names of all the videos and their corresponding channels?',                        
                        '2. Which channels have the most number of videos, and how many videos do they have?',
                        '3. What are the top 10 most viewed videos and their respective channels?',
                        '4. How many comments were made on each video, and what are their corresponding video names?',
                        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                        '8.What are the names of all the channels that have published videos in the year 2022?',
                        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'                                                    
                        ))

# Sql query output based on the youtube query selection
def sql_queries(selectbox, button4):
    conn = psycopg2.connect(
        database='youtube',
        user="postgres",
        password="****",
        host="127.0.0.1",
        port="SQL Port"
    )
    conn.autocommit = True
    cursor = conn.cursor()
    if selectbox == '1. What are the names of all the videos and their corresponding channels?' and button4:
        cursor.execute('''SELECT channel_name,videotitle FROM VIDEOS;
                        ''')
        d = cursor.fetchall()
        data = []
        for i in d:
            data.append(i)
        df = pd.DataFrame(data)
        df.columns = ['Channel Name', 'Video Description']
        st.write(df)

    if selectbox == '2. Which channels have the most number of videos, and how many videos do they have?' and button4:
        cursor.execute('''SELECT channel_name, COUNT(videoid) as no_video 
                        FROM videos GROUP by channel_name
                        ORDER BY no_video DESC 
                        LIMIT 1;''')
        data = cursor.fetchall()
        for i in data:
            st.write('Channels have the most number of videos: ',i[0])
            st.write('No of video that channel have: ',i[1])

    if selectbox == '3. What are the top 10 most viewed videos and their respective channels?' and button4:
        cursor.execute('''
                    SELECT channel_name, videotitle, video_viewcount FROM videos
                    ORDER BY video_viewcount DESC
                    LIMIT 10;
                ''')
        d= cursor.fetchall()
        data=[]
        for i in d:
            data.append(i)
        df = pd.DataFrame(data)
        df.columns = ['Channel Name', 'Video Description','Video View Count']
        st.write(df)

    if selectbox == '4. How many comments were made on each video, and what are their corresponding video names?' and button4:
        cursor.execute('''
                    SELECT videotitle, video_commentcount FROM videos
                    ORDER BY video_commentcount DESC;
                ''')
        d= cursor.fetchall()
        data=[]
        for i in d:
            data.append(i)
        df = pd.DataFrame(data)
        df.columns = ['Video Description', 'Video Commentcount']
        st.write(df)

    if selectbox == '5. Which videos have the highest number of likes, and what are their corresponding channel names?' and button4:
        cursor.execute('''
                    SELECT channel_name, videotitle, video_likecount FROM videos
                    ORDER BY video_likecount DESC
                    LIMIT 1;
                ''')
        d= cursor.fetchall()
        data=[]
        for i in d:
            data.append(i)
        df = pd.DataFrame(data)
        df.columns = ['Channel Name', 'Video Description', 'Video Like Count']
        st.write(df)

    if selectbox == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?' and button4:
        cursor.execute('''
                    SELECT videotitle, video_likecount FROM videos;
                ''')
        d= cursor.fetchall()
        data=[]
        for i in d:
            data.append(i)
        df = pd.DataFrame(data)
        df.columns = ['Video Description', 'Video Like Count']
        st.write(df)

    if selectbox == '7. What is the total number of views for each channel, and what are their corresponding channel names?' and button4:
        cursor.execute('''
                    SELECT title, viewcount FROM channels;
                ''')
        d= cursor.fetchall()
        data=[]
        for i in d:
            data.append(i)
        df = pd.DataFrame(data)
        df.columns = ['Video Description', 'Video Viewcount']
        st.write(df)

    if selectbox == '8.What are the names of all the channels that have published videos in the year 2022?' and button4:
        cursor.execute('''
                    SELECT channel_name FROM videos
                    WHERE publisheddate BETWEEN '2021-12-31' AND '2023-01-01'
                    GROUP BY channel_name;
                ''')
        d= cursor.fetchall()
        data=[]
        for i in d:
            data.append(i)
        df = pd.DataFrame(data)
        df.columns = ['Channel Name']
        st.write(df)

    if selectbox == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?' and button4:
        cursor.execute('''
                    SELECT channel_name, ROUND(AVG(videoduration),0) FROM videos
                    GROUP BY channel_name;
                ''')
        d= cursor.fetchall()
        data=[]
        for i in d:
            data.append(i)
        df = pd.DataFrame(data)
        df.columns = ['Channel Name', 'Average Video Duration']
        st.write(df)

    if selectbox == '10. Which videos have the highest number of comments, and what are their corresponding channel names?' and button4:
        cursor.execute('''
                    SELECT channel_name, videotitle, video_commentcount FROM videos
                    ORDER BY video_commentcount DESC
                    LIMIT 1;
                ''')
        d= cursor.fetchall()
        data=[]
        for i in d:
            data.append(i)
        df = pd.DataFrame(data)
        df.columns = ['Channel Name','Video Description', 'Video Commentcount']
        st.write(df)
    
    conn.commit()
    cursor.close()
    conn.close()

# Get channel ID using channel name in Streamlit
if channel_input != "" and button1:
    channel_details = channel_name(channel_input)
    st.dataframe(channel_details)
elif channel_input == "" and button1:
    st.write('Youtube channel name is not entered. Kindly enter the channel name')

# Upload channel details in Mongodb using streamlit application
if button2:
    upload_channel_to_mongodb(channel_id)

# Transfer channel datas from Mongodb to Sql using streamlit application
if button3:
    try:
        create_table()
        data_to_sql(channel_col)
    except Exception as error:
        st.write('Error occured while uploading channel details in SQL', error)
    else:   
        st.write('Channel details are transfered to SQL successfully')

# Sql query output based on the youtube query selection
if button4:
    sql_queries(selectbox, button4)
