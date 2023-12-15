#------------------------------Requeried Linraries--------------------#

from googleapiclient.discovery import build
import pymysql
import mysql.connector
import pymongo
from pymongo.mongo_client import MongoClient
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
from datetime import datetime, timedelta
from datetime import datetime


#---------------------Api key connection.................................#

api_service_name ="youtube"
api_version = "v3"
api_key ="AIzaSyDSMQilxbYlhnLPV55QkkPA1dKitzTxwEQ"
youtube=build(api_service_name,api_version,developerKey=api_key)


#function to retive channel data from youtube
def get_channel_data(channel_id):
    response = youtube.channels().list(
      part="snippet,contentDetails,statistics",
      id=channel_id
      ).execute()

    #store the desired channel data which are extracted from youtube into a dictionary
    for i in response["items"]:
         channel_data={'Channel_name': i['snippet']["title"],
              'Channel_id':i["id"],
              'Subscribers' : i['statistics']['subscriberCount'],
              'Views':i['statistics']['viewCount'],
              'Total_Videos' : i['statistics']['videoCount'],
              'channel_description':i['snippet']['description'],
              'Playlist_id' : i['contentDetails']['relatedPlaylists']['uploads']}
               #add more field as we needed

    return channel_data


#function to retive playlist info from youtube API
def get_video_ids(channel_id):
    response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
        ).execute()
        
    playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']


    video_id=[]
    next_page_token = None
    while True:
        id_response = youtube.playlistItems().list(
            part = "snippet",
            playlistId = playlist_Id,
            maxResults=50,
            pageToken = next_page_token
            ).execute()

        for id in (id_response['items']):
            video_id.append(id['snippet']['resourceId']['videoId'])
            next_page_token = id_response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_id

def convert_duration(duration_string):
    # By calling timedelta() without any arguments, the duration
    # object is initialized with a duration of 0 days, 0 seconds, and 0 microseconds. Essentially, it sets the initial duration to zero.
    duration_string = duration_string[2:]  # Remove "PT" prefix
    duration = timedelta()
    
    # Extract hours, minutes, and seconds from the duration string
    if 'H' in duration_string:
        hours, duration_string = duration_string.split('H')
        duration += timedelta(hours=int(hours))
    
    if 'M' in duration_string:
        minutes, duration_string = duration_string.split('M')
        duration += timedelta(minutes=int(minutes))
    
    if 'S' in duration_string:
        seconds, duration_string = duration_string.split('S')
        duration += timedelta(seconds=int(seconds))
    
    # Format duration as H:MM:SS
    duration_formatted = str(duration)
    if '.' in duration_formatted:
        hours, rest = duration_formatted.split(':')
        minutes, seconds = rest.split('.')
        duration_formatted = f'{int(hours)}:{int(minutes):02d}:{int(seconds):02d}'
    else:
        duration_formatted = duration_formatted.rjust(8, '0')
    
    return duration_formatted




def convert_timestamp(timestamp):
    datetime_obj = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    formatted_time = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_time

#function to retive video info from youtube API
def get_video_data(Video_id):
    videodata=[]
    for videoid in Video_id:
        video_data_response=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=videoid
        ).execute()

        for vd in video_data_response['items']:
            videodata.append({'video_name': vd['snippet']["title"],
                              'vedio_id':vd['id'],
                              'channel_name':vd['snippet']['channelTitle'],
                              'channel_id':vd['snippet']['channelId'],
                              'Published_date':convert_timestamp(vd['snippet']['publishedAt']),
                              'video_description':vd['snippet']['description'],
                              'Views' : vd['statistics'].get('viewCount'),
                              'Likes' : vd['statistics'].get('likeCount'),
                              'Comment_Count': vd['statistics'].get('commentCount'),
                              'Duration': convert_duration(vd['contentDetails']['duration'])
                              })
    return videodata



#function to retive comment info from youtube API
def get_comment_details(Video_id):
    comment_data=[]
    try:
        for video in Video_id:
            comment_response = youtube.commentThreads().list(
                part="snippet",
                videoId=video,
                maxResults=10
                ).execute()

            for cm in comment_response['items']:
                info ={'Comment_id':cm['snippet']['topLevelComment']['id'],
                    'vedioid':cm['snippet']['topLevelComment']['snippet']['videoId'],
                    'comment_author' :cm['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'comment_text':cm['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'comment_publishedDate':cm['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
                                
                comment_data.append(info)

    except:
        pass
    return comment_data



# function to get  PLaylist information from the YouTube API
def get_playlist_data(channel_id):
    # Retrieve the PLaylist information from the YouTube API
    playlist_response = youtube.playlists().list(
        part = "snippet,contentDetails",
        channelId = channel_id,
        maxResults = 50
    ).execute()

    #store the desired playlist data which are extracted from youtube into a dictionary with dynamite key
    playlist_data= []
    for pl in playlist_response['items']:
        playlist_data.append({'playlist_id' : pl['id'],
                           'channel_id' : pl['snippet']['channelId'],
                           'title' : pl['snippet']['title']})

    return playlist_data


#mongoDB connection
url="mongodb+srv://abinash17:abi17@cluster0.yxlczno.mongodb.net/?retryWrites=true&w=majority"
client=MongoClient(url)
db=client.youtubedata07
collection =db.ch_data

#insert data into mongoDB
def get_all_details(channel_id):
    Channel_data=get_channel_data(channel_id)
    Playlist_data=get_playlist_data(channel_id)
    Video_id=get_video_ids(channel_id)
    Video_data=get_video_data(Video_id)
    Comment_data=get_comment_details(Video_id)
    

    data_file={"Channel_Details":Channel_data,"Playlist_details":Playlist_data,"Video_data":Video_data,"Comments":Comment_data}

    return data_file


# Connect to MongoDB and Function to store data in MongoDB
def upload_to_mongoDB(all_data):
    url="mongodb+srv://abinash17:abi17@cluster0.yxlczno.mongodb.net/?retryWrites=true&w=majority"
    client=MongoClient(url)
    db=client.youtubedata07
    collection =db.ch_data
    collection.insert_one(all_data)


#sql connection & table_creation
def create_table():
    mysql_connection=mysql.connector.connect(
        host="localhost", #127.0.0.1
            user="root",
            password=""
            #database='youtubedata'
        )
    cursor=mysql_connection.cursor()
        
    cursor.execute("create database if not exists youtubedata07") 
    cursor.execute("use youtubedata07")
    create_table='''CREATE TABLE IF NOT EXISTS channels_data
                        (Channel_name VARCHAR(100),
                        Channel_id VARCHAR(100) PRIMARY KEY,
                        Subscribers BIGINT,
                        Views BIGINT,
                        Total_vedios BIGINT,
                        Channel_description TEXT,
                        Playlist_id VARCHAR(100))'''
        
    cursor.execute(create_table)
    mysql_connection.commit()


    create_playlist_table='''CREATE TABLE IF NOT EXISTS playlist_data(
                            Playlist_id VARCHAR(50) PRIMARY KEY,
                            Channel_id VARCHAR(100),
                            Playlist_title VARCHAR(100))'''

    cursor.execute(create_playlist_table)
    mysql_connection.commit()

    create_videotable = '''
            CREATE TABLE IF NOT EXISTS video_details (
            title VARCHAR(500),
            video_id VARCHAR(255) PRIMARY KEY,
            channel_id VARCHAR(500),
            published_date DATE,
            video_description TEXT, 
            views BIGINT,
            likes BIGINT,
            comments BIGINT,
            time_duration VARCHAR(200))'''
    cursor.execute(create_videotable)
    mysql_connection.commit()
        
        
    create_comment_table = '''
                CREATE TABLE IF NOT EXISTS comment_details (
                Comment_id VARCHAR(100) PRIMARY KEY,
                Comment_text TEXT,
                Comment_author TEXT,
                Comment_published_at DATE,
                Video_id VARCHAR(255))'''
        
    cursor.execute(create_comment_table)
    mysql_connection.commit()

    cursor.close()
    mysql_connection.close()
    
    return True  

#migrate data from MongoDB to SQL      
def insert_into_sql():

    mysql_connection=mysql.connector.connect(
        host="localhost", #127.0.0.1
            user="root",
            password="",
            database='youtubedata07'
        )
    cursor=mysql_connection.cursor()

    url="mongodb+srv://abinash17:abi17@cluster0.yxlczno.mongodb.net/?retryWrites=true&w=majority"
    client=MongoClient(url)
    db=client.youtubedata07
    collection =db.ch_data

    ch_details=[]
    for data in collection.find({},{"_id":0,'Channel_Details':1}):
        ch_details.append(data['Channel_Details'])

    pl_details=[]
    for data in collection.find({},{'Playlist_details':1}):
        for i in range(len(data['Playlist_details'])):
            pl_details.append(data['Playlist_details'][i])
    
    video_dat = []   
    for data in collection.find({},{"_id":0,'Video_data':1}):
        for j in range(len(data['Video_data'])):
            video_dat.append(data['Video_data'][j])
        
    comment_dat = []    
    for data in collection.find({},{"_id":0,'Comments':1}):
        for k in range(len(data['Comments'])):
            comment_dat.append(data['Comments'][k])

    df=pd.DataFrame(ch_details)
    df1=pd.DataFrame(pl_details)
    df2=pd.DataFrame(video_dat)
    df3=pd.DataFrame(comment_dat)

    # Inserting channel details
    for index,row in df.iterrows(): 
        #print(index,row)
        insert_channeldetails = '''INSERT IGNORE INTO channels_data (Channel_name,Channel_id,Subscribers,Views,Total_vedios,Channel_description,Playlist_id)
                                   VALUES(%s,%s,%s,%s,%s,%s,%s)'''
    
        values=(row['Channel_name'],row['Channel_id'],row['Subscribers'],row['Views'],row['Total_Videos'],row['channel_description'],row['Playlist_id'])
        cursor.execute(insert_channeldetails, values)
        mysql_connection.commit()

    #Inserting Playlist_details
    for index,row in df1.iterrows():
        #print(index,row)
        insert_playlistdetails='''INSERT IGNORE INTO playlist_data(Playlist_id,Channel_id,Playlist_title)
                                  values(%s,%s,%s)'''

        
        values =(row['playlist_id'],row['channel_id'],row['title'])
        cursor.execute( insert_playlistdetails,values)
        mysql_connection.commit()              


    # Inserting video details
    for index,row in df2.iterrows():
        insert_videodetails = '''INSERT IGNORE INTO video_details (
        title, video_id,channel_id, published_date, video_description, views, likes, comments,time_duration)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        values =( row['video_name'],row['vedio_id'],row['channel_id'],row['Published_date'],row['video_description'],row['Views'],row['Likes'],row['Comment_Count'],row['Duration'])
        cursor.execute(insert_videodetails, values)
        mysql_connection.commit() 

    # Inserting comment details   
    for index,row in df3.iterrows():
        #print(index,row)
        insert_commentdetails='''INSERT IGNORE INTO comment_details(Comment_id,Comment_text,Comment_author,Comment_published_at,Video_id )
                                 values(%s,%s,%s,%s,%s)'''

        
        values =(row['Comment_id'],row['comment_text'],row['comment_author'],row['comment_publishedDate'],row['vedioid'])
        cursor.execute(insert_commentdetails,values)
        mysql_connection.commit()


#mongoDB connection
url="mongodb+srv://abinash17:abi17@cluster0.yxlczno.mongodb.net/?retryWrites=true&w=majority"
client=MongoClient(url)
db=client.youtubedata07
collection =db.ch_data

def show_channel_table():
        ch_details=[]
        for data in collection.find({},{"_id":0,'Channel_Details':1}):
                ch_details.append(data['Channel_Details'])
        df5=st.dataframe(ch_details )

        return df5


def show_playlist_table():
    pl_details=[]
    for data in collection.find({},{'Playlist_details':1}):
        for i in range(len(data['Playlist_details'])):
            pl_details.append(data['Playlist_details'][i])

    df6=st.dataframe(pl_details)

    return df6

def show_video_table():
    video_dat = []   
    for data in collection.find({},{"_id":0,'Video_data':1}):
        for j in range(len(data['Video_data'])):
            video_dat.append(data['Video_data'][j])

    df7=st.dataframe(video_dat)

    return df7

def show_comment_table():
    comment_dat = []    
    for data in collection.find({},{"_id":0,'Comments':1}):
        for k in range(len(data['Comments'])):
            comment_dat.append(data['Comments'][k])
    df8=st.dataframe(comment_dat)

    return df8

#streamlit
def main():
    with st.sidebar:
        choice=option_menu(None,["HOME","COLLECT DATA and LOADING TO MONGODB","SQL DATA WAREHOUSE","CHANNEL QUERIES"],
                        icons=["HOUSE-DOOR-FILL"],
                        default_index=0,
                        orientation="vertical")

    if choice == "HOME":                 
        st.title(":red[YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]")
        st.header("skill take away")
        st.caption("Python Scripting")
        st.caption("Data Collection")
        st.caption("MongoDB")
        st.caption("Api Integration")
        st.caption("Data Managenent using MongoDB and Sql")

    

    if choice == "COLLECT DATA and LOADING TO MONGODB":
        st.title(":red[Data Collection and Load Channel Data to MongoDB]")
        
        channel_id=st.text_input("Enter the YouTube Channel ID..Get Channel ID From Channel Page")
        if st.button("collect Data"):
            ch_id=[]
            for ch_data in collection.find({},{"_id":0,'Channel_Details':1}):
                ch_id.append(ch_data['Channel_Details']["Channel_id"])

            if channel_id in ch_id:
                st.success("Given channel_id already exists")

            else:
                #all_data=get_all_details(channel_id)
                st.success("DATA COLLECTED SUCCESFULLY")
        if st.button("Store TO MongoDB"):
            with st.spinner('Please Wait for it...'):
                st.write(":red[Data collection successfully Stored to MongoDB]")
                st.write("The uploaded Details")
                all_data=get_all_details(channel_id)
                upload_to_mongoDB(all_data)
                st.write(all_data)
                



    if choice=="SQL DATA WAREHOUSE":
        mysql_connection=mysql.connector.connect(
            host="localhost", #127.0.0.1
            user="root",
            password="",
            database='youtubedata07'
        )
        cursor=mysql_connection.cursor()

        st.title(":red[Data Migration from MongoDB to SQL]")
        
        if st.button("Migrate to sql"):
            create_table()
            insert_into_sql()
            st.write("Migrated succcesfully")
            #st.success(create_table)
            #st.success(insert_table)


        show_table=st.radio("select the table for view",("CHANNELS","PLAYLIST","VIDEOS","COMMENTS"),index=None)

        if show_table == "CHANNELS":
             show_channel_table()

        elif show_table == "PLAYLIST":
             show_playlist_table()

        elif show_table == "VIDEOS":
             show_video_table()
                        
        elif show_table == "COMMENTS":
             show_comment_table()
            
    if choice == "CHANNEL QUERIES":
         mysql_connection=mysql.connector.connect(
              host="localhost", #127.0.0.1
              user="root",
              password="",
              database='youtubedata07'
                )
         cursor=mysql_connection.cursor()
        
         st.title(":red[Select any question to get Insights]")
        
         question = st.selectbox('Select your Queries',
                                ('1.What are the names of all the videos and their corresponding channels?',
                                '2.Which channels have the most number of videos, and how many videos do they have?',
                                '3.What are the top 10 most viewed videos and their respective channels?',
                                '4.How many comments were made on each video, and what are their corresponding video names?',
                                '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
                                '6.What is the total number of likes for each video, and what are their corresponding video names?',
                                '7.What is the total number of views for each channel, and what are their corresponding channel names?',
                                '8.What are the names of all the channels that have published videos in the year 2022?',
                                '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                '10.Which videos have the highest number of comments, and what are their corresponding channel names?'),index=None)
    
        
         if question == '1.What are the names of all the videos and their corresponding channels?':
                query1="select title AS Video_Title,channel_name AS Channel_Name FROM video_details AS A INNER JOIN channels_data AS B ON A.channel_id=B.channel_id"
                cursor.execute(query1)
                q1=cursor.fetchall()
                df=pd.DataFrame(q1,columns=cursor.column_names)
                st.write(df)
         elif question == '2.Which channels have the most number of videos, and how many videos do they have?':
                query2="select channel_name,Total_vedios as NO_OF_Videos from Channels_data ORDER BY Total_vedios DESC"
                cursor.execute(query2)
                q2=cursor.fetchall()
                df=pd.DataFrame(q2,columns=cursor.column_names)
                st.write(df)
         elif question == '3.What are the top 10 most viewed videos and their respective channels?':
                query3 = "select B.channel_name AS Channel_Name,title AS Video_Title,A.views AS Views from video_details as a inner join channels_data as b on a.channel_id=b.channel_id order by views desc limit 10"
                cursor.execute(query3)
                q3=cursor.fetchall()
                df=pd.DataFrame(q3,columns=cursor.column_names)
                st.write(df)
         elif question == '4.How many comments were made on each video, and what are their corresponding video names?':
                query4= "select title AS Video_Title,comments from video_details order by comments desc"
                cursor.execute(query4)
                q4=cursor.fetchall()
                df=pd.DataFrame(q4,columns=cursor.column_names)
                st.write(df)
         elif question == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
                query5 = "select B.channel_name AS Channel_Name,title AS Video_Title,A.likes AS Likes from video_details as a inner join channels_data as b on a.channel_id=b.channel_id order by likes desc limit 10"
                cursor.execute(query5)
                q5=cursor.fetchall()
                df=pd.DataFrame(q5,columns=cursor.column_names)
                st.write(df) 
         elif question == '6.What is the total number of likes for each video, and what are their corresponding video names?':
                query6= "select title AS Video_Title, likes from video_details order by likes desc"
                cursor.execute(query6)
                q6=cursor.fetchall()
                df=pd.DataFrame(q6,columns=cursor.column_names)
                st.write(df)            
         elif question == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
                query7= "select Channel_name,Views from channels_data"
                cursor.execute(query7)
                q7=cursor.fetchall()
                df=pd.DataFrame(q7,columns=cursor.column_names)
                st.write(df)
         elif question == '8.What are the names of all the channels that have published videos in the year 2022?':
                query8= "select A.title AS Video_Title,A.published_date as ReleaseDate,B.channel_name AS Channel_Name from video_details as a inner join channels_data as b on a.channel_id=b.channel_id where published_date  BETWEEN '2022-01-01' AND '2022-12-31' "
                cursor.execute(query8)
                q8=cursor.fetchall()
                df=pd.DataFrame(q8,columns=cursor.column_names)
                st.write(df)
         elif question == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
                query9= "select B.channel_name AS Channel_Name,avg(A.time_duration) as Avgduration from video_details as a inner join channels_data as b on a.channel_id=b.channel_id group by  Channel_Name"
                cursor.execute(query9)
                q9=cursor.fetchall()
                df=pd.DataFrame(q9,columns=cursor.column_names)
                st.write(df)
         elif question == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
                query10= "select B.channel_name AS Channel_Name,A.title AS Video_Title,A.comments as TotalComments from video_details as A inner join channels_data as B on a.channel_id=b.channel_id order by A.comments desc limit 10"
                cursor.execute(query10)
                q10=cursor.fetchall()
                df=pd.DataFrame(q10,columns=cursor.column_names)
                st.write(df)


if __name__ == '__main__':
        main()