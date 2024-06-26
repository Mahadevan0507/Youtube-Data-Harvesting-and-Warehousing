from googleapiclient.discovery import build
import pymongo
import json
import psycopg2
import streamlit as st
import pandas as pd
import certifi
ca=certifi.where()
import PIL
from PIL import Image


#API key connection

def Api_connect():
    Api_Id="AIzaSyC7uiKclPszYlB29VyJHjz-jC8rOSHDDIs"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()


#get channels information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data


# get video ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    response1=youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=Playlist_Id,
                                        maxResults=10).execute()
    for i in range(len(response1['items'])):
        video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
    return video_ids

#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)    
    return video_data


#get comment information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=10
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data


#upload to mongoDB
client=pymongo.MongoClient("mongodb+srv://mahadevan0507:mahadb0507@cluster0.b4uamu0.mongodb.net/?retryWrites=true&w=majority",tlsCAFile=ca)
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully"

import pandas as pd
from sqlalchemy import create_engine

# Step 1: Connect to your SQL database using SQLAlchemy
# Replace 'database_type' with your database type (e.g., 'postgresql', 'mysql', 'sqlite', etc.)
# Replace 'username', 'password', 'hostname', 'port', and 'database_name' with your database credentials
engine = create_engine('database_type://username:password@hostname:port/database_name')

# Step 2: Convert your DataFrame to a SQL table
# Replace 'df' with your DataFrame variable name and 'table_name' with your desired SQL table name
df.to_sql('table_name', engine, if_exists='replace', index=False)

# Step 3: Push the SQL table to your database
# This will create or replace the table named 'table_name' in your database with the data from the DataFrame

#Table creation for channels,playlists,videos,comments
def channelstable(channel_name_s):
   

def tables(channel_name):

    news= channelstable(channel_name)
    if news:
        st.write(news)
    else:
        videostable(channel_name)
        comments_table(channel_name)

    return "Tables Created Successfully"

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3

#Streamlit part
st.set_page_config(page_title="Youtube Project",page_icon=":clapper")
st.markdown(""" <style> body {background-color: #1E1E1E;color:white} </style>""",unsafe_allow_html=True)
#st.sidebar.image("Youtube-Icon.png", use_column_width=True)
#st.image(Image.open(r"C:\Users\ADMIN\Desktop\Newfolder\Youtube-Icon.jpg"), width=100)

channel_id = st.sidebar.text_input("**:blue[ID to gain details]**")

if st.sidebar.button("**:green[STORATION]**"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)

# Main page content
# Main page content
col1, col2 = st.columns([1, 3])
with col1:
    st.image(Image.open(r"C:\Users\ADMIN\Desktop\Newfolder\Youtube-Icon.jpg"), width=200)
with col2:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING #guvi_Capstone]")

all_channels = []
coll1 = db["channel_details"]
for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
    all_channels.append(ch_data["channel_information"]["Channel_Name"])

unique_channel = st.sidebar.selectbox("**:blue[Channel name]**", all_channels)



if st.sidebar.button("**:green[Connect || SQL]**"):
    # code for migrating to SQL goes here
    Table=tables(unique_channel)
    st.success(Table)

show_table = st.sidebar.selectbox("**:blue[TABLE FOR VIEW]**", ("CHANNELS", "VIDEOS", "COMMENTS"))

if show_table == "CHANNELS":
    st.write("**:green[CHANNEL TABLE]**")
    show_channels_table()
elif show_table == "VIDEOS":
    st.write("**:green[VIDEOS TABLE]**")
    show_videos_table()
elif show_table == "COMMENTS":
    st.write("**:green[COMMENTS TABLE]**")
    show_comments_table()


#SQL Connection

mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="SQL0507",
                      database="ytp1",
                      port="5432")

cursor=mydb.cursor()


question=st.sidebar.selectbox("**:blue[GIVEN QUERY]**",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. Videos with higest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))

if question=="1. All the videos and the channel name":
    st.write("**:green[QUERY TABLE]**")
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. channels with most number of videos":
    st.write("**:green[QUERY TABLE]**")
    st.write(":green[QUERY TABLE]")
    query2='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    st.write("**:green[QUERY TABLE]**")
    query3='''select views as views,channel_name as channelname,title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. comments in each videos":
    st.write("**:green[QUERY TABLE]**")
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5. Videos with higest likes":
    st.write("**:green[QUERY TABLE]**")
    query5='''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. likes of all videos":
    st.write("**:green[QUERY TABLE]**")
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. views of each channel":
    st.write("**:green[QUERY TABLE]**")
    query7='''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="8. videos published in the year of 2022":
    st.write("**:green[QUERY TABLE]**")
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":
    st.write("**:green[QUERY TABLE]**")
    query9=''' SELECT channel_name AS channelname, AVG(CAST(SUBSTRING(duration FROM 'PT([0-9]+)M') AS INTEGER) * 60 + CAST(SUBSTRING(duration FROM 'PT[0-9]+M([0-9]+)S') AS INTEGER)) AS averageduration FROM videos GROUP BY channel_name '''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    Table9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        Table9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(Table9)
    st.write(df1)

elif question=="10. videos with highest number of comments":
    st.write("**:green[QUERY TABLE]**")
    query10='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)
