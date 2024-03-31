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

def APIc():
    Api_Id="AIzaSyDrnHoejl1zcq0P_4P0-XCoFlZ5vTmMIjM"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    return youtube
youtube=APIc()


#channels information
def GCI(CHID):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=CHID
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                CHID=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

def GVIds(CHID):
    VIDS=[]
    response=youtube.channels().list(id=CHID,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    response1=youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=Playlist_Id,
                                        maxResults=10).execute()
    for i in range(len(response1['items'])):
        VIDS.append(response1['items'][i]['snippet']['resourceId']['videoId'])
    return VIDS

#get video information
def GVI(VIDS):
    videodata=[]
    for video_id in VIDS:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    CHID=item['snippet']['channelId'],
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
            videodata.append(data)    
    return videodata


#get comment information
def GCOMI(VIDS):
    Comment_data=[]
    try:
        for video_id in VIDS:
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
db=client["YTPT"]

def channel_details(CHID):
    ch_details=GCI(CHID)
    vi_ids=GVIds(CHID)
    vi_details=GVI(vi_ids)
    com_details=GCOMI(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully"


#Table creation for channels,playlists,videos,comments
def CTBL(CNAME1):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="SQL0507",
                        database="ytp1",
                        port="5432")
    cursor=mydb.cursor()



    create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                        CHID varchar(80) primary key,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(80))'''
    cursor.execute(create_query)
    mydb.commit()



    SCD= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":CNAME1},{"_id":0}):
        SCD.append(ch_data["channel_information"])

    df_SC= pd.DataFrame(SCD)



    for index,row in df_SC.iterrows():
        insert_query='''insert into channels(Channel_Name ,
                                            CHID,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['CHID'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            news= f"Your Provided Channel {CNAME1} is Already exists"        
            return news



#CREATING AND INSERTING  VIDEO TABLE
def VTBL(CNAME1):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="SQL0507",
                        database="ytp1",
                        port="5432")
    cursor=mydb.cursor()

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    CHID varchar(100),
                                                    Video_Id varchar(30) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50)
                                                        )'''

    cursor.execute(create_query)
    mydb.commit()


    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":CNAME1},{"_id":0}):
        single_channel_details.append(ch_data["video_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])



    for index,row in df_single_channel.iterrows():
            insert_query='''insert into videos(Channel_Name,
                                                    CHID,
                                                    Video_Id,
                                                    Title,
                                                    Tags,
                                                    Thumbnail,
                                                    Description,
                                                    Published_Date,
                                                    Duration,
                                                    Views,
                                                    Likes,
                                                    Comments,
                                                    Favorite_Count,
                                                    Definition,
                                                    Caption_Status
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        

            values=(row['Channel_Name'],
                    row['CHID'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status']
                    )

            
            cursor.execute(insert_query,values)
            mydb.commit()

#CREATING AND INSERTING  COMMENT TABLE

def COTBL(CNAME1):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="SQL0507",
                        database="ytp1",
                        port="5432")
    cursor=mydb.cursor()


    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                        )'''

    cursor.execute(create_query)
    mydb.commit()

    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":CNAME1},{"_id":0}):
        single_channel_details.append(ch_data["comment_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])

    for index,row in df_single_channel.iterrows():
            insert_query='''insert into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published
                                                )
                                                
                                                values(%s,%s,%s,%s,%s)'''
            
            
            values=(row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published']
                    )

            
            cursor.execute(insert_query,values)
            mydb.commit()

#ENTERING CHANNEL NAME AND SHOWING DATA
def tables(channel_name):
    XY= CTBL(channel_name)
    if XY:
        st.write(XY)
    else:
        VTBL(channel_name)
        COTBL(channel_name)
    return "Tables Created Successfully"

def SCT():
    chlist=[]
    db=client["YTPT"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        chlist.append(ch_data["channel_information"])
    df1=st.dataframe(chlist)

    return df1

def SVT():
    vilist=[]
    db=client["YTPT"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vilist.append(vi_data["video_information"][i])
    df2=st.dataframe(vilist)

    return df2

def SCOT():
    comlist=[]
    db=client["YTPT"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            comlist.append(com_data["comment_information"][i])
    df3=st.dataframe(comlist)
    return df3

#streamlit part

st.set_page_config(page_title="Youtube Project",page_icon=":clapper")
#st.sidebar.image("Youtube-Icon.png", use_column_width=True)
#st.image(Image.open(r"C:\Users\ADMIN\Desktop\Newfolder\Youtube-Icon.jpg"), width=100)

CHID = st.sidebar.text_input("**:blue[ID to gain details]**")

if st.sidebar.button("**:green[STORATION]**"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["CHID"])

    if CHID in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_details(CHID)
        st.success(insert)


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

ST = st.sidebar.selectbox("**:blue[TABLE FOR VIEW]**", ("CHANNELS", "VIDEOS", "COMMENTS"))

if ST == "CHANNELS":
    st.write("**:green[CHANNEL TABLE]**")
    SCT()
elif StopIteration == "VIDEOS":
    st.write("**:green[VIDEOS TABLE]**")
    SVT()
elif ST == "COMMENTS":
    st.write("**:green[COMMENTS TABLE]**")
    SCOT()

#SQL Connection

mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="SQL0507",
                        database="ytp1",
                        port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. All the videos and the channel name",
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
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5. Videos with higest likes":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. likes of all videos":
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. views of each channel":
    query7='''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="8. videos published in the year of 2022":
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    TBL=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        TBL.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(TBL)
    st.write(df1)

elif question=="10. videos with highest number of comments":
    query10='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)