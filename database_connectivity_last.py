import re
import mysql.connector
import Youtube_Data_Extraction_Last as yde

#Mysql Database connection
def mysql_connection():
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        port='3306',
        database='youtube_datasets')
    return mydb
#mydb=mysql_connection()


#published data extraction
def pub_datetime(published_date):
  return ' '.join(published_date.rstrip('Z').split('T'))

#duration extraction
def cal_duration(duration):
  temp_list=[int(x) for x in re.findall('\d+', duration)][::-1]
  cal_list=[1,60,3600,3600*24]
  duration_in_second=0
  for x,y in zip(temp_list,cal_list[:len(temp_list)]):
    duration_in_second+=x*y
  return duration_in_second

#Data insertion in tables
def insert_data_in_tables(mydb,result):
    mycursor=mydb.cursor()#access mysql database
    #Channel Table data
    channel_uid=result['Channel_Name']['channel_id']  #primary key
    channel_name=result['Channel_Name']['channel_name']
    channel_type=result['Channel_Name']['Channel_Type']
    channel_views=result['Channel_Name']['channel_views']
    channel_description=result['Channel_Name']['channel_description']
    channel_status=result['Channel_Name']['channel_status']
    no_of_playlists=len(result['Channel_Name']['playlist_id'])
    #query for creating Table
    query1="""create table IF NOT EXISTS channel(channel_id varchar(255), channel_name varchar(255), channel_type varchar(255),
    channel_views int, channel_description text, channel_status varchar(255), no_of_playlists int, primary key(channel_id))"""
    mycursor.execute(query1)
    mydb.commit()
    #execute query to insert in channel table
    query="""INSERT INTO channel
    (channel_id,channel_name,channel_type,channel_views, channel_description,channel_status,no_of_playlists)
     SELECT %s, %s, %s, %s, %s, %s, %s  WHERE NOT exists (SELECT * FROM channel where channel_id = %s)"""
    field_values=(channel_uid,channel_name,channel_type,channel_views, channel_description,channel_status,no_of_playlists,channel_uid)
    mycursor.execute(query, field_values)
    mydb.commit()

    #playlist table creation
    query2 = """create table IF NOT EXISTS playlist(playlist_id varchar(255), playlist_name varchar(255), no_of_videos int,
    channel_id varchar(255), primary key(playlist_id), foreign key(channel_id) references channel(channel_id))"""
    mycursor.execute(query2)
    mydb.commit()
    #PlaylistTable Data

    if(yde.total_no_of_playlists>no_of_playlists):
        yde.total_no_of_playlists=no_of_playlists
    for i in range(yde.total_no_of_playlists):
        playlist_uid=result[f'Playlist_Id_{i+1}']['playlist_id'] #primary key
        playlist_name=result[f'Playlist_Id_{i+1}']['playlist_name']
        no_of_videos=result[f'Playlist_Id_{i+1}']['no_of_videos']
        channel_id=result['Channel_Name']['channel_id']   #foreign key
        query="""INSERT INTO playlist
        (playlist_id,playlist_name,no_of_videos,channel_id) select %s,%s,%s,%s
        WHERE NOT EXISTS (SELECT * FROM playlist WHERE playlist_id = %s)"""
        field_values=(playlist_uid,playlist_name,no_of_videos,channel_id,playlist_uid)
        mycursor.execute(query, field_values)
        mydb.commit()
    #video table creation
        query3 = """create table IF NOT EXISTS video(video_id varchar(255), video_name varchar(255), video_description text, published_date datetime,
        view_count int, like_count int, dislike_count int, favorite_count int, comment_count int, duration int, 
        thumbnail varchar(255), caption_status varchar(255), category_type varchar(255), actual_comment_count int,
        playlist_id varchar(255), primary key(video_id), foreign key(playlist_id) references playlist(playlist_id))"""
        mycursor.execute(query3)
        mydb.commit()
    #video table Data
        for j in range(no_of_videos):
            video_uid=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Video_id'] #primary key
            video_name=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Video_Name']
            video_description=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Video_Description']
            published_date=pub_datetime(result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Published_Date'])
            views_count=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Views_Count']
            likes_count=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Likes_Count']
            dislikes_count=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Dislikes_Count']
            favorite_count=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Dislikes_Count']
            comment_count=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Comment_Count']
            duration=cal_duration(result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Duration'])
            thumbnail=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Thumbnail']
            caption_status=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['caption_status']
            Category_type=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Category_type']
            no_of_comments=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['no_of_comments']
            video_playlist_id=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['playlist_id']  #foreign key
            query = """INSERT INTO video
            (video_id,video_name,video_description,published_date,view_count,like_count,dislike_count,favorite_count,comment_count,duration,thumbnail,caption_status,Category_type,actual_comment_count,playlist_id)
             select %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
             WHERE NOT EXISTS (SELECT * FROM video WHERE video_id = %s)"""
            field_values = (video_uid,video_name,video_description,published_date,views_count,likes_count,dislikes_count,favorite_count,comment_count,duration,thumbnail,caption_status,Category_type,no_of_comments,video_playlist_id,video_uid)
            mycursor.execute(query, field_values)
            mydb.commit()
    #comment table creation
            query4 = """create table IF NOT EXISTS comment(comment_id varchar(255), comment_text text, comment_author varchar(255),
            comment_published_date datetime, video_id varchar(255), primary key(comment_id), foreign key(video_id) references video(video_id))"""
            mycursor.execute(query4)
            mydb.commit()
    #Comment Table Data
            for k in range(no_of_comments):
                comment_uid=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Comments'][f'Comment_Id_{k+1}']['Comment_Id'] #primary key
                comment_text=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Comments'][f'Comment_Id_{k+1}']['Comment_Text']
                comment_author=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Comments'][f'Comment_Id_{k+1}']['Comment_Author']
                comment_published_date=pub_datetime(result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Comments'][f'Comment_Id_{k+1}']['Comment_PublishedAt'])
                video_id=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Video_id']  #foreign key
                query = """INSERT INTO comment
                        (comment_id,comment_text,comment_author,comment_published_date,video_id) 
                        select %s,%s,%s,%s,%s
                        WHERE NOT EXISTS (SELECT * FROM comment WHERE comment_id = %s)"""
                field_values = (comment_uid,comment_text,comment_author,comment_published_date,video_id,comment_uid)
                mycursor.execute(query, field_values)
                mydb.commit()
