import pymongo
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st
import pymysql
import requests
import retrying
import pandas as pd
from datetime import datetime

st.title('Youtube Data Analysis')

# Connect to MongoDB
client = pymongo.MongoClient('myconnectionlink')
database = client['Youtubetrial']
channel_collection = database['channels_Data']

# Set up YouTube API client
api_key = 'myapikey'
youtube = build('youtube', 'v3', developerKey=api_key)

def youtube_analysis_page():
    # Get channel details from user
    channel_id = st.text_input('Enter Channel ID:')
    if st.button('Retrieve and Insert'):
        try:
            def get_channel_data(youtube, channel_id):
                request = youtube.channels().list(
                    part='snippet,contentDetails,statistics',
                    id=channel_id
                )
                response = request.execute()
                return response

            channel_data = get_channel_data(youtube, channel_id)
            channel_name = channel_data['items'][0]['snippet']['title']
            channel_video_count = channel_data['items'][0]['statistics']['videoCount']
            channel_subscriber_count = channel_data['items'][0]['statistics']['subscriberCount']
            channel_view_count = channel_data['items'][0]['statistics']['viewCount']
            channel_description = channel_data['items'][0]['snippet']['description']
            channel_playlist_id = channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

            # Add your code here to retrieve channel details using the YouTube API
            channel = {
                "Channel_Details": {
                    "Channel_Name": channel_name,
                    "ChannelId": channel_id,
                    "Total_Videos": channel_video_count,
                    "Subscriber_Count": channel_subscriber_count,
                    "Views": channel_view_count,
                },
                "Videos": []
            }

            @retrying.retry(wait_fixed=2000, stop_max_attempt_number=3)
            def get_video_ids(youtube, channel_playlist_id):
                request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId=channel_playlist_id,
                    maxResults=50
                )
                response = request.execute()

                video_ids = []

                for item in response['items']:
                    video_ids.append(item['contentDetails']['videoId'])

                next_page_token = response.get('nextPageToken')
                more_pages = True

                while more_pages:
                    if next_page_token is None:
                        more_pages = False
                    else:
                        request = youtube.playlistItems().list(
                            part='contentDetails',
                            playlistId=channel_playlist_id,
                            maxResults=50,
                            pageToken=next_page_token
                        )
                        response = request.execute()

                        for item in response['items']:
                            video_ids.append(item['contentDetails']['videoId'])

                        next_page_token = response.get('nextPageToken')

                return video_ids

            video_ids = get_video_ids(youtube, channel_playlist_id)

            def get_video_details(youtube, video_ids):
                all_video_details = []

                for i in range(0, len(video_ids), 50):
                    request = youtube.videos().list(
                        part='snippet,statistics',
                        id=','.join(video_ids[i:i + 50])
                    )
                    response = request.execute()

                    for video in response['items']:
                        video_details = {
                            'VideoId': video['id'],
                            'Title': video['snippet']['title'],
                            'PublishedAt': video['snippet']['publishedAt'],
                            'Views': video['statistics']['viewCount'],
                            'Likes': video['statistics'].get('likeCount',0),
                            'Comments_Count':video['statistics'].get('commentCount',0),
                            'Comments': []
                        }
                        all_video_details.append(video_details)

                return all_video_details

            video_details = get_video_details(youtube, video_ids)

            for video in video_details:
                # Retrieve comments for each video
                @retrying.retry(wait_fixed=2000, stop_max_attempt_number=3)
                def get_video_comments(youtube, video_id):
                    try:
                        request = youtube.commentThreads().list(
                            part='snippet',
                            videoId=video_id,
                            maxResults=100
                        )
                        response = request.execute()

                        comments = []

                        for item in response['items']:
                            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                            comments.append(comment)

                        next_page_token = response.get('nextPageToken')
                        more_pages = True

                        while more_pages:
                            if next_page_token is None:
                                more_pages = False
                            else:
                                request = youtube.commentThreads().list(
                                    part='snippet',
                                    videoId=video_id,
                                    maxResults=100,
                                    pageToken=next_page_token
                                )
                                response = request.execute()

                                for item in response['items']:
                                    comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                                    comments.append(comment)

                                next_page_token = response.get('nextPageToken')

                        return comments
                    except HttpError as e:
                        if e.resp.status == 403:
                            st.warning(f'Comments disabled for video')
                        else:
                            raise

                video_comments = get_video_comments(youtube, video['VideoId'])
                if video_comments:
                    video['Comments'].extend(video_comments)

                channel['Videos'].append(video)

            # Insert channel and video data into MongoDB
            channel_collection.insert_one(channel)

            st.success('Channel data retrieved and inserted successfully.')
        except HttpError as e:
            st.error(f'An HTTP error occurred: {e}')

# Connect to MySQL database
# Connect to MySQL database
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='mydbpassword',
            database='youtubetrial'
        )

        # Create tables if they don't exist
        with connection.cursor() as cursor:
            create_channel_table_query = '''
            CREATE TABLE IF NOT EXISTS channels (
                ChannelId VARCHAR(255) PRIMARY KEY,
                Channel_Name VARCHAR(255),
                Total_Videos INT,
                Subscriber_Count INT,
                Views INT
            )
            '''
            cursor.execute(create_channel_table_query)

            create_video_table_query = '''
            CREATE TABLE IF NOT EXISTS videos (
                VideoId VARCHAR(255) PRIMARY KEY,
                Title VARCHAR(255),
                Published_date DATETIME,
                Likes INT,
                Views INT,
                Comments_count INT,
                ChannelId VARCHAR(255),
                FOREIGN KEY (ChannelId) REFERENCES channels(ChannelId)
            )
            '''
            cursor.execute(create_video_table_query)

            create_comment_table_query = '''
            CREATE TABLE IF NOT EXISTS comments (
                CommentId INT AUTO_INCREMENT PRIMARY KEY,
                VideoId VARCHAR(255),
                Comment TEXT,
                FOREIGN KEY (VideoId) REFERENCES videos(VideoId)
            )
            '''
            cursor.execute(create_comment_table_query)

            connection.commit()
          # Retrieve channel data from MongoDB
            channel_data = channel_collection.find_one({})

         # Insert channel data into MySQL
            channel_values = (
             channel_data["Channel_Details"]["ChannelId"],
             channel_data["Channel_Details"]["Channel_Name"],
             int(channel_data["Channel_Details"]["Total_Videos"]),
             int(channel_data["Channel_Details"]["Subscriber_Count"]),
             int(channel_data["Channel_Details"]["Views"])
            )
            cursor.execute('''
            INSERT INTO channels (ChannelId, Channel_Name, Total_Videos, Subscriber_Count, Views)
            VALUES (%s, %s, %s, %s, %s)
            ''', channel_values)

           # Retrieve video data from MongoDB
            video_data = channel_data["Videos"]

          # Insert video data into MySQL
            for video in video_data:
               video_values = (
               video["VideoId"],
               channel_data["Channel_Details"]["ChannelId"],
               video["Title"],
               datetime.strptime(video["PublishedAt"], '%Y-%m-%dT%H:%M:%SZ'),  # Convert to datetime object
               int(video["Views"]),
               int(video["Likes"]),
               int(video["Comments_Count"])
               )
               cursor.execute('''
               INSERT INTO videos (VideoId, ChannelId, Title, Published_date, Views, Likes, Comments_Count)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ''', video_values)

             # Retrieve comment data from MongoDB
               comments = video["Comments"]

             # Insert comment data into MySQL
               for comment in comments:
                 comment_values = (
                   video["VideoId"],
                   comment
                 )
                 cursor.execute('''
                  INSERT INTO comments (VideoId, Comment)
                  VALUES (%s, %s)
                  ''', comment_values)

        connection.commit()
        st.success('Data migrated to MySQL database!')

def SQL_queries_page():
    st.title("SQL Queries")

    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='mydbpassword',
        database='youtubetrial'
    )

    def execute_query(query_number, query):
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        st.subheader(f'Query Results ({query_number})')
        df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
        st.dataframe(df)
        #for result in results:
            #st.write(result)

    def query1():
        st.subheader("What are the names of all the videos and their corresponding channels?")
        query = """
        SELECT videoid, channelid FROM youtubetrial.videos;
        """
        execute_query(1, query)

    def query2():
        st.subheader("Which channels have the most number of videos, and how many videos do they have?")
        query = """
        SELECT Channel_Name, Total_videos FROM channels ORDER BY Total_Videos DESC;
        """
        execute_query(2, query)

    def query3():
        st.subheader("What are the top 10 most viewed videos and their respective channels?")
        query = """
        SELECT channels.Channel_Name, videos.Title, videos.Views FROM channels JOIN videos ON videos.channelId=channels.channelId ORDER BY videos.Views DESC LIMIT 10;
        """
        execute_query(3, query)

    def query4():
        st.subheader("How many comments were made on each video, and what are their corresponding video names?")
        query = """
        SELECT videos.Title, videos.Comments_count FROM videos;
        """
        execute_query(4, query)

    def query5():
        st.subheader("Which videos have the highest number of likes, and what are their corresponding channel names?")
        query = """
        SELECT channels.Channel_Name, videos.Likes FROM channels JOIN videos ON videos.channelId=channels.channelId ORDER BY videos.Likes DESC;
        """
        execute_query(5, query)

    def query6():
        st.subheader("What is the total number of likes and dislikes for each video, and what are their corresponding video names?")
        query = """
        SELECT videos.Title, videos.Likes FROM videos ORDER BY videos.Likes DESC;
        """
        execute_query(6, query)

    def query7():
        st.subheader("What is the total number of views for each channel, and what are their corresponding channel names?")
        query = """
        SELECT channels.Channel_Name, channels.Views FROM channels ORDER BY channels.Views DESC;
        """
        execute_query(7, query)

    def query8():
        st.subheader("What are the names of all the channels that have published videos in the year 2022?")
        query = """
        SELECT channels.Channel_Name, videos.Published_date FROM channels JOIN videos ON videos.channelId=channels.channelId WHERE YEAR(videos.Published_date) = 2022;
        """
        execute_query(8, query)

    def query9():
        st.subheader("What is the average duration of all videos in each channel, and what are their corresponding channel names?")
        st.write("No results for Query 9 as duration field is not considered")

    def query10():
        st.subheader("Which videos have the highest number of comments, and what are their corresponding channel names?")
        query = """
        SELECT channels.Channel_Name, videos.Comments_count FROM channels JOIN videos ON videos.channelId=channels.channelId ORDER BY videos.Comments_count DESC;
        """
        execute_query(10, query)

    def userquery():
        st.subheader("Enter your own query")
        st.warning(" Note: Three tables Channels with columns Channel_Name,ChannelId,Views,Total_videos,subscriber_count; videos with columns ChannelId,VideoId,Title,Likes,Comments_count,Views,Published_date;Commments with columns commentId,comment and videoId")
        query = st.text_area('Enter your SQL query:')
        if st.button('Execute'):
            execute_query(11,query)


    def main():
        pages = {
            'Query 1': query1,
            'Query 2': query2,
            'Query 3': query3,
            'Query 4': query4,
            'Query 5': query5,
            'Query 6': query6,
            'Query 7': query7,
            'Query 8': query8,
            'Query 9': query9,
            'Query 10': query10,
            'Execute SQL Query':userquery
        }

        st.sidebar.title('Navigation')
        selected_page = st.sidebar.radio('Go to', tuple(pages.keys()))

        if selected_page in pages:
            pages[selected_page]()

    main()

mainpages = {
    'Youtube Analysis': youtube_analysis_page,
    'SQL Queries': SQL_queries_page
}
st.sidebar.title('Navigation')
selected_page = st.sidebar.radio('Go to', tuple(mainpages.keys()))

if selected_page in mainpages:
 mainpages[selected_page]()
