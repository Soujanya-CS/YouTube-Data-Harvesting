import pymongo
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st
import pymysql
import requests
import retrying
import pandas as pd


st.title(':Black[Youtube Data Analysis]')

# Connect to MongoDB
client = pymongo.MongoClient('mongodb+srv://username:****@cluster0.stgpqbt.mongodb.net/')
database = client['YoutubeFinal']
channel_collection = database['channelsData']
video_collection = database['videosData']
comment_collection = database['commentsData']

# Set up YouTube API client
api_key = 'yourapi'
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
            }
        }
        channel_collection.insert_one(channel)
        st.success('Channel details inserted into MongoDB!')

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
            all_video_stats = []

            for i in range(0, len(video_ids), 50):
                request = youtube.videos().list(
                    part='snippet,statistics',
                    id=','.join(video_ids[i:i + 50])
                )
                response = request.execute()

                for video in response['items']:
                    video_stats = {
                        'VideoId': video['id'],
                        'Title': video['snippet']['title'],
                        'Published_date': video['snippet']['publishedAt'],
                        'Likes': video['statistics'].get('likeCount', 0),
                        'Views': video['statistics'].get('viewCount', 0),
                        'Comments_count': video['statistics'].get('commentCount', 0),
                        'ChannelId': channel_id
                    }
                    all_video_stats.append(video_stats)

            return all_video_stats

        video_details = get_video_details(youtube, video_ids)

        # Insert video details into MongoDB
        if video_details:
            video_collection.insert_many(video_details)
            st.success('Video details inserted into MongoDB!')
        else:
            st.warning('No video details found or unable to retrieve video details.')
            
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
                st.warning(f'Comments disabled for video: {video_id}')
              else:
                 raise

        # Insert comments into MongoDB
        if video_details:
            for video in video_details:
                video_id = video['VideoId']
                video_comments = get_video_comments(youtube, video_id)
                if video_comments:
                    video['Comments'] = video_comments
                    comment_collection.insert_one(video)
            st.success('Comments inserted into MongoDB!')
        else:
            st.warning('No videos found or unable to retrieve comments.')

        # Connect to MySQL database
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='mysql_password',
            database='youtubedata'
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

        # Migrate data from MongoDB to MySQL
        with connection.cursor() as cursor:
            # Migrate channel details
            channels = channel_collection.find()
            if channels is not None:
                for channel in channels:
                    insert_channel_query = '''
                    INSERT IGNORE INTO channels (ChannelId, Channel_Name, Total_Videos, Subscriber_Count, Views)
                    VALUES (%s, %s, %s, %s, %s)
                    '''
                    cursor.execute(insert_channel_query, (
                        channel['Channel_Details']['ChannelId'],
                        channel['Channel_Details']['Channel_Name'],
                        channel['Channel_Details']['Total_Videos'],
                        channel['Channel_Details']['Subscriber_Count'],
                        channel['Channel_Details']['Views']
                    ))

            # Migrate video details
            videos = video_collection.find()
            if videos is not None:
                for video in videos:
                    insert_video_query = '''
                    INSERT IGNORE INTO videos (VideoId, Title, Published_date, Likes, Views, Comments_count, ChannelId)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    '''
                    cursor.execute(insert_video_query, (
                        video['VideoId'],
                        video['Title'],
                        video['Published_date'],
                        video['Likes'],
                        video['Views'],
                        video['Comments_count'],
                        video['ChannelId'],
                    ))
            
            video_comments = comment_collection.find()
            if video_comments is not None:
                for video_comment in video_comments:
                    for comment in video_comment['Comments']:
                        insert_comment_query = '''
                        INSERT IGNORE INTO comments (VideoId, Comment)
                        VALUES (%s, %s)
                        '''
                        cursor.execute(insert_comment_query, (
                            video_comment['VideoId'],
                            comment
                        ))

        connection.commit()
        st.success('Data migrated to MySQL database!')

    except HttpError as e:
        error_message = eval(e.content)['error']['message']
        st.error(f'YouTube API error: {error_message}')
    except pymongo.errors.PyMongoError as e:
        st.error(f'MongoDB error: {str(e)}')
    except pymysql.Error as e:
        st.error(f'MySQL error: {str(e)}')
    except requests.exceptions.RequestException as e:
        st.error(f'Request error: {str(e)}')
    except Exception as e:
        st.error(f'An error occurred: {str(e)}')

def SQL_queries_page():
    st.title("SQL Queries")

    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='mysql-password',
        database='youtubedata'
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
        SELECT videoid, channelid FROM youtubedata.videos;
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








