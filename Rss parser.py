#!/usr/bin/env python
# coding: utf-8

# In[2]:


pip install feedparser


# In[5]:


import feedparser

feeds = [
    "http://rss.cnn.com/rss/cnn_topstories.rss",
    "http://qz.com/feed",
    "http://feeds.foxnews.com/foxnews/politics",
    "http://feeds.reuters.com/reuters/businessNews",
    "http://feeds.feedburner.com/NewshourWorld",
    "https://feeds.bbci.co.uk/news/world/asia/india/rss.xml"
]

articles = []

for feed_url in feeds:
    feed = feedparser.parse(feed_url)
    for entry in feed.entries:
        article = {
            'title': entry.get('title', ''),  # Use get to handle missing 'title' attribute
            'content': entry.get('summary', ''),  # Use get to handle missing 'summary' attribute
            'pub_date': entry.get('published', ''),  # Use get to handle missing 'published' attribute
            'source_url': entry.get('link', '')  # Use get to handle missing 'link' attribute
        }
        articles.append(article)


# In[4]:


pip install psycopg2


# In[5]:


import psycopg2 


# In[6]:


pip install sqlalchemy


# In[7]:


from sqlalchemy import create_engine, Column, String, DateTime, Integer, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError

# Replace 'postgres', 'Password', 'Rss' with your actual PostgreSQL credentials
DB_USERNAME = 'postgres'
DB_PASSWORD = 'Password'
DB_NAME = 'Rss'

# SQLAlchemy engine setup
engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@localhost/{DB_NAME}')

# SQLAlchemy session setup
Session = sessionmaker(bind=engine)
session = Session()

# Define a NewsArticle table
metadata = MetaData()
news_article_table = Table(
    'news_articles',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('title', String),
    Column('content', String),
    Column('pub_date', DateTime),
    Column('source_url', String),
)

# Create the table
try:
    metadata.create_all(engine)
    print("Table 'news_articles' created successfully.")
except ProgrammingError as e:
    print(f"Error creating table: {e}")

# Close the session
session.close()


# In[9]:


pip install celery


# In[15]:


import feedparser
from sqlalchemy import create_engine, Column, String, DateTime, Integer, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from celery import Celery
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# Download NLTK data
nltk.download('vader_lexicon')

# Replace 'postgres', 'Password', 'Rss' with your actual PostgreSQL credentials
DB_USERNAME = 'postgres'
DB_PASSWORD = 'Password'
DB_NAME = 'Rss'

# SQLAlchemy engine setup
engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@localhost/{DB_NAME}')

# SQLAlchemy session setup
Session = sessionmaker(bind=engine)
session = Session()

# Define a NewsArticle table
metadata = MetaData()
news_article_table = Table(
    'news_articles',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('title', String),
    Column('content', String),
    Column('pub_date', DateTime),
    Column('source_url', String),
    Column('category', String),  # Added category column
)

# Create the table if not exists
metadata.create_all(engine, checkfirst=True)

# Celery setup
app = Celery('news_processor', broker='pyamqp://guest@localhost//')

# Celery task to process news articles
@app.task
def process_article(article):
    # Sentiment analysis
    sid = SentimentIntensityAnalyzer()
    sentiment_score = sid.polarity_scores(article['content'])
    
    # Assign category based on sentiment score
    if sentiment_score['compound'] >= 0.05:
        category = 'Positive/Uplifting'
    elif sentiment_score['compound'] <= -0.05:
        category = 'Terrorism/Protest/Political Unrest/Riot'
    else:
        category = 'Others'

    # Update database with category
    article['category'] = category
    update_database_with_category(article)

def update_database_with_category(article):
    # Insert or update the database with the assigned category
    existing_article = session.query(news_article_table).filter_by(source_url=article['source_url']).first()

    if existing_article:
        # Update the category if the article already exists
        session.query(news_article_table).filter_by(source_url=article['source_url']).update({'category': article['category']})
    else:
        # Insert a new article if it doesn't exist
        session.execute(news_article_table.insert().values(**article))

    # Commit the changes
    session.commit()

# List of RSS feeds
feeds = [
    "http://rss.cnn.com/rss/cnn_topstories.rss",
    "http://qz.com/feed",
    "http://feeds.foxnews.com/foxnews/politics",
    "http://feeds.reuters.com/reuters/businessNews",
    "http://feeds.feedburner.com/NewshourWorld",
    "https://feeds.bbci.co.uk/news/world/asia/india/rss.xml"
]

# Iterate through RSS feeds
for feed_url in feeds:
    feed = feedparser.parse(feed_url)
    for entry in feed.entries:
        article = {
            'title': entry.get('title', ''),
            'content': entry.get('summary', ''),
            'pub_date': entry.get('published', ''),
            'source_url': entry.get('link', '')
        }
        process_article.delay(article)  # Send article to Celery task for processing

# Close the session
session.close()


# In[16]:


import logging

logging.basicConfig(filename='news_app.log', level=logging.INFO)

try:
    # Main application logic here
    pass
except Exception as e:
    logging.error(f"An error occurred: {e}")

