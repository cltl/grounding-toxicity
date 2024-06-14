import praw
import pandas as pd
from datetime import datetime
import time
import configparser

# Load configuration settings
config = configparser.ConfigParser()
config.read('config.ini')

# Reddit API Setup
reddit = praw.Reddit(
    client_id=config['REDDIT']['CLIENT_ID'],
    client_secret=config['REDDIT']['CLIENT_SECRET'],
    user_agent=config['REDDIT']['USER_AGENT']
)

# Load paths and search settings from config
EVENT_FILE_PATH = config['PATHS']['EVENT_FILE_PATH']
OUTPUT_PATH = config['PATHS']['OUTPUT_PATH']
RESULT_LIMIT = None if config['SEARCH']['RESULT_LIMIT'] == 'None' else int(config['SEARCH']['RESULT_LIMIT'])

def enrich_submission_data(submissions_df, event_profile, index):
    """Add event-related information to each submission DataFrame."""
    event_info = {
        'event_title': event_profile['event_title'][index],
        'event_id': event_profile['event_id'][index],
        'event_link': event_profile['event_link'][index],
        'language': event_profile['language'][index],
        'keyword_manual': event_profile['Manual_keywords'][index],
        'first_published_date': event_profile['first_published_date'][index]
    }
    for key, value in event_info.items():
        submissions_df[key] = len(submissions_df) * [value]
    return submissions_df

def search_submissions(keywords, after_date, language, event_title):
    """Search for Reddit submissions matching the given keywords and criteria."""
    submissions_list = []
    for keyword in keywords:
        time.sleep(2)  # Sleep to comply with rate limits
        for submission in reddit.subreddit('all').search(keyword, sort='top', limit=RESULT_LIMIT):
            submission_date = datetime.fromtimestamp(submission.created_utc)
            if submission_date > after_date:
                submissions_list.append([
                    keyword, language, event_title, submission.id, submission.title,
                    submission.selftext, submission.subreddit, submission.url, submission_date
                ])
        print(f'Total results for "{keyword}": {len(submissions_list)}')
    submissions_df = pd.DataFrame(submissions_list, columns=[
        'keyword', 'language', 'event_title', 'submission_id', 'submission_title',
        'submission_selftext', 'subreddit', 'submission_url', 'submission_date'
    ])
    file_name = f'{OUTPUT_PATH}{language}_{event_title}.csv'
    submissions_df.to_csv(file_name)
    print(f'Results written to {file_name}')

def main():
    event_profile_df = pd.read_csv(EVENT_FILE_PATH)
    for index, event_info in event_profile_df.iterrows():
        print(f'Processing event {index + 1}/{len(event_profile_df)} in language {event_info["language"]}')
        after_date = datetime.strptime(event_info['first_published_date'], '%m/%d/%Y')
        keywords = event_info['Manual_keywords'].split('#')
        search_submissions(keywords, after_date, event_info['language'], event_info['english_titles'])

if __name__ == "__main__":
    main()
