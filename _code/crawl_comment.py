import praw
import pandas as pd
import configparser

# Load configuration settings
config = configparser.ConfigParser()
config.read('config.ini')

# Reddit Client Setup
reddit = praw.Reddit(client_id=config['REDDIT']['CLIENT_ID'],
                     client_secret=config['REDDIT']['CLIENT_SECRET'],
                     user_agent=config['REDDIT']['USER_AGENT'])

# Constants from Config
NO_SUBMISSION = int(config['SETTINGS']['NO_SUBMISSION'])
LANG_ID = config['SETTINGS']['LANG_ID']
OUTPUT_PATH = config['SETTINGS']['OUTPUT_PATH']
FILE_PATH = config['SETTINGS']['FILE_PATH']

def collect_comments(subreddit_info):
    sub_name, sub_lang, sub_topic = subreddit_info
    subr = reddit.subreddit(sub_name)
    comment_list = []
    print(f'Processing subreddit {sub_name} with language {sub_lang}')

    try:
        for submission in subr.top(limit=NO_SUBMISSION):
            extract_comments(submission, sub_name, sub_lang, sub_topic, comment_list)
    except Exception as e:
        print(f'Exception occurred in subreddit {sub_name}: {e}')
        return comment_list, str(e)
    return comment_list, None

def extract_comments(submission, sub_name, sub_lang, sub_topic, comment_list):
    submission.comments.replace_more(limit=0)
    for comment in submission.comments.list():
        comment_list.append([
            sub_name, submission.title, submission.id, submission.selftext, comment.body,
            comment.author, comment.id, comment.parent_id, comment.created_utc, sub_lang, sub_topic
        ])

def save_comments_to_file(comments, sub_name, sub_lang):
    if comments:
        df = pd.DataFrame(comments, columns=[
            'subreddit', 'subm_title', 'subm_id', 'subm_body', 'comment_body', 'author_name', 'comment_id',
            'comment_parrent', 'created_utc', 'subr_lang', 'subr_topic'
        ])
        file_path = f'{OUTPUT_PATH}{sub_lang}__{sub_name}.csv'
        df.to_csv(file_path)
        print(f'Results written to {file_path}')
    else:
        print(f'No comments to write for {sub_name}')

def main():
    subreddit_df = pd.read_csv(FILE_PATH)
    failed_subreddits = []

    for index, row in subreddit_df.iterrows():
        comments, error = collect_comments((row['sub_name'], row['sub_lang_G'], row['Topic']))
        if error:
            failed_subreddits.append(row['sub_name'])
            continue
        save_comments_to_file(comments, row['sub_name'], row['sub_lang_G'])

    if failed_subreddits:
        failed_df = subreddit_df[subreddit_df['sub_name'].isin(failed_subreddits)]
        failed_path = f'{OUTPUT_PATH}pd_banned_subr_{LANG_ID}.csv'
        failed_df.to_csv(failed_path)
        print(f'List of banned subreddits written to {failed_path}')

if __name__ == '__main__':
    main()
