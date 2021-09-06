import sqlite3
import json
import re
import time
import shutil
import os

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler


def remove_puct(sentence):
    sentence_wo_punct = re.sub('[^A-Za-z0-9 ]+', '', sentence)
    return sentence_wo_punct

def sentence_length_calc(sentence):
    sentence_length = len(sentence.split())
    return sentence_length

def write_to_sql(filename):
    connection = sqlite3.connect("/data/database/lingomooAPP.db")
    cursor = connection.cursor()

    command1 = """CREATE TABLE IF NOT EXISTS
    sentences(sentence_id INTEGER PRIMARY KEY, date_scrapped TEXT, article_tag TEXT, article_url TEXT, article_image_src TEXT, website TEXT, sentence TEXT, sentence_length INTEGER, difficulty_score FLOAT)"""

    cursor.execute(command1)

    # reads from the JSON file to transfer the news articles into the SQL database
    with open(filename) as json_file:
        data = json.load(json_file)

        for l in data:
            date_scrapped = l['datetime']
            article_tag = l['tag']
            article_url = l['article_url']
            article_image_src = l['article_image_src']
            website = l['website']

            for sentence_article in l['article']:
                sentence = sentence_article.replace("READ MORE:", "")
                sentence = sentence.replace('"', '')
                
                sentence_length = sentence_length_calc(remove_puct(sentence_article))
                difficulty_score = 0
                print(sentence)
                # only adding the sentences with the word count between 7 and 15
                if sentence_length > 7 and sentence_length < 15:
                    cursor.execute("INSERT INTO sentences (date_scrapped, article_tag, article_url, article_image_src, website, sentence, sentence_length, difficulty_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (date_scrapped, article_tag, article_url, article_image_src, website, sentence, sentence_length, difficulty_score))
                    connection.commit()

    # deletes the dublicate lines containing the same sentece
    cursor.execute("DELETE FROM sentences WHERE sentence_id NOT IN (SELECT min(sentence_id) FROM sentences GROUP BY sentence)")
    connection.commit() 
    cursor.close()

    shutil.move(filename, "/data/archive/json/" + os.path.basename(filename))

if __name__ == "__main__":
    patterns = ["*.json"]
    ignore_patterns = None
    ignore_directories = False
    case_sensitive = True
    my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)

    def on_created(event):
        print(f"hey, {event.src_path} has been created!")
        write_to_sql(event.src_path)

    my_event_handler.on_created = on_created

    path = "/data/today/json/"
    go_recursively = True
    my_observer = Observer()
    my_observer.schedule(my_event_handler, path, recursive=go_recursively)
    my_observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        my_observer.stop()
        my_observer.join()