import sqlite3
import json
import re

def remove_puct(sentence):
    sentence_wo_punct = re.sub('[^A-Za-z0-9 ]+', '', sentence)
    return sentence_wo_punct

def sentence_length_calc(sentence):
    sentence_length = len(sentence.split())
    return sentence_length

connection = sqlite3.connect("/data/lingomooAPP.db")
cursor = connection.cursor()

command1 = """CREATE TABLE IF NOT EXISTS
sentences(sentence_id INTEGER PRIMARY KEY, sentence TEXT, sentence_length INTEGER, difficulty_score FLOAT)"""

cursor.execute(command1)

# reads from the JSON file to transfer the news articles into the SQL database
with open('/data/articles_2021-08-11-18-40-24.json') as json_file:
    data = json.load(json_file)

    for l in data:

        for sentence_article in l['article']:
            sentence = sentence_article
            sentence_length = sentence_length_calc(remove_puct(sentence_article))
            difficulty_score = 0

            # only adding the sentences with the word count between 7 and 15
            if sentence_length > 7 and sentence_length < 15:
                cursor.execute("INSERT INTO sentences (sentence, sentence_length, difficulty_score) VALUES (?, ?, ?)",
                   (sentence, sentence_length, difficulty_score))
                connection.commit()

# deletes the dublicate lines containing the same sentece
cursor.execute("DELETE FROM sentences WHERE sentence_id NOT IN (SELECT min(sentence_id) FROM sentences GROUP BY sentence)")
connection.commit() 

cursor.close()
