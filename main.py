import sqlite3
import pandas
from csv import DictReader, DictWriter
from collections import namedtuple

# Change values with the spreadsheets name
untranslated = 'untranslated.xlsx'
translated = 'translated.xlsx'

# Converting xlsx to csv from both spreadsheets
read_file = pandas.read_excel(untranslated)
read_file.to_csv('base/untranslated.csv', index=None, header=True)
read_file = pandas.read_excel(translated)
read_file.to_csv('base/translated.csv', index=None, header=True)

SentenceMaster = namedtuple('Sentence', 'id version original_text translation')
SentenceStaging = namedtuple('Sentence', 'original_text translation')

# Creating sqlite database
con = sqlite3.Connection('base/sentences.sqlite3')
cursor = con.cursor()

# Creating database tables
cursor.execute('CREATE TABLE IF NOT EXISTS sentence_master ('
               '    id     VARCHAR(1500)  NOT NULL,'
               '    version       VARCHAR(1500) NOT NULL,'
               '    original_text  VARCHAR(1500) NOT NULL,'
               '    translation VARCHAR(1500) NOT NULL'
               ');')

cursor.execute('CREATE TABLE IF NOT EXISTS sentence_staging ('
               '    original_text  VARCHAR(1500) NOT NULL,'
               '    translation VARCHAR(1500) NOT NULL'
               ');')


# Reading the csv spreadsheets and filling the table in the database
with open('base/untranslated.csv') as file:
    def process():
        for row in DictReader(file):
            yield (row["ID"], row["Repository Version"], row["Original Text"], row["Translate"])

    cursor.executemany(
        'INSERT INTO sentence_master (id, version, original_text, translation) VALUES (?, ?, ?, ?);',
        process()
    )
    con.commit()

with open('base/translated.csv') as file:
    def process():
        for row in DictReader(file):
            yield (row["Original Text"], row["Translate"])

    cursor.executemany(
        'INSERT INTO sentence_staging (original_text, translation) VALUES (?, ?);',
        process()
    )
    con.commit()


# Funcion to write the result spreadsheet
def save(filename, cursor):
    with open(filename, 'w', newline='') as file:
        fields = ['ID', 'Repository Version', 'Original Text', 'Translate']
        writer = DictWriter(file, fieldnames=fields)
        writer.writeheader()

        for row in cursor.fetchall():
            writer.writerow(dict(zip(fields, row)))


# SQL queries
cursor.execute(
    '''
    SELECT mst.id,
           mst.version,
           mst.original_text,
           stg."translation"
      FROM sentence_master mst
           INNER JOIN sentence_staging stg on(mst.original_text = stg.original_text)
     WHERE stg."translation" IS NOT NULL AND stg."translation" != '';
    '''
)
save('result/translations_updated.csv', cursor)

cursor.execute(
    '''
    SELECT mst.id,
           mst.version,
           mst.original_text,
           stg."translation"
      FROM sentence_master mst
           LEFT JOIN sentence_staging stg on(mst.original_text = stg.original_text)
     WHERE stg."translation" IS NULL OR stg."translation" = '';
    '''
)
save('result/errors.csv', cursor)
con.close()
