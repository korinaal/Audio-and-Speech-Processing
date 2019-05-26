import sqlite3
import collections

'''
Transfer the data from millionsong database to ours, keeping the relevant fields/columns.
'''

conn_a = sqlite3.connect('match.db')
conn_b = sqlite3.connect('subset_track_metadata.db')
c_a = conn_a.cursor()
c_b = conn_b.cursor()

# relevant fields/columns
a = ["track_id", "title", "artist_id", "artist_name", "release", "year", "duration"]
Song = collections.namedtuple('Song', a)
c_b.execute('SELECT ' + ', '.join(a) + ' FROM songs')

for row in c_b.fetchall():
    song = Song(*row)
    print(song)
    c_a.execute('INSERT INTO millionsong VALUES (?, ?, ?, ?, ?, ?, ?)', song)

conn_a.commit()
conn_a.close()
conn_b.close()