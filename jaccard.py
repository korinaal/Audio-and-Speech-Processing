import sqlite3
import textdistance as td
from collections import defaultdict

conn = sqlite3.connect('match.db')
cur = conn.cursor()
cur.execute('''SELECT itunes.track_title, millionsong.track_title
FROM itunes INNER JOIN matches 
ON itunes.track_id = matches.i_track_id
INNER JOIN millionsong ON matches.m_track_id = millionsong.track_id''')
d = defaultdict(int)
for row in cur:
    a, b = row[0].lower(), row[1].lower()
    dist = td.jaccard(a, b)
    key = int(dist*10)/10
    print(dist, a, b)
    d[key] = d[key] + 1
conn.close()
