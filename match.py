# import the required libraries

import sqlite3
import collections
import requests
import urllib
from time import sleep
import sys

# store the first part of the url into a variable
API = 'https://itunes.apple.com/search?'

# get access to databases

# millionsong database
conn = sqlite3.connect('subset_track_metadata.db')
# our database
conn_b = sqlite3.connect('match.db')
c = conn.cursor()
c_b = conn_b.cursor()

# country of origin of song
countries = ['US', 'DE', 'BR', 'BZ']

# limitations in symbols for txt files
illegal_symbols = r'/\:*?"<>|'

# make a namedtuple with the necessary attributes for each song
m_Song = collections.namedtuple('m_Song', ["track_id", "song_id", "title", "artist_name", "release", "duration","year"])
i_Song = collections.namedtuple('i_Song', ["track_id", "track_title", "artist_id", "artist_name", "album", "album_id", "year", "duration", "url", "path"])
c.execute('SELECT track_id, song_id, title, artist_name, release, duration, year FROM songs')

# open and read the file that contains the number of the last matched song
with open('last.txt') as f:
    last = int(f.read())

# for each of 10000 songs from the millionsong database
for i, row in enumerate(c.fetchall()):
    
    # skip the matched songs
    if i < last:
        print('Skipping', i)
        continue
    # write the new value of the last matched song
    with open('last.txt', 'w') as f:
        f.write(str(i))
    # used to stop searching when a song is matched
    found = False
    print(row)
    # initialize the namedtuple using the row's fields 
    song = m_Song(*row)
    # get each artist, separated by ";"
    artists = song.artist_name.split(';')
    # search for each country
    for country in countries:
        # API's parameters
        query = {'country': country, 'media': 'music'}   
        # search for each artist  
        for artist in artists:
            # search based on song title and artist name
            query['term'] = song.title + ' ' + artist
            # encode query and form request url
            url = API + urllib.parse.urlencode(query)
            print(url)
            # deal with API's limitations
            sleep(4)
            response = requests.get(url)
            if response:
                response_json = response.json()
                # if any results have been returned, at least one match has been found
                if response_json['resultCount'] != 0:
                    print('Found:', song)
                    found = True
                    break
            else:
                # API's request rate exceeded 
                print('Error 403 at:', song)
                sys.exit()
        if found:
            break
    if not found:
        print('Not found:', song)
        continue
    # consider the match to be the first result
    match = response_json['results'][0]
    # construct a path to store the 30 sec clip
    path = match['trackName'] + ' ' + match['collectionName'] + '.m4a'
    # delete each forbidden symbol in path
    for symbol in illegal_symbols:
        path = path.replace(symbol, '')
    path = './music/' + path
    # initialize the namedtuple for iTunes, using the matched song
    i_song = i_Song(match['trackId'], match['trackName'], match['artistId'], match['artistName'], match['collectionName'], match['collectionId'], match['releaseDate'], match['trackTimeMillis']/1000, match['previewUrl'], path)
    # retrieve the 30 sec clip and write it in path
    music_response = requests.get(i_song.url)
    with open(path, 'wb') as f:
        f.write(music_response.content)
    try:    
        # insert into itunes, matches the corresponding entries
        c_b.execute('INSERT INTO itunes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', i_song)
        c_b.execute('INSERT INTO matches VALUES (?, ?)', (i_song.track_id, song.track_id))
        conn_b.commit()
    except sqlite3.IntegrityError as e:
        # in case a unique constraint has been violated
        print(e)

conn.close()
conn_b.close()