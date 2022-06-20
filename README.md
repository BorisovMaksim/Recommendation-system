# Spotify Recommendation System

Spotify Recommendation System can recommend tracks from Spotify's Million Playlist Dataset given initial set of tracks uri ( which are identifiers from Spotift platform). 




## Basic Usage

![alt text](https://drive.google.com/uc?export=view&id=1T7Za5Iv80PpgWa69YCPetACJ56egbENB)


## Data
 Sampled from the over 4 billion public playlists on Spotify, this dataset of 1 million playlists consist of over 2 million unique tracks by nearly 300,000 artists, and represents the largest public dataset of music playlists in the world. The dataset includes public playlists created by US Spotify users between January 2010 and November 2017.
 
## Data Storage
Playlist data was processed from 1000 json files and stored in PostreSQL database with tables:
1. playlist
2. track 
3. playlist_track



![alt text](https://drive.google.com/uc?export=view&id=1KXL-F5pftarXtigDsEW86t3PZSG9w_0V)

Where playlist_track is a link table between playlist_id and track_id.

## Modelling
1. **Content-based filtering** -Try to reccomend similar tracks (Similarity model).

2. **Collaborative filtering** - Try to find similar playlists and recommend tracks from them (Annoy).

### Models
#### Similarity model
Recommend tracks by finding it's closest neighbours in the cosine similarity sense

![alt text](https://drive.google.com/uc?export=view&id=123XlfB7hkja6FaQVRFy4nkCTdT76xMXB)



#### Annoy

Annoy (Approximate Nearest Neighbors Oh Yeah) is a C++ library with 
Python bindings to search for points in space that are close to a given query point. 


![alt text](https://drive.google.com/uc?export=view&id=1XP2GJ6uHTb4O3QzOSW05ngL4dakxDaB2)

Why is this useful? If you want to find nearest neighbors and you have many CPU's, you only need to build the index once.  Any process will be able to load the index into memory and will be able to do lookups immediately. It is used at Spotify for music recommendations.



## Metric
In the following, we denote the ground truth set of tracks by $G$
and the ordered list of recommended tracks by $R$.

**R-precision** is the number of retrieved relevant tracks divided by
the number of known relevant tracks (i.e., the number of withheld tracks):

$$R-precision  =  \frac{|G \cap R|}{|G|}$$



