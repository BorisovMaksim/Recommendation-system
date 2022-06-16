from app import App


def main(tracks_uri, n):
    system = App(model_name='annoy')
    system.collect_data()
    system.extract_features()
    system.train()
    similar_tracks = system.model.predict(tracks_uri=tracks_uri, n=n)
    return similar_tracks


if __name__ == '__main__':
    main(tracks_uri=["spotify:track:7EDxhByqnzl1B4VJfMXZf4",
                     "spotify:track:3f0MU7LerrhPX2sJxkGyNa",
                     "spotify:track:7I10ft1XHvVcYMJGNcevU8",
                     "spotify:track:4ezv28TGOKCdxjDWPSwVf6",
                     "spotify:track:6Wv7WQ9ymnLR6g40Cuud6v"], n=3)
