from app import App



def main(model_name, tracks_uri, n):
    system = App(model_name=model_name)
    system.collect_data()
    system.extract_features()
    system.train()
    similar_tracks = system.model.predict(tracks_uri=tracks_uri, n=n)
    print(similar_tracks)
    return similar_tracks


if __name__ == '__main__':
    main(model_name="annoy", tracks_uri=["spotify:track:5yVb28fGlg5INf581ihyhl",
                                         "spotify:track:3VdRAvHOqPhWFIbCrW15Pe",
                                         "spotify:track:5ec7ipz0tV2oNzWnAtR53G",
                                         "spotify:track:2BTnLcf4QA05mB0VpXdCLw",
                                         "spotify:track:3GLklY1nu9hS4JebAA6hS3"], n=10)
