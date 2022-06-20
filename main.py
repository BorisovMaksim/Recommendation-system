from app import App
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--n', type=int, help='number of recommendations')
parser.add_argument('--tracks', nargs='+')
args = parser.parse_args()


def main(tracks_uri, n):
    system = App(model_name='annoy')
    system.collect_data()
    system.extract_features()
    system.train()
    similar_tracks = system.model.predict(tracks_uri=tracks_uri, n=n)
    print(similar_tracks)
    return similar_tracks


if __name__ == '__main__':
    main(tracks_uri=args.tracks, n=args.n)

