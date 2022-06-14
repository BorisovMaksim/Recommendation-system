from app import App
import json
from flask import Flask
from flask_restful import reqparse, Api, Resource

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('tracks', type=list, location='form')
parser.add_argument('n', type=int, location='form')

system = App(model_name='annoy')
system.collect_data()
system.extract_features()
system.train()


class Playlist(Resource):

    def get(self):
        return """You can use POST for track recommendation! For example 
        curl http://localhost:5000/recommend -d 'n=5&
        tracks=["spotify:track:7EDxhByqnzl1B4VJfMXZf4",  "spotify:track:3f0MU7LerrhPX2sJxkGyNa"]' -X POST -v"""

    def post(self):
        args = parser.parse_args()
        tracks_uri = json.loads("".join(args['tracks']))
        similar_tracks = system.model.predict(tracks_uri=tracks_uri, n=args['n'])
        return similar_tracks.to_json()


api.add_resource(Playlist, '/recommend')
if __name__ == '__main__':
    app.run(debug=True)
