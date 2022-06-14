from flask import Flask
from flask_restful import Api, Resource, reqparse


class Playlist(Resource):
    def get(self, tracks):
        for quote in ai_quotes:
            if(quote["id"] == id):
                return quote, 200
        return "Quote not found", 404

