from flask import Flask, render_template, jsonify
from flask_cors import CORS
import json


def create_app():
    app = Flask(__name__, static_folder='./static/')
    CORS(app)

    @app.route('/')
    def index():
        return render_template('index.html')

    return app
