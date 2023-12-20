from os import path

basedir = path.abspath(path.dirname(__file__))


class Config:
    SECRET_KEY = '\x15i_P\x90\x8f\x82(\xb4\xaf\xec\xc3'
    DEBUG = True

    # Database
    SQLALCHEMY_DATABASE_URI = "sqlite:////" + basedir + "/test.db"
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
