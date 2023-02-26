import logging
import requests
import feedparser
import sqlite3
import argparse
import aria2p
import configparser


parser = argparse.ArgumentParser(description="Upload video to Minio")
parser.add_argument("-c", "--config", help="Config file path", default="/etc/nyamedia-video/config.ini")
parser.add_argument("-m", "--mode", help="Mode: magnet, minio, sync, init", default="minio")
parser.add_argument("-d", "--database", help="Database file path", default="/etc/nyamedia-video/data.sqlite")
args = parser.parse_args()
config_file = args.config
config = configparser.ConfigParser()
config.read(config_file)


class DBHelper:

    def __init__(self, dbname=args.database):
        self.dbname = dbname
        self.conn = sqlite3.connect(dbname, check_same_thread=False)

    def setup(self):
        stmt = ('''
        CREATE TABLE IF NOT EXISTS series
        (
        series_id INTEGER PRIMARY KEY NOT NULL,
        rss_link TEXT,
        rss_source TEXT
        );

        CREATE TABLE IF NOT EXISTS missions
        (
        id INTEGER PRIMARY KEY NOT NULL,
        series_id INTEGER NOT NULL,
        info_hash TEXT NOT NULL
        )
        ''')
        try:
            self.conn.executescript(stmt)
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(e)

    def add_series(self, series_id, rss_link, rss_source):
        stmt = "INSERT INTO series (series_id, rss_link, rss_source) VALUES (?, ?, ?)"
        args = (series_id, rss_link, rss_source)
        try:
            self.conn.execute(stmt, args)
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(e)

    def del_series(self, series_id):
        stmt = "DELETE FROM series WHERE series_id = (?)"
        args = (series_id,)
        try:
            self.conn.execute(stmt, args)
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(e)

    def edit_series(self, series_id, rss_link):
        stmt = "UPDATE series SET rss_link = (?) WHERE series_id = (?)"
        args = (rss_link, series_id)
        try:
            self.conn.execute(stmt, args)
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(e)

    def list_series(self):
        stmt = "SELECT * FROM series"
        try:
            return self.conn.execute(stmt).fetchall()
        except sqlite3.Error as e:
            logging.error(e)

    def add_mission(self, series_id, info_hash):
        stmt = "INSERT INTO missions (series_id, info_hash) VALUES (?, ?)"
        args = (series_id, info_hash)
        try:
            self.conn.execute(stmt, args)
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(e)

    def list_mission(self, series_id):
        stmt = "SELECT * FROM missions WHERE series_id = (?)"
        args = (series_id,)
        try:
            return self.conn.execute(stmt, args).fetchall()
        except sqlite3.Error as e:
            logging.error(e)


def fetch_series_name(series_id: int):
    series_info = requests.get(config['API']['API_HOST'] + "/api/v1/series/" + str(series_id)).json()
    return series_info["data"]["series"]["name"]


def add_rss():
    db = DBHelper()
    series_id = input("Input series id: ")
    name = fetch_series_name(series_id)
    print("Series name: " + name)
    rss_link = input("Input rss url: ")
    rss_source = input("Input rss source: (nyaa)")
    db.add_series(series_id, rss_link, rss_source)


def fetch_rss():
    db = DBHelper()
    ar = aria2p.API(
        aria2p.Client(
            host=config['ARIA2']['AR_HOST'],
            port=config['ARIA2']['AR_PORT'],
            secret=config['ARIA2']['AR_SECRET']
        )
    )
    series_list = db.list_series()
    for series in series_list:
        series_id = series[0]
        rss_url = series[1]
        rss_source = series[2]
        rss_result = feedparser.parse(rss_url)
        missions = db.list_mission(series_id)

        if "entries" in rss_result:
            for entry in rss_result.entries:
                if rss_source == "nyaa":
                    info_hash = entry.nyaa_infohash
                elif rss_source == "dmhy":
                    info_hash = entry.enclosures[0].href
                else:
                    print("未支持的 rss 数据源")
                    return
                mission_exists = False
                for mission in missions:
                    if info_hash == mission[2]:
                        mission_exists = True
                        break
                if mission_exists:
                    break
                else:
                    magnet_file = entry.link
                    download_dir_ar = "/downloads" + "/" + str(series_id) + "/"
                    ar.add_magnet(magnet_file, options={"dir": download_dir_ar})
                    db.add_mission(series_id, info_hash)
        else:
            print("rss 数据源未发现内容。")


if __name__ == '__main__':
    if args.mode == "init":
        db = DBHelper()
        db.setup()
    elif args.mode == "add":
        add_rss()
    else:
        fetch_rss()