import io
import hashlib
import sqlite3

import boto3
from etaprogress.progress import ProgressBar
import requests

from .compressors import Pngquant, Jpegoptim


class Importer:

    services = {
        'osm': 'http://tile.openstreetmap.org/{zoom}/{col}/{row}.png',
        'satellite': 'http://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/Tile/{zoom}/{row}/{col}.jpg'
    }

    compressors = {
        'osm': Pngquant(),
        'satellite': Jpegoptim(),
    }

    extensions = {
        'osm': 'png',
        'satellite': 'jpg',
    }

    water = {
        'osm': 'c9bc878a43ceba4bb9367aabf87db2f32f1c0789',
        'satellite': None,
    }

    def __init__(self, db_filename, s3_bucket_name, aws_access_key_id,
                 aws_secret_access_key):
        self.db = sqlite3.connect(db_filename)

        s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)
        self.bucket = s3.Bucket(s3_bucket_name)

        self.cursor = self.db.cursor()

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS tiles (
                service VARCHAR(15),
                zoom INTEGER,
                row INTEGER,
                column INTEGER,
                water BOOLEAN
            );
        """)

    def already_done_tile(self, service, zoom, row, column):
        self.cursor.execute("""
            SELECT 1
            FROM tiles
            WHERE service = ? AND zoom = ? AND row = ? AND column = ?
        """, [service, zoom, row, column])

        row = self.cursor.fetchone()
        return row is not None

    def get_done_tiles(self, service, zoom, row):
        self.cursor.execute("""
            SELECT column
            FROM tiles
            WHERE service = ? AND zoom = ? AND row = ?
        """, [service, zoom, row])

        return list(row[0] for row in self.cursor)

    def set_done_tile(self, service, zoom, row, column, water):
        self.cursor.execute("""
            INSERT INTO tiles VALUES (?, ?, ?, ?, ?)
        """, [service, zoom, row, column, water])

    def shasum(self, data):
        m = hashlib.sha1()
        m.update(data)
        return m.hexdigest()

    def download_tile(self, service, zoom, row, column, ignore_water=True):
        service_url = self.services[service]
        compressor = self.compressors[service]
        extension = self.extensions[service]
        water = self.water[service]

        url = service_url.format(zoom=zoom, col=column, row=row)
        res = requests.get(url)

        if res.status_code == requests.codes.ok:
            is_water = self.shasum(res.content) == water
            if not is_water or not ignore_water:
                data = compressor.compress(res.content)
                key = f'tiles/{service}/{zoom}/{row}/{column}.{extension}'
                self.bucket.upload_fileobj(io.BytesIO(data), key)

            self.set_done_tile(service, zoom, row, column, is_water)
        else:
            print(f"Cannot download tile: {url}")

    def __call__(self, service, zoom, boundary=None, ignore_water=True):
        count = 2 ** zoom
        ccount = count * count
        uploaded = 0

        bar = ProgressBar(ccount)

        for row in range(0, count):
            tiles = self.get_done_tiles(service, zoom, row)
            for col in range(0, count):
                if col not in tiles:
                    self.download_tile(service, zoom, row, col, ignore_water)
                    uploaded += 1

                bar.numerator += 1
                print(bar, end='\r')

            self.db.commit()

        print()
        print(f'Total uploaded: {uploaded}/{ccount}')
