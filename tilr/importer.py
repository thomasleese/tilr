import io
import hashlib
import sqlite3

import boto3
from etaprogress.progress import ProgressBar
import requests

from .boundaries import Boundary
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

    boundaries = {
        # left, top, right, bottom
        'world': Boundary(-180, 85, 180, -85),
        'united_kingdom': Boundary(-9, 62, 2, 49.8),
        'europe': Boundary(-13.5, 71.8, 54, 10.4),
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

    def get_done_tiles(self, service, zoom):
        self.cursor.execute("""
            SELECT row, column
            FROM tiles
            WHERE service = ? AND zoom = ?
        """, [service, zoom])

        return set(tuple(row) for row in self.cursor)

    def set_done_tiles(self, rows):
        self.cursor.executemany("""
            INSERT INTO tiles VALUES (?, ?, ?, ?, ?)
        """, rows)
        self.db.commit()

    def shasum(self, data):
        m = hashlib.sha1()
        m.update(data)
        return m.hexdigest()

    def download_tile(self, service, zoom, row, column):
        service_url = self.services[service]

        url = service_url.format(zoom=zoom, col=column, row=row)
        res = requests.get(url)

        if res.status_code == requests.codes.ok:
            return res.content
        else:
            print(f"Cannot download tile: {url}")
            return None

    def is_water(self, service, data):
        return self.shasum(data) == self.water[service]

    def s3_key(self, service, zoom, row, column):
        extension = self.extensions[service]
        return f'tiles/{service}/{zoom}/{row}/{column}.{extension}'

    def compress_tile(self, service, data):
        compressor = self.compressors[service]
        return compressor.compress(data)

    def upload_to_s3(self, service, zoom, row, column, data):
        data = self.compress_tile(service, data)
        key = self.s3_key(service, zoom, row, column)
        self.bucket.upload_fileobj(io.BytesIO(data), key)

    def __call__(self, service, zoom, boundary, ignore_water=True):
        count = 2 ** zoom
        uploaded = 0

        boundary = self.boundaries[boundary]

        left, top, right, bottom = boundary.tile_bounds(zoom)

        print(f'Downloading {service} tiles at zoom level {zoom}')
        print('Boundaries:', left, top, '->', right, bottom)

        total_count = (bottom - top) * (right - left)

        bar = ProgressBar(total_count)

        done_tiles = self.get_done_tiles(service, zoom)

        tiles_to_be_done = []

        for row in range(top, bottom):
            for col in range(left, right):
                if (row, col) not in done_tiles:
                    tile_data = self.download_tile(service, zoom, row, col)
                    if tile_data is None:
                        continue

                    is_water = self.is_water(service, tile_data)
                    if not is_water or not ignore_water:
                        self.upload_to_s3(service, zoom, row, col, tile_data)
                        uploaded += 1

                    tiles_to_be_done.append((service, zoom, row, col, is_water))

                    print(bar, end='\r')

                bar.numerator += 1

            self.set_done_tiles(tiles_to_be_done)
            tiles_to_be_done = []

        print()
        print(f'Total uploaded: {uploaded}/{total_count}')
