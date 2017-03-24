from argparse import ArgumentParser, FileType

import yaml

from .importer import Importer


def main():
    parser = ArgumentParser()
    parser.add_argument('-c', '--config', default='config.yaml', type=FileType('r'))
    parser.add_argument('-b', '--boundary')
    parser.add_argument('-w', '--ignore-water', default=True, action='store_false')
    parser.add_argument('service', choices=['mq', 'osm', 'satellite'])
    parser.add_argument('zoom_levels', type=int, nargs='+')

    args = parser.parse_args()

    config = yaml.load(args.config)

    db_filename = config['database']
    s3_bucket_name = config['s3']['bucket']
    aws_access_key_id = config['aws']['access_key_id']
    aws_secret_access_key = config['aws']['secret_access_key']

    importer = Importer(db_filename, s3_bucket_name,
                        aws_access_key_id, aws_secret_access_key)

    for zoom_level in args.zoom_levels:
        importer(
            args.service,
            zoom_level,
            boundary=args.boundary,
            ignore_water=args.ignore_water
        )
