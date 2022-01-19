import argparse
import csv
from dataclasses import dataclass
import requests


@dataclass
class CmrCollection:
    shortname: str
    version: str
    concept_id: str
    url: str


@dataclass
class CmrFetcher:
    session = requests.Session()

    def __init__(self, cmr_env, cmr_provider):
        self.base_cmr_url = f'https://cmr.{cmr_env}.earthdata.nasa.gov/search'
        self.cmr_url = f'{self.base_cmr_url}/collections.umm_json?provider={cmr_provider}&page_size=100'

    def get_locations(self):
        collections = []

        csa = None
        while True:
            headers = {'CMR-Search-After': csa}
            res = self.session.get(self.cmr_url,
                                   headers=headers)
            if not res.ok:
                return f"Error: {res.content}"
            for ele in res.json()['items']:
                collection = ele['umm']
                collection_meta = ele['meta']
                concept_id = collection_meta['concept-id']
                url = f'{self.base_cmr_url}/concepts/{concept_id}.html'
                args = [collection.get(key) for key in ['ShortName', 'Version']] + [concept_id, url]
                collections.append(
                    CmrCollection(*args)
                )

            csa = res.headers.get('CMR-Search-After')
            if not csa:
                break

        return collections

    @staticmethod
    def write_csv(collections):
        schema = ['shortname', 'version', 'concept_id', 'url']
        with open('collections.csv', 'w', newline='') as csv_file:
            file_writer = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            file_writer.writerow([h for h in schema])
            for collection in collections:
                file_writer.writerow([collection.shortname, collection.version, collection.concept_id, collection.url])


def main():
    parser = argparse.ArgumentParser(
        description='This script can be used to get the shortname, version, concept ID, and landing page url for'
                    'collections in ')
    required = parser.add_argument_group('required arguments')
    required.add_argument('-e', '--environment', choices=['prod', 'sit', 'uat'], dest='environment', required=True,
                          help='CMR environment')
    required.add_argument('-p', '--provider', dest='provider', required=True,
                          help='CMR environment')

    args = parser.parse_args()

    t = CmrFetcher(cmr_env=args.environment, cmr_provider=args.provider)
    collections = t.get_locations()
    t.write_csv(collections)


if __name__ == '__main__':
    main()

