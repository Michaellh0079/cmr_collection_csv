import csv
import json

import lxml.etree
import requests


class CmrCollection:
    def __init__(self, shortname='', version='', concept_id='', url=''):
        self.shortname = shortname
        self.version = version
        self.concept_id = concept_id
        self.url = url


class CmrFetcher:
    def __init__(self):
        self.session = requests.Session()

    def get_locations(self):
        collections = []

        csa = None
        while True:
            headers = {'CMR-Search-After': csa}
            res = self.session.get('https://cmr.uat.earthdata.nasa.gov/search/collections?provider=GHRC_CLOUD',
                                   headers=headers)
            root = lxml.etree.fromstring(res.text.encode())
            for reference in root.find('references'):
                collections.append(
                    CmrCollection(concept_id=reference.find('id').text, url=reference.find('location').text)
                )

            csa = res.headers.get('CMR-Search-After')
            if not csa:
                break

        return collections

    def get_metadata(self, locations):
        for location in locations:
            res = self.session.get(location.url)
            try:
                root = lxml.etree.fromstring(res.text.encode())
                location.shortname = root.findtext('ShortName')
                location.version = root.findtext('VersionId')
            except lxml.etree.XMLSyntaxError:
                content = json.loads(res.text)
                location.shortname = content.get('ShortName')
                location.version = content.get('Version')

    @staticmethod
    def write_csv(collections):
        schema = ['shortname', 'version', 'concept_id', 'url']
        with open('collections.csv', 'w', newline='') as csv_file:
            file_writer = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            file_writer.writerow([h for h in schema])
            for collection in collections:
                file_writer.writerow([collection.shortname, collection.version, collection.concept_id, collection.url])


def main():
    t = CmrFetcher()
    collections = t.get_locations()
    t.get_metadata(collections)
    t.write_csv(collections)


if __name__ == '__main__':
    main()
