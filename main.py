import csv
import json

import lxml.etree
import requests
from bs4 import BeautifulSoup


class CmrCollection:
    def __init__(self, shortname='', version='', concept_id='', url=''):
        self.shortname = shortname
        self.version = version
        self.concept_id = concept_id
        self.url = url


class Thing:
    def __init__(self):
        self.session = requests.Session()

    def fetch_session(self, url, headers):
        """
        Establishes a session for requests.
        """
        return self.session.get(url, headers=headers, verify=False)

    def html_request(self, url_path: str, headers: dict = None):
        """
        :param url_path: The base URL where the files are served
        :param headers: Headers for the http request
        :return: The html of the page if the fetch is successful
        """
        opened_url = self.fetch_session(url=url_path, headers=headers)
        return BeautifulSoup(opened_url.text, features='html.parser')

    def headers_request(self, url_path: str):
        """
        Performs a head request for the given url.
        :param url_path The URL for the request
        :return Results of the request
        """
        return self.session.head(url_path).headers

    def get_locations(self):
        collections = []

        csa = None
        while True:
            headers = {'CMR-Search-After': csa, 'Accept': 'application/xml'}
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
            res = self.session.get(location.url, headers={'Accept': 'application/xml'})
            try:
                root = lxml.etree.fromstring(res.text.encode())
                location.shortname = root.findtext('ShortName')
                location.version = root.findtext('VersionId')
            except lxml.etree.XMLSyntaxError:
                content = json.loads(res.text)
                location.shortname = content.get('ShortName')
                location.version = content.get('Version')
                pass

    @staticmethod
    def write_csv(collections):
        schema = ['shortname', 'version', 'concept_id', 'url']
        with open('temp.csv', 'w', newline='') as csvfile:
            file_writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            file_writer.writerow([h for h in schema])
            for collection in collections:
                file_writer.writerow([collection.shortname, collection.version, collection.concept_id, collection.url])
        pass


def main():
    t = Thing()
    collections = t.get_locations()
    t.get_metadata(collections)
    t.write_csv(collections)
    pass


if __name__ == '__main__':
    main()
