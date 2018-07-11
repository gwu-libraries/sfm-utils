import urllib.parse as urlparse
import requests


class ApiClient:
    """
    A client for SFM-UI's API.
    """
    def __init__(self, base_url):
        self.base_url = base_url

    @staticmethod
    def _clean_params(params):
        clean_params = {}
        for k, v in params.items():
            if v is not None:
                if isinstance(v, (list, tuple)):
                    # Get rid of empty lists
                    if v:
                        clean_params[k] = ",".join(v)
                else:
                    clean_params[k] = v
        return clean_params

    def _get(self, url_part, params):
        url = urlparse.urljoin(self.base_url, url_part)
        resp = requests.get(url, params=self._clean_params(params))
        resp.raise_for_status()
        while resp:
            resp_json = resp.json()
            for item in resp_json['results']:
                yield item
            if resp_json['next']:
                resp = requests.get(resp_json['next'])
                resp.raise_for_status()
            else:
                resp = None

    def warcs(self, collection_id=None, seed_ids=None, harvest_date_start=None, harvest_date_end=None,
              created_date_start=None, created_date_end=None):
        """
        Iterator over WARC model objects.

        :param collection_id: Limit WARCs to this collection
        :param seed_ids: Limit WARCs to this list of seeds
        :param harvest_date_start: Limit to WARCs whose harvest started after this datetime
        :param harvest_date_end: Limit to WARCs whose harvest started before this datetime
        :param created_date_start: Limit to WARCs created after this datetime
        :param created_date_end: Limit to WARCs created before this datetime
        :return: WARC iterator
        """
        params = dict()
        params["collection"] = collection_id
        params["seed"] = seed_ids
        params["harvest_date_start"] = harvest_date_start
        params["harvest_date_end"] = harvest_date_end
        params["created_date_start"] = created_date_start
        params["created_date_end"] = created_date_end
        return self._get("/api/v1/warcs/", params)

    def collections(self, collection_id_startswith=None):
        """
        Iterator over Collection model objects.

        :param collection_id_startswith: Limit to collections whose collection_id starts with this value
        :return: Collection iterator
        """
        params = dict()
        if collection_id_startswith:
            params["collection_startswith"] = collection_id_startswith
        return self._get("/api/v1/collections/", params)
