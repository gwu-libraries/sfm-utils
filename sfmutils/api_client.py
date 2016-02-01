import urlparse
import requests


class ApiClient:
    """
    A client for SFM-UI's API.
    """
    def __init__(self, base_url):
        self.base_url = base_url

    def _get(self, url_part, params):
        clean_params = {}
        for k, v in params.items():
            if v is not None:
                if isinstance(v, (list, tuple)):
                    # Get rid of empty lists
                    if v:
                        clean_params[k] = ",".join(v)
                else:
                    clean_params[k] = v
        url = urlparse.urljoin(self.base_url, url_part)
        resp = requests.get(url, params=clean_params)
        resp.raise_for_status()
        return resp.json()

    def warcs(self, seedset_id=None, seed_ids=None, harvest_date_start=None, harvest_date_end=None):
        """
        Iterator over WARC model objects.

        :param seedset_id: Limit WARCs to this seedset
        :param seed_ids: Limit WARCs to this list of seeds
        :param harvest_date_start: Limit to WARCs who harvest started after this datetime
        :param harvest_date_end: Limit to WARCs who harvest started before this datetime
        :return:
        """
        params = dict()
        params["seedset"] = seedset_id
        params["seed"] = seed_ids
        params["harvest_date_start"] = harvest_date_start
        params["harvest_date_end"] = harvest_date_end
        warcs = self._get("/api/v1/warcs/", params)
        for warc in warcs:
            yield warc
