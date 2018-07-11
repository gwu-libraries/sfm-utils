#!/usr/bin/env python3

from sfmutils.api_client import ApiClient
import argparse
import logging
import sys

log = logging.getLogger(__name__)


def main(sys_argv):
    # Arguments
    parser = argparse.ArgumentParser(description="Return WARC filepaths for passing to other commandlines.")
    parser.add_argument("--harvest-start", help="ISO8601 datetime after which harvest was performed. For example, "
                                                "2015-02-22T14:49:07Z")
    parser.add_argument("--harvest-end", help="ISO8601 datetime before which harvest was performed. For example, "
                                              "2015-02-22T14:49:07Z")
    parser.add_argument("--warc-start", help="ISO8601 datetime after which WARC was created. For example, "
                                             "2015-02-22T14:49:07Z")
    parser.add_argument("--warc-end", help="ISO8601 datetime before which WARC was created. For example, "
                                           "2015-02-22T14:49:07Z")
    default_api_base_url = "http://api:8080"
    parser.add_argument("--api-base-url", help="Base url of the SFM API. Default is {}.".format(default_api_base_url),
                        default=default_api_base_url)
    parser.add_argument("--debug", type=lambda v: v.lower() in ("yes", "true", "t", "1"), nargs="?",
                        default="False", const="True")
    parser.add_argument("--newline", action="store_true", help="Separates WARCs by newline instead of space.")
    parser.add_argument("collection", nargs="+", help="Limit to WARCs of this collection. "
                                                      "Truncated collection ids may be used.")

    # Explicitly using sys.argv so that can mock out for testing.
    args = parser.parse_args(sys_argv[1:])

    # Logging
    logging.basicConfig(format='%(asctime)s: %(name)s --> %(message)s',
                        level=logging.DEBUG if args.debug else logging.INFO)
    logging.getLogger("requests").setLevel(logging.DEBUG if args.debug else logging.INFO)

    api_client = ApiClient(args.api_base_url)
    collection_ids = []
    for collection_id_part in args.collection:
        log.debug("Looking up collection id part %s", collection_id_part)
        if len(collection_id_part) == 32:
            collection_ids.append(collection_id_part)
        else:
            collections = list(api_client.collections(collection_id_startswith=collection_id_part))
            if len(collections) == 0:
                print("No matching collections for {}".format(collection_id_part))
                sys.exit(1)
            elif len(collections) > 1:
                print("Multuple matching collections for {}".format(collection_id_part))
                sys.exit(1)
            else:
                collection_ids.append(collections[0]["collection_id"])
    warc_filepaths = set()
    for collection_id in collection_ids:
        log.debug("Looking up warcs for %s", collection_id)
        warcs = api_client.warcs(collection_id=collection_id, harvest_date_start=args.harvest_start,
                                 harvest_date_end=args.harvest_end,
                                 created_date_start=args.warc_start, created_date_end=args.warc_end)
        for warc in warcs:
            warc_filepaths.add(warc["path"])
    sep = "\n" if args.newline else " "
    return sep.join(sorted(warc_filepaths))


if __name__ == "__main__":
    print(main(sys.argv))
