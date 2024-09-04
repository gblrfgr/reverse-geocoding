import argparse
import logging
import nominatim
import polars as pl
import json
import typing
import collections

# Basic setup
logger = logging.getLogger(__name__)

# Represents the coordinate and label for a single building in the dataset
BuildingCoord = collections.namedtuple(
    "BuildingCoord", ["longitude", "latitude", "label"]
)


def get_coords(doc: typing.Any) -> list[BuildingCoord]:
    """Extracts longitude-latitude coordinates from an input JSON object

    Args:
        doc (typing.Any): the JSON object (output of JSON.load/s)

    Returns:
        list[BuildingCoord]: a list of longitude-latitude coordinates
        accompanied by the corresponding building label
    """
    # Each building in the input data is represented as a single feature in the
    # doc. We can then decompose those features to get what we want. TODO: add
    # validation here
    return [
        BuildingCoord(
            building["geometry"]["coordinates"][0],
            building["geometry"]["coordinates"][1],
            building["properties"]["Label"],
        )
        for building in doc["features"]
    ]


def main():
    parser = argparse.ArgumentParser(
        prog="reverse-geocode",
        description="Converts GeoJSON data to a CSV table with street addresses",
    )
    parser.add_argument("filename")
    parser.add_argument("-o", "--output")
    parser.add_argument("-l", "--logfile")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        filename=args.logfile,
        level=(logging.DEBUG if args.verbose else logging.WARNING),
        format="%(asctime)s\t%(levelname)s:\t%(message)s",
    )

    try:
        f = open(args.filename, "r")
    except FileNotFoundError:
        logger.critical("File \"%s\" not found", args.filename)
        exit(-1)
    else:
        with f:
            coords = get_coords(json.load(f))
    print(coords)


if __name__ == "__main__":
    main()
