import argparse
import logging
import json
import typing
import csv
import collections
import asyncio
import aiohttp

# Basic setup
logger = logging.getLogger(__name__)

# Represents the coordinate and label for a single building in the dataset
BuildingCoord = collections.namedtuple(
    "BuildingCoord", ["longitude", "latitude", "label"]
)
# Represents the street address, coordinate, and label for a single building in the dataset
BuildingInfo = collections.namedtuple(
    "BuildingInfo", ["longitude", "latitude", "street_address", "label"]
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


async def get_address(
    session: aiohttp.ClientSession, coord: BuildingCoord
) -> BuildingInfo:
    async with session.get(
        "http://localhost:8088/reverse",
        params={
            "lat": str(coord.latitude),
            "lon": str(coord.longitude),
            "format": "json",
            "zoom": 18,
        },
    ) as response:
        body = await response.json()
        print(body)
        return BuildingInfo(
            coord.longitude, coord.latitude, body["display_name"], coord.label
        )


async def get_addresses(coords: list[BuildingCoord]) -> list[asyncio.Future[BuildingInfo]]:
    async with aiohttp.ClientSession() as session:
        tasks = [get_address(session, coord) for coord in coords]
        results = await asyncio.gather(*tasks)
    return results


async def main():
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
        logger.critical('File "%s" not found', args.filename)
        exit(-1)
    else:
        with f:
            coords = get_coords(json.load(f))
    addresses = await get_addresses(coords)

    with open("test.csv", "w", newline="") as outputfile:
        writer = csv.writer(outputfile)
        writer.writerow(["Latitude", "Longitude", "Street Address", "Label"])
        writer.writerows(addresses)
    print(len([address for address in addresses]))


if __name__ == "__main__":
    asyncio.run(main())
