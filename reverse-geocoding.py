import argparse
import logging
import json
import typing
import csv
import collections
import asyncio
import aiohttp
import os
import time

if os.name == "nt":
    import psutil
else:
    import resource

# Basic setup
logger = logging.getLogger(__name__)
cumulative_lookup_time = 0  # Cumulative seconds spent on Nominatim requests
cumulative_lookup_lock = asyncio.Lock()  # Avoid race conditions
total_lookup_time = 0  # Time to complete all Nominatim requests

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
    # doc. We can then decompose those features to get what we want.
    # TODO: add validation here
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
    """Retrieve address information for a single building using the Nominatim
    API. Uses the highest level of detail possible.

    Args:
        session (aiohttp.ClientSession): the current aiohttp session
        coord (BuildingCoord): the coordinate and label of the building

    Returns:
        BuildingInfo: coordinate, street address, and label of the building
    """
    # TODO: remove hardcoded URL
    # TODO: add error handling
    global cumulative_lookup_time
    global cumulative_lookup_lock
    started = time.time()
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
    async with cumulative_lookup_lock:
        cumulative_lookup_time += time.time() - started
    return BuildingInfo(
        coord.longitude, coord.latitude, body["display_name"], coord.label
    )


async def get_addresses(
    coords: list[BuildingCoord],
) -> list[asyncio.Future[BuildingInfo]]:
    """Gather building information for a list of building coordinates

    Args:
        coords (list[BuildingCoord]): building coordinates to query

    Returns:
        list[asyncio.Future[BuildingInfo]]: futures for building information
    """
    async with aiohttp.ClientSession() as session:
        tasks = [get_address(session, coord) for coord in coords]
        results = await asyncio.gather(*tasks)
    return results


async def main():
    global total_lookup_time
    global cumulative_lookup_time

    parser = argparse.ArgumentParser(
        prog="reverse-geocode",
        description="Converts GeoJSON data to a CSV table with street addresses",
    )
    parser.add_argument("filename", help="input file name")
    parser.add_argument(
        "-o", "--output", required=True, help="output file name"
    )
    parser.add_argument(
        "-l",
        "--logfile",
        required=False,
        nargs=1,
        help="output file for logging (not required, by default will log to stdout)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        required=False,
        help="turn on to get profiling and other info logs",
    )
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
            # TODO: handle JSON errors
            coords = get_coords(json.load(f))
    # Through the power of async/await black magic, we can perform a large
    # amount of queries at the same time!
    started = time.time()
    buildings = await get_addresses(coords)
    total_lookup_time = time.time() - started

    try:
        f = open(args.output, "w", newline="")
    except OSError:
        logger.critical("Error creating output file")
        exit(-1)
    else:
        with f:
            writer = csv.writer(f)
            writer.writerow(
                ["Latitude", "Longitude", "Street Address", "Label"]
            )
            writer.writerows(buildings)
    unique_addresses = set([building.street_address for building in buildings])

    if os.name == "nt":
        peakmem = psutil.Process().memory_info().peak_wset // 1024
    else:
        peakmem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    logging.info("Peak memory usage is %i KiB", peakmem)
    logging.info(
        "Time to perform all lookups is %f seconds (wall clock time)",
        total_lookup_time,
    )
    logging.info(
        "Average lookup time is %f seconds (wall clock time)",
        cumulative_lookup_time / len(buildings),
    )
    logging.info(
        "%i total coordinates, %i unique addresses",
        len(buildings),
        len(unique_addresses),
    )


if __name__ == "__main__":
    asyncio.run(main())
