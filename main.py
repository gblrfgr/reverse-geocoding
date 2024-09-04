import argparse
import logging
import nominatim
import polars as pl
import json


def main():
    parser = argparse.ArgumentParser(
        prog="reverse-geocode",
        description="Converts GeoJSON data to a CSV table with street addresses",
    )
    parser.add_argument("filename")
    parser.add_argument("-o", "--output")
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()
    print(args.filename)


if __name__ == "__main__":
    main()
