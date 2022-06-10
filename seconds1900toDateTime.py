#!/bin/env python

import datetime
import argparse

secondsSince1900ToEpoch=2208988800

parser = argparse.ArgumentParser(description='Convert date in seconds since 1900 format to date/time string')
parser.add_argument('--debug', action='store_true', default=False, required=False, help='Enable debug logging')
parser.add_argument(dest='secondsSince1900', help='Date/time in seconds since 1900')
args = parser.parse_args()

secondsSince1970 = int(args.secondsSince1900) - secondsSince1900ToEpoch
datetime = datetime.datetime.fromtimestamp(secondsSince1970)
print(datetime)
