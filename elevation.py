import asyncio
import json
import os
import typing as typ
from functools import wraps
from pprint import pprint
from time import time
import aiohttp

import polyline
import requests

import geopandas as gpd

from coords_encoder import encode_coords
from exemple_data import ex_file


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        # print(f"func:{f.__name__} args:[{args}, {kw}] \n took: {te-ts:2.4f} sec")
        print(f"func:{f.__name__} took: {te-ts:2.4f} sec")
        return result
    return wrap


class Elevation(object):
    def __init__(self, j_file: typ.Union[str, bytes], api_key: str):
        self.locations = json.loads(j_file).get("locations")
        self.api_key = api_key
        # self.google_url = "https://maps.googleapis.com/maps/api/elevation/json?locations=enc:gfo}"
        self.google_url = "https://maps.googleapis.com/maps/api/elevation/json?locations="

    def coords_list(self):
        """

        :return:
        """
        # TIME python elevation.py  0,97s user 0,46s system 235% cpu 0,607 total
        # @timing: func:'coords_list' args:[(<__main__.Elevation object at 0x7f7490af25b0>,), {}] took: 0.0027 sec
        return [
            (
                str(item.get("lat")), str(item.get("lon"))
            ) for item in self.locations
        ]

    def coords_df(self):
        """
        Not Used now
        Mainly for test time difference between list-comprehension and df in list-comprehension
        :return:
        """
        # TIME python elevation.py  0,98s user 0,47s system 229% cpu 0,633 total
        # @timing: func:'coords_df' args:[(<__main__.Elevation object at 0x7f8d234515b0>,), {}] took: 0.0065 sec
        # gdf = gpd.GeoDataFrame(self.locations)
        # return [tuple(x) for x in gdf[["lat", "lon"]].to_numpy()]
        # TIME python elevation.py  0,90s user 0,54s system 233% cpu 0,616 total
        # @timing: func:'coords_df' args:[(<__main__.Elevation object at 0x7fe3323e85b0>,), {}] took: 0.0065 sec
        return [tuple(x) for x in gpd.GeoDataFrame(self.locations)[["lat", "lon"]].to_numpy()]

    def coords_string(self, coords: list) -> str:
        """
        Create a correct string forum coordinates to use it in request
        Example:
        https://maps.googleapis.com/maps/api/elevation/json?locations=-21.8303712,-49.127649|-21.8302281,-49.1275669|-21.8300812, -49.1274859
        :param coords:
        :return:
        """
        return '|'.join([f"{it[0]}, {it[1]}" for it in coords]).replace(" ", "")

    def create_batch_for_polyline(
            self, batch_size: int = int(os.environ.get("BATCH_SIZE", 100))
    ) -> typ.Generator:
        """Not used now for future
        This code encode the string of coords to polyline
        https://developers.google.com/maps/documentation/utilities/polylinealgorithm
        """
        coords = self.coords_list()
        ln = len(coords)
        for ndx in range(0, ln, batch_size):
            yield coords[ndx:min(ndx + batch_size, ln)]

    def batch_coords_to_str(self, batch_size: int = int(os.environ.get("BATCH_SIZE", 300))):
        """
        Devides coordinates list to batches
        :param batch_size:
        :return:
        """
        coords = self.coords_list()
        ln = len(coords)
        for ndx in range(0, ln, batch_size):
            yield self.coords_string(
                coords[ndx:min(ndx + batch_size, ln)]
            )

    def encode_polyline_list(self) -> list:
        """
        Not used now. https://developers.google.com/maps/documentation/elevation/overview#Locations
        :return:
        """
        en = polyline.encode(self.coords_list(), 5, geojson=True)
        # print(en)
        return en

    def batch_polyline_encode(self):
        """
        Not used. https://developers.google.com/maps/documentation/elevation/overview#Locations
        :return:
        """
        result = []
        for batch in self.create_batch_for_polyline():
            result.append(polyline.encode(batch, 5, geojson=True))
        return result

    @timing
    def make_coords_strings_list(self) -> list:
        """
        list of coordinates strings for used it in request.
        :return:
        """
        return [
            c_str for c_str in self.batch_coords_to_str()
        ]

    # NOTE It can be useful but not now!
    @staticmethod
    async def make_async_request(session, url) -> json:
        """
        Async representation of request to the google API
        :param session: Aiohttp Session
        :param url: Target url
        :return: dict like object
        """
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()

    async def fetch_data(self, urls: list) -> typ.Coroutine:
        """ Gather many HTTP call made async
        Args:
            urls: a list of string
        Return:
            responses: A list of dict like object containing http response
        NOTE: Max task timeout 5 min!!! By default to change it use "timeout" variable
        in ClientSession. Regard to memory leek.
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for url in urls:
                tasks.append(
                    self.make_async_request(
                        session,
                        url,
                    )
                )
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            return responses

    def make_request(self, coords_str) -> json:
        """
        Synchronous version of request to google API
        :param coords_str: one item from self.make_coords_strings_list
        :return: JSON object
        """
        uri = f"{self.google_url}{coords_str}&key={self.api_key}"
        # print(f"{uri}")
        # print(f"len {len(uri)}")
        with requests.get(uri) as resp:
            return resp

    def generate_urls(self) -> typ.List[str]:
        """
        Use in Async requests
        :return: List with target urls
        """
        return [
            f"{self.google_url}{coords_str}&key={self.api_key}"
            for coords_str in self.make_coords_strings_list()
        ]


    @timing
    def get_elevation_from_request(self, resp_res: dict) -> typ.List[str]:
        """
        Get elevation parameter from etch response.
        :param resp_res: dict like obj
        :return: list of elevation params
        """
        return [item.get("elevation") for item in resp_res["results"]]

    @timing
    def get_google_data(self) -> typ.List[dict]:
        """
        Function to get google data
        :return:
        """
        c_strs = self.make_coords_strings_list()
        # print(f"str len: {len(c_strs)}") # count of requests
        elevation_list = []
        for item in c_strs:
            resp = self.make_request(item)
            if resp.status_code == 200:
                elevation_list += self.get_elevation_from_request(resp.json())
        return elevation_list

    @timing
    def get_google_data_async(self):
        """
        Get google data. Create elevations list.
        :return:
        """
        responses = asyncio.run(self.fetch_data(self.generate_urls()))
        elevations = []
        for item in responses:
            elevations += self.get_elevation_from_request(item)
        return elevations

    @timing
    def get_average_elevation(self, asynchronous: bool = False) -> float:
        """
        Calculate average. Error -> ValueError: Length of values (n) does not match length of index (n)
        where n it is count of items getted and must be.
        Full example of error: ValueError: Length of values (5) does not match length of index (1273)
        It can be when we get not all data from google.
        :return: one float value
        """
        income_data_df = gpd.GeoDataFrame(self.locations)
        income_data_df["elevation"] = self.get_google_data() \
            if not asynchronous \
            else self.get_google_data_async()
        # This is max not average:
        # print(income_data_df["z"].max())
        # print(income_data_df["elevation"].max())
        # return income_data_df["z"].max() - income_data_df["elevation"].max()
        # This is average:
        # print(income_data_df["z"].mean())
        # print(income_data_df["elevation"].mean())
        return income_data_df["z"].mean() - income_data_df["elevation"].mean()


if __name__ == '__main__':
    apk = os.environ.get("API_KEY")
    obj = Elevation(
        ex_file,
        apk
    )
    # print(obj.exc())
    # print(obj.encode_polyline())
    # print(obj.coords_list())
    # obj.coords_list()
    # print(f"Coords length {len(obj.coords_list())}")
    # print(obj.encode_polyline())
    # print(f"Length {len(obj.encode_polyline())}")
    # print(f"Coords str {obj.coords_string()}")
    # print(f"Length of coords string {len(obj.coords_string())}")
    # pprint(obj.get_google_data())
    # pprint(obj.coords_df())
    # obj.coords_df()
    # obj.get_google_data()
    # print(obj.get_average_elevation())
    # print(obj.get_google_data_async())
    print(obj.get_average_elevation(asynchronous=True))
