import calendar
from datetime import datetime

import matplotlib.pyplot as plt
import mlai.plot as plot
import numpy as np
import osmnx as ox
import warnings
from shapely.geometry.point import Point

from .util import get_nsew


def get_pois(region):
    """Gets all of the POIS from OSM within the region."""

    warnings.filterwarnings('ignore')

    tags = {"amenity": True,
            "buildings": True,
            "historic": True,
            "leisure": True,
            "shop": True,
            "tourism": True}

    north, south, east, west = get_nsew(*region)

    return ox.geometries_from_bbox(north, south, east, west, tags)


def get_tourisms(region):
    pois = get_pois(region)
    return pois[pois.tourism.notna()]


def get_osm_geom(region):
    """Gets the geometry of a region from OSM (e.g. roads)"""
    north, south, east, west = get_nsew(*region)

    graph = ox.graph_from_bbox(north, south, east, west)

    # Retrieve nodes and edges
    return ox.graph_to_gdfs(graph)


def get_pois_over_regions(regions: dict):
    """Loops over a dictionary of regions and provides the POIs for each key, indexed by those keys"""

    features = {}
    for name, region in regions.items():
        features[name] = get_pois(region)

    return features


def get_sales_over_regions(database, regions):
    """Loops over a dictionary of regions and provides the house sales for each key, indexed by those keys"""

    sales = {}
    for name, region in regions.items():
        sales[name] = database.get_prices_in_region(*region, limit=100000)

    return sales


def to_timestamps(l):
    """Converts a list of dates to a list of timestamps"""
    return [calendar.timegm(d.timetuple()) for d in l]


def from_timestamps(l):
    """Converts a list of timestamps to a list of dates"""
    return [datetime.utcfromtimestamp(t) for t in l]


def get_lobf_timesteps(x, y):
    """Gets the line of best fit (linear regression) start and end points, given the x axis is a list of datetimes"""
    ts = to_timestamps(x)
    m, b = np.polyfit(ts, y, 1)
    s = np.linspace(min(ts), max(ts), 2)

    return from_timestamps(s), m * s + b


def scatter(x, y, title, x_label, y_label):
    """Plots an arbitrary scatter plot in my style"""

    fig = plt.figure(figsize=(12, 6), dpi=80)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)

    plt.scatter(x, y, c="#22AA99AA", edgecolors='none')

    fig.tight_layout()


def scatter_dates(x, y, title, x_label, y_label, lobf=False):
    """Plots an arbitrary scatter plot where the x axis is a list of dates"""

    fig = plt.figure(figsize=(12, 6), dpi=80)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)

    plt.scatter(x, y, c="#22AA99AA", edgecolors='none')

    if lobf:
        plt.plot(*get_lobf_timesteps(x, y), c="#AA2299", linewidth=3)

    fig.tight_layout()


def scatter_date_vs_price(data, place, lobf=False):
    """Plots the sale prices of a set of houses against the date that the sale was made on"""

    x = data[place].date_of_transfer
    y = data[place].price
    scatter_dates(x, y, f"{place} Date against Price", "Date", "Price (??)", lobf=lobf)


def scatter_date_vs_log_price(data, place, lobf=False):
    """Plots the logs of the sale prices of a set of houses against the date that the sale was made on"""

    x = data[place].date_of_transfer
    y = np.log(data[place].price)
    scatter_dates(x, y, f"{place} Date against Log Price", "Date", "Log Price", lobf=lobf)


def plot_region(region, pois=None):
    """Plots the geometry of a region"""

    scatter_over_region([], [], [], region, pois)


def scatter_over_region(x, y, color_scale, region, pois=None):
    """Plots a scatter plot of some coordinates (x, y) over the geometry of a region"""

    nodes, edges = get_osm_geom(region)

    fig, ax = plt.subplots(figsize=plot.big_figsize)

    # Plot street edges
    edges.plot(ax=ax, linewidth=1, zorder=1, edgecolor="dimgray")

    # Plot scatter points
    if len(x) > 0:
        color_scale = np.array([float(v) for v in color_scale])
        color_scale -= np.min(color_scale)
        plt.scatter(x, y, c=color_scale, alpha=0.5, zorder=4)

    # Set bounaries
    north, south, east, west = get_nsew(*region)
    ax.set_xlim([west, east])
    ax.set_ylim([south, north])
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")

    # Plot all POIs
    if pois is not None:
        pois.plot(ax=ax, color="blue", zorder=3, alpha=0.7, markersize=10)

    plt.tight_layout()


def plot_log_price_over_region(sales, regions, features, name):
    """Plots the logs of the sale prices of a set of houses over a region"""
    scatter_over_region(sales[name].longitude, sales[name].lattitude, np.log(sales[name].price), regions[name],
                        features[name])


def get_pois_centroids(region):
    """Gets the centroids of all of the POIs in a region"""

    pois = get_pois(region)
    return [v.centroid for v in pois.geometry]


def get_all_distances(centroids, latitude, longitude):
    """Gets the distances of a list of centroids to a position"""

    return [c.distance(Point(longitude, latitude)) for c in centroids]


def get_closest_distances(pois, sales):
    centroids = [v.centroid for v in pois.geometry]
    return [min(get_all_distances(centroids, lat, long)) for lat, long in zip(sales.lattitude, sales.longitude)]


def get_clossness_matrix(centroids, latitudes, longitudes, cutoff=0.005):
    """Gets a boolean matrix of POI closer than the cutoff to the set of latitudes and longitudes"""

    poi_distances = np.array([get_all_distances(centroids, lat, lon) for lat, lon in zip(latitudes, longitudes)])
    return poi_distances < cutoff


def split_data(data, index, cutoff):
    within_locations = data[np.array(index) < cutoff]
    outside_locations = data[np.array(index) >= cutoff]

    return within_locations, outside_locations
