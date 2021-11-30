def get_nsew(latitude, longitude, distance):
    north = latitude + distance / 2
    south = latitude - distance / 2
    west = longitude - distance / 2
    east = longitude + distance / 2

    return north, south, east, west
