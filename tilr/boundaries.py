import math


def num2deg(xtile, ytile, zoom):
    """Convert x/y tile coordinates to latitude and longitude."""

    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return lat_deg, lon_deg


def deg2num(lat_deg, lon_deg, zoom):
    """Convert latitude and longitude to x/y tile coordinates."""

    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return xtile, ytile


class Boundary:
    """Represents a generic rectangular boundary."""

    def __init__(self, left, top, right, bottom):
        self.left = left
        self.right = right
        self.top = top
        self.bottom = bottom

    def tile_bounds(self, zoom_level):
        """Get x/y tile coordinates for this boundary."""

        count = 2 ** zoom_level

        top_left = deg2num(self.top, self.left, zoom_level)
        bottom_right = deg2num(self.bottom, self.right, zoom_level)

        left = top_left[0]
        top = top_left[1]
        right = bottom_right[0]
        bottom = bottom_right[1]

        left = max(left, 0)
        top = max(top, 0)
        right = min(right + 1, count)
        bottom = min(bottom + 1, count)

        return left, top, right, bottom
