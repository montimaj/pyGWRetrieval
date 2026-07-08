"""
Spatial interpolation of water-table depth for pyGWRetrieval.

This module turns point groundwater-level observations into continuous
water-table depth maps (rasters) for a chosen temporal window — per month,
per year, or a custom period. Several simple interpolation methods are
supported:

    - ``idw``      : Inverse Distance Weighting (pure NumPy, always available)
    - ``kriging``  : Ordinary Kriging (requires ``pykrige``)
    - ``linear``   : Delaunay-based linear interpolation (requires ``scipy``)
    - ``cubic``    : Clough-Tocher cubic interpolation (requires ``scipy``)
    - ``nearest``  : Nearest-neighbor (requires ``scipy``)
    - ``rbf``      : Radial basis function / thin-plate spline (requires ``scipy``)

The grid resolution is specified directly in **meters**: observations are
projected to a metric CRS (an appropriate UTM zone by default, or a
user-supplied ``target_crs``) so that a cell size in meters is meaningful.

Examples
--------
>>> from pyGWRetrieval import GroundwaterRetrieval, WaterTableInterpolator
>>> gw = GroundwaterRetrieval(start_date='2021-01-01')
>>> data = gw.get_data_by_shapefile('basin.shp')
>>> interp = WaterTableInterpolator(data)
>>> # 1 km annual water-table depth maps, one per year, via IDW
>>> grids = interp.interpolate(period='annual', method='idw', grid_size_m=1000)
>>> grids['2021'].to_geotiff('wt_depth_2021.tif')
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

from .temporal import TemporalAggregator
from .spatial import _get_utm_crs

logger = logging.getLogger(__name__)

# Interpolation methods and the optional dependency each needs.
IDW = 'idw'
KRIGING = 'kriging'
SCIPY_METHODS = ('linear', 'cubic', 'nearest', 'rbf')
VALID_METHODS = (IDW, KRIGING) + SCIPY_METHODS

# Minimum well count required for a period to be interpolated. Delaunay-based
# methods (linear/cubic) need at least a triangle.
_MIN_POINTS = {'linear': 4, 'cubic': 4, 'rbf': 3, 'nearest': 1, IDW: 1, KRIGING: 3}


@dataclass
class InterpolationResult:
    """
    A single interpolated water-table depth grid for one temporal window.

    Attributes
    ----------
    grid : np.ndarray
        2D array of shape ``(ny, nx)``; ``grid[j, i]`` is the value at
        ``(x[i], y[j])``. Cells with no estimate (or outside the clip
        boundary) are ``NaN``.
    x, y : np.ndarray
        1D arrays of cell-center coordinates (meters, ascending) in ``crs``.
    crs : str
        EPSG string of the projected grid CRS (e.g. ``'EPSG:32612'``).
    method : str
        Interpolation method used.
    period : str
        Label of the temporal window (e.g. ``'2021-07'``, ``'2021'``).
    grid_size_m : float
        Grid cell size in meters.
    points_xy : np.ndarray
        ``(n, 2)`` observation coordinates (meters) that fed the interpolation.
    values : np.ndarray
        ``(n,)`` observed values at ``points_xy``.
    """

    grid: np.ndarray
    x: np.ndarray
    y: np.ndarray
    crs: str
    method: str
    period: str
    grid_size_m: float
    points_xy: np.ndarray = field(repr=False)
    values: np.ndarray = field(repr=False)

    @property
    def extent(self) -> Tuple[float, float, float, float]:
        """matplotlib ``imshow`` extent (left, right, bottom, top) in meters."""
        half = self.grid_size_m / 2.0
        return (self.x.min() - half, self.x.max() + half,
                self.y.min() - half, self.y.max() + half)

    @property
    def transform(self):
        """Affine transform (north-up) for writing a GeoTIFF. Needs ``affine``."""
        from affine import Affine
        half = self.grid_size_m / 2.0
        return (Affine.translation(self.x.min() - half, self.y.max() + half)
                * Affine.scale(self.grid_size_m, -self.grid_size_m))

    def to_xarray(self):
        """Return the grid as an ``xarray.DataArray`` (requires ``xarray``)."""
        import xarray as xr
        return xr.DataArray(
            self.grid,
            coords={'y': self.y, 'x': self.x},
            dims=('y', 'x'),
            name='water_table_depth',
            attrs={'crs': self.crs, 'method': self.method,
                   'period': self.period, 'grid_size_m': self.grid_size_m},
        )

    def to_geotiff(self, filepath: Union[str, Path]) -> None:
        """
        Write the grid to a GeoTIFF (requires ``rasterio``).

        The raster is stored north-up (top row = northernmost).
        """
        try:
            import rasterio
        except ImportError as exc:  # pragma: no cover - optional dep
            raise ImportError(
                "Writing GeoTIFFs requires 'rasterio'. Install with "
                "`pip install pyGWRetrieval[interp]` or `pip install rasterio`."
            ) from exc

        # Flip so row 0 is the northernmost row (north-up).
        arr = np.flipud(self.grid).astype('float32')
        with rasterio.open(
            filepath, 'w', driver='GTiff',
            height=arr.shape[0], width=arr.shape[1], count=1,
            dtype='float32', crs=self.crs, transform=self.transform,
            nodata=np.nan,
        ) as dst:
            dst.write(arr, 1)
        logger.info(f"Wrote GeoTIFF to {filepath}")

    def plot(self, ax=None, cmap='RdYlBu_r', show_points=True,
             colorbar=True, title=None, **imshow_kwargs):
        """
        Plot the interpolated grid, optionally overlaying observation points.

        Returns the matplotlib ``Axes``.
        """
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=(8, 7))

        im = ax.imshow(self.grid, origin='lower', extent=self.extent,
                       cmap=cmap, aspect='equal', **imshow_kwargs)
        if show_points and len(self.values):
            ax.scatter(self.points_xy[:, 0], self.points_xy[:, 1],
                       c=self.values, cmap=cmap, edgecolors='k',
                       linewidths=0.5, s=30, zorder=3)
        if colorbar:
            plt.colorbar(im, ax=ax, label='Water-table depth', shrink=0.8)
        ax.set_title(title or f"{self.method.upper()} — {self.period}")
        ax.set_xlabel(f"Easting (m, {self.crs})")
        ax.set_ylabel("Northing (m)")
        return ax


class WaterTableInterpolator:
    """
    Build gridded water-table depth maps from point observations.

    Parameters
    ----------
    data : pd.DataFrame
        Groundwater level data (e.g. the output of ``GroundwaterRetrieval``).
    value_column : str, optional
        Column with the water-level value to interpolate. Default ``'lev_va'``.
    site_column : str, optional
        Well identifier column. Default ``'site_no'``.
    date_column : str, optional
        Datetime column used for temporal windowing. Default ``'datetime'``.
    lat_column, lon_column : str, optional
        Latitude/longitude columns. Defaults ``'dec_lat_va'`` / ``'dec_long_va'``.
    crs : str, optional
        CRS of the lat/lon columns. Default ``'EPSG:4326'``.
    """

    def __init__(
        self,
        data: pd.DataFrame,
        value_column: str = 'lev_va',
        site_column: str = 'site_no',
        date_column: str = 'datetime',
        lat_column: str = 'dec_lat_va',
        lon_column: str = 'dec_long_va',
        crs: str = 'EPSG:4326',
    ):
        for col in (value_column, site_column, date_column, lat_column, lon_column):
            if col not in data.columns:
                raise ValueError(f"Column '{col}' not found in data")

        self.data = data.copy()
        self.value_column = value_column
        self.site_column = site_column
        self.date_column = date_column
        self.lat_column = lat_column
        self.lon_column = lon_column
        self.crs = crs

        # One representative coordinate per well (first non-null).
        coords = (
            self.data[[site_column, lat_column, lon_column]]
            .dropna(subset=[lat_column, lon_column])
            .drop_duplicates(subset=site_column)
            .set_index(site_column)
        )
        self._well_coords = coords
        logger.info(
            f"Initialized WaterTableInterpolator with {len(self.data):,} records, "
            f"{len(coords)} located wells"
        )

    # ------------------------------------------------------------------ public
    def interpolate(
        self,
        period: str = 'annual',
        method: str = IDW,
        grid_size_m: float = 1000.0,
        agg_func: str = 'mean',
        target_crs: Optional[str] = None,
        boundary=None,
        clip_to_boundary: bool = True,
        fill_outside: Optional[str] = None,
        padding_m: float = 0.0,
        min_points: Optional[int] = None,
        # IDW options
        power: float = 2.0,
        n_neighbors: Optional[int] = None,
        # Kriging options
        variogram_model: str = 'spherical',
        # Custom period options
        months: Optional[Sequence[int]] = None,
        period_name: str = 'custom',
        water_year: bool = False,
    ) -> Dict[str, InterpolationResult]:
        """
        Interpolate water-table depth onto a grid for each temporal window.

        Parameters
        ----------
        period : {'all', 'monthly', 'annual', 'custom'}
            Temporal window. ``'all'`` produces one map from the whole record
            (e.g. a long-term mean); ``'custom'`` requires ``months``.
        method : {'idw', 'kriging', 'linear', 'cubic', 'nearest', 'rbf'}
            Interpolation method. Default ``'idw'``.
        grid_size_m : float
            Grid cell size in meters. Default 1000 m.
        agg_func : str
            How to aggregate multiple observations per well within a window
            (e.g. ``'mean'``, ``'median'``). Default ``'mean'``.
        target_crs : str, optional
            Projected CRS for the grid. Default: an appropriate UTM zone for the
            data centroid.
        boundary : shapely geometry or GeoDataFrame, optional
            Area of interest. Used to set the grid extent and (if
            ``clip_to_boundary``) to mask cells outside it. Assumed to be in
            ``crs`` (lat/lon) unless it carries its own CRS.
        clip_to_boundary : bool
            Mask grid cells whose centers fall outside ``boundary``. Default True.
        fill_outside : str, optional
            Backfill cells the primary ``method`` leaves empty using this
            secondary method (e.g. ``'nearest'`` or ``'idw'``). Useful with
            ``'linear'`` / ``'cubic'``, which only interpolate inside the convex
            hull of the wells and return NaN beyond it. Default: no backfill.
        padding_m : float
            Extra margin (meters) added around the extent. Default 0.
        min_points : int, optional
            Minimum wells with data required to interpolate a window. Defaults
            to a method-appropriate value.
        power : float
            IDW distance power. Default 2.
        n_neighbors : int, optional
            If set, IDW uses only the nearest ``n_neighbors`` wells (needs
            ``scipy``). Default: use all wells.
        variogram_model : str
            Kriging variogram model (e.g. ``'spherical'``, ``'exponential'``,
            ``'gaussian'``, ``'linear'``). Default ``'spherical'``.
        months : sequence of int, optional
            Months (1-12) defining a custom period.
        period_name : str
            Name for the custom period. Default ``'custom'``.
        water_year : bool
            For annual aggregation, use the water year (Oct-Sep). Default False.

        Returns
        -------
        dict of str -> InterpolationResult
            Mapping of window label (e.g. ``'2021-07'``, ``'2021'``) to result.
        """
        method = method.lower()
        if method not in VALID_METHODS:
            raise ValueError(
                f"Invalid method '{method}'. Options: {list(VALID_METHODS)}"
            )
        if fill_outside is not None:
            fill_outside = fill_outside.lower()
            if fill_outside not in VALID_METHODS:
                raise ValueError(
                    f"Invalid fill_outside '{fill_outside}'. "
                    f"Options: {list(VALID_METHODS)}"
                )
        if min_points is None:
            min_points = _MIN_POINTS[method]

        # 1. Per-well, per-window aggregated values.
        windows = self._aggregate_windows(
            period, agg_func, months, period_name, water_year
        )
        if not windows:
            logger.warning("No temporal windows produced from the data.")
            return {}

        # 2. Metric CRS + boundary in that CRS.
        target_crs = target_crs or self._auto_utm_crs()
        boundary_proj = self._project_boundary(boundary, target_crs)

        # 3. Interpolate each window.
        results: Dict[str, InterpolationResult] = {}
        for label, well_values in windows.items():
            px, py, pv = self._well_points(well_values, target_crs)
            if len(pv) < min_points:
                logger.info(
                    f"Skipping window '{label}': {len(pv)} wells "
                    f"(< {min_points} required for '{method}')"
                )
                continue

            xs, ys = self._make_grid(px, py, grid_size_m, boundary_proj, padding_m)
            grid = self._run_method(
                method, px, py, pv, xs, ys,
                power=power, n_neighbors=n_neighbors,
                variogram_model=variogram_model,
            )
            # Backfill cells the primary method left empty (e.g. linear/cubic
            # do not extrapolate beyond the convex hull of the wells).
            if fill_outside is not None and fill_outside != method:
                holes = np.isnan(grid)
                if holes.any():
                    backstop = self._run_method(
                        fill_outside, px, py, pv, xs, ys,
                        power=power, n_neighbors=n_neighbors,
                        variogram_model=variogram_model,
                    )
                    grid = np.where(holes, backstop, grid)
            if clip_to_boundary and boundary_proj is not None:
                grid = self._mask_outside(grid, xs, ys, boundary_proj)

            results[label] = InterpolationResult(
                grid=grid, x=xs, y=ys, crs=str(target_crs), method=method,
                period=label, grid_size_m=float(grid_size_m),
                points_xy=np.column_stack([px, py]), values=pv,
            )
            logger.info(
                f"Window '{label}': interpolated {len(pv)} wells onto a "
                f"{grid.shape[1]}x{grid.shape[0]} grid ({method})"
            )

        return results

    # ------------------------------------------------------------- aggregation
    def _aggregate_windows(
        self, period, agg_func, months, period_name, water_year
    ) -> Dict[str, pd.Series]:
        """Return {window_label: Series(index=site_no, value)} per window."""
        aggregator = TemporalAggregator(
            self.data, date_column=self.date_column,
            value_column=self.value_column, site_column=self.site_column,
        )

        if period == 'all':
            # A single window: aggregate every record per well over the whole
            # record (e.g. a long-term mean water table).
            grouped = self.data.groupby(self.site_column)[self.value_column]
            values = grouped.agg(agg_func).dropna()
            return {'all': values} if len(values) else {}
        elif period == 'monthly':
            agg = aggregator.to_monthly(agg_func=agg_func, include_count=False)
            agg['label'] = (agg['year'].astype(int).astype(str) + '-'
                            + agg['month'].astype(int).astype(str).str.zfill(2))
        elif period == 'annual':
            agg = aggregator.to_annual(agg_func=agg_func, water_year=water_year)
            agg['label'] = agg['year'].astype(int).astype(str)
        elif period == 'custom':
            if not months:
                raise ValueError("period='custom' requires `months` (e.g. [6,7,8])")
            agg = aggregator.to_custom_period(months=months, period_name=period_name)
            agg['label'] = (f"{period_name}_" + agg['year'].astype(int).astype(str))
        else:
            raise ValueError(
                f"Invalid period '{period}'. Options: 'monthly', 'annual', 'custom'"
            )

        windows: Dict[str, pd.Series] = {}
        for label, grp in agg.groupby('label'):
            windows[label] = grp.set_index(self.site_column)['value']
        return dict(sorted(windows.items()))

    def _well_points(self, well_values: pd.Series, target_crs):
        """Project wells that have a value for this window into ``target_crs``."""
        merged = self._well_coords.join(well_values.rename('value'), how='inner')
        merged = merged.dropna(subset=['value'])

        import pyproj
        transformer = pyproj.Transformer.from_crs(
            self.crs, target_crs, always_xy=True
        )
        x, y = transformer.transform(
            merged[self.lon_column].to_numpy(), merged[self.lat_column].to_numpy()
        )
        return np.asarray(x), np.asarray(y), merged['value'].to_numpy(dtype=float)

    # --------------------------------------------------------------- grid + crs
    def _auto_utm_crs(self) -> str:
        lat = self._well_coords[self.lat_column].astype(float)
        lon = self._well_coords[self.lon_column].astype(float)
        crs = _get_utm_crs(float(lon.mean()), float(lat.mean()))
        return f"EPSG:{crs.to_epsg()}"

    @staticmethod
    def _make_grid(px, py, grid_size_m, boundary_proj, padding_m):
        if boundary_proj is not None:
            minx, miny, maxx, maxy = boundary_proj.bounds
        else:
            minx, miny, maxx, maxy = px.min(), py.min(), px.max(), py.max()
        minx -= padding_m; miny -= padding_m
        maxx += padding_m; maxy += padding_m

        nx = max(2, int(np.ceil((maxx - minx) / grid_size_m)) + 1)
        ny = max(2, int(np.ceil((maxy - miny) / grid_size_m)) + 1)
        xs = minx + np.arange(nx) * grid_size_m
        ys = miny + np.arange(ny) * grid_size_m
        return xs, ys

    def _project_boundary(self, boundary, target_crs):
        """Return a shapely geometry in ``target_crs`` (or None)."""
        if boundary is None:
            return None

        import geopandas as gpd
        from shapely.ops import transform as shp_transform
        import pyproj

        if isinstance(boundary, gpd.GeoDataFrame):
            gdf = boundary if boundary.crs else boundary.set_crs(self.crs)
            geom = gdf.to_crs(target_crs).union_all()
            return geom

        # Plain shapely geometry, assumed to be in self.crs (lat/lon).
        transformer = pyproj.Transformer.from_crs(
            self.crs, target_crs, always_xy=True
        )
        return shp_transform(transformer.transform, boundary)

    @staticmethod
    def _mask_outside(grid, xs, ys, boundary_proj):
        """Set grid cells whose centers fall outside the boundary to NaN."""
        XX, YY = np.meshgrid(xs, ys)
        try:
            from shapely import contains_xy  # shapely >= 2.0
            inside = contains_xy(boundary_proj, XX, YY)
        except ImportError:  # pragma: no cover - shapely 1.8 fallback
            from shapely.vectorized import contains
            inside = contains(boundary_proj, XX, YY)
        out = grid.copy()
        out[~inside] = np.nan
        return out

    # -------------------------------------------------------------- dispatchers
    def _run_method(self, method, px, py, pv, xs, ys, **kw):
        if method == IDW:
            return self._idw(px, py, pv, xs, ys,
                             power=kw['power'], n_neighbors=kw['n_neighbors'])
        if method == KRIGING:
            return self._kriging(px, py, pv, xs, ys,
                                 variogram_model=kw['variogram_model'])
        return self._scipy_grid(method, px, py, pv, xs, ys)

    @staticmethod
    def _idw(px, py, pv, xs, ys, power=2.0, n_neighbors=None, chunk=50000):
        """Inverse-distance-weighted interpolation (NumPy; optional KDTree)."""
        XX, YY = np.meshgrid(xs, ys)
        gx = XX.ravel()
        gy = YY.ravel()
        out = np.full(gx.shape, np.nan, dtype=float)

        tree = None
        if n_neighbors:
            try:
                from scipy.spatial import cKDTree
                tree = cKDTree(np.column_stack([px, py]))
                k = int(min(n_neighbors, len(pv)))
            except ImportError:
                logger.warning("scipy not available; IDW using all wells.")

        for s in range(0, gx.size, chunk):
            gxb, gyb = gx[s:s + chunk], gy[s:s + chunk]
            if tree is not None:
                dist, idx = tree.query(np.column_stack([gxb, gyb]), k=k)
                if k == 1:
                    dist = dist[:, None]
                    idx = idx[:, None]
                vals = pv[idx]
            else:
                dist = np.sqrt((gxb[:, None] - px[None, :]) ** 2
                               + (gyb[:, None] - py[None, :]) ** 2)
                vals = np.broadcast_to(pv[None, :], dist.shape)

            # Exact hits (distance 0) -> take that well's value directly; their
            # infinite weights are ignored below and overwritten afterwards.
            exact = dist == 0
            has_exact = exact.any(axis=1)
            block = np.full(gxb.shape, np.nan, dtype=float)
            with np.errstate(divide='ignore', invalid='ignore'):
                w = 1.0 / np.power(dist, power)
                wsum = w.sum(axis=1)
                good = np.isfinite(wsum) & (wsum > 0)
                block[good] = (w[good] * vals[good]).sum(axis=1) / wsum[good]
            if has_exact.any():
                # One or more co-located observations: average them.
                exact_mean = (vals * exact).sum(axis=1) / exact.sum(axis=1)
                block[has_exact] = exact_mean[has_exact]
            out[s:s + chunk] = block

        return out.reshape(XX.shape)

    @staticmethod
    def _kriging(px, py, pv, xs, ys, variogram_model='spherical'):
        try:
            from pykrige.ok import OrdinaryKriging
        except ImportError as exc:
            raise ImportError(
                "method='kriging' requires 'pykrige'. Install with "
                "`pip install pyGWRetrieval[interp]` or `pip install pykrige`."
            ) from exc
        ok = OrdinaryKriging(
            px, py, pv, variogram_model=variogram_model,
            enable_plotting=False, coordinates_type='euclidean',
        )
        z, _ = ok.execute('grid', xs, ys)
        return np.ma.filled(np.asarray(z, dtype=float), np.nan)

    @staticmethod
    def _scipy_grid(method, px, py, pv, xs, ys):
        try:
            from scipy.interpolate import griddata, RBFInterpolator
        except ImportError as exc:
            raise ImportError(
                f"method='{method}' requires 'scipy'. Install with "
                "`pip install pyGWRetrieval[interp]` or `pip install scipy`."
            ) from exc
        XX, YY = np.meshgrid(xs, ys)
        pts = np.column_stack([px, py])
        if method == 'rbf':
            # A global thin-plate spline is severely ill-conditioned for many
            # scattered points over a large domain (values can blow up to 1e30+).
            # Use a local neighborhood plus light smoothing for stability.
            n = len(pv)
            neighbors = None if n <= 40 else int(min(n, max(24, 2 * n ** 0.5)))
            rbf = RBFInterpolator(
                pts, pv, kernel='thin_plate_spline',
                neighbors=neighbors, smoothing=1e-3,
            )
            flat = rbf(np.column_stack([XX.ravel(), YY.ravel()]))
            return flat.reshape(XX.shape)
        return griddata(pts, pv, (XX, YY), method=method)


def idw_at_points(
    source_xy: np.ndarray,
    source_values: np.ndarray,
    target_xy: np.ndarray,
    power: float = 2.0,
    n_neighbors: int = 12,
    leave_one_out: bool = False,
) -> np.ndarray:
    """
    Inverse-distance-weighted estimate at arbitrary target points.

    Unlike ``WaterTableInterpolator`` (which fills a regular grid), this
    predicts values at scattered locations — useful for cross-validation and
    for checking whether a candidate well is consistent with a surface defined
    by other wells.

    Parameters
    ----------
    source_xy : array (n, 2)
        Coordinates of the observations (in a metric CRS).
    source_values : array (n,)
        Observed values.
    target_xy : array (m, 2)
        Locations to estimate at.
    power : float
        Inverse-distance power. Default 2.
    n_neighbors : int
        Number of nearest observations to use. Default 12.
    leave_one_out : bool
        If True, ``target_xy`` is assumed identical to ``source_xy`` and each
        point is estimated from the *other* points (excludes itself). Used to
        characterize a surface's own spatial variability.

    Returns
    -------
    np.ndarray, shape (m,)
        Estimated values at ``target_xy``.
    """
    from scipy.spatial import cKDTree

    source_xy = np.asarray(source_xy, dtype=float)
    source_values = np.asarray(source_values, dtype=float)
    target_xy = np.asarray(target_xy, dtype=float)

    n = len(source_values)
    tree = cKDTree(source_xy)
    k = min(n_neighbors + (1 if leave_one_out else 0), n)
    dist, idx = tree.query(target_xy, k=k)
    if k == 1:
        dist = dist[:, None]
        idx = idx[:, None]
    if leave_one_out:
        dist = dist[:, 1:]  # drop the nearest neighbor (each point itself)
        idx = idx[:, 1:]

    out = np.full(target_xy.shape[0], np.nan, dtype=float)
    exact = dist == 0
    has_exact = exact.any(axis=1)
    vals = source_values[idx]
    with np.errstate(divide='ignore', invalid='ignore'):
        w = 1.0 / np.power(dist, power)
        wsum = w.sum(axis=1)
        good = np.isfinite(wsum) & (wsum > 0)
        out[good] = (w[good] * vals[good]).sum(axis=1) / wsum[good]
    if has_exact.any():
        # One or more co-located observations: average them.
        exact_mean = (vals * exact).sum(axis=1) / exact.sum(axis=1)
        out[has_exact] = exact_mean[has_exact]
    return out
