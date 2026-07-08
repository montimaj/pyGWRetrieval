"""
Upper Colorado River Basin (WBD HU2 = 14) groundwater workflow.

Steps:
  1. Download field groundwater-level measurements (and daily values) from
     1985 to present for the basin polygon, using the modern USGS Water Data
     OGC API (dataretrieval >= 1.2.0). The API handles the large basin bbox with
     native request chunking, and field measurements provide broad spatial
     coverage (hundreds of wells) that the deprecated NWIS endpoint could not.
  2. Keep depth-to-water measurements (USGS parameter 72019, feet below land
     surface) so every well is on the same footing.
  3. Use confirmed UNCONFINED (water-table) wells (aqfr_type_cd == 'U') as a
     reference, and rather than discarding the UNCLASSIFIED and MIXED wells,
     keep those whose mean depth-to-water is consistent with the water-table
     surface defined by the unconfined wells (within the surface's own
     leave-one-out variability). Confined wells are excluded outright.
  4. Build a long-term MEAN water-table depth map (1985-present) at 1 km
     resolution, clipped to the basin, using several interpolation methods
     (IDW, Ordinary Kriging, RBF/thin-plate, linear, nearest) and compare them
     side by side.
"""
import argparse
import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyproj

from pyGWRetrieval import (
    GroundwaterRetrieval,
    WaterTableInterpolator,
    idw_at_points,
    setup_logging,
)
from pyGWRetrieval.spatial import get_geometry_from_shapefile, merge_geometries

setup_logging(level=logging.INFO)
log = logging.getLogger("ucrb")

SHP = "UCRB_WBDHU2/UCRB_WBDHU2.shp"
END = "2026-07-08"
TARGET_CRS = "EPSG:32612"  # UTM zone 12N — metric CRS covering the basin
METHODS = ["idw", "kriging", "rbf", "linear", "nearest"]

parser = argparse.ArgumentParser(
    description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument("--start-date", default="1985-01-01",
                    help="Start date (YYYY-MM-DD). Default 1985-01-01.")
parser.add_argument("--grid-size-m", type=float, default=1000.0,
                    help="Grid resolution in meters. Default 1000.")
parser.add_argument(
    "--consistency-pct", type=float, default=90.0,
    help="Acceptance band for gap-filling unclassified/mixed wells, as a "
         "percentile of the unconfined wells' leave-one-out IDW residuals. "
         "Higher = more inclusive (looser). Default 90.",
)
args = parser.parse_args()
START = args.start_date
GRID_M = args.grid_size_m
CONSISTENCY_PCT = args.consistency_pct

# 1. Download field measurements + daily values for the basin (Water Data OGC
#    API, native chunking, clipped to the polygon).
gw = GroundwaterRetrieval(start_date=START, end_date=END, data_sources=["gwlevels", "dv"])
data = gw.get_data_by_shapefile(SHP)
if data.empty:
    raise SystemExit("No groundwater data retrieved.")

gw.to_parquet("output/ucrb_gwl_1985_present.parquet")
gw.wells.to_file("output/ucrb_wells.geojson", driver="GeoJSON")

# 2. Depth-to-water only (parameter 72019).
depth = data[data["parameter_cd"] == "72019"].copy()
if depth.empty:
    raise SystemExit("No depth-to-water (72019) records available.")
log.info(
    f"Depth-to-water (72019): {len(depth):,} of {len(data):,} records from "
    f"{depth['site_no'].nunique()} of {data['site_no'].nunique()} wells"
)

# 3. Classify wells by aquifer type. Water-table depth is only physically
#    meaningful for unconfined aquifers, so confined (C) wells are excluded and
#    confirmed unconfined (U) wells form the reference set. Discovery already
#    attached aqfr_type_cd to gw.wells.
aqfr = gw.wells.set_index(gw.wells["site_no"].astype(str))["aqfr_type_cd"]
pw = depth.groupby(depth["site_no"].astype(str))["lev_va"].mean().dropna()  # per-well mean depth
pw = pw[pw.index.isin(aqfr.index)]
types = aqfr.reindex(pw.index)

# 4. Gap-fill the UNCLASSIFIED and MIXED wells (aqfr_type_cd not U/C) instead of
#    discarding them: keep a candidate only if its mean depth-to-water is
#    consistent with the water-table surface defined by the confirmed unconfined
#    wells. The acceptance band is the 90th percentile of the unconfined wells'
#    own leave-one-out IDW residuals (i.e. the surface's natural variability).
tf = pyproj.Transformer.from_crs("EPSG:4326", TARGET_CRS, always_xy=True)
coords = gw.wells.set_index(gw.wells["site_no"].astype(str))[["dec_long_va", "dec_lat_va"]]
xy = pd.DataFrame(
    dict(zip(["x", "y"], tf.transform(coords["dec_long_va"].values, coords["dec_lat_va"].values))),
    index=coords.index,
).reindex(pw.index)

is_u = types == "U"
is_cand = ~types.isin(["U", "C"])  # unclassified (None), mixed (M), and other non-confined
u_xy = xy[is_u].values
u_val = pw[is_u].values

loo = np.abs(pw[is_u].values - idw_at_points(u_xy, u_val, u_xy, leave_one_out=True))
tol = np.nanpercentile(loo, CONSISTENCY_PCT)
cand_pred = idw_at_points(u_xy, u_val, xy[is_cand].values)
accepted = np.abs(pw[is_cand].values - cand_pred) <= tol

keep_sites = set(pw[is_u].index) | set(pw[is_cand].index[accepted])
wtd = depth[depth["site_no"].astype(str).isin(keep_sites)].copy()
log.info(
    f"WTD wells: {is_u.sum()} unconfined + {int(accepted.sum())} of {int(is_cand.sum())} "
    f"unclassified/mixed kept (<= {tol:.1f} ft from the unconfined surface) "
    f"= {len(keep_sites)} total"
)
if len(keep_sites) < 4:
    raise SystemExit("Too few wells for interpolation.")

# 5. Long-term mean water-table depth, interpolated to a 1 km grid.
basin = merge_geometries(get_geometry_from_shapefile(SHP))  # EPSG:4326 polygon
interp = WaterTableInterpolator(wtd, value_column="lev_va", date_column="datetime")

results = {}
for method in METHODS:
    grids = interp.interpolate(
        period="all",          # single map: mean over the whole record
        method=method,
        grid_size_m=GRID_M,
        agg_func="mean",
        target_crs=TARGET_CRS,  # same metric CRS as the consistency gap-fill
        boundary=basin,        # extent + clip to the basin polygon
        clip_to_boundary=True,
    )
    if "all" not in grids:
        log.warning(f"Method '{method}' produced no grid (too few wells?).")
        continue
    res = grids["all"]
    results[method] = res
    res.to_geotiff(f"output/ucrb_wtd_mean_{method}.tif")

if not results:
    raise SystemExit("No interpolated grids produced.")

# Shared color scale across methods for a fair comparison.
sample = next(iter(results.values()))
vmin, vmax = np.nanpercentile(sample.values, [2, 98])

# Basin outline in the projected grid CRS, for overlay.
from pyGWRetrieval.spatial import _get_utm_crs  # noqa: E402
import geopandas as gpd  # noqa: E402
basin_proj = (
    gpd.GeoSeries([basin], crs="EPSG:4326").to_crs(sample.crs).iloc[0]
)

# 4. Comparison figure: observations + one panel per method.
fig, axes = plt.subplots(2, 3, figsize=(16, 11), constrained_layout=True)
axes = axes.ravel()

# Panel 0: observed mean depth-to-water per well.
ax = axes[0]
xs, ys = basin_proj.exterior.xy
ax.plot(xs, ys, color="k", lw=1)
sc = ax.scatter(sample.points_xy[:, 0], sample.points_xy[:, 1],
                c=sample.values, cmap="RdYlBu_r", vmin=vmin, vmax=vmax,
                edgecolors="k", linewidths=0.5, s=45)
ax.set_title(f"Observations ({len(sample.values)} wells)")
ax.set_aspect("equal")

# Panels 1..N: interpolated grids.
im = None
for ax, method in zip(axes[1:], METHODS):
    if method not in results:
        ax.set_visible(False)
        continue
    res = results[method]
    im = ax.imshow(res.grid, origin="lower", extent=res.extent,
                   cmap="RdYlBu_r", vmin=vmin, vmax=vmax, aspect="equal")
    bx, by = basin_proj.exterior.xy
    ax.plot(bx, by, color="k", lw=1)
    ax.set_title(method.upper())

for ax in axes:
    ax.set_xticks([]); ax.set_yticks([])

cbar = fig.colorbar(im, ax=axes, shrink=0.6, location="right")
cbar.set_label("Mean water-table depth (ft below land surface)")
fig.suptitle(
    "Upper Colorado River Basin — mean water-table depth (1985-present)\n"
    f"1 km grid, {len(sample.values)} wells, interpolation methods compared",
    fontsize=14,
)
fig.savefig("wtd_methods_comparison.png", dpi=150, bbox_inches="tight")
log.info("Saved wtd_methods_comparison.png")

# 5. Summary.
print("\n=== MEAN WATER-TABLE DEPTH (1985-present, ft below land surface) ===")
print(f"Wells with depth-to-water: {len(sample.values)}")
print(f"Grid: {GRID_M} m, {sample.grid.shape[1]}x{sample.grid.shape[0]} cells")
for method, res in results.items():
    g = res.grid[np.isfinite(res.grid)]
    print(f"  {method:8s}: valid cells={g.size:>7,}  "
          f"min={g.min():7.2f}  mean={g.mean():7.2f}  max={g.max():7.2f}")
