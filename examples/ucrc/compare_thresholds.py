"""
Compare gap-fill acceptance thresholds for the UCRB water-table depth map.

Reads the data produced by ``retrieve_ucrb.py`` (``output/*.parquet`` and
``output/ucrb_wells.geojson``) and renders side-by-side IDW maps for a tighter
(default 60th percentile) and a looser (default 90th percentile) acceptance
band on the unclassified/mixed well gap-fill — with no re-download.

    python retrieve_ucrb.py            # first, to produce output/
    python compare_thresholds.py       # then, to render the comparison
"""
import argparse

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyproj

from pyGWRetrieval import WaterTableInterpolator, idw_at_points
from pyGWRetrieval.spatial import get_geometry_from_shapefile, merge_geometries

SHP = "UCRB_WBDHU2/UCRB_WBDHU2.shp"
PARQUET = "output/ucrb_gwl_1985_present.parquet"
WELLS = "output/ucrb_wells.geojson"
TARGET_CRS = "EPSG:32612"  # UTM zone 12N
GRID_M = 1000

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--tight-pct", type=float, default=60.0)
parser.add_argument("--loose-pct", type=float, default=90.0)
args = parser.parse_args()

data = pd.read_parquet(PARQUET)
wells = gpd.read_file(WELLS)
wells["site_no"] = wells["site_no"].astype(str)
depth = data[data["parameter_cd"] == "72019"].copy()
depth["site_no"] = depth["site_no"].astype(str)

aqfr = wells.set_index("site_no")["aqfr_type_cd"]
pw = depth.groupby("site_no")["lev_va"].mean().dropna()
pw = pw[pw.index.isin(aqfr.index)]
types = aqfr.reindex(pw.index)

tf = pyproj.Transformer.from_crs("EPSG:4326", TARGET_CRS, always_xy=True)
coords = wells.set_index("site_no")[["dec_long_va", "dec_lat_va"]]
xy = pd.DataFrame(
    dict(zip(["x", "y"], tf.transform(coords["dec_long_va"].values, coords["dec_lat_va"].values))),
    index=coords.index,
).reindex(pw.index)

is_u = types == "U"
is_cand = ~types.isin(["U", "C"])
u_xy, u_val = xy[is_u].values, pw[is_u].values
loo = np.abs(u_val - idw_at_points(u_xy, u_val, u_xy, leave_one_out=True))
cand_pred = idw_at_points(u_xy, u_val, xy[is_cand].values)
basin = merge_geometries(get_geometry_from_shapefile(SHP))


def scenario(pct):
    tol = np.nanpercentile(loo, pct)
    accepted = np.abs(pw[is_cand].values - cand_pred) <= tol
    keep = set(pw[is_u].index) | set(pw[is_cand].index[accepted])
    wtd = depth[depth["site_no"].isin(keep)]
    interp = WaterTableInterpolator(wtd, value_column="lev_va", date_column="datetime")
    res = interp.interpolate(period="all", method="idw", grid_size_m=GRID_M,
                             target_crs=TARGET_CRS, boundary=basin)["all"]
    return dict(res=res, tol=tol, nacc=int(accepted.sum()), n=len(keep))


pcts = [args.tight_pct, args.loose_pct]
scen = {p: scenario(p) for p in pcts}
allv = np.concatenate([scen[p]["res"].values for p in pcts])
vmin, vmax = np.nanpercentile(allv, [2, 98])
basin_proj = gpd.GeoSeries([basin], crs="EPSG:4326").to_crs(TARGET_CRS).iloc[0]

fig, axes = plt.subplots(2, 2, figsize=(12, 12), constrained_layout=True)
im = None
for row, pct in enumerate(pcts):
    s = scen[pct]
    res = s["res"]
    bx, by = basin_proj.exterior.xy
    axw = axes[row, 0]
    axw.plot(bx, by, color="k", lw=1)
    axw.scatter(res.points_xy[:, 0], res.points_xy[:, 1], c=res.values, cmap="RdYlBu_r",
                vmin=vmin, vmax=vmax, edgecolors="k", linewidths=0.2, s=10)
    axw.set_title(f"{pct:.0f}th pct (tol={s['tol']:.0f} ft) — {s['n']} wells\n"
                  f"({s['nacc']} unclassified/mixed kept)")
    axw.set_aspect("equal")
    axm = axes[row, 1]
    im = axm.imshow(res.grid, origin="lower", extent=res.extent, cmap="RdYlBu_r",
                    vmin=vmin, vmax=vmax, aspect="equal")
    axm.plot(bx, by, color="k", lw=1)
    axm.set_title(f"IDW water-table depth ({pct:.0f}th pct)")
for ax in axes.ravel():
    ax.set_xticks([]); ax.set_yticks([])
cbar = fig.colorbar(im, ax=axes, shrink=0.55, location="right")
cbar.set_label("Mean water-table depth (ft below land surface)")
fig.suptitle(
    f"UCRB — gap-fill acceptance threshold: {pcts[0]:.0f}th vs {pcts[1]:.0f}th percentile\n"
    "(tighter vs looser inclusion of unclassified/mixed wells)", fontsize=13)
fig.savefig("wtd_threshold_comparison.png", dpi=150, bbox_inches="tight")
for pct in pcts:
    s = scen[pct]
    print(f"{pct:.0f}th pct: tol={s['tol']:.1f} ft, {s['nacc']} kept, {s['n']} wells")
print("Saved wtd_threshold_comparison.png")
