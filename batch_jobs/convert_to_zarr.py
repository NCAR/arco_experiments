import intake
import xarray as xr
import zarr
import dask
from dask.distributed import Client, LocalCluster, wait
import os

# Setup a local Dask cluster (adjust workers as needed)
cluster = LocalCluster(n_workers=5, threads_per_worker=1)
client = Client(cluster)

print(cluster)

# Intake catalog
catalog_url = "your_catalog_file_or_url_here"
cat = intake.open_esm_datastore(catalog_url)

# Load dataset metadata
df = cat.df.dropna(subset=["path"])
paths = df["path"].tolist()

# Target output directory
target_dir = "/path/to/zarr/outputs"

def convert_to_zarr(nc_path):
    try:
        ds = xr.open_dataset(nc_path, chunks={})  # Let Dask chunk automatically
        fname = os.path.basename(nc_path).replace(".nc", ".zarr")
        zarr_path = os.path.join(target_dir, fname)
        ds.to_zarr(zarr_path, mode="w")
        print(f"Converted: {nc_path} -> {zarr_path}")
    except Exception as e:
        print(f"Failed to convert {nc_path}: {e}")

# Submit to Dask
futures = [client.submit(convert_to_zarr, path) for path in paths]

# Wait for completion
_ = wait(futures)

client.close()
cluster.close()

