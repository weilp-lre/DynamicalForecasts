import asyncio
import logging
from packaging.version import Version
import socket

import aiohttp
import xarray as xr
import zarr


logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
logger = logging.getLogger("diagnostics")


# Dependency versions
min_xr_version = "2025.1.2"
if Version(xr.__version__) >= Version(min_xr_version):
    logger.info(f"Using xarray version {xr.__version__}")
else:
    logger.error(
        f"xarray version >= {min_xr_version} required for zarr v3 support, found {xr.__version__}"
    )
    exit(1)

min_zarr_version = "3.0.8"
if Version(zarr.__version__) >= Version(min_zarr_version):
    logger.info(f"Using zarr version {zarr.__version__}")
else:
    logger.error(
        f"zarr version >= {min_zarr_version} required, found {zarr.__version__}"
    )
    exit(1)


# DNS lookup
addr_infos = socket.getaddrinfo(
    "data.dynamical.org", 443, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP
)
if len(addr_infos) > 0:
    ip_addresses = [str(addr_info[-1][0]) for addr_info in addr_infos]
    logger.info(
        f"DNS lookup: Found {len(ip_addresses)} IP addresses for data.dynamical.org: {' '.join(ip_addresses)}"
    )
else:
    logger.error("DNS lookup: No IP addresses found for data.dynamical.org")
    exit(1)


# TCP connection
connected = False
for addr_info in addr_infos:
    family, socktype, proto, canonname, sockaddr = addr_info
    try:
        with socket.socket(family, socktype, proto) as s:
            s.settimeout(5)
            s.connect(sockaddr)
            logger.info(f"Successfully established TCP connection to {sockaddr}")
            connected = True
            break
    except Exception as e:
        logger.warning(f"Failed to connect to {sockaddr}: {e}")
if not connected:
    logger.error(
        "Could not establish a TCP connection to any address for data.dynamical.org"
    )
    exit(1)


# HTTP requests
async def http_test(base_url: str, test_paths: list[str]):
    async with aiohttp.ClientSession() as session:
        for path in test_paths:
            url = base_url + path
            async with session.get(url) as response:
                log_method = logger.info if response.status == 200 else logger.error
                log_method(f"HTTP {response.status} {url}")


GFS_FORECAST_URL = "https://data.dynamical.org/noaa/gfs/forecast/latest.zarr/"
TEST_PATHS = ["zarr.json", "longitude/c/0"]  # read json metadata and compressed values
asyncio.run(http_test(GFS_FORECAST_URL, TEST_PATHS))

# Xarray open
ds = xr.open_zarr(GFS_FORECAST_URL, chunks=None, decode_timedelta=True)
logger.info(f"Opened {ds.attrs["dataset_id"]} ({ds.attrs["dataset_version"]}) dataset.")

logger.info("Checks completed successfully.")
