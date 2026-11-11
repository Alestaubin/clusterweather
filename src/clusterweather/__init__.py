"""clusterweather: which Slurm cluster should you work on today?"""
try:
    from importlib.metadata import version as _version
    __version__ = _version("clusterweather")
except Exception:                                   # running from a source checkout
    __version__ = "0.0.0+local"
