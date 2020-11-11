import pkg_resources

from fest_nationbuilder.nationbuilder import NationBuilder

try:
    __version__ = pkg_resources.get_distribution(__package__).version
except pkg_resources.DistributionNotFound:
    __version__ = None
