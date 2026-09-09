"""Pure analysis core.

Every module in this package is free of infrastructure: it imports only the
standard library, ``dpkt``, and the analyzer's domain model. No Kafka, no HTTP,
no database, no configuration. That constraint is what lets the metrics be
verified against synthetic captures with known ground truth.
"""

from network_analyzer.analysis.metrics_calculator import analyze_capture
from network_analyzer.analysis.packet_reader import read_packets
from network_analyzer.analysis.tcp_analyzer import TcpAnalyzer, TcpMetrics
from network_analyzer.analysis.volume_analyzer import VolumeAnalyzer, VolumeTotals

__all__ = [
    "TcpAnalyzer",
    "TcpMetrics",
    "VolumeAnalyzer",
    "VolumeTotals",
    "analyze_capture",
    "read_packets",
]
