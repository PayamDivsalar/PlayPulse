"""Pure analysis core.

This package may import only ``dpkt``, the Python standard library,
``network_analyzer.core.models``, and ``network_analyzer.exceptions``. It must
never import Kafka, HTTP clients, or ``network_analyzer.config``. That
constraint is what lets the metrics be verified against synthetic captures
with known ground truth.
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
