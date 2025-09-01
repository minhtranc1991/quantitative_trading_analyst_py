from hftbacktest.data.utils import binancefutures
from hftbacktest.data.utils.snapshot import create_last_snapshot

file_name = 'btcusdt_20250119'
data = binancefutures.convert(
    f'data/{file_name}.gz',
    output_filename=f'data/{file_name}.npz',
    combined_stream=True,
    buffer_size=500_000_000
)

# Builds End of Day snapshot. It will be used for the initial snapshot for next day.
_ = create_last_snapshot(
    [f'data/{file_name}.npz'],
    tick_size=0.1,
    lot_size=0.001,
    output_snapshot_filename=f'data/{file_name}_eod.npz'
)