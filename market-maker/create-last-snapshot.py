from hftbacktest.data.utils.snapshot import create_last_snapshot

file_name = 'btcusdt_20250118'
# Builds End of Day snapshot. It will be used for the initial snapshot for next day.
_ = create_last_snapshot(
    [f'data/{file_name}.npz'],
    tick_size=0.1,
    lot_size=0.001,
    output_snapshot_filename=f'data/{file_name}_eod.npz'
)