import os
import sys
from pathlib import Path

from config import get_checkpoint_dir

first_version_to_clean = int(sys.argv[1])
base_dir = get_checkpoint_dir()
sources_dir = f'{base_dir}/offsets'
files_in_checkpoint = os.listdir(sources_dir)
last_checkpoint = -1
for checkpoint in files_in_checkpoint:
    checkpoint_number = -1
    if checkpoint.isnumeric():
        checkpoint_number = int(checkpoint)
    if checkpoint_number > last_checkpoint:
        last_checkpoint = checkpoint_number

for version in range(first_version_to_clean, last_checkpoint + 1):
    print(f'Removing files for {version}')

    Path(f'{base_dir}/offsets/{version}').unlink(missing_ok=True)
    Path(f'{base_dir}/offsets/.{version}.crc').unlink(missing_ok=True)

    Path(f'{base_dir}/commits/{version}').unlink(missing_ok=True)
    Path(f'{base_dir}/commits/.{version}.crc').unlink(missing_ok=True)

    # state is always saved with the next micro-batch version, that's why we do vesion+1
    state_version_to_remove = version + 1
    Path(f'{base_dir}/state/0/0/{state_version_to_remove}.zip').unlink(missing_ok=True)
    Path(f'{base_dir}/state/0/0/.{state_version_to_remove}.zip.crc').unlink(missing_ok=True)
