## Disk tabsle:

[data block 1]
...
[data block N]
[index block]
offset_start_index_block count_index

## Data block

[ k1_size v1_size key1 value1 ... kN_size vN_size keyN valueN keys_offsets size_keys_offsets ]


## Index block

[ size_key last_key_block1 offset ... size_key last_key_blockN offset ]