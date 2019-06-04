# disk_store
A project to store and access records as memory mapped files

## Planned obejctives
1. Use a series of files mapped to memory to store and retrieve records
2. Use a disk data structure (like B-Tree) to index keys and offsets of records for efficient search
3. Expose this as a library so that it can be used as an object storage engine

## Good to have
1. A compacting functionality that compacts disk files periodically and removes all deleted records
2. Work on achieving atomicity and durability by using maybe a write ahead log
3. Support for concurrent writes
