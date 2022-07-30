# CatalogFS_Lister

**CatalogFS_Lister** is a script for creating indexes (snapshots) of your data.

Perfect for indexing backups on disconnected HDD, SSD, CD, DVD or any other storage.

Index includes full file tree with all `metadata` (`names`, `sizes`, `ctime`, `atime`, `mtime`) and optional `SHA-256` hashes but no actual file data content and thus has small size. These indexes (can be called catalogs) have the same hierarchy of directories and files as the original directory and take almost no disk space but allow to check what was present in original directories or backups.

**NOTE: IT IS NOT A SCRIPT FOR MAKING BACKUPS BECAUSE NO ACTUAL FILE DATA IS STORED.**

But it's a **very** convenient to keep track of your backups, especially ones that are not easily connectable like external USB disks, CDs, flash or remote drives.

`CatalogFS` FUSE-filesystem can be used to view listed files from index as they were in the source (with the original size, dates, permissions). Or you can simply view indexed files in any file-manager but size and other metadata will be different.

The ability of `CatalogFS` to show the original metadata including sizes of files allows to view snapshots using any file manager (Dolphin, Nautilus and etc.), use tools to analyze the occupied space distribution (`Filelight`, `Disk Usage Analyzer`, `Baobab` and etc) and even properly compare directories with your backup snapshots.

Both `CatalogFS` filesystem and `CatalogFS_Lister` script can be used separately with great results, but using them provides the best experience.

See `CatalogFS` filesystem project for details.


## How to use CatalogFS

#### To create an index (snapshot) of your data (e.g. external backup drive, CD/DVD and etc.)

1. Create a directory to store your index (snapshot):

   ```
   $ mkdir "/home/user/my_music_collection"
   ```

2. Make an index (snapshot) of data you want:

 - It can be done using `CatalogFS_Lister` python script (**recommended**):
 
   ```
   $ ./catalogfs_lister.py "/media/cdrom" "/home/user/my_music_collection"
   ```

   Using this script is a recommended way because it's faster and has an optional ability to calculate and save hashes of files:
   
   ```
   $ ./catalogfs_lister.py --sha256 "/media/cdrom" "/home/user/my_music_collection"
   ```
   
   Note that hashes calculations are quite slow for obvious reasons.

   More information on `CatalogFS_Lister` python script is available in help:
   
   ```
   $ ./catalogfs_lister.py --help
   ```

 - Or you can mount `CatalogFS` over an empty directory and copy data files there using any file manager or commands in terminal.
   
   Note that modification and other times won't stay original because of copying process.

   ```
   $ ./catalogfs "/home/user/my_music_collection"
   
   $ cp -RT "/media/cdrom" "/home/user/my_music_collection"
   
   $ fusermount -u "/home/user/my_music_collection"
   ```

   During this copy process the actual data **IS NOT** stored in the index, only metadata is.

   Saving files to `CatalogFS` is almost instant but reading files from the source is slower. Any copying tool will spend time to actually read the entire source file.


#### To view previously created index (snapshot) of your data.

You can view the index (snapshot) as it is, with any file manager it's already a lot. But if you want to view it with original file-sizes, stats and/or modification times you should mount the index with `CatalogFS` filesystem as described below.

1. Mount the index (snapshot) to any directory.

   You can simply mount `CatalogFS` over the same index directory. It will temporary hide index files showing ones with fake size and other stats:
   
   ```
   $ ./catalogfs "/home/user/my_music_collection"
   ```

   One should consider mounting the `CatalogFS` index read-only to avoid accidental change of the index if necessary (e.g. to preserve index of backup unmodified). To do so one can pass read-only (ro) option:
   
   ```
   $ ./catalogfs -o ro "/home/user/my_music_collection"
   ```

   Or you can mount it to another directory.
   ```
   $ mkdir "/home/user/my_music_collection_catalogfs_view"
   
   $ ./catalogfs -o ro --source="/home/user/my_music_collection" "/home/user/my_music_collection_catalogfs_view"
   ```

   The mounted directory will show all files from the source (e.g. CD or backup disk) except it is not possible to read (open, view) the content of any file.

   More information on `CatalogFS` commandline is also available in help:
   
   ```
   $ ./catalogfs --help
   ```

2. After using and viewing of index (snapshot) - you should unmount it. It can be done the same way as any other `FUSE` filesystem with a command `fusermount -u mountpoint_path`, for example:

   In case of mounting over the original index:
   
   ```
   $ fusermount -u "/home/user/my_music_collection"
   ```

   Or in case of mounting to a different directory:
   
   ```
   $ fusermount -u "/home/user/my_music_collection_catalogfs_view"
   ```


## Command-line usage:
```
Usage: catalogfs_lister.py [-h] [-s] [-c] [-d] [-x] source_dir output_dir

Make a CatalogFS-compartible index (snapshot) of the source directory.

positional arguments:
  source_dir                source directory to make index of.
  output_dir                output directory (preferably empty) for placing index files.

optional arguments:
  -h, --help                show this help message and exit.
  -s, --sha256              calculate and store SHA256 hashes (much slower).
  -c, --continue            continue indexing (ignore and skip existing output files)
  -d, --data-only           take only information that is needed to compare the
                            content to allow easy comparing and diff.
  -t, --data-and-time-only  take only information that is needed to compare the
                            content and modification time.
  -x, --source-is-cfsfiles  source directory already has only CatalogFS-files
                            (small files with meta-information).
```


## License
Copyright (C) 2020-present Zakhar Semenov

This program can be distributed under the terms of the GNU GPLv3 or later.


