#!/usr/bin/python3
# -*- encoding: utf8 -*-
'''
@author: Zakhar Semenov
@version: 3.0RC6
This program can be distributed under the terms of the GNU GPLv3 or later.
'''

'''
CatalogFS_Lister is a script for creating indexes (snapshots) of your data.
Perfect for indexing backups on disconnected HDD, SSD, CD, DVD or any other storages.

Index includes full file tree with all metadata (file names, sizes, times)
and optional SHA-256 hashes BUT no actual file content; thus, index has small size.
These indexes (can be called catalogs) have CatalogFS-compartible format.
The index has the same hierarchy of directories and files as the original directory.
Saved CatalogFS snapshots take almost no disk space but allow to check what was
present in directories or backups.

NOTE: IT IS NOT A SCRIPT FOR MAKING BACKUPS BECAUSE NO ACTUAL FILE DATA IS STORED.

But it's a VERY convenient way to keep track of your backups, especially ones that
are not easily connectable like external USB disks, CDs, flash or remote drives.

CatalogFS FUSE filesystem can be used for viewing listed files from index as 
they were in the source (with the original size, dates, permissions).

The ability of CatalogFS to show the original metadata including sizes of files allows 
to view snapshots using any file manager (Dolphin, Nautilus and etc.), use tools to
analyze the occupied space distribution (Filelight, Disk Usage Analyzer, Baobab and etc)
and even properly compare directories with your backup snapshots.

Both CatalogFS filesystem and CatalogFS_Lister script can be used separately 
with great results, but using them together provides the best experience.

See CatalogFS FUSE filesystem project for details.

'''

'''

--------------------
How to use CatalogFS
--------------------


To create an index (snapshot) of your data (e.g. external backup drive, CD/DVD and etc.)

1. Create a directory to store your index (snapshot):
   $ mkdir "/home/user/my_music_collection"

2. Make an index (snapshot) of data you want:

 - It can be done using CatalogFS_Lister python script (recommended):
   $ ./catalogfs_lister.py "/media/cdrom" "/home/user/my_music_collection"

   Using this script is a recommended way because it's faster and has an optional ability 
   to calculate and save hashes of files:
   $ ./catalogfs_lister.py --sha256 "/media/cdrom" "/home/user/my_music_collection"
   Note that hashes calculations are quite slow for obvious reasons.

   More information on CatalogFS_Lister python script is available in help:
   $ ./catalogfs_lister.py --help

 - Or you can mount CatalogFS over an empty directory and copy data files there using any 
   file manager or commands in terminal.
   Note that modification and other times won't stay original because of copying process.

   $ ./catalogfs "/home/user/my_music_collection"
   $ cp -RT "/media/cdrom" "/home/user/my_music_collection"
   $ fusermount -u "/home/user/my_music_collection"

   During this copy process the actual data IS NOT stored in the index, only metadata is.

   Saving files to CatalogFS is almost instant but reading files from the source is slower.
   Any copying tool will spend time to actually read the entire source file.


To view previously created index (snapshot) of your data.

You can view the index (snapshot) as it is, with any file manager it's already a lot.
But if you want to view it with original file-sizes, stats and/or modification times 
you should mount the index with CatalogFS filesystem as described below.

1. Mount the index (snapshot) to any directory.

   You can simply mount catalogfs over the same index directory.
   It will temporary hide index files showing ones with fake size and other stats:
   $ ./catalogfs "/home/user/my_music_collection"

   One should consider mounting the CatalogFS index read-only to avoid accidental
   change of the index if necessary (e.g. to preserve index of backup unmodified).
   To do so one can pass read-only (ro) option:
   $ ./catalogfs -o ro "/home/user/my_music_collection"

   Or you can mount it to another directory.
   $ mkdir "/home/user/my_music_collection_catalogfs_view"
   $ ./catalogfs -o ro --source="/home/user/my_music_collection" "/home/user/my_music_collection_catalogfs_view"

   The mounted directory will show all files from the source (e.g. CD or backup disk)
   except it is not possible to read (open, view) the content of any file.

   More information on CatalogFS commandline is also available in help:
   $ ./catalogfs --help

2. After using and viewing of index (snapshot) - you should unmount it.

   It can be done the same way as any other FUSE filesystem
   with a command "fusermount -u mountpoint_path":

   In case of mounting over the original index:
   $ fusermount -u "/home/user/my_music_collection"

   Or in case of mounting to a different directory:
   $ fusermount -u "/home/user/my_music_collection_catalogfs_view"

'''

'''
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

'''


# All CFSFile format versions
import os
import sys
import stat
import shutil
from pathlib import Path
import argparse
import hashlib
CFSFILE_VERSION_1: int = 1
CFSFILE_VERSION_2: int = 2
CFSFILE_VERSION_3: int = 3
CFSFILE_CURRENT_VERSION: int = CFSFILE_VERSION_3

# Current CFSFile format's constants
FORMAT_FIELD_DELIMITER: str = '='
FORMAT_NEW_LINE_CHAR: str = '\n'
FORMAT_NEW_LINE_CHAR_2: str = '\r'
FORMAT_NEW_LINE_LENGTH: int = 1
FORMAT_TRIMMING_CHARS: str = ' \t\r\n'

# Limit maximum stats file to 1MiB. More than enough for any stat file possible.
FORMAT_MAX_FILE_SIZE: int = 1048576

# Header strings for CFSFiles
FORMAT_HEADER_NAME: str = 'CatalogFS'
FORMAT_HEADER_TO_WRITE: str = f"{FORMAT_HEADER_NAME}{FORMAT_FIELD_DELIMITER}{CFSFILE_CURRENT_VERSION}{FORMAT_NEW_LINE_CHAR}"
FORMAT_HEADER_PREFIX_OLD_FORMAT: str = 'CatalogFS.File.'


class bcolors:
    """
    Codes of colors for terminal messages.
    """
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class CFSFile:
    """
    Main class for keeping CFSFile data (info about file).
    Class is similar to os.stat_result but not the same.
    """

    def __init__(self):
        self.size: int = None
        self.blocks: int = None
        self.mode: int = None
        self.uid: int = None
        self.gid: int = None
        self.atime: int = None
        self.mtime: int = None
        self.ctime: int = None
        self.atimensec: int = None
        self.mtimensec: int = None
        self.ctimensec: int = None
        self.nlink: int = None
        self.blksize: int = None
        self.sha256: str = None


def print_ok(s: str):
    """
    Print message with OK-color.

    :param s: message
    :type s: str
    """
    print(f'{bcolors.OKGREEN}[ OK  ]{bcolors.ENDC}: {s}', flush=True)


def print_error(s: str):
    """
    Print message with Error-color.

    :param s: message
    :type s: str
    """
    print(f'{bcolors.FAIL}[ERROR]{bcolors.ENDC}: {s}', flush=True)


def correct_utf8_pathstring(s: str) -> str:
    """
    Correct path by replacing non-utf8 chars.
    Used only for printing to terminal or logs

    :param s: path to correct
    :type s: str
    :return: corrected path string
    :rtype: str
    """
    return str(s).encode('utf-8', 'replace').decode('utf-8')


def does_exist(path: Path, expected_type: int) -> bool:
    """
    Check if file (can be symlink) exists with provided path and optional type.
    Types are: 0 (any type), stat.S_IFDIR, stat.S_IFREG, stat.S_IFLNK.
    Uses lstat to avoid wrong result of exists() function for invalid symlink.

    :param path: path to check
    :type path: Path
    :param expected_type: type to check against, 0 means any type (no check)
    :type expected_type: int
    :return: True if the file exists and has excepted type, False otherwise
    :rtype: bool
    """
    try:
        # we use lstat to avoid wrong result of exists() function for invalid symlink
        st: os.stat_result = os.lstat(path)

        # we use if-cases instead of bit operations over st_mode to be more portable in the future
        if expected_type == 0:
            return True
        elif expected_type == stat.S_IFDIR:
            return stat.S_ISDIR(st.st_mode)
        elif expected_type == stat.S_IFREG:
            return stat.S_ISREG(st.st_mode)
        elif expected_type == stat.S_IFLNK:
            return stat.S_ISLNK(st.st_mode)
        else:
            return False  # we cannot process other types, so no valid checks are possible

    except OSError:
        return False
    return True


def is_old_format_cfsfile(data: str) -> bool:
    """
    Check if data string has a header of old format (v1 or v2).

    :param data: string to check
    :type data: str
    :return: True if string has a header of old format, False otherwise
    :rtype: bool
    """
    return data.startswith(FORMAT_HEADER_PREFIX_OLD_FORMAT)


def find_next_newline_in_string(s: str, start_position: int) -> int:
    """
    Find position of the nearest new line char (two options for this char are supported).

    :param s: string to search in
    :type s: str
    :param start_position: start position to search from
    :type start_position: int
    :return: position of new line char in string or -1 if not found
    :rtype: int
    """
    # Avoid 2 find() calls and relocation of string for enumerate() for speed

    for i in range(start_position, len(s)):
        if s[i] == FORMAT_NEW_LINE_CHAR or s[i] == FORMAT_NEW_LINE_CHAR_2:
            return i

    return -1


def cfsfile_get_next_option_pair(data: str, start_position: int) -> (int, str, str):
    """
    Get option=value pair from the data string.

    :param data: string to parse and get pair from
    :type data: str
    :param start_position: position to start from
    :type start_position: int
    :raises RuntimeError: Invalid string in CatalogFS file
    :return: (new position to use for further parsing, option, value)
    :rtype: (int, str, str)
    """
    if start_position >= len(data):
        return (-1, '', '')

    current_pos: int = start_position

    while current_pos < len(data):

        line_end: int = find_next_newline_in_string(data, current_pos)
        if line_end == -1:
            # Consider EOF (end of data string actually) to be a line end
            line_end = len(data)

        param_end: int = data.find(
            FORMAT_FIELD_DELIMITER, current_pos, line_end)

        if param_end == -1:
            if len(data[current_pos:line_end].strip(FORMAT_TRIMMING_CHARS)) > 0:
                raise RuntimeError('Invalid string in CatalogFS file')

            # Go further, skipping whitespace line.
            current_pos = line_end + FORMAT_NEW_LINE_LENGTH
            continue

        option = data[current_pos:param_end].strip(FORMAT_TRIMMING_CHARS)
        value_start: int = param_end + len(FORMAT_FIELD_DELIMITER)

        value = data[value_start:line_end]

        current_pos = line_end + FORMAT_NEW_LINE_LENGTH

        # Return the first non-whitespace line
        return (current_pos, option, value)

    # No non-whitespace line found
    return (-1, '', '')


def cfsfile_extract_int(field_value_str: str) -> int:
    """
    Extract integer from value string.

    :param field_value_str: field to extract from
    :type field_value_str: str
    :raises RuntimeError: Invalid field value, not an integer
    :return: integer value of the string
    :rtype: int
    """
    try:
        field_value_int: int = int(
            field_value_str.strip(FORMAT_TRIMMING_CHARS))
    except:
        raise RuntimeError(
            f'Invalid field value, not an integer: {field_value_str}')

    return field_value_int


def fill_cfsfile_from_string(data: str, cfs_file: CFSFile) -> None:
    """
    Fill CFSFile class from a string with CFSFile content

    :param data: string with CFSFile content to parse
    :type data: str
    :param cfs_file: CFSFile to fill
    :type cfs_file: CFSFile
    :raises RuntimeError: CatalogFS file expected but no valid header found
    :raises RuntimeError: CatalogFS file has invalid version string
    :raises RuntimeError: CatalogFS file has unsupported version
    :raises RuntimeError: Unknown param name in CatalogFS file
    """
    current_pos: int = 0

    # Check header (first option-value pair)
    (body_start, header_name, version_str) = cfsfile_get_next_option_pair(
        data, current_pos)

    if header_name != FORMAT_HEADER_NAME:
        raise RuntimeError('CatalogFS file expected but no valid header found')

    try:
        version_int: int = int(version_str)
    except:
        raise RuntimeError(
            f'CatalogFS file has invalid version string: "{version_str}"')

    if version_int != CFSFILE_VERSION_3:
        raise RuntimeError(
            f'CatalogFS file has unsupported version (file version: "{version_int}")')

    # Other option-value pairs

    current_pos = body_start

    while True:

        (new_pos, option, value) = cfsfile_get_next_option_pair(
            data, current_pos)

        if new_pos == -1:
            # finished reading
            break

        current_pos = new_pos

        param: str = option

        if param == 'size':
            cfs_file.size = cfsfile_extract_int(value)
        elif param == 'blocks':
            cfs_file.blocks = cfsfile_extract_int(value)
        elif param == 'mode':
            cfs_file.mode = cfsfile_extract_int(value)
        elif param == 'uid':
            cfs_file.uid = cfsfile_extract_int(value)
        elif param == 'gid':
            cfs_file.gid = cfsfile_extract_int(value)
        elif param == 'atime':
            cfs_file.atime = cfsfile_extract_int(value)
        elif param == 'mtime':
            cfs_file.mtime = cfsfile_extract_int(value)
        elif param == 'ctime':
            cfs_file.ctime = cfsfile_extract_int(value)
        elif param == 'atimensec':
            cfs_file.atimensec = cfsfile_extract_int(value)
        elif param == 'mtimensec':
            cfs_file.mtimensec = cfsfile_extract_int(value)
        elif param == 'ctimensec':
            cfs_file.ctimensec = cfsfile_extract_int(value)
        elif param == 'nlink':
            cfs_file.nlink = cfsfile_extract_int(value)
        elif param == 'blksize':
            cfs_file.blksize = cfsfile_extract_int(value)
        elif param == 'sha256':
            cfs_file.sha256 = value.strip(FORMAT_TRIMMING_CHARS)
        else:
            raise RuntimeError('Unknown param name in CatalogFS file')

    return cfs_file


def old_format_extract_next_int(data: str, start_position: int) -> (int, int):
    """
    Extract integer value from the string starting from start position.
    Used for Old formats only (v1 and v2).

    :param data: string to extract from
    :type data: str
    :param start_position: start position in the string
    :type start_position: int
    :raises RuntimeError: Invalid field value, not an integer
    :return: (new position to use for further parsing, extracted integer value)
    :rtype: (int, int)
    """
    new_line: str = '\n'

    field_end: int = data.find(new_line, start_position)

    if field_end == -1:
        # maybe it's the end of the string
        field_end = len(data)

    field_value_str: str = data[start_position:field_end].strip()

    try:
        field_value_int: int = int(field_value_str)
    except:
        raise RuntimeError(
            f'Invalid field value, not an integer: {field_value_str}')

    return (field_end + len(new_line), field_value_int)


def old_format_extract_next_string(data: str, start_position: int) -> (int, str):
    """
    Extract string value from the string starting from start position.
    Used for Old formats only (v1 and v2).

    :param data: string to extract from
    :type data: str
    :param start_position: start position in the string
    :type start_position: int
    :return: (new position to use for further parsing, extracted string value)
    :rtype: (int, str)
    """

    new_line: str = '\n'

    field_end: int = data.find(new_line, start_position)

    if field_end == -1:
        # maybe it's the end of the string
        field_end = len(data)

    field_value_str: str = data[start_position:field_end].strip()

    return (field_end + len(new_line), field_value_str)


def old_format_extract_next_path(data: str, start_position: int) -> (int, str):
    """
    Extract path value from the string starting from start position.
    Used for Old formats only (v1 and v2).
    Path value may have newlines in it (any chars except \0),
    so the end of the path is considered to be \0 followed by newline.

    :param data: string to extract from
    :type data: str
    :param start_position: start position in the string
    :type start_position: int
    :return: (new position to use for further parsing, extracted path value)
    :rtype: (int, str)
    """

    path_end: str = '\0\n'

    field_end: int = data.find(path_end, start_position)

    if field_end == -1:
        # maybe it's the end of the string
        field_end = len(data)

    field_value_path: str = data[start_position:field_end].rstrip('\0')

    return (field_end + len(path_end), field_value_path)


def old_format_fill_cfsfile_body_from_string(data: str, version: int, cfs_file: CFSFile) -> None:
    """
    Fill CFSFile from a string with CFSFile content for old versions.
    Used for Old formats only (v1 and v2).
    Checks that version is valid.

    :param data: string with CFSFile content to parse
    :type data: str
    :param version: version of CFSFile format used for string
    :type version: int
    :param cfs_file: CFSFile to fill
    :type cfs_file: CFSFile
    :raises RuntimeError: Unknown format version of CatalogFS file
    :raises RuntimeError: Invalid string in CatalogFS file
    :raises RuntimeError: Unknown param name in CatalogFS file
    """
    # NOTE version is not that important here because version 2 is compartible with version 1
    if version != CFSFILE_VERSION_1 and version != CFSFILE_VERSION_2:
        raise RuntimeError(
            f'Unknown format version of CatalogFS file: {version}')

    # We have to work with some fields like filename
    # and filepath that can have ANY chars except \0,
    # so we have to manually parse data string field
    # after field without split('\n') and etc.

    field_sep: str = ': '
    new_line: str = '\n'
    current_pos: int = 0

    while current_pos < len(data):

        line_end: int = data.find(new_line, current_pos)
        param_end: int = data.find(field_sep, current_pos, line_end)

        if param_end == -1:
            if len(data[current_pos:line_end].strip(FORMAT_TRIMMING_CHARS)) > 0:
                raise RuntimeError('Invalid string in CatalogFS file')
            current_pos = line_end + len(new_line)
            continue

        param = data[current_pos:param_end]
        value_start: int = param_end + len(field_sep)

        new_pos: int = 0
        if param == 'size':
            new_pos, cfs_file.size = old_format_extract_next_int(
                data, value_start)
        elif param == 'blocks':
            new_pos, cfs_file.blocks = old_format_extract_next_int(
                data, value_start)
        elif param == 'mode':
            new_pos, cfs_file.mode = old_format_extract_next_int(
                data, value_start)
        elif param == 'uid':
            new_pos, cfs_file.uid = old_format_extract_next_int(
                data, value_start)
        elif param == 'gid':
            new_pos, cfs_file.gid = old_format_extract_next_int(
                data, value_start)
        elif param == 'atime':
            new_pos, cfs_file.atime = old_format_extract_next_int(
                data, value_start)
        elif param == 'mtime':
            new_pos, cfs_file.mtime = old_format_extract_next_int(
                data, value_start)
        elif param == 'ctime':
            new_pos, cfs_file.ctime = old_format_extract_next_int(
                data, value_start)
        elif param == 'atimensec':
            new_pos, cfs_file.atimensec = old_format_extract_next_int(
                data, value_start)
        elif param == 'mtimensec':
            new_pos, cfs_file.mtimensec = old_format_extract_next_int(
                data, value_start)
        elif param == 'ctimensec':
            new_pos, cfs_file.ctimensec = old_format_extract_next_int(
                data, value_start)
        elif param == 'nlink':
            new_pos, cfs_file.nlink = old_format_extract_next_int(
                data, value_start)
        elif param == 'blksize':
            new_pos, cfs_file.blksize = old_format_extract_next_int(
                data, value_start)
        elif param == 'sha256':
            new_pos, cfs_file.sha256 = old_format_extract_next_string(
                data, value_start)
        elif param == 'name':
            new_pos, _ = old_format_extract_next_path(
                data, value_start)
        elif param == 'path':
            new_pos, _ = old_format_extract_next_path(
                data, value_start)
        else:
            raise RuntimeError('Unknown param name in CatalogFS file')

        current_pos = new_pos

    return cfs_file


def old_format_fill_cfsfile_from_string(data: str, cfs_file: CFSFile) -> None:
    """
    Fill CFSFile from a string with CFSFile content for old versions.
    Used for Old formats only (v1 and v2).

    :param data: string with CFSFile content to parse
    :type data: str
    :param cfs_file: CFSFile to fill
    :type cfs_file: CFSFile
    :raises RuntimeError: CatalogFS file expected but no valid header found
    :raises RuntimeError: CatalogFS file has invalid version string
    :raises RuntimeError: CatalogFS file has invalid old-format version
    """
    new_line: str = '\n'
    header_end: int = data.find(new_line, 0)

    if header_end == -1:
        raise RuntimeError('CatalogFS file expected but no valid header found')

    header: str = data[0:header_end]

    if not header.startswith(FORMAT_HEADER_PREFIX_OLD_FORMAT):
        raise RuntimeError('CatalogFS file expected but no valid header found')

    version_str: str = header[len(FORMAT_HEADER_PREFIX_OLD_FORMAT):]
    try:
        version_int: int = int(version_str)
    except:
        raise RuntimeError(
            f'CatalogFS file has invalid version string: "{version_str}"')

    if version_int != CFSFILE_VERSION_1 and version_int != CFSFILE_VERSION_2:
        raise RuntimeError(
            f'CatalogFS file has invalid old-format version (file version: "{version_int}")')

    body_data: str = data[header_end + len(new_line):]

    return old_format_fill_cfsfile_body_from_string(body_data, version_int, cfs_file)


def read_cfsfile(filepath: Path, cfs_file: CFSFile) -> None:
    """
    Read CFSFile from the file's content. 
    Supports different file format versions.

    :param filepath: file to read from
    :type filepath: Path
    :param cfs_file: CFSFile to save the result to
    :type cfs_file: CFSFile
    """
    data: str = filepath.read_text(encoding='utf-8', errors='strict')

    # Check for older versions of format (v1 and v2)
    if is_old_format_cfsfile(data):
        old_format_fill_cfsfile_from_string(data, cfs_file)
    else:
        fill_cfsfile_from_string(data, cfs_file)


def write_cfsfile(source_st: os.stat_result,
                  cfs_file: CFSFile,
                  output_cfsfile_file: Path,
                  flag_data_only: bool,
                  flag_data_and_time_only: bool) -> bool:
    """
    Write CFSFile information to the output file and use os.stat_result for output file's actual stat
    Supports custom sets of fields in the output file - size-only and data-and-time-only

    :param source_st: stat to apply to the output file after creation
    :type source_st: os.stat_result
    :param cfs_file: CFSFile data to store in the output file's content
    :type cfs_file: CFSFile
    :param output_cfsfile_file: path of file to save CFSFile information to
    :type output_cfsfile_file: Path
    :param flag_data_only: save to the output file only fields that describe data content (size, checksum)
    :type flag_data_only: bool
    :param flag_data_and_time_only: save to the output file only fields that describe data content and modification time
    :type flag_data_and_time_only: bool
    :return: True on success, False on error
    :rtype: bool
    """
    data: str = ''

    if flag_data_only:
        data = (FORMAT_HEADER_TO_WRITE +
                (f"size{FORMAT_FIELD_DELIMITER}{cfs_file.size}{FORMAT_NEW_LINE_CHAR}" if cfs_file.size is not None else '') +
                (f"sha256{FORMAT_FIELD_DELIMITER}{cfs_file.sha256}{FORMAT_NEW_LINE_CHAR}" if cfs_file.sha256 is not None else ''))
    elif flag_data_and_time_only:
        data = (FORMAT_HEADER_TO_WRITE +
                (f"size{FORMAT_FIELD_DELIMITER}{cfs_file.size}{FORMAT_NEW_LINE_CHAR}" if cfs_file.size is not None else '') +
                (f"mtime{FORMAT_FIELD_DELIMITER}{int(cfs_file.mtime)}{FORMAT_NEW_LINE_CHAR}" if cfs_file.mtime is not None else '') +
                (f"mtimensec{FORMAT_FIELD_DELIMITER}{cfs_file.mtimensec}{FORMAT_NEW_LINE_CHAR}" if cfs_file.mtimensec is not None and cfs_file.mtimensec != 0 else '') +
                (f"sha256{FORMAT_FIELD_DELIMITER}{cfs_file.sha256}{FORMAT_NEW_LINE_CHAR}" if cfs_file.sha256 is not None else ''))
    else:
        data = (FORMAT_HEADER_TO_WRITE +
                (f"size{FORMAT_FIELD_DELIMITER}{cfs_file.size}{FORMAT_NEW_LINE_CHAR}" if cfs_file.size is not None else '') +
                (f"blocks{FORMAT_FIELD_DELIMITER}{cfs_file.blocks}{FORMAT_NEW_LINE_CHAR}" if cfs_file.blocks is not None else '') +
                (f"mode{FORMAT_FIELD_DELIMITER}{cfs_file.mode}{FORMAT_NEW_LINE_CHAR}" if cfs_file.mode is not None else '') +
                (f"uid{FORMAT_FIELD_DELIMITER}{cfs_file.uid}{FORMAT_NEW_LINE_CHAR}" if cfs_file.uid is not None else '') +
                (f"gid{FORMAT_FIELD_DELIMITER}{cfs_file.gid}{FORMAT_NEW_LINE_CHAR}" if cfs_file.gid is not None else '') +
                (f"atime{FORMAT_FIELD_DELIMITER}{int(cfs_file.atime)}{FORMAT_NEW_LINE_CHAR}" if cfs_file.atime is not None else '') +
                (f"mtime{FORMAT_FIELD_DELIMITER}{int(cfs_file.mtime)}{FORMAT_NEW_LINE_CHAR}" if cfs_file.mtime is not None else '') +
                (f"ctime{FORMAT_FIELD_DELIMITER}{int(cfs_file.ctime)}{FORMAT_NEW_LINE_CHAR}" if cfs_file.ctime is not None else '') +
                (f"atimensec{FORMAT_FIELD_DELIMITER}{cfs_file.atimensec}{FORMAT_NEW_LINE_CHAR}" if cfs_file.atimensec is not None and cfs_file.atimensec != 0 else '') +
                (f"mtimensec{FORMAT_FIELD_DELIMITER}{cfs_file.mtimensec}{FORMAT_NEW_LINE_CHAR}" if cfs_file.mtimensec is not None and cfs_file.mtimensec != 0 else '') +
                (f"ctimensec{FORMAT_FIELD_DELIMITER}{cfs_file.ctimensec}{FORMAT_NEW_LINE_CHAR}" if cfs_file.ctimensec is not None and cfs_file.ctimensec != 0 else '') +
                (f"nlink{FORMAT_FIELD_DELIMITER}{cfs_file.nlink}{FORMAT_NEW_LINE_CHAR}" if cfs_file.nlink is not None else '') +
                (f"blksize{FORMAT_FIELD_DELIMITER}{cfs_file.blksize}{FORMAT_NEW_LINE_CHAR}" if cfs_file.blksize is not None else '') +
                (f"sha256{FORMAT_FIELD_DELIMITER}{cfs_file.sha256}{FORMAT_NEW_LINE_CHAR}" if cfs_file.sha256 is not None else ''))

    output_cfsfile_file.write_text(data, encoding='utf-8', errors='strict')

    # Try to set mode if possible (not important)
    try:
        os.chmod(output_cfsfile_file, source_st.st_mode)
    except:
        print_error(
            f'Failed to chmod file: {correct_utf8_pathstring(output_cfsfile_file)}')

    # Try to set uid/gid if possible (not important)
    try:
        os.chown(output_cfsfile_file, source_st.st_uid, source_st.st_gid)
    except:
        print_error(
            f'Failed to chown file: {correct_utf8_pathstring(output_cfsfile_file)}')

    # Try to set atime and mtime if possible (not important)
    # ctime cannot be set using python as far as I know
    try:
        os.utime(output_cfsfile_file, (source_st.st_atime, source_st.st_mtime))
    except:
        print_error(
            f'Failed to set utime for file: {correct_utf8_pathstring(output_cfsfile_file)}')

    print_ok(
        f'File was listed: {correct_utf8_pathstring(output_cfsfile_file)}')

    return True


def create_cfsfile_from_regularfile(st: os.stat_result) -> CFSFile:
    """
    Create CFSFile with stat values from the regular file's os.stat_result.

    :param st: stat to take values from
    :type st: os.stat_result
    :return: new CFSFile filled with values from os.stat_result
    :rtype: CFSFile
    """
    cfs_file: CFSFile = CFSFile()

    cfs_file.size = st.st_size
    cfs_file.blocks = st.st_blocks
    cfs_file.mode = st.st_mode
    cfs_file.uid = st.st_uid
    cfs_file.gid = st.st_gid
    cfs_file.atime = st.st_atime
    cfs_file.mtime = st.st_mtime
    cfs_file.ctime = st.st_ctime
    cfs_file.atimensec = st.st_atime_ns % 1000000000
    cfs_file.mtimensec = st.st_mtime_ns % 1000000000
    cfs_file.ctimensec = st.st_ctime_ns % 1000000000
    cfs_file.nlink = st.st_nlink
    cfs_file.blksize = st.st_blksize
    # not filling:
    cfs_file.sha256: str = None

    return cfs_file


def copy_symlink(source_file: Path, output_file: Path) -> bool:
    """
    Copy symlink directly (not following other symlinks)

    :param source_file: source symlink file path
    :type source_file: Path
    :param output_file: target symlink file path
    :type output_file: Path
    :return: always True (currently) or throws on error
    :rtype: bool
    """
    shutil.copy2(source_file, output_file, follow_symlinks=False)

    print_ok(f'Symlink was copied: {correct_utf8_pathstring(output_file)}')

    return True


def create_directory(source_directory: Path, output_directory: Path, skip_existing: bool) -> bool:
    """
    Create an output directory using path of source directory.
    Checks if source directory is a symlink and also creates symlink in that case.
    Output directory has mode 0o777.

    :param source_directory: path of source directory or symlink
    :type source_directory: Path
    :param output_directory: path of output directory to create
    :type output_directory: Path
    :param skip_existing: if True then it's OK for output directory to exists, otherwise it's an error
    :type skip_existing: bool
    :return: True on success, False on error
    :rtype: bool
    """
    if does_exist(output_directory, stat.S_IFDIR):
        if skip_existing:
            print_ok(
                f'Output directory already exists, skipping it: {correct_utf8_pathstring(output_directory)}')
            return True
        else:
            print_error(
                f'Output directory already exists and will not be modified: {correct_utf8_pathstring(output_directory)}')
            return False  # Do nothing to prevent any modification of existing directories
    elif does_exist(output_directory, 0):
        print_error(
            f'Cannot create output directory because something already has the same name: {correct_utf8_pathstring(output_directory)}')
        return False

    st: os.stat_result = os.lstat(source_directory)

    if stat.S_ISLNK(st.st_mode):
        print_ok(
            f'Directory is a symlink: {correct_utf8_pathstring(source_directory)}')
        return copy_symlink(source_directory, output_directory)

    if not stat.S_ISDIR(st.st_mode):
        print_error(
            f'Internal error. Source directory is not a directory: {correct_utf8_pathstring(source_directory)}')
        return False

    output_directory.mkdir(mode=0o777, parents=False, exist_ok=False)

    print_ok(
        f'Directory was created: {correct_utf8_pathstring(output_directory)}')

    return True


def update_directory(source_directory: Path, output_directory: Path) -> bool:
    """
    Update chmod, chown and utime of output directory based on source one

    :param source_directory: path of directory to take values from
    :type source_directory: Path
    :param output_directory: path of target directory to set values to
    :type output_directory: Path
    :return: True on success, False on error
    :rtype: bool
    """
    st: os.stat_result = os.lstat(source_directory)

    if stat.S_ISLNK(st.st_mode):
        print_ok(
            f'Skipping update for the directory as it is a symlink: {correct_utf8_pathstring(source_directory)}')
        return True

    if not stat.S_ISDIR(st.st_mode):
        print_error(
            f'Internal error. Provided directory is not actually a directory: {correct_utf8_pathstring(source_directory)}')
        return False

   # Try to set mode if possible (not important)
    try:
        os.chmod(output_directory, st.st_mode)
    except:
        print_error(
            f'Failed to chmod directory: {correct_utf8_pathstring(output_directory)}')

    # Try to set uid/gid if possible (not important)
    try:
        os.chown(output_directory, st.st_uid, st.st_gid)
    except:
        print_error(
            f'Failed to chown directory: {correct_utf8_pathstring(output_directory)}')

    # Try to set atime and mtime if possible (not important)
    # ctime can not be set by python as far as I know
    try:
        os.utime(output_directory, (st.st_atime, st.st_mtime))
    except:
        print_error(
            f'Failed to set utime for directory: {correct_utf8_pathstring(output_directory)}')

    print_ok(
        f'Directory was updated: {correct_utf8_pathstring(output_directory)}')

    return True


def process_one_file(source_file: Path,
                     output_cfsfile_file: Path,
                     skip_existing: bool,
                     flag_source_is_cfsfile: bool,
                     flag_sha256: bool,
                     flag_data_only: bool,
                     flag_data_and_time_only: bool) -> bool:
    """
    Creates real CFSfile at provided path with stat from source file.

    Supports source file to be a CFSfile file already.
    In this case the stat is taken from the source file's content.

    Can optionally calculate SHA256 checksum and save it into the output file.
    Supports custom sets of fields in output file - size-only and data-and-time-only

    :param source_file: path of source file to get stat of or from
    :type source_file: Path
    :param output_cfsfile_file: output CFSfile file path
    :type output_cfsfile_file: Path
    :param skip_existing: if True and output file exists it will be skipped without error
    :type skip_existing: bool
    :param flag_source_is_cfsfile: source file is a CFSfile, read stat from its content
    :type flag_source_is_cfsfile: bool
    :param flag_sha256: calculate and save SHA256 checksum of source file to the output file
    :type flag_sha256: bool
    :param flag_data_only: save to the output file only fields that describe data content (size, checksum)
    :type flag_data_only: bool
    :param flag_data_and_time_only: save to the output file only fields that describe data content and modification time
    :type flag_data_and_time_only: bool
    :return: True on success, False on error
    :rtype: bool
    """
    if does_exist(output_cfsfile_file, stat.S_IFREG) or does_exist(output_cfsfile_file, stat.S_IFLNK):
        if skip_existing:
            print_ok(
                f'Output file already exists, skipping it: {correct_utf8_pathstring(output_cfsfile_file)}')
            return True
        else:
            print_error(
                f'Output file already exists and will not be modified: {correct_utf8_pathstring(output_cfsfile_file)}')
            return False
    elif does_exist(output_cfsfile_file, 0):
        print_error(
            f'Cannot create output file because something already has the same name: {correct_utf8_pathstring(output_cfsfile_file)}')
        return False

    st: os.stat_result = os.lstat(source_file)

    if not stat.S_ISREG(st.st_mode) and not stat.S_ISLNK(st.st_mode):
        print_error(
            f'Source file was skipped as it is not a regular file nor symlink: {correct_utf8_pathstring(source_file)}')
        return False

    res: bool = False

    if stat.S_ISREG(st.st_mode):

        if flag_source_is_cfsfile:

            if st.st_size > FORMAT_MAX_FILE_SIZE:
                print_error(
                    f'File is too big ({st.st_size} bytes) to be a valid CatalogFS file: {correct_utf8_pathstring(source_file)}')
                return False

            cfs_file: CFSFile = create_cfsfile_from_regularfile(st)

            try:
                read_cfsfile(source_file, cfs_file)
            except Exception as e:
                print_error(
                    f'Failed to read CFSFile, error "{str(e)}" for file: {correct_utf8_pathstring(source_file)}')
                return False

            res = write_cfsfile(
                source_st=st,
                cfs_file=cfs_file,
                output_cfsfile_file=output_cfsfile_file,
                flag_data_only=flag_data_only,
                flag_data_and_time_only=flag_data_and_time_only)
        else:
            sha256_str: str = sha256_wrapper(source_file, flag_sha256)

            cfs_file: CFSFile = create_cfsfile_from_regularfile(st)
            cfs_file.sha256 = sha256_str

            res = write_cfsfile(
                source_st=st,
                cfs_file=cfs_file,
                output_cfsfile_file=output_cfsfile_file,
                flag_data_only=flag_data_only,
                flag_data_and_time_only=flag_data_and_time_only)

    elif stat.S_ISLNK(st.st_mode):
        res = copy_symlink(source_file, output_cfsfile_file)

    else:
        print_error(
            f'Internal error. File is not a regular file nor symlink: {correct_utf8_pathstring(source_file)}')
        return False  # should never happen

    return res


def sha256_checksum(filename: Path, block_size=65536) -> str:
    """
    Calculate SHA256 checksum for the file

    :param filename: path of file to calculate the checksum of
    :type filename: Path
    :param block_size: bock size for reading file, default is 65536
    :type block_size: int, optional
    :return: SHA256 checksum as a string
    :rtype: str
    """
    hasher = hashlib.sha256()
    with open(filename, 'rb') as f:
        while True:
            data = f.read(block_size)
            if not data:
                break
            hasher.update(data)

        # for block in iter(lambda: f.read(block_size), b''):
        #     hasher.update(block)
        return hasher.hexdigest()


def sha256_wrapper(filename: Path, flag_sha256: bool) -> str:
    """
    Wrapper for possible calculation of SHA256 checksum for the file (if needed)

    :param filename: path of file to calculate the checksum of
    :type filename: Path
    :param flag_sha256: is actual checksum calculation needed
    :type flag_sha256: bool
    :return: SHA256 checksum or an empty string on error or if no calculation is needed
    :rtype: str
    """
    sha256_str: str = None
    if flag_sha256:
        try:
            sha256_str = sha256_checksum(filename)
        except PermissionError:
            print_error(
                f'Permission error when tried to calculate SHA256 for file: {correct_utf8_pathstring(filename)}')
        except Exception as e:
            print_error(
                f'Exception "{repr(e)}" was caught when tried to calculate SHA256 for file: {correct_utf8_pathstring(filename)}')
    return sha256_str


def walktree(root_source_path: Path,
             root_output_path: Path,
             flag_source_is_cfsfiles: bool,
             flag_sha256: bool,
             flag_continue: bool,
             flag_data_only: bool,
             flag_data_and_time_only: bool) -> None:
    """
    Recursively walk around the source directory to index all files and store the results to the output directory accordingly

    :param root_source_path: path of the source directory to make index of
    :type root_source_path: Path
    :param root_output_path: output directory for placing index files (preferably empty)
    :type root_output_path: Path
    :param flag_source_is_cfsfiles: source directory has only CatalogFS-files already
    :type flag_source_is_cfsfiles: bool
    :param flag_sha256: calculate and store SHA256 checksum (much slower)
    :type flag_sha256: bool
    :param flag_continue: continue indexing (ignore and skip existing output files)
    :type flag_continue: bool
    :param flag_data_only: store only information that is useful for comparing the file content (easy comparing and diff)
    :type flag_data_only: bool
    :param flag_data_and_time_only: store only information that is needed for comparing the file content and modification time
    :type flag_data_and_time_only: bool
    """
    for dirpath, directories, filenames in os.walk(root_source_path, topdown=True, followlinks=False):

        source_path: Path = Path(dirpath)
        dir_subpath: Path = Path(dirpath).relative_to(root_source_path)
        output_path: Path = Path(root_output_path / dir_subpath)

        # All directories in source_path
        for d in directories:
            try:

                source_directory: Path = Path(source_path) / d
                output_directory: Path = Path(output_path) / d

                try:
                    str(source_directory).encode('utf-8').decode('utf-8')
                except UnicodeError:
                    # I decided to process directories with incorrect (non utf-8) names
                    print_error(
                        f'Directory has incorrect UTF-8 name or path but still will be processed: {correct_utf8_pathstring(source_directory)}')

                st: os.stat_result = os.lstat(source_directory)
                if stat.S_ISDIR(st.st_mode) and not os.access(source_directory, os.R_OK):
                    print_error(
                        f'Directory is not accessible and its content will be skipped: {correct_utf8_pathstring(source_directory)}')

                skip_existing: bool = flag_continue
                create_directory(source_directory=source_directory,
                                 output_directory=output_directory,
                                 skip_existing=skip_existing)

            except Exception as e:
                res = False
                print_error(
                    f'Exception "{repr(e)}" was caught for directory: {correct_utf8_pathstring(source_directory)}')

        # Now all files in source_path
        for f in filenames:

            res: bool = False
            try:

                source_file: Path = Path(source_path) / f
                output_cfsfile_file: Path = Path(output_path) / f
                #path_to_save: Path = source_file.relative_to(root_source_path)

                # is_correct_utf8: bool = False
                try:
                    str(source_file).encode('utf-8').decode('utf-8')
                    # is_correct_utf8 = True
                except UnicodeError:
                    # I decided to process files with incorrect (non utf-8) names
                    # is_correct_utf8 = False
                    print_error(
                        f'File has incorrect UTF-8 name or path but still will be processed: {correct_utf8_pathstring(source_file)}')

                # NOTE: I decided to process files with incorrect (non utf-8) names
                # should_we_continue = not is_correct_utf8
                should_we_continue = True

                skip_existing: bool = flag_continue
                if should_we_continue:
                    res = process_one_file(
                        source_file=source_file,
                        output_cfsfile_file=output_cfsfile_file,
                        skip_existing=skip_existing,
                        flag_source_is_cfsfile=flag_source_is_cfsfiles,
                        flag_sha256=flag_sha256,
                        flag_data_only=flag_data_only,
                        flag_data_and_time_only=flag_data_and_time_only)

            except Exception as e:
                res = False
                print_error(
                    f'Exception "{repr(e)}" was caught for file: {correct_utf8_pathstring(Path(dirpath) / f)}')

            if not res:
                print_error(
                    f'File was skipped: {correct_utf8_pathstring(Path(dirpath) / f)}')

    # Modify permissions, uid/gid and utimes of directories in the whole tree
    # It should be after file creation because otherwise permissions
    # can prevent from proper indexing
    for dirpath, directories, filenames in os.walk(root_source_path, topdown=True, followlinks=False):

        source_path: Path = Path(dirpath)
        dir_subpath: Path = Path(dirpath).relative_to(root_source_path)
        output_path: Path = Path(root_output_path / dir_subpath)

        for d in directories:
            try:

                source_directory: Path = Path(source_path) / d
                output_directory: Path = Path(output_path) / d

                update_directory(source_directory,
                                 output_directory)

            except Exception as e:
                res = False
                print_error(
                    f'Exception "{repr(e)}" was caught for directory: {correct_utf8_pathstring(source_directory)}')


def main() -> int:
    """
    Main function, the entry point

    :return: 0 on success, non-zero on error
    :rtype: int
    """

    parser = argparse.ArgumentParser(
        description='Make a CatalogFS-compartible index (snapshot) of the source directory.',
        allow_abbrev=False)
    parser.add_argument('source_dir', type=str,
                        help='source directory to make index of')
    parser.add_argument('output_dir', type=str,
                        help='output directory (preferably empty) for placing index files (preferably empty)')

    parser.add_argument('-s', '--sha256', dest='flag_sha256', action='store_true',
                        help='calculate and store SHA256 hashes (much slower)')
    parser.add_argument('-c', '--continue', dest='flag_continue', action='store_true',
                        help='continue indexing (ignore and skip existing output files)')
    parser.add_argument('-d', '--data-only', dest='flag_data_only', action='store_true',
                        help='store only information that is useful for comparing the file content (easy comparing and diff)')
    parser.add_argument('-t', '--data-and-time-only', dest='flag_data_and_time_only', action='store_true',
                        help='store only information that is needed for comparing the file content and modification time')
    parser.add_argument('-x', '--source-is-cfsfiles', dest='flag_source_is_cfsfiles', action='store_true',
                        help='source directory already has only CatalogFS-files (small files with meta-information)')
    args = parser.parse_args()

    source_dir_input: str = args.source_dir
    output_dir_input: str = args.output_dir
    flag_source_is_cfsfiles: bool = args.flag_source_is_cfsfiles
    flag_sha256: bool = args.flag_sha256
    flag_continue: bool = args.flag_continue
    flag_data_only: bool = args.flag_data_only
    flag_data_and_time_only: bool = args.flag_data_and_time_only

    if flag_source_is_cfsfiles and flag_sha256:
        print_error(
            f'SHA256 calculation cannot be used when source files are CatalogFS-files.')
        return -5

    try:
        source_dir = Path(source_dir_input).resolve(strict=True)
    except FileNotFoundError:
        print_error(f'Source directory does not exist.')
        return -1
    except:
        print_error(f'Failed to use source directory as a path.')
        return -2

    try:
        output_dir = Path(output_dir_input).resolve(strict=True)
    except FileNotFoundError:
        # print_error(f'Output directory does not exist.')

        print_error(
            f'Output directory does not exist, it will be created.')

        # if does_exist(output_directory, stat.S_IFDIR):
        Path(output_dir_input).mkdir(parents=True, exist_ok=False)
        print_ok(
            f'Output directory was created: {correct_utf8_pathstring(output_dir_input)}')

        # try to resolve again
        try:
            output_dir = Path(output_dir_input).resolve(strict=True)
        except:
            print_error(f'Failed to use output directory as a path.')
            return -3
    except:
        print_error(f'Failed to use output directory as a path.')
        return -4

    walktree(source_dir,
             output_dir,
             flag_source_is_cfsfiles=flag_source_is_cfsfiles,
             flag_sha256=flag_sha256,
             flag_continue=flag_continue,
             flag_data_only=flag_data_only,
             flag_data_and_time_only=flag_data_and_time_only)

    return 0


"""
Global space, starting point of actual execution
"""
if __name__ == '__main__':
    sys.exit(main())
