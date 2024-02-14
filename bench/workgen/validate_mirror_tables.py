#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.


# ------------------------------------------------------------------------------
# Given a database directory generated by Workgen, find and compare the mirrored
# tables. Returns zero on success if all mirror pairs contain the same data.
# Returns a non-zero value on failure if any mirror pair contains a mismatch.
#
# Note that the wiredtiger/tools directory must be on your PYTHONPATH.
# ------------------------------------------------------------------------------
import os, pathlib, re, sys
from contextlib import redirect_stdout
from wt_tools_common import wiredtiger_open
from wt_cmp_uri import wiredtiger_compare_uri

# Print usage and exit with a failure status.
def usage_exit():

    print('Usage: python3 validate_mirror_tables database_dir')
    print('  database_dir is a POSIX pathname to a WiredTiger home directory')
    sys.exit(1)

# Given a database directory, return all Workgen tables using the WiredTiger metadata file.
def get_wiredtiger_db_files(connection):

    session = connection.open_session()
    c = session.open_cursor('metadata:', None, None)

    prefix = 'file:'
    c.set_key(prefix)
    c.search_near()

    files = []
    for k,_ in c:
        if not k.startswith(prefix):
            break
        if k.startswith(f'{prefix}WiredTiger'):
            continue
        # Skip the prefix and remove the extension.
        files.append(k[len(prefix):-3])

    c.close()
    session.close()
    return files

# Given a list of database tables, return a list of mirrored uri pairs.
def get_mirrors(connection, db_dir, db_files):

    db_files_remaining = set(db_files)
    mirrors = []
    session = connection.open_session()
    metadata_cursor = session.open_cursor('metadata:', None, None)

    for filename in db_files:
        if filename not in db_files_remaining:
            continue
        db_files_remaining.remove(filename)
        mirror_filename = get_mirror_file(metadata_cursor, f'table:{filename}')

        # At this point, there is no guarantee that the database contains all the base/mirror pairs.
        # It is possible to have a base and no associated mirror and vice-versa. This may happen
        # when a drop is occurring when a snapshot of the database is taken and fed into this
        # script.
        if mirror_filename and mirror_filename in db_files_remaining:
            db_files_remaining.remove(mirror_filename)
            mirrors.append([f'{db_dir}/table:{filename}',
                            f'{db_dir}/table:{mirror_filename}'])

    metadata_cursor.close()
    session.close()
    return mirrors

# Get the mirror for the specified file by examining the file's metadata. Mirror names
# are stored in the 'app_metadata' by Workgen when mirroring is enabled. If the file has
# a mirror, the name of the mirror is returned. Otherwise, the function returns None.
# It is possible that the requested file does not exist in the metadata file, return None in this
# scenario as well.
def get_mirror_file(metadata_cursor, filename):

    # It is possible that the file requested does not exist in the metadata file.
    try:
       metadata = metadata_cursor[filename]
    except Exception as e:
        if e.__class__.__name__ == 'KeyError':
            return None

    result = re.findall('app_metadata="([^"]*)"', metadata)
    mirror = None

    if result:
        app_metadata = dict((a.strip(), b.strip())
            for a, b in (element.split('=')
                for element in result[0].split(',')))

        if app_metadata.get('workgen_dynamic_table') == 'true' and \
           app_metadata.get('workgen_table_mirror') != None :
            mirror = app_metadata['workgen_table_mirror']
            mirror = mirror.split(':')[1]

    return mirror

# ------------------------------------------------------------------------------

def main(sysargs):

    if len(sysargs) != 1:
        usage_exit()

    db_dir = sysargs[0]

    connection = wiredtiger_open(db_dir, 'readonly')
    db_files = get_wiredtiger_db_files(connection)
    mirrors = get_mirrors(connection, db_dir, db_files)
    connection.close()
    failure_count = 0

    for item in mirrors:
        stdout = None
        try:
            with open(os.devnull, "w") as f, redirect_stdout(f):
                stdout = wiredtiger_compare_uri(item)
        except SystemExit as e:
            if e.code != 0:
                print(f"Mirror mismatch {item}: {stdout}")
                failure_count += 1

    if failure_count == 0:
        print(f"Successfully validated {len(mirrors)} table mirrors in " \
              f"database directory '{db_dir}'.")
    else:
        print(f"Mirrored table validation failed for {failure_count} of " \
              f"{len(mirrors)} table mirrors in database directory '{db_dir}'.")

    sys.exit(1 if failure_count > 0 else 0)

# ------------------------------------------------------------------------------

if __name__ == "__main__":

    main(sys.argv[1:])
