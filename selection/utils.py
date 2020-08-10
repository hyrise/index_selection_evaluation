import hashlib
import logging
import os
import urllib.request
import zipfile

from .workload import Workload


# --- Unit conversions ---
# Storage
def b_to_mb(b):
    return b / 1000 / 1000


def mb_to_b(mb):
    return mb * 1000 * 1000


# Time
def s_to_ms(s):
    return s * 1000


# --- Index selection utilities ---
def indexes_by_table(indexes):
    indexes_by_table = {}
    for index in indexes:
        table = index.table()
        if table not in indexes_by_table:
            indexes_by_table[table] = []

        indexes_by_table[table].append(index)

    return indexes_by_table


def get_utilized_indexes(
    workload, indexes_per_query, cost_evaluation, detailed_query_information=False
):
    utilized_indexes_workload = set()
    query_details = {}
    for query, indexes in zip(workload.queries, indexes_per_query):
        (
            utilized_indexes_query,
            cost_with_indexes,
        ) = cost_evaluation.which_indexes_utilized_and_cost(query, indexes)
        utilized_indexes_workload |= utilized_indexes_query

        if detailed_query_information:
            cost_without_indexes = cost_evaluation.calculate_cost(
                Workload([query]), indexes=[]
            )

            query_details[query] = {
                "cost_without_indexes": cost_without_indexes,
                "cost_with_indexes": cost_with_indexes,
                "utilized_indexes": utilized_indexes_query,
            }

    return utilized_indexes_workload, query_details


# --- Join Order Benchmark utilities ---

IMDB_LOCATION = "https://archive.org/download/imdb_20200624/imdb.zip"
IMDB_FILE_NAME = "imdb.zip"
IMDB_TABLE_DIR = "imdb_data"
IMDB_TABLE_NAMES = [
    "aka_name",
    "aka_title",
    "cast_info",
    "char_name",
    "company_name",
    "company_type",
    "comp_cast_type",
    "complete_cast",
    "info_type",
    "keyword",
    "kind_type",
    "link_type",
    "movie_companies",
    "movie_info",
    "movie_info_idx",
    "movie_keyword",
    "movie_link",
    "name",
    "person_info",
    "role_type",
    "title",
]


def _clean_up(including_table_dir=False):
    if os.path.exists(IMDB_FILE_NAME):
        os.remove(IMDB_FILE_NAME)

    if including_table_dir and os.path.exists(IMDB_TABLE_DIR):
        for file in os.listdir(IMDB_TABLE_DIR):
            os.remove("./%s/%s" % (IMDB_TABLE_DIR, file))
        os.rmdir(IMDB_TABLE_DIR)


def _files_exist():
    for table_name in IMDB_TABLE_NAMES:
        if not os.path.exists(os.path.join(IMDB_TABLE_DIR, table_name + ".csv")):
            return False

    return True


def download_and_uncompress_imdb_data():
    if _files_exist():
        logging.info("IMDB already present.")
        return True

    logging.critical("Retrieving the IMDB dataset - this may take a while.")

    # We are going to calculate the md5 hash later, on-the-fly while downloading
    hash_md5 = hashlib.md5()

    url = urllib.request.urlopen(IMDB_LOCATION)
    meta = url.info()
    file_size = int(meta["Content-Length"])

    file = open(IMDB_FILE_NAME, "wb")

    logging.info(f"Downloading: {IMDB_FILE_NAME} ({b_to_mb(file_size):.3f} MB)")

    already_retrieved = 0
    block_size = 8192
    try:
        while True:
            buffer = url.read(block_size)
            if not buffer:
                break

            hash_md5.update(buffer)

            already_retrieved += len(buffer)
            file.write(buffer)
            status = (
                f"Retrieved {already_retrieved * 100.0 / file_size:3.2f}% of the data"
            )
            # chr(8) refers to a backspace. In conjunction with end="\r", this overwrites
            # the previous status value and achieves the right padding.
            status = f"{status}{chr(8) * (len(status) + 1)}"
            print(status, end="\r")
    except Exception:
        logging.critical(
            "Aborting. Something went wrong during the download. Cleaning up."
        )
        _clean_up()
        return False

    file.close()
    logging.critical("Validating integrity...")

    hash_dl = hash_md5.hexdigest()

    if hash_dl != "1b5cf1e8ca7f7cb35235a3c23f89d8e9":
        logging.critical("Aborting. MD5 checksum mismatch. Cleaning up.")
        _clean_up()
        return False

    logging.critical("Downloaded file is valid.")
    logging.critical("Unzipping the file...")

    try:
        zip = zipfile.ZipFile(IMDB_FILE_NAME, "r")
        zip.extractall(IMDB_TABLE_DIR)
        zip.close()
    except Exception:
        logging.critical("Aborting. Something went wrong during unzipping. Cleaning up.")
        _clean_up(including_table_dir=True)
        return False

    logging.critical("Deleting the archive file.")
    _clean_up()
    return True
