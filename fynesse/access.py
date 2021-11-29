from . import config

import mysql.connector
import urllib.request
import wget
import os
import csv
from contextlib import contextmanager

DATABASE_NAME = "ads_data_jo429"


def is_site_up(url: str):
    """Checks if a http request to a url returns 200 OK"""
    return urllib.request.urlopen(url).getcode() == 200


class Database:
    def __init__(self, url: str, username: str, password: str):
        self.url = url
        self.username = username
        self.password = password

    @staticmethod
    def _make_database(cursor):
        """Makes the database, only needsd to ever be run once"""
        command = f"CREATE DATABASE {DATABASE_NAME}"
        cursor.execute(command)

    @contextmanager
    def make_cursor(self, commit_after=True):
        try:
            conn = mysql.connector.connect(
                user=self.username,
                password=self.password,
                host=self.url,
                database=DATABASE_NAME,
                allow_local_infile=True
            )
        except mysql.connector.ProgrammingError:
            # If the database doesn't exist yet, make it
            conn = mysql.connector.connect(
                user=self.username,
                password=self.password,
                host=self.url
            )
            with conn.cursor() as cursor:
                Database._make_database(cursor)
            conn.close()

            return self.make_cursor()

        with conn.cursor() as cursor:
            yield cursor

        if commit_after:
            conn.commit()
        conn.close()

    def remake_pp_data_table(self):
        """Remakes the UK Price Paid data table, dropping it if it already exists.

        The table schema is written by Dale Potter and can be found on Github here:
        https://github.com/dalepotter/uk_property_price_data/blob/master/create_db.sql

        The index schema is written by Christian Cabrera, Carl Henrik Ek and Neil D. Lawrence
        and can be found here:
        https://mlatcl.github.io/ads/
        """

        with self.make_cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS `pp_data`")

        command = """
        CREATE TABLE IF NOT EXISTS `pp_data` (
          `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
          `price` int(10) unsigned NOT NULL,
          `date_of_transfer` date NOT NULL,
          `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
          `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
          `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
          `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
          `street` tinytext COLLATE utf8_bin NOT NULL,
          `locality` tinytext COLLATE utf8_bin NOT NULL,
          `town_city` tinytext COLLATE utf8_bin NOT NULL,
          `district` tinytext COLLATE utf8_bin NOT NULL,
          `county` tinytext COLLATE utf8_bin NOT NULL,
          `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
          `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
          `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
    
          primary key(`db_id`) 
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
    
        CREATE INDEX `pp.postcode` USING HASH
          ON `pp_data`
            (postcode);
        CREATE INDEX `pp.date` USING HASH
          ON `pp_data` 
            (date_of_transfer)
        """

        with self.make_cursor(False) as cursor:
            cursor.execute(command)

    def load_pp_data_into_table(self):
        """Loops over years and parts and populates the UK Price Paid data table.
        Guesses how many parts exist by going until a URL doesn't resolve
        """
        for year in range(1995, 2022):
            part = 1
            while self.load_pp_data_part_into_table(year, part):
                part += 1

    def _insert_pp_csv_row_by_row(self, filename: str):
        command = """
        INSERT INTO pp_data(transaction_unique_identifier, price, date_of_transfer, 
        postcode, property_type, new_build_flag, tenure_type, 
        primary_addressable_object_name, secondary_addressable_object_name, 
        street, locality, town_city, district, county, ppd_category_type, 
        record_status )
        VALUES('%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');
        """

        with open(filename, "r") as file:
            csv_data = csv.reader(file)

            with self.make_cursor() as cursor:
                for row in csv_data:
                    print("Inserting row {}", row)
                    cursor.execute(command, row)

    def _insert_pp_csv_at_once(self, filename: str):
        command = """
        LOAD DATA LOCAL INFILE %s INTO TABLE `pp_data`
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"'
        LINES STARTING BY '' TERMINATED BY '\n';
        """

        with self.make_cursor() as cursor:
            cursor.execute(command, [filename])

    def load_pp_data_part_into_table(self, year: int, part: int) -> bool:
        """Populates the UK Price Paid data table using data published by the UK government.
        Returns True if population was successful

        See here: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

        Price Paid Data is released under the Open Government Licence (OGL).
        Under the OGL, HM Land Registry permits you to use the Price Paid Data for
        commercial or non-commercial purposes. However, OGL does not cover the use
        of third party rights, which they are not authorised to license.
        """

        # Build URL
        url = f"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-{str(year)}-part{str(part)}.csv"

        # Check if the url is accessible
        if not is_site_up(url):
            return False

        # Get the CSV into disk
        print(f"Downloading Year {str(year)} Part {str(part)}")
        filename = wget.download(url)

        try:
            # Upload csv to database
            self._insert_pp_csv_at_once(filename)
        finally:
            # Delete the CSV on disk
            os.remove(filename)
            pass

        return True
