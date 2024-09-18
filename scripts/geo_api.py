from dotenv import load_dotenv
from scrape.estates_scraping.db.db import get_db
import geopy
import os

load_dotenv()
HERE_GEO_API = os.getenv('HERE_GEO_API')

geocoder = geopy.geocoders.HereV7(
    apikey=HERE_GEO_API
)

"""
    The script is supposed to take the data that does not have assigned geodata yet
    , call an API and commit the data back to the DB
"""

table_def = {
    'street': 'street'
    , 'houseNumber': 'house_num'
    , 'postalCode': 'postal_cd'
    , 'district': 'district'
    , 'city': 'city'
    , 'county': 'county'
    , 'state': 'state'
    , 'countryCode': 'country'
}

with get_db() as db:
    cursor = db.cursor()

    cursor.execute(
        """
            SELECT flat_id, latitude, longitude
            FROM postgres.public.flats_geo
            WHERE country IS NULL -- take the non-populated rows only
            ORDER BY flat_id
            LIMIT 990 -- API limit is 1000
        """
    )
    results = [row for row in cursor.fetchall()]

    if results:
        for result in results:
            location = geocoder.reverse((result[1], result[2]), exactly_one=True)  # latitude and longitude
            address = location.raw['address']

            for db_field in table_def.keys():
                if db_field not in address:
                    address[db_field] = None

            cursor.execute(
                """
                    UPDATE postgres.public.flats_geo
                    SET street = %s
                        , house_num = %s
                        , postal_cd = %s
                        , district = %s
                        , city = %s
                        , county = %s
                        , state = %s
                        , country = %s
                    WHERE flat_id = %s
                """
                , (
                    address['street']
                    , address['houseNumber']
                    , address['postalCode']
                    , address['district']
                    , address['city']
                    , address['county']
                    , address['state']
                    , address['countryCode']
                    , result[0]
                )
            )

        print(f"Total rows added: {len(results)}")
        db.commit()
