import os
from datetime import date, timedelta # To define date range of data
import requests # To define http request to be make
import pandas as pd # Convert data received from copernicus API in easier format
import geopandas as gpd # Convert Pandas dataframe in Geo pandas will allow us to use metadata and geoemtry.
from shapely.geometry import shape # To convert raw Geometry data


# Copernicus User email from environment variable
# Make sure to set this environment variable before running the script:
# On Windows (CMD): set COPERNICUS_USERNAME=your_email@example.com
# On Linux/macOS (Bash/Zsh): export COPERNICUS_USERNAME=your_email@example.com
copernicus_user = "jeonjangwon0709@gmail.com"
# Copernicus User Password from environment variable
# On Windows (CMD): set COPERNICUS_PASSWORD=your_password
# On Linux/macOS (Bash/Zsh): export COPERNICUS_PASSWORD=your_password
copernicus_password = "Wjswkddnjs:0709"

# WKT Representation of BBOX of AOI (Area of Interest)
# This polygon covers the Uiseong-Andong area in Gyeongbuk, South Korea.
ft = "POLYGON ((128.43097221573794 36.596134048691056, 128.43097221573794 36.358167796116376, 129.48217772082512 36.358167796116376, 129.48217772082512 36.596134048691056, 128.43097221573794 36.596134048691056))"
# Sentinel satellite that you are interested in
data_collection = "SENTINEL-2"

# Define the fire period for Uiseong wildfire (based on provided information)
# Wildfire start date: March 22, 2025
# Wildfire end date: March 28, 2025
wildfire_start_date = date(2024, 12, 20)
wildfire_end_date = date(2024, 12, 20)

# Calculate data collection start date (2 days before wildfire start)
collection_start_date = wildfire_start_date - timedelta(days=10)
collection_start_date_string = collection_start_date.strftime("%Y-%m-%d")

# Calculate data collection end date (2 days after wildfire end)
collection_end_date = wildfire_end_date + timedelta(days=10)
collection_end_date_string = collection_end_date.strftime("%Y-%m-%d")

# Define directory to save downloaded Sentinel data
save_directory = "downloaded_sentinel_data"
# Create the directory if it does not exist
os.makedirs(save_directory, exist_ok=True)


def get_keycloak(username: str, password: str) -> str:
    """
    Obtains an access token from the Keycloak authentication server.
    """
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    try:
        r = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data,
        )
        r.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        # Catch specific request exceptions for better error reporting
        print(f"Error during Keycloak token creation: {e}")
        if 'r' in locals() and r.json():
            raise Exception(f"Response from the server was: {r.json()}") from e
        else:
            raise Exception(f"Keycloak token creation failed. No valid response received.") from e
    return r.json()["access_token"]


# Construct the OData query to fetch available Sentinel-2 products
# The ContentDate/Start filter is corrected to be within the desired range.
json_ = requests.get(
    f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{data_collection}' and OData.CSC.Intersects(area=geography'SRID=4326;{ft}') and ContentDate/Start ge {collection_start_date_string}T00:00:00.000Z and ContentDate/Start le {collection_end_date_string}T23:59:59.999Z&$count=True&$top=1000"
).json()

p = pd.DataFrame.from_dict(json_["value"]) # Fetch available dataset
if p.shape[0] > 0 : # If we get data back
    p["geometry"] = p["GeoFootprint"].apply(shape)
    # Convert pandas dataframe to Geopandas dataframe by setting up geometry
    productDF = gpd.GeoDataFrame(p).set_geometry("geometry")
    # Remove L1C dataset as per project requirements (focus on L2A)
    productDF = productDF[~productDF["Name"].str.contains("L1C")]
    print(f"Total L2A tiles found: {len(productDF)}")
    productDF["identifier"] = productDF["Name"].str.split(".").str[0]
    allfeat = len(productDF)

    if allfeat == 0: # If L2A tiles are not available in current query
        print(f"No L2A tiles found for the period from {collection_start_date_string} to {collection_end_date_string} in the specified AOI.")
    else: # If L2A tiles are available in current query
        # download all tiles from server
        for index, feat in enumerate(productDF.iterfeatures()):
            try:
                # Create requests session
                session = requests.Session()
                # Get access token based on username and password
                keycloak_token = get_keycloak(copernicus_user, copernicus_password)

                session.headers.update({"Authorization": f"Bearer {keycloak_token}"})
                url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({feat['properties']['Id']})/$value"
                response = session.get(url, allow_redirects=False)

                # Handle redirects to actual download URL
                while response.status_code in (301, 302, 303, 307):
                    url = response.headers["Location"]
                    response = session.get(url, allow_redirects=False)

                print(f"Downloading product ID: {feat['properties']['Id']}")
                # Using verify=False for simplicity, consider proper SSL verification in production
                file = session.get(url, verify=True, allow_redirects=True)

                # Save the downloaded content to a zip file in the specified directory
                with open(
                    f"{save_directory}/{feat['properties']['identifier']}.zip", # Location to save zip from copernicus
                    "wb",
                ) as p:
                    print(f"Saving file: {feat['properties']['Name']}")
                    p.write(file.content)
            except Exception as e:
                # General exception for any issues during download
                print(f"Problem with server or download for product {feat['properties']['Id']}: {e}")
                print("Skipping to the next product...")
else : # If no tiles found for given date range and AOI at all
    print('No data found for the initial query based on date range and AOI.')