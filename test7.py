import matplotlib.pyplot as plt
import pystac_client
import planetary_computer
import geopandas as gpd
import rioxarray
import streamlit as st
import leafmap.foliumap as leafmap
import sqlalchemy
from get_conn import get_connection_uri
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import io
import os
import numpy as np
from datetime import datetime


# Inicjalizacja klienta Blob Storage
def get_blob_service_client():
    account_name = os.getenv('AZURE_STORAGE_ACCOUNT')
    account_url = f"https://{account_name}.blob.core.windows.net"
    credential = DefaultAzureCredential()
    return BlobServiceClient(account_url, credential)


# Funkcja odczytu z Blob Storage
def blob_read(blob_name):
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client("geotiff", blob_name)

    stream = io.BytesIO()
    blob_client.download_blob().readinto(stream)
    stream.seek(0)

    return rioxarray.open_rasterio(stream)


# Funkcja do zapisu statystyk do bazy danych
def save_stats_to_db(index_name, stats, cloud_cover):
    engine = sqlalchemy.create_engine(get_connection_uri())

    with engine.connect() as conn:
        conn.execute(
            sqlalchemy.text("""
            INSERT INTO raster_stats (
                index_name, 
                min_value, 
                max_value, 
                mean_value, 
                std_dev, 
                cloud_cover, 
                calculation_date
            ) VALUES (
                :index_name, 
                :min_value, 
                :max_value, 
                :mean_value, 
                :std_dev, 
                :cloud_cover, 
                :calculation_date
            )
            """),
            {
                'index_name': index_name,
                'min_value': stats['Min'],
                'max_value': stats['Max'],
                'mean_value': stats['Średnia'],
                'std_dev': stats['Odchylenie standardowe'],
                'cloud_cover': cloud_cover,
                'calculation_date': datetime.now()
            }
        )
        conn.commit()


# Wczytywanie danych
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)
time_range = "2024-04-01/2025-04-30"
bbox = [16.8, 51.04, 17.17, 51.21]
search = catalog.search(collections=["sentinel-2-l2a"], bbox=bbox, datetime=time_range)
items = search.item_collection()

selected_item = min(items, key=lambda item: item.properties["eo:cloud_cover"])


# Wczytywanie danego pasma
def load_band(item, band_name, match=None):
    band = rioxarray.open_rasterio(item.assets[band_name].href, overview_level=1).squeeze()
    band = band.astype("float32") / 10000.0
    if match is not None:
        band = band.rio.reproject_match(match)
    return band


# Obliczanie wskaźników
def calc_index(index):
    if index == "NDVI":
        red = load_band(selected_item, "B04")
        nir = load_band(selected_item, "B08")
        ndvi = (nir - red) / (nir + red)
        return ndvi
    elif index == "NDII":
        swir = load_band(selected_item, "B11")
        nir = load_band(selected_item, "B08", match=swir)
        ndii = (nir - swir) / (nir + swir)
        return ndii
    elif index == "NDBI":
        swir = load_band(selected_item, "B11")
        nir = load_band(selected_item, "B08", match=swir)
        ndbi = (swir - nir) / (swir + nir)
        return ndbi
    elif index == "NDWI":
        green = load_band(selected_item, "B03")
        nir = load_band(selected_item, "B08")
        ndwi = (green - nir) / (green + nir)
        return ndwi


# STREAMLIT UI
st.title("Wizualizacja wskaźników")

index = st.selectbox("Wybierz wskaźnik", ["NDVI", "NDII", "NDBI", "NDWI"])
index_data = calc_index(index)
cmap = st.selectbox("Mapa kolorów", ["RdYlGn", "coolwarm", "RdGy", "CMRmap"])

# Obliczanie statystyk
clean_data = index_data.where(~np.isnan(index_data))
stats = {
    "Min": float(np.nanmin(clean_data)),
    "Max": float(np.nanmax(clean_data)),
    "Średnia": float(np.nanmean(clean_data)),
    "Odchylenie standardowe": float(np.nanstd(clean_data))
}

# Wyświetlanie statystyk
with st.expander("Statystyki wskaźnika"):
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Minimalna wartość", f"{stats['Min']:.4f}")
        st.metric("Maksymalna wartość", f"{stats['Max']:.4f}")
    with col2:
        st.metric("Średnia wartość", f"{stats['Średnia']:.4f}")
        st.metric("Odchylenie standardowe", f"{stats['Odchylenie standardowe']:.4f}")

# Przycisk do zapisu statystyk
if st.button("Zapisz statystyki do bazy danych"):
    try:
        save_stats_to_db(
            index_name=index,
            stats=stats,
            cloud_cover=selected_item.properties["eo:cloud_cover"]
        )
        st.success("Statystyki zostały zapisane do bazy danych!")
    except Exception as e:
        st.error(f"Błąd podczas zapisywania do bazy danych: {e}")

# Wizualizacja
blob_name = f"{index}_{cmap}.tif"
raster = blob_read(blob_name)

fig, ax = plt.subplots(figsize=(8, 6))
im = ax.imshow(raster, cmap=cmap)
plt.colorbar(im, ax=ax, label=index)
ax.axis("off")
st.pyplot(fig)