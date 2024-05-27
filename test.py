import folium
import pandas as pd
import base64
from folium.features import DivIcon

data = pd.read_csv('C:\\Users\\Mannan\\Downloads\\test2.csv')

delhi_map = folium.Map(location=[28.6139, 77.2090], zoom_start=11)

# Create a custom marker icon with AQI value inside and color based on AQI level
def create_marker_icon(aqi):
    if aqi is None:
        return None
    aqi_rounded = int(round(aqi))  # Round off AQI value and convert to integer
    if aqi_rounded <= 50:
        color = '#34A12B'  # Good AQI
    elif aqi_rounded <= 100:
        color = '#D4CC0F'  # Moderate AQI
    elif aqi_rounded <= 150:
        color = '#FFA500'  # Unhealthy for Sensitive Groups AQI
    elif aqi_rounded <= 200:
        color = '#9A3C3D'  # Unhealthy AQI
    elif aqi_rounded <= 300:
        color = '#800080'  # Very Unhealthy AQI
    else:
        color = '#800000'  # Hazardous AQI

    # Create an SVG icon with AQI value inside and colored marker
    svg = f'''<svg width="33" height="44" viewBox="0 0 35 45" xmlns="http://www.w3.org/2000/svg"><path d="M28.205 3.217H6.777c-2.367 0-4.286 1.87-4.286 4.179v19.847c0 2.308 1.919 4.179 4.286 4.179h5.357l5.337 13.58 5.377-13.58h5.357c2.366 0 4.285-1.87 4.285-4.179V7.396c0-2.308-1.919-4.179-4.285-4.179" fill="{color}"/><g opacity=".15" transform="matrix(1.0714 0 0 -1.0714 -233.22 146.783)"><path d="M244 134h-20c-2.209 0-4-1.746-4-3.9v-18.525c0-2.154 1.791-3.9 4-3.9h5L233.982 95 239 107.675h5c2.209 0 4 1.746 4 3.9V130.1c0 2.154-1.791 3.9-4 3.9m0-1c1.654 0 3-1.301 3-2.9v-18.525c0-1.599-1.346-2.9-3-2.9h-5.68l-.25-.632-4.084-10.318-4.055 10.316-.249.634H224c-1.654 0-3 1.301-3 2.9V130.1c0 1.599 1.346 2.9 3 2.9h20" fill="#231f20"/></g>
    <text x="50%" y="40%" font-size="18" fill="white" font-weight="bold" text-anchor="middle" dominant-baseline="middle">{aqi_rounded}</text>
    </svg>'''
    svg_base64 = base64.b64encode(svg.encode()).decode()
    return f"data:image/svg+xml;base64,{svg_base64}"

for index, row in data.iterrows():
    pm25_concentration = row['pollutant_id']
    pm10_concentration = row['pollutant_max']

    aqi = calculate_aqi(pm25_concentration, pm10_concentration)
    marker_icon = create_marker_icon(aqi)
    if marker_icon is not None:
        marker = folium.Marker(location=[row['latitude'], row['longitude']], icon=folium.CustomIcon(marker_icon), popup=f"{row['station']}: AQI {int(round(aqi))}")
        marker.add_to(delhi_map)

delhi_map.save("C:\\Users\\Mannan\\Downloads\\delhi_aqi_map1.html")