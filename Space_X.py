import requests
import pandas as pd
import numpy as np
import datetime

# Mostrar todas las columnas y el contenido completo
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# Función para obtener el nombre del cohete (booster version) desde la API de SpaceX
def getBoosterVersion(data):
    booster_versions = []
    for rocket_id in data['rocket']:
        if rocket_id:
            try:
                response = requests.get(f"https://api.spacexdata.com/v4/rockets/{rocket_id}")
                response.raise_for_status()  # Lanza error si la respuesta no es 200
                rocket_info = response.json()
                booster_versions.append(rocket_info.get('name', 'Unknown'))
            except (requests.exceptions.RequestException, KeyError) as e:
                print(f"Error al obtener datos del cohete {rocket_id}: {e}")
                booster_versions.append('Unknown')
        else:
            booster_versions.append('Unknown')
    return booster_versions

def getLaunchSite(data):
    longitudes = []
    latitudes = []
    launch_sites = []

    for pad_id in data['launchpad']:
        if pad_id:
            try:
                response = requests.get(f"https://api.spacexdata.com/v4/launchpads/{pad_id}")
                response.raise_for_status()
                site_info = response.json()
                longitudes.append(site_info.get('longitude', None))
                latitudes.append(site_info.get('latitude', None))
                launch_sites.append(site_info.get('name', 'Unknown'))
            except (requests.exceptions.RequestException, KeyError) as e:
                print(f"Error con el launchpad {pad_id}: {e}")
                longitudes.append(None)
                latitudes.append(None)
                launch_sites.append('Unknown')
        else:
            longitudes.append(None)
            latitudes.append(None)
            launch_sites.append('Unknown')

    return longitudes, latitudes, launch_sites

def getPayloadData(data):
    payload_masses = []
    orbits = []

    for payload_id in data['payloads']:
        if payload_id:
            try:
                response = requests.get(f"https://api.spacexdata.com/v4/payloads/{payload_id}")
                response.raise_for_status()
                payload_info = response.json()
                payload_masses.append(payload_info.get('mass_kg', None))
                orbits.append(payload_info.get('orbit', 'Unknown'))
            except (requests.exceptions.RequestException, KeyError) as e:
                print(f"Error al obtener datos del payload {payload_id}: {e}")
                payload_masses.append(None)
                orbits.append('Unknown')
        else:
            payload_masses.append(None)
            orbits.append('Unknown')

    return payload_masses, orbits

def getCoreData(data):
    block = []
    reused_count = []
    serial = []
    outcome = []
    flights = []
    gridfins = []
    reused = []
    legs = []
    landing_pad = []

    for core in data['cores']:
        if core['core'] is not None:
            try:
                response = requests.get(f"https://api.spacexdata.com/v4/cores/{core['core']}")
                response.raise_for_status()
                core_info = response.json()
                block.append(core_info.get('block', None))
                reused_count.append(core_info.get('reuse_count', None))
                serial.append(core_info.get('serial', None))
            except (requests.exceptions.RequestException, KeyError) as e:
                print(f"Error con el core {core['core']}: {e}")
                block.append(None)
                reused_count.append(None)
                serial.append(None)
        else:
            block.append(None)
            reused_count.append(None)
            serial.append(None)

        outcome.append(f"{core.get('landing_success', None)} {core.get('landing_type', None)}")
        flights.append(core.get('flight', None))
        gridfins.append(core.get('gridfins', None))
        reused.append(core.get('reused', None))
        legs.append(core.get('legs', None))
        landing_pad.append(core.get('landpad', None))

    return {
        'Block': block,
        'ReusedCount': reused_count,
        'Serial': serial,
        'Outcome': outcome,
        'Flights': flights,
        'GridFins': gridfins,
        'Reused': reused,
        'Legs': legs,
        'LandingPad': landing_pad
    }

# Obtener los datos en vivo desde la API
spacex_url = "https://api.spacexdata.com/v4/launches/past"
response = requests.get(spacex_url)
data = response.json()
df = pd.json_normalize(data)
print(df.head())  # Muestra las primeras 5 filas

# Cargar archivo estático como respaldo (opcional)
static_json_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DS0321EN-SkillsNetwork/datasets/API_call_spacex_api.json'
static_response = requests.get(static_json_url)
static_data = static_response.json()
df_static = pd.json_normalize(static_data)
print(df_static.head())  # También muestra las primeras 5 del respaldo

# Subconjunto con columnas relevantes
df = df[['rocket', 'payloads', 'launchpad', 'cores', 'flight_number', 'date_utc']]

# Eliminar filas con múltiples cores o payloads
df = df[df['cores'].map(len) == 1]
df = df[df['payloads'].map(len) == 1]

# Extraer el único valor de cada lista
df['cores'] = df['cores'].map(lambda x: x[0])
df['payloads'] = df['payloads'].map(lambda x: x[0])

# Convertir la fecha a datetime y luego extraer solo la fecha
df['date'] = pd.to_datetime(df['date_utc']).dt.date

# Filtrar por fecha máxima
df = df[df['date'] <= datetime.date(2020, 11, 13)]

## Variables globales
BoosterVersion = []
PayloadMass = []
Orbit = []
LaunchSite = []
Outcome = []
Flights = []
GridFins = []
Reused = []
Legs = []
LandingPad = []
Block = []
ReusedCount = []
Serial = []
Longitude = []
Latitude = []

BoosterVersion = getBoosterVersion(df)
BoosterVersion[0:5]
Longitude, Latitude, LaunchSite = getLaunchSite(df)
PayloadMass, Orbit = getPayloadData(df)
core_data = getCoreData(df)


# Asignar los valores obtenidos del diccionario
Block = core_data['Block']
ReusedCount = core_data['ReusedCount']
Serial = core_data['Serial']
Outcome = core_data['Outcome']
Flights = core_data['Flights']
GridFins = core_data['GridFins']
Reused = core_data['Reused']
Legs = core_data['Legs']
LandingPad = core_data['LandingPad']

# Verificar tamaños
print("Tamaño esperado:", len(df))
print("BoosterVersion:", len(BoosterVersion))
print("PayloadMass:", len(PayloadMass))
print("Orbit:", len(Orbit))
print("LaunchSite:", len(LaunchSite))  # Este incluye nombres de sitios, no longitudes o latitudes
print("Outcome:", len(Outcome))
print("Flights:", len(Flights))
print("GridFins:", len(GridFins))
print("Reused:", len(Reused))
print("Legs:", len(Legs))
print("LandingPad:", len(LandingPad))
print("Block:", len(Block))
print("ReusedCount:", len(ReusedCount))
print("Serial:", len(Serial))
print("Longitude:", len(Longitude))
print("Latitude:", len(Latitude))

launch_dict = {
    'FlightNumber': list(df['flight_number']),
    'Date': list(df['date']),
    'BoosterVersion': BoosterVersion,
    'PayloadMass': PayloadMass,
    'Orbit': Orbit,
    'LaunchSite': LaunchSite,
    'Outcome': Outcome,
    'Flights': Flights,
    'GridFins': GridFins,
    'Reused': Reused,
    'Legs': Legs,
    'LandingPad': LandingPad,
    'Block': Block,
    'ReusedCount': ReusedCount,
    'Serial': Serial,
    'Longitude': Longitude,
    'Latitude': Latitude
}

launch_df = pd.DataFrame(launch_dict)

# Separar el campo 'Outcome'
launch_df[['LandingSuccess', 'LandingType']] = launch_df['Outcome'].str.split(' ', expand=True)

print(launch_df.info())
print("\nResumen estadístico:\n")
print(launch_df.describe(include='all'))

# Filtrar solo los lanzamientos del Falcon 9
data_falcon9 = launch_df[launch_df['BoosterVersion'] == 'Falcon 9'].reset_index(drop=True)

# Mostrar primeras filas para confirmar
print(data_falcon9.head())

# Revisar datos faltantes
print(launch_df.isnull().sum())

# Vista previa
print(launch_df.head())
