from package.geometa import GeoMeta
from package.logger import Timed
from package.mcr.data import NetworkType, OSMData
from package.mcr.steps.bicycle import BicycleStepBuilder
from package.mcr.steps.car import PersonalCarStepBuilder
from package.mcr.steps.public_transport import PublicTransportStepBuilder
from package.mcr.steps.walking import WalkingStepBuilder
from package.minute_city import minute_city

CAR_CONFIG = "car"
BICYCLE_AND_PUBLIC_TRANSPORT_CONFIG = "bicycle_public_transport"
BICYCLE_CONFIG = "bicycle"
WALKING_CONFIG = "walking"
PUBLIC_TRANSPORT_CONFIG = "public_transport"

ALL_CONFIGS = [
    WALKING_CONFIG,
    BICYCLE_CONFIG,
    PUBLIC_TRANSPORT_CONFIG,
    CAR_CONFIG,
    BICYCLE_AND_PUBLIC_TRANSPORT_CONFIG,
]


def get_car_only_config_with_data(geo_meta, geo_data):
    with Timed.info("Fetching POI for runtime optimization"):
        pois = minute_city.fetch_pois_for_area(geo_meta.boundary, geo_data.osm_nodes)  # type: ignore

    driving_nodes, driving_edges, _ = geo_data.additional_networks[NetworkType.DRIVING]
    car_step = PersonalCarStepBuilder(
        geo_data.osm_nodes,  # type: ignore
        geo_data.osm_edges,  # type: ignore
        driving_nodes,  # type: ignore
        driving_edges,  # type: ignore
        pois,
    )

    initial_steps = []
    repeating_steps = [[car_step]]
    return initial_steps, repeating_steps


def get_bicycle_public_transport_config_with_data(
    geo_meta,
    geo_data,
    bicycle_price_function: str,
    bicycle_location_path: str,
    structs_path: str,
    stops_path: str,
):
    with Timed.info("Fetching POI for runtime optimization"):
        pois = minute_city.fetch_pois_for_area(geo_meta.boundary, geo_data.osm_nodes)  # type: ignore

    cycling_nodes, cycling_edges, _ = geo_data.additional_networks[NetworkType.CYCLING]
    bicycle_step = BicycleStepBuilder(
        bicycle_price_function,
        bicycle_location_path,
        geo_meta,
        geo_data.osm_nodes,  # type: ignore
        geo_data.osm_edges,  # type: ignore
        cycling_nodes,  # type: ignore
        cycling_edges,  # type: ignore
        pois,
    )

    public_transport_step = PublicTransportStepBuilder(
        structs_path,
        stops_path,
        geo_data.nxgraph,
    )

    walking_step = WalkingStepBuilder(
        geo_data.osm_nodes,
        geo_data.osm_edges,
        pois,
    )

    initial_steps = [[walking_step]]
    repeating_steps = [
        [bicycle_step, public_transport_step],
        [walking_step],
    ]

    return initial_steps, repeating_steps


def get_bicycle_only_config_with_data(
    geo_meta,
    geo_data,
    bicycle_price_function: str,
    bicycle_location_path: str,
):
    with Timed.info("Fetching POI for runtime optimization"):
        pois = minute_city.fetch_pois_for_area(geo_meta.boundary, geo_data.osm_nodes)  # type: ignore

    cycling_nodes, cycling_edges, _ = geo_data.additional_networks[NetworkType.CYCLING]
    bicycle_step = BicycleStepBuilder(
        bicycle_price_function,
        bicycle_location_path,
        geo_meta,
        geo_data.osm_nodes,  # type: ignore
        geo_data.osm_edges,  # type: ignore
        cycling_nodes,  # type: ignore
        cycling_edges,  # type: ignore
        pois,
    )

    walking_step = WalkingStepBuilder(
        geo_data.osm_nodes,
        geo_data.osm_edges,
        pois,
    )

    initial_steps = [[walking_step]]
    repeating_steps = [
        [bicycle_step],
        [walking_step],
    ]

    return initial_steps, repeating_steps


def get_walking_only_config_with_data(geo_meta, geo_data):
    with Timed.info("Fetching POI for runtime optimization"):
        pois = minute_city.fetch_pois_for_area(geo_meta.boundary, geo_data.osm_nodes)  # type: ignore

    walking_step = WalkingStepBuilder(
        geo_data.osm_nodes,
        geo_data.osm_edges,
        pois,
    )

    initial_steps = [[walking_step]]
    repeating_steps = []

    return initial_steps, repeating_steps


def get_public_transport_only_config_with_data(
    geo_meta,
    geo_data,
    structs_path: str,
    stops_path: str,
):
    with Timed.info("Fetching POI for runtime optimization"):
        pois = minute_city.fetch_pois_for_area(geo_meta.boundary, geo_data.osm_nodes)  # type: ignore

    walking_step = WalkingStepBuilder(
        geo_data.osm_nodes,
        geo_data.osm_edges,
        pois,
    )
    public_transport_step = PublicTransportStepBuilder(
        structs_path,
        stops_path,
        geo_data.nxgraph,
    )

    initial_steps = [[walking_step]]
    repeating_steps = [[public_transport_step], [walking_step]]

    return initial_steps, repeating_steps

def get_car_only_config(geo_meta_path: str, city_id: str):
    geo_meta = GeoMeta.load(geo_meta_path)
    geo_data = OSMData(geo_meta, city_id, additional_network_types=[NetworkType.DRIVING])
    return get_car_only_config_with_data(geo_meta, geo_data)


def get_bicycle_public_transport_config(
    geo_meta_path: str,
    city_id: str,
    bicycle_price_function: str,
    bicycle_location_path: str,
    structs_path: str,
    stops_path: str,
):
    geo_meta = GeoMeta.load(geo_meta_path)
    geo_data = OSMData(geo_meta, city_id, additional_network_types=[NetworkType.CYCLING])
    return get_bicycle_public_transport_config_with_data(
        geo_meta, geo_data, bicycle_price_function, bicycle_location_path, structs_path, stops_path)


def get_bicycle_only_config(
    geo_meta_path: str,
    city_id: str,
    bicycle_price_function: str,
    bicycle_location_path: str,
):
    geo_meta = GeoMeta.load(geo_meta_path)
    geo_data = OSMData(geo_meta, city_id, additional_network_types=[NetworkType.CYCLING])
    return get_bicycle_only_config_with_data(geo_meta, geo_data, bicycle_price_function, bicycle_location_path)


def get_walking_only_config(geo_meta_path: str, city_id: str):
    geo_meta = GeoMeta.load(geo_meta_path)
    geo_data = OSMData(geo_meta, city_id)
    return get_walking_only_config_with_data(geo_meta, geo_data)


def get_public_transport_only_config(
    geo_meta_path: str,
    city_id: str,
    structs_path: str,
    stops_path: str,
):
    geo_meta = GeoMeta.load(geo_meta_path)
    geo_data = OSMData(geo_meta, city_id)
    return get_public_transport_only_config_with_data(geo_meta, geo_data, structs_path, stops_path)
