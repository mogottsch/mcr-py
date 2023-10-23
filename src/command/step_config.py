from package.geometa import GeoMeta
from package.logger import Timed
from package.mcr.data import NetworkType, OSMData
from package.mcr.steps.bicycle import BicycleStepBuilder
from package.mcr.steps.car import PersonalCarStepBuilder
from package.mcr.steps.public_transport import PublicTransportStepBuilder
from package.mcr.steps.walking import WalkingStepBuilder
from package.minute_city import minute_city

CAR_CONFIG = "car"
BICYCLE_AND_PUBLIC_TRANSPORT_CONFIG = "bicycle_+_public_transport"

ALL_CONFIGS = [
    BICYCLE_AND_PUBLIC_TRANSPORT_CONFIG,
    CAR_CONFIG,
]


def get_car_only_config(
    geo_meta_path: str,
    city_id: str,
    start_node_id: int,
):
    geo_meta = GeoMeta.load(geo_meta_path)

    geo_data = OSMData(
        geo_meta,
        city_id,
        additional_network_types=[NetworkType.DRIVING],
    )

    with Timed.info("Fetching POI for runtime optimization"):
        pois = minute_city.fetch_pois_for_area(
            geo_meta.boundary, geo_data.osm_nodes  # type: ignore
        )

    driving_nodes, driving_edges, _ = geo_data.additional_networks[NetworkType.DRIVING]
    car_step = PersonalCarStepBuilder(
        geo_data.osm_nodes,  # type: ignore
        geo_data.osm_edges,  # type: ignore
        driving_nodes,  # type: ignore
        driving_edges,  # type: ignore
        pois,
        start_node_id,
    )

    initial_steps = [[car_step]]
    repeating_steps = []

    return initial_steps, repeating_steps


def get_bicycle_public_transport_config(
    geo_meta_path: str,
    city_id: str,
    bicycle_price_function: str,
    bicycle_location_path: str,
    structs_path: str,
    stops_path: str,
):
    geo_meta = GeoMeta.load(geo_meta_path)
    geo_data = OSMData(
        geo_meta,
        city_id,
        additional_network_types=[NetworkType.CYCLING],
    )

    with Timed.info("Fetching POI for runtime optimization"):
        pois = minute_city.fetch_pois_for_area(
            geo_meta.boundary, geo_data.osm_nodes  # type: ignore
        )

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
