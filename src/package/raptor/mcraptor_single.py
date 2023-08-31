from copy import deepcopy
from typing import Generic, Tuple
from typing_extensions import Any, Self

from package.key import S, T
from package.logger import rlog
from package.raptor.bag import L, Bag, RouteBag
from package.raptor.data import DataQuerier


class McRaptorSingle(Generic[L, S, T]):
    def __init__(
        self,
        structs_dict: dict,
        default_transfer_time: int,
        label_class: type[L],
    ):
        self.dq = DataQuerier(
            structs_dict,
            footpaths=None,
        )

        self.default_transfer_time = default_transfer_time

        self.label_class = label_class

    def run(
        self,
        bags: dict[str, Bag],
    ) -> dict[str, Bag]:
        output_bags, marked_stops = self.init_vars(bags)

        Q = self.collect_Q(marked_stops)

        output_bags, marked_stops = self.process_routes(Q, bags, output_bags)

        if len(marked_stops) == 0:
            rlog.info("No updates")
        return output_bags

    def init_vars(
        self,
        bags: dict[str, Bag],
    ) -> Tuple[dict[str, Bag], set[str]]:
        n_missing_stops = 0
        for stop_id in self.dq.stop_id_set:
            if stop_id not in bags:
                bags[stop_id] = Bag()
                n_missing_stops += 1
        rlog.info(f"Added {n_missing_stops} missing stops to bags ({n_missing_stops / len(self.dq.stop_id_set) * 100:.2f}%)")

        output_bags = deepcopy(bags)

        marked_stops = set(bags.keys())

        return output_bags, marked_stops

    def collect_Q(
        self: Self,
        marked_stops: set[str],
    ) -> dict[str, tuple[str, int]]:
        Q: dict[str, tuple[str, int]] = {}
        for stop_id in marked_stops:
            for route_id in self.dq.get_routes_serving_stop(stop_id):
                idx = self.dq.get_idx_of_stop_in_route(stop_id, route_id)
                if route_id not in Q:
                    Q[route_id] = (stop_id, idx)
                    continue

                # if our stop is closer to the start than the existing one, we replace it
                _, existing_idx = Q[route_id]
                if idx < existing_idx:
                    Q[route_id] = (stop_id, idx)
        return Q

    def process_routes(
        self: Self,
        Q: dict[str, tuple[str, int]],
        bags: dict[str, Bag],
        output_bags: dict[str, Bag],
    ) -> tuple[dict[str, Bag], set[str]]:
        marked_stops = set()
        for route_id, (stop_id, idx) in Q.items():
            route_bag = RouteBag[L, S, T](
                self.dq,
            )

            for stop_id in self.dq.iterate_stops_in_route_from_idx(route_id, idx):
                output_bags, marked_stops, route_bag = self.process_route(
                    route_id,
                    stop_id,
                    bags,
                    output_bags,
                    route_bag,
                    marked_stops,
                )
        return output_bags, marked_stops

    def process_route(
        self,
        route_id: str,
        stop_id: str,
        bags: dict[str, Bag],
        output_bags: dict[str, Bag],
        route_bag: RouteBag,
        marked_stops: set[str],
    ) -> tuple[dict[str, Bag], set[str], RouteBag]:
        # first step - update arrival times in route bag
        route_bag.update_along_trip(stop_id)

        # second step - merge route_bag into stop_bag
        output_stop_bag = output_bags[stop_id]
        is_any_added = output_stop_bag.merge(
            route_bag.to_bag().update_before_stop_bag_merge(stop_id),
        )
        if is_any_added:
            marked_stops.add(stop_id)

        stop_bag = bags[stop_id]
        # third step - merge stop_bag into route_bag
        self.merge_bag_into_route_bag(
            route_bag,
            stop_bag,
            route_id,
            stop_id,
        )
        return output_bags, marked_stops, route_bag

    # TODO: should this be moved into RouteBag?
    def merge_bag_into_route_bag(
        self,
        route_bag: RouteBag,
        bag: Bag,
        route_id: str,
        stop_id: str,
    ):
        for label in bag:
            res = self.dq.earliest_trip(
                route_id,
                stop_id,
                label.arrival_time + self.default_transfer_time,
            )
            if res is None:
                continue
            trip, departure_time = res
            label = label.copy()
            label.update_before_route_bag_merge(departure_time, stop_id)
            route_bag.add_if_necessary(label, trip)


def bags_to_human_readable(bags: dict[str, Bag]) -> dict[str, Any]:
    return {stop_id: bag.to_human_readable() for stop_id, bag in bags.items()}
