from package.mcr.steps.interface import StepBuilder
from mcr_py import GraphCache
from package.mcr.steps.mlc import MLCStep


class WalkingStep(MLCStep):
    NAME = "walking"


class WalkingStepBuilder(StepBuilder):
    step = WalkingStep

    def __init__(
        self,
        graph_cache: GraphCache,
        to_internal: dict,
        from_internal: dict,
    ):
        self.kwargs = {
            "graph_cache": graph_cache,
            "to_internal": to_internal,
            "from_internal": from_internal,
        }
