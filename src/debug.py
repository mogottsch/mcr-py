# add . to module name
# import sys
# sys.path.append("./src/")

from typing import Optional
from package import storage, strtime
from package.raptor.example_labels import ActivityDurationLabel
from package.structs import build
from package.raptor.mcraptor import McRaptor
from package.raptor import bag
from typing_extensions import Self

from package.tracer.tracer import TraceFootpath, TraceStart, TraceTrip

footpaths_dict = storage.read_any_dict("../data/footpaths.pkl")
footpaths_dict = footpaths_dict["footpaths"]

structs_dict = storage.read_any_dict("../data/structs.pkl")
build.validate_structs_dict(structs_dict)


label_class = ActivityDurationLabel

mcraptor = McRaptor(structs_dict, footpaths_dict, 2, 60, {}, {}, label_class)
bags = mcraptor.run("818", "", "15:00:00")
print(bags["825"])
print("success")
