use mlc::{
    bag::Weights,
    mlc::{Bags, MLC},
};
use petgraph::{graph::NodeIndex, Graph};
use polars_core::export::ahash::HashMap;
use pyo3::prelude::*;

#[pyclass]
pub struct PyBags {
    bags: Bags,
}

#[pyfunction]
pub fn run_mlc(_py: Python, raw_edges: Vec<HashMap<&str, &PyAny>>, start_node_id: usize) -> PyBags {
    let g = parse_graph(raw_edges);
    let mlc = MLC::new(g).unwrap();
    let bags = mlc.run(start_node_id);

    PyBags { bags }
}

fn parse_graph(raw_edges: Vec<HashMap<&str, &PyAny>>) -> Graph<u64, Weights> {
    Graph::<u64, Weights>::from_edges(raw_edges.iter().map(|edge| {
        let u = edge.get("u").unwrap().extract::<usize>().unwrap();
        let v = edge.get("v").unwrap().extract::<usize>().unwrap();
        let weights = edge.get("weights").unwrap().extract::<Vec<u64>>().unwrap();
        (NodeIndex::new(u), NodeIndex::new(v), Weights(weights))
    }))
}
