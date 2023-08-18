use log::info;
use std::collections::{HashMap, HashSet};

use mlc::{
    bag::{Bag, Label},
    mlc::{Bags, MLC},
};
use pyo3::{prelude::*, types::PyList};

use super::graph_cache::GraphCache;

pub struct PyBags(Bags<usize>);

#[pyclass]
pub struct PyLabel {
    #[pyo3(get)]
    pub values: Vec<u64>,
    #[pyo3(get)]
    pub hidden_values: Option<Vec<u64>>,
    #[pyo3(get)]
    pub path: Vec<usize>,
    #[pyo3(get)]
    pub node_id: usize,
}

impl IntoPy<PyObject> for PyBags {
    fn into_py(self, py: Python) -> PyObject {
        let bags = self.0;
        let mut py_bags = HashMap::new();
        for (node_id, bag) in bags.iter() {
            let py_labels = PyList::empty(py);
            for label in bag.labels.iter() {
                let py_label = PyLabel {
                    values: label.values.clone(),
                    hidden_values: label.hidden_values.clone(),
                    path: label.path.clone(),
                    node_id: label.node_id,
                };
                py_labels.append(py_label.into_py(py)).unwrap();
            }
            py_bags.insert(*node_id, py_labels);
        }
        py_bags.into_py(py)
    }
}

// #[pyfunction]
// pub fn run_mlc(_py: Python, raw_edges: Vec<HashMap<&str, &PyAny>>, start_node_id: usize) -> PyBags {
//     io::stdout().flush().unwrap();
//     let g = parse_graph(raw_edges);
//     let mut mlc = MLC::new(g).unwrap();
//     mlc.set_start_node(start_node_id);
//     let bags = mlc.run().unwrap();
//
//     PyBags(bags.clone())
// }

#[pyfunction]
pub fn run_mlc(_py: Python, graph_cache: &GraphCache, start_node_id: usize) -> PyBags {
    let g = graph_cache.graph.as_ref().unwrap();
    let mut mlc = MLC::new(g).unwrap();
    mlc.set_start_node(start_node_id);
    let bags = mlc.run().unwrap();

    PyBags(bags.clone())
}

#[pyfunction]
pub fn run_mlc_with_node_and_time(
    _py: Python,
    graph_cache: &GraphCache,
    start_node_id: usize,
    time: usize,
) -> PyBags {
    let g = graph_cache.graph.as_ref().unwrap();
    let mut mlc = MLC::new(g).unwrap();
    mlc.set_start_node_with_time(start_node_id, time);
    let bags = mlc.run().unwrap();

    PyBags(bags.clone())
}

#[pyfunction]
pub fn run_mlc_with_bags(
    _py: Python,
    graph_cache: &GraphCache,
    bags: HashMap<usize, Vec<&PyAny>>,
) -> PyBags {
    // convert the PyAny's to Labels
    let mut converted_bags: Bags<usize> = HashMap::new();
    for (node_id, py_labels) in bags.iter() {
        let mut labels = HashSet::new();
        for py_label in py_labels {
            let values = py_label
                .getattr("values")
                .unwrap()
                .extract::<Vec<u64>>()
                .unwrap();
            let hidden_values = py_label
                .getattr("hidden_values")
                .unwrap()
                .extract::<Option<Vec<u64>>>()
                .unwrap();
            let path = py_label
                .getattr("path")
                .unwrap()
                .extract::<Vec<usize>>()
                .unwrap();
            let node_id = py_label
                .getattr("node_id")
                .unwrap()
                .extract::<usize>()
                .unwrap();
            let label = Label {
                values,
                hidden_values,
                path,
                node_id,
            };
            labels.insert(label);
        }
        converted_bags.insert(*node_id, Bag { labels });
    }

    let g = graph_cache.graph.as_ref().unwrap();
    let mut mlc = MLC::new(g).unwrap();
    mlc.set_bags(converted_bags);

    let bags = mlc.run().unwrap();

    PyBags(bags.clone())
}
