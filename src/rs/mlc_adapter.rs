use std::collections::HashMap;

use mlc::mlc::{Bags, MLC};
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
