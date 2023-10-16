use std::collections::{HashMap, HashSet};

use mlc::{
    bag::{Bag, Label},
    mlc::{Bags, MLC},
};
use pyo3::{prelude::*, types::PyList};

use super::{
    graph_cache::GraphCache,
    label::{next_bike_tariff, next_bike_without_tariff},
};

pub struct PyBags(Bags<usize>);

#[pyclass]
pub struct PyLabel {
    #[pyo3(get)]
    pub values: Vec<u64>,
    #[pyo3(get)]
    pub hidden_values: Vec<u64>,
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
    disable_paths: Option<bool>,
    update_label_func: Option<String>,
    enable_limit: Option<bool>,
) -> PyBags {
    let g = graph_cache.graph.as_ref().unwrap();
    let mut mlc = MLC::new(g).unwrap();
    if let Some(disable_paths) = disable_paths {
        mlc.set_disable_paths(disable_paths);
    }
    let update_label_func = update_label_func.unwrap_or_else(|| "none".to_string());
    let update_label_func = UpdateLabelFunc::from_str(&update_label_func).get_func();
    if let Some(func) = update_label_func {
        mlc.set_update_label_func(func);
    }
    if let Some(enable_limit) = enable_limit {
        mlc.set_enable_limit(enable_limit);
    }
    mlc.set_start_node_with_time(start_node_id, time);
    let bags = mlc.run().unwrap();

    PyBags(bags.clone())
}

#[derive(Debug)]
enum UpdateLabelFunc {
    NextBikeTariff,
    NextBikeNoTariff,
    None,
}

impl UpdateLabelFunc {
    fn from_str(func_name: &str) -> Self {
        match func_name {
            "next_bike_tariff" => UpdateLabelFunc::NextBikeTariff,
            "next_bike_no_tariff" => UpdateLabelFunc::NextBikeNoTariff,
            "none" => UpdateLabelFunc::None,
            _ => panic!("Unknown update label function: {}", func_name),
        }
    }

    fn get_func(&self) -> Option<fn(&Label<usize>, &Label<usize>) -> Label<usize>> {
        match self {
            UpdateLabelFunc::NextBikeTariff => Some(next_bike_tariff),
            UpdateLabelFunc::NextBikeNoTariff => Some(next_bike_without_tariff),
            UpdateLabelFunc::None => None,
        }
    }
}

#[pyfunction]
pub fn run_mlc_with_bags(
    _py: Python,
    graph_cache: &GraphCache,
    bags: HashMap<usize, Vec<&PyAny>>,
    update_label_func: Option<String>,
    disable_paths: Option<bool>,
    enable_limit: Option<bool>,
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
                hidden_values: hidden_values.unwrap_or(vec![]),
                path,
                node_id,
            };
            labels.insert(label);
        }
        converted_bags.insert(*node_id, Bag { labels });
    }

    let g = graph_cache.graph.as_ref().unwrap();
    let mut mlc = MLC::new(g).unwrap();
    if let Some(enable_limit) = enable_limit {
        mlc.set_enable_limit(enable_limit);
    }
    if let Some(disable_paths) = disable_paths {
        mlc.set_disable_paths(disable_paths);
    }
    let update_label_func = update_label_func.unwrap_or_else(|| "none".to_string());
    let update_label_func = UpdateLabelFunc::from_str(&update_label_func).get_func();
    if let Some(func) = update_label_func {
        mlc.set_update_label_func(func);
    }
    mlc.set_bags(converted_bags);

    let bags = mlc.run().unwrap();

    PyBags(bags.clone())
}
