use log::info;
use mlc::bag::{Weight, WeightsTuple};
use petgraph::{graph::NodeIndex, Directed, Graph};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

#[pyclass]
pub struct GraphCache {
    pub graph: Option<Arc<MLCGraph>>,
}

type MLCGraph = Graph<(), WeightsTuple, Directed>;

#[pymethods]
impl GraphCache {
    #[new]
    fn new() -> Self {
        GraphCache { graph: None }
    }

    fn set_graph(&mut self, raw_edges: Vec<HashMap<&str, &PyAny>>) {
        let graph = parse_graph(raw_edges);
        self.graph = Some(Arc::new(graph));
    }

    fn summary(&self) -> PyResult<()> {
        if let Some(graph) = &self.graph {
            info!("Nodes: {}", graph.node_count());
            info!("Edges: {}", graph.edge_count());
            Ok(())
        } else {
            Err(PyValueError::new_err("Graph not set"))
        }
    }

    fn validate_node_id(&self, node_id: usize) -> PyResult<()> {
        if let Some(graph) = &self.graph {
            if node_id < graph.node_count() {
                Ok(())
            } else {
                Err(PyValueError::new_err(format!(
                    "Node id {} is not in graph",
                    node_id
                )))
            }
        } else {
            Err(PyValueError::new_err("Graph not set"))
        }
    }

    fn get_edge_weights(&self, start_node_id: usize, end_node_id: usize) -> PyResult<Vec<u64>> {
        if let Some(graph) = &self.graph {
            let edge = graph
                .find_edge(NodeIndex::new(start_node_id), NodeIndex::new(end_node_id))
                .ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "Edge ({}, {}) not found",
                        start_node_id, end_node_id
                    ))
                })?;
            let weights = graph.edge_weight(edge).unwrap().weights.clone();
            Ok(weights)
        } else {
            Err(PyValueError::new_err("Graph not set"))
        }
    }
}

fn parse_graph(raw_edges: Vec<HashMap<&str, &PyAny>>) -> MLCGraph {
    Graph::<(), WeightsTuple, Directed>::from_edges(raw_edges.iter().map(|edge| {
        let u = edge.get("u").unwrap().extract::<usize>().unwrap();
        let v = edge.get("v").unwrap().extract::<usize>().unwrap();
        // // wait 0.02 seconds
        // std::thread::sleep(std::time::Duration::from_millis(20));
        let weights: Vec<Weight> =
            parse_weights(edge.get("weights").expect("weights not found")).unwrap();

        let hidden_weights: Vec<Weight> = edge
            .get("hidden_weights")
            .map(parse_weights)
            .transpose()
            .unwrap()
            .unwrap_or(vec![]);

        let weights_tuple = WeightsTuple {
            weights,
            hidden_weights,
        };
        (NodeIndex::new(u), NodeIndex::new(v), weights_tuple)
    }))
}

fn parse_weights(raw_weights: &&PyAny) -> Result<Vec<u64>, String> {
    let raw_weights = raw_weights
        .extract::<String>()
        .map_err(|_| "Failed to extract string".to_string())?;
    // remove first and last character (brackets)
    let raw_weights = &raw_weights[1..raw_weights.len() - 1];
    let weights: Result<Vec<u64>, _> = raw_weights
        .split(",")
        .map(|x| {
            x.parse::<u64>()
                .map_err(|_| format!("Failed to parse weight: {}", x))
        })
        .collect();

    weights.map_err(|e| e.to_string())
}
