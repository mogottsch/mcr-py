use log::info;
use mlc::bag::{Weight, WeightsTuple};
use petgraph::{graph::NodeIndex, Graph, Undirected};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

#[pyclass]
pub struct GraphCache {
    pub graph: Option<Arc<MLCGraph>>,
}

type MLCGraph = Graph<(), WeightsTuple, Undirected>;

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
}

fn parse_graph(raw_edges: Vec<HashMap<&str, &PyAny>>) -> MLCGraph {
    Graph::<(), WeightsTuple, Undirected>::from_edges(raw_edges.iter().map(|edge| {
        let u = edge.get("u").unwrap().extract::<usize>().unwrap();
        let v = edge.get("v").unwrap().extract::<usize>().unwrap();
        // // wait 0.02 seconds
        // std::thread::sleep(std::time::Duration::from_millis(20));
        let weights: Vec<Weight> = parse_weights(edge.get("weights").expect("weights not found"));

        let hidden_weights: Option<Vec<Weight>> = edge.get("hidden_weights").map(parse_weights);

        let weights_tuple = WeightsTuple {
            weights,
            hidden_weights,
        };
        (NodeIndex::new(u), NodeIndex::new(v), weights_tuple)
    }))
}

fn parse_weights(raw_weights: &&PyAny) -> Vec<Weight> {
    let raw_weights = raw_weights.extract::<String>().unwrap();
    // remove first and last character (brackets)
    let raw_weights = &raw_weights[1..raw_weights.len() - 1];
    let weights = raw_weights
        .split(", ")
        .map(|x| x.parse::<u64>().unwrap())
        .collect::<Vec<u64>>();
    weights.into()
}
