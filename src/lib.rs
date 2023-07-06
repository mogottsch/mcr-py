use bimap::BiMap;
use fast_paths::{FastGraph, InputGraph};
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3_polars::PyDataFrame;
use rayon::prelude::*;
use std::collections::HashMap;

fn doube_series(s: &Series) -> Series {
    s * 2
}

#[pyfunction]
fn double(pydf: PyDataFrame) -> PyDataFrame {
    let mut df: DataFrame = pydf.into();
    // multiple all values by 2
    let df = df.apply("a", doube_series).unwrap();

    PyDataFrame(df.clone())
}

struct Edge {
    u: i64,
    v: i64,
    length: f64,
}

#[pyfunction]
fn query_multiple_one_to_many(
    py: Python,
    raw_edges: Vec<HashMap<&str, &PyAny>>,
    source_target_nodes_map: HashMap<i64, Vec<i64>>,
) -> HashMap<usize, HashMap<usize, f64>> {
    let mut edges = parse_edges(raw_edges);

    let mut translator = NodeIdTranslator::new(&edges);
    translator.edges_to_fast_graph_id(&mut edges);

    let graph = build_graph(edges);

    let source_target_nodes_distance_map: HashMap<usize, HashMap<usize, f64>> =
        py.allow_threads(|| {
            source_target_nodes_map
                .into_iter()
                .collect::<Vec<(i64, Vec<i64>)>>()
                .par_iter()
                .map(|(source, targets)| {
                    let source = translator.to_fast_graph_id(*source);
                    let targets = targets
                        .into_iter()
                        .map(|target| translator.to_fast_graph_id(*target))
                        .collect::<Vec<i64>>();

                    let mut path_calculator = fast_paths::create_calculator(&graph);

                    let distances = targets
                        .iter()
                        .map(|target| {
                            let target = *target;
                            let path = path_calculator.calc_path(
                                &graph,
                                source.try_into().unwrap(),
                                target.try_into().unwrap(),
                            );

                            let distance = path
                                .as_ref()
                                .map(|path| path.get_weight() as f64 / WEIGHT_PRECISION_MULTIPLIER)
                                .unwrap_or(std::f64::INFINITY);

                            (
                                translator
                                    .to_original_id(target.try_into().unwrap())
                                    .try_into()
                                    .unwrap(),
                                distance,
                            )
                        })
                        .collect::<HashMap<usize, f64>>();

                    (
                        translator
                            .to_original_id(source.try_into().unwrap())
                            .try_into()
                            .unwrap(),
                        distances,
                    )
                })
                .collect()
        });

    source_target_nodes_distance_map
}

fn parse_edges(edges: Vec<HashMap<&str, &PyAny>>) -> Vec<Edge> {
    edges
        .into_iter()
        .map(|edge| Edge {
            u: edge.get("u").unwrap().extract().unwrap(),
            v: edge.get("v").unwrap().extract().unwrap(),
            length: edge.get("length").unwrap().extract().unwrap(),
        })
        .collect::<Vec<Edge>>()
}

struct NodeIdTranslator {
    node_ids: BiMap<i64, i64>,
}

impl NodeIdTranslator {
    fn new(edges: &Vec<Edge>) -> NodeIdTranslator {
        NodeIdTranslator {
            node_ids: BiMap::with_capacity(edges.len()),
        }
    }

    fn edges_to_fast_graph_id(&mut self, edges: &mut Vec<Edge>) {
        let mut id = 0;
        for edge in edges {
            let u = edge.u;
            let v = edge.v;
            if self.node_ids.contains_left(&u) {
                edge.u = self.node_ids.get_by_left(&u).unwrap().clone();
            } else {
                self.node_ids.insert(u, id);
                edge.u = id;
                id += 1;
            }
            if self.node_ids.contains_left(&v) {
                edge.v = self.node_ids.get_by_left(&v).unwrap().clone();
            } else {
                self.node_ids.insert(v, id);
                edge.v = id;
                id += 1;
            }
        }
    }

    fn to_fast_graph_id(&self, id: i64) -> i64 {
        self.node_ids.get_by_left(&id).unwrap().clone()
    }

    fn to_original_id(&self, id: i64) -> i64 {
        self.node_ids.get_by_right(&id).unwrap().clone()
    }
}

fn build_graph(edges: Vec<Edge>) -> FastGraph {
    let mut input_graph = InputGraph::new();

    for edge in edges {
        let u: usize = edge.u.try_into().unwrap();
        let v: usize = edge.v.try_into().unwrap();
        let length: usize = (edge.length * WEIGHT_PRECISION_MULTIPLIER) as usize;
        input_graph.add_edge(u, v, length);
        input_graph.add_edge(v, u, length);
    }
    input_graph.freeze();

    let fast_graph = fast_paths::prepare(&input_graph);
    fast_graph
}

static WEIGHT_PRECISION_MULTIPLIER: f64 = 100.0;

#[pymodule]
fn mcr_py(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(double, m)?)?;
    m.add_function(wrap_pyfunction!(query_multiple_one_to_many, m)?)?;
    Ok(())
}
