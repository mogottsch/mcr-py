use log::info;
use pyo3::prelude::*;
use pyo3_log::{Caching, Logger};
use rs::graph_cache::GraphCache;
use rs::mlc_adapter::{run_mlc, run_mlc_with_bags, run_mlc_with_node_and_time, PyLabel};

mod rs;

#[pyfunction]
fn log_something() {
    info!("Something!");
}

#[pymodule]
fn mcr_py(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    let _ = Logger::new(_py, Caching::LoggersAndLevels)?.install();

    m.add_function(wrap_pyfunction!(run_mlc, m)?)?;
    m.add_function(wrap_pyfunction!(log_something, m)?)?;
    m.add_function(wrap_pyfunction!(run_mlc_with_bags, m)?)?;
    m.add_function(wrap_pyfunction!(run_mlc_with_node_and_time, m)?)?;

    m.add_class::<GraphCache>()?;
    m.add_class::<PyLabel>()?;
    Ok(())
}
