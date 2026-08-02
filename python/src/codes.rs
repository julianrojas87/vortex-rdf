//! The Dictionary-layout code path: matched rows as zero-copy `u32` term-code
//! columns plus a dictionary handle, mirroring the JS bindings' lazy payload
//! (`js/src/store.rs::match_columns`). Python decodes each distinct code once
//! and never materializes per-occurrence term strings.

use std::os::raw::{c_int, c_void};

use pyo3::exceptions::PySystemError;
use pyo3::ffi;
use pyo3::prelude::*;
use vortex_array::arrays::PrimitiveArray;
use vortex_rdf_core::DictSnapshot;

/// An immutable handle on a store's term dictionary. Decodes term codes to
/// their N-Triples strings; safe to keep across store mutations (the snapshot
/// is frozen at creation).
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct TermDict {
    pub(crate) snapshot: DictSnapshot,
}

#[pymethods]
impl TermDict {
    /// The N-Triples string for `code`, or `None` when the code is out of
    /// this dictionary's range.
    fn decode(&self, code: u32) -> Option<String> {
        self.snapshot.decode(code)
    }

    /// Decode a batch of codes in one call, releasing the GIL for the whole
    /// batch — the bulk companion to [`decode`](Self::decode) for large
    /// result sets (each FSST-backed decode pays a per-term decompression;
    /// per-code Python calls additionally pay the FFI round-trip and hold
    /// the GIL throughout).
    fn decode_many(&self, py: Python<'_>, codes: Vec<u32>) -> Vec<Option<String>> {
        py.detach(|| codes.into_iter().map(|c| self.snapshot.decode(c)).collect())
    }

    fn __len__(&self) -> usize {
        self.snapshot.len()
    }

    fn __repr__(&self) -> String {
        format!("TermDict(len={})", self.snapshot.len())
    }
}

/// One matched term-code column, exposed to Python zero-copy through the
/// buffer protocol: `memoryview(col).cast("I")` views the Rust memory
/// directly. The column is read-only and owns (refcounts) its backing array.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct U32Column {
    pub(crate) prim: PrimitiveArray,
}

#[pymethods]
impl U32Column {
    fn __len__(&self) -> usize {
        self.prim.len()
    }

    fn __repr__(&self) -> String {
        format!("U32Column(len={})", self.prim.len())
    }

    /// Fills `view` over the raw u32 data; the exported buffer holds a
    /// reference to this object, so the memory outlives every memoryview.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let data = slf.get().prim.as_slice::<u32>();
        let ret = unsafe {
            ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr(),
                data.as_ptr() as *mut c_void,
                std::mem::size_of_val(data) as ffi::Py_ssize_t,
                1, // read-only
                flags,
            )
        };
        if ret != 0 {
            return Err(PyErr::take(slf.py())
                .unwrap_or_else(|| PySystemError::new_err("PyBuffer_FillInfo failed")));
        }
        Ok(())
    }
}
