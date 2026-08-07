//! The Dictionary-layout code path: matched rows as zero-copy `u32` term-code
//! columns plus a dictionary handle, mirroring the JS bindings' lazy payload
//! (`js/src/store.rs::match_columns`). Python decodes each distinct code once
//! and never materializes per-occurrence term strings.

use std::os::raw::{c_int, c_void};

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PySystemError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use vortex_buffer::Buffer;
use vortex_rdf_core::DictSnapshot;

/// An immutable handle on a store's term dictionary. Decodes term codes to
/// their N-Triples strings; safe to keep across store mutations (the snapshot
/// is frozen at creation).
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct TermDict {
    pub(crate) snapshot: DictSnapshot,
}

impl TermDict {
    /// The GIL-released bulk decode behind [`decode_many`](Self::decode_many)
    /// once the codes are owned. Releasing the GIL needs owned codes: pyo3's
    /// borrowed buffer views (`ReadOnlyCell`) may not cross a GIL release,
    /// and lending Python-owned memory to a detached thread would race a
    /// writable exporter mutated from another Python thread anyway.
    fn decode_owned(&self, py: Python<'_>, codes: Vec<u32>) -> Vec<Option<String>> {
        py.detach(|| codes.into_iter().map(|c| self.snapshot.decode(c)).collect())
    }
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
    ///
    /// `codes` is preferably a u32 buffer (`memoryview(col).cast("I")`,
    /// `array("I", ...)`, a `uint32` NumPy array), read in one bulk copy
    /// with no per-element Python-int conversion. A byte-typed buffer — the
    /// raw view a [`U32Column`] itself exports — is reinterpreted as
    /// native-endian u32s, so a column from `match_codes` passes directly.
    /// Any other sequence of ints still works, at one `PyLong` extraction
    /// per code.
    fn decode_many(
        &self,
        py: Python<'_>,
        codes: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<Option<String>>> {
        if let Ok(buf) = PyBuffer::<u32>::get(codes) {
            return Ok(self.decode_owned(py, buf.to_vec(py)?));
        }
        if let Ok(buf) = PyBuffer::<u8>::get(codes) {
            let bytes = buf.to_vec(py)?;
            if !bytes.len().is_multiple_of(4) {
                return Err(PyValueError::new_err(format!(
                    "byte buffer of {} bytes is not a whole number of u32 codes",
                    bytes.len()
                )));
            }
            let codes = bytes
                .chunks_exact(4)
                .map(|b| u32::from_ne_bytes(b.try_into().expect("chunks_exact(4)")))
                .collect();
            return Ok(self.decode_owned(py, codes));
        }
        Ok(self.decode_owned(py, codes.extract()?))
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
/// directly. The column is read-only and owns (refcounts) its backing buffer.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct U32Column {
    pub(crate) codes: Buffer<u32>,
}

#[pymethods]
impl U32Column {
    fn __len__(&self) -> usize {
        self.codes.len()
    }

    fn __repr__(&self) -> String {
        format!("U32Column(len={})", self.codes.len())
    }

    /// Fills `view` over the raw u32 data; the exported buffer holds a
    /// reference to this object, so the memory outlives every memoryview.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let data = slf.get().codes.as_slice();
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
