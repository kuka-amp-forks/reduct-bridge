//! Empty: reduct-bridge is its binary. This target only lets another Cargo
//! workspace depend on the package, so that it resolves reduct-bridge's
//! dependencies with its own and builds the binary (Bazel's `gen_binaries`).
