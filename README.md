# DelayedArray backends for TileDB

This package implements [**DelayedArray**](https://github.com/Bioconductor/DelayedArray) backend 
for [TileDB](https://tiledb.com/) to read, write and store dense and sparse arrays.
The resulting `TileDBArray` objects are directly compatible with any Bioconductor package that accepts `DelayedArray` objects,
serving as a swap-in replacement for the predominant [`HDF5Array`](https://github.com/Bioconductor/HDF5Array)
that is currently used throughout the Bioconductor ecosystem for representing large datasets.

To install, we suggest:

- Installing the [R interface to TileDB](https://github.com/TileDB-Inc/TileDB-R/);
this package has been tested with **tiledb** version 0.6.0.
- Installing the development version of the **DelayedArray** framework, 
which is as simple as `BiocManager::install("DelayedArray", version="devel")` on R 4.0.0.
- Installing this package with `BiocManager::install("LTLA/TileDBArray")`.

See the vignettes and documentation for usage examples.
