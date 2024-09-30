# DelayedArray backends for TileDB

|Build|Status|
|-----|----|
| Bioc-release | [![](http://bioconductor.org/shields/build/release/bioc/TileDBArray.svg)](http://bioconductor.org/checkResults/release/bioc-LATEST/TileDBArray) |
| Bioc-devel   | [![](http://bioconductor.org/shields/build/devel/bioc/TileDBArray.svg)](http://bioconductor.org/checkResults/devel/bioc-LATEST/TileDBArray) | 

This package implements [**DelayedArray**](https://github.com/Bioconductor/DelayedArray) backend 
for [TileDB](https://tiledb.com/) to read, write and store dense and sparse arrays.
The resulting `TileDBArray` objects are directly compatible with any Bioconductor package that accepts `DelayedArray` objects,
serving as a swap-in replacement for the predominant [`HDF5Array`](https://github.com/Bioconductor/HDF5Array)
that is currently used throughout the Bioconductor ecosystem for representing large datasets.
See the [official Bioconductor landing page](https://bioconductor.org/packages/devel/bioc/html/TileDBArray.html) for more details.
