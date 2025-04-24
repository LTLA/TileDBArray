# This tests the reading capability of our beloved TileDBArray.
# library(testthat); library(TileDBArray); source("test-write.R")

DI <- matrix(rpois(10000, 5), nrow=200)
DD <- matrix(rnorm(10000), nrow=200)
DL <- DD > 0
DC <- matrix(sample(letters, 10000, replace=TRUE), nrow=200)

SD <- Matrix::rsparsematrix(50, 50, density=0.1)
SL <- SD > 0

ref <- list(DI, DD, DL, DC, SD, SL)

test_that("writing works as expected", {
    for (x in seq_along(ref)) {
        r <- ref[[x]]
        t <- as(r, "TileDBArray")

        expect_equivalent(as.matrix(r), as.matrix(t))
        expect_identical(type(r), type(t))

        # No dimnames yet.
        expect_identical(rownames(r), rownames(t))
        expect_identical(colnames(r), colnames(t))

        # Automatically sparse.
        expect_identical(is_sparse(r), is_sparse(t))
    }
})

test_that("handles and restores row names correctly", {
    Y <- DD
    rownames(Y) <- sprintf("GENE_%i", seq_len(nrow(Y)))
    colnames(Y) <- sprintf("SAMPLE_%i", seq_len(ncol(Y)))

    Z <- as(Y, "TileDBArray")
    expect_identical(dimnames(Y), dimnames(Z))

    # Still the case when pulling from the path.
    AA <- TileDBArray(seed(Z)@path)
    expect_identical(dimnames(Y), dimnames(AA))
})

test_that("can shunt between sparse and non-sparse, regardless of the format", {
    for (x in seq_along(ref)) {
        r <- ref[[x]]

        t <- writeTileDBArray(r, sparse=FALSE)
        expect_identical(is_sparse(t), FALSE)
        expect_equivalent(as.matrix(r), as.matrix(t))

        t <- writeTileDBArray(r, sparse=TRUE)
        expect_identical(is_sparse(t), TRUE)
        expect_equivalent(as.matrix(r), as.matrix(t))
    }
})

test_that("responds to the path", {
    path <- tempfile()
    expect_false(file.exists(path))
    out <- writeTileDBArray(DD, path=path)
    expect_true(file.exists(path))

    # As a global variable.
    path <- tempfile()
    expect_false(file.exists(path))

    setTileDBPath(path)
    out <- as(DD, "TileDBArray")
    expect_true(file.exists(path))

    # Unsets itself properly.
    setTileDBPath()
    expect_null(getTileDBPath())
})

test_that("stores non-unity offsets correctly", {
    path <- tempfile()
    expect_false(file.exists(path))
    out <- writeTileDBArray(DD, path=path, offset=c(-5, 10))
    expect_true(file.exists(path))

    x <- tiledb::tiledb_array(path)
    s <- tiledb::schema(x)
    dims <- tiledb::dimensions(s)
    doms <- lapply(dims, tiledb::domain)
    expect_identical(c(-5L, 10L), vapply(doms, function(x) x[1L], 0L))
})

test_that("other global variables behave as expected", {
    expect_identical(getTileDBExtent(), 100L)
    setTileDBExtent(50L)
    expect_identical(getTileDBExtent(), 50L)
    setTileDBExtent(NULL)
    expect_identical(getTileDBExtent(), 100L)

    expect_identical(getTileDBAttr(), "x")
    setTileDBAttr("blah")
    expect_identical(getTileDBAttr(), "blah")
    setTileDBAttr(NULL)
    expect_identical(getTileDBAttr(), "x")

    default <- getTileDBContext()
    setTileDBContext("blah")
    expect_identical(getTileDBContext(), "blah")
    setTileDBContext(NULL)
    expect_identical(getTileDBContext(), default)
})
