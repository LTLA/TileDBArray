# This tests the reading capability of our beloved TileDBArray.
# library(testthat); library(TileDBArray); source("test-read.R")

DI <- matrix(rpois(10000, 5), nrow=200)
XI <- as(DI, "TileDBArray")

DD <- matrix(rnorm(10000), nrow=200)
XD <- as(DD, "TileDBArray")

DL <- DD > 0
XL <- as(DL, "TileDBArray")

SD <- Matrix::rsparsematrix(50, 50, density=0.1)
YD <- as(SD, "TileDBArray")

SL <- SD > 0
YL <- as(SL, "TileDBArray")

tdb <- list(XI, XD, XL, YD, YL)
ref <- list(DI, DD, DL, SD, SL)

test_that("basic read operations work correctly ", {
    for (x in seq_along(tdb)) {
        r <- ref[[x]]
        t <- tdb[[x]]

        expect_identical(dim(r), dim(t))
        expect_identical(type(r), type(t))
    }
})

test_that("subsetting works correctly ", {
    for (x in seq_along(tdb)) {
        r <- ref[[x]]
        t <- tdb[[x]]

        for (i in sample(nrow(r), 10)) {
            expect_identical(r[i,] , t[i,])
        }

        for (j in sample(ncol(r), 10)) {
            expect_identical(r[,j] , t[,j])
        }
    }
})

test_that("matrix subset extraction works correctly ", {
    for (x in seq_along(tdb)) {
        r <- ref[[x]]
        t <- tdb[[x]]

        i <- sample(nrow(r), 20, replace=TRUE)
        j <- sample(ncol(r), 20, replace=TRUE)

        expect_equivalent(as.matrix(r[i,]), as.matrix(t[i,]))
        expect_equivalent(as.matrix(r[,j]), as.matrix(t[,j]))
        expect_equivalent(as.matrix(r[i,j]), as.matrix(t[i,j]))
    }
})

test_that("more complex matrix operations work correctly", {
    for (x in seq_along(tdb)) {
        r <- ref[[x]]
        t <- tdb[[x]]

        expect_equal(colSums(r), colSums(t))
        expect_equal(rowSums(r), rowSums(t))

        v <- matrix(rnorm(ncol(r)*2), ncol=2)
        expect_equivalent(as.matrix(r %*% v), as.matrix(t %*% v))

        expect_equivalent(as.matrix(t(r)), as.matrix(t(t)))
    }
})

test_that("read operations work correctly with a non-zero offset", {
    XI <- writeTileDBArray(DI, offset=c(5L, -5L))
    expect_equivalent(as.matrix(XI), as.matrix(DI))

    YD <- writeTileDBArray(SD, offset=c(-10L, 10L))
    expect_equivalent(as.matrix(YD), as.matrix(SD))
})

test_that("extract_array works in a wide variety of scenarios", {
    XI <- writeTileDBArray(DI, offset=c(5L, -5L))
    YD <- writeTileDBArray(SD, offset=c(-10L, 10L))
    tests <- list(
        list(mat=DI, ref=XI),
        list(mat=YD, ref=SD)
    )

    for (x in tests) {
        NR <- nrow(x$ref)
        NC <- ncol(x$ref)

        for (i in 1:7) {
            if (i == 1L) {
                indices <- list(NULL, NULL)
            } else if (i == 2L) {
                indices <- list(NULL, 1:NC) # sorted, unique
            } else if (i == 3L) {
                indices <- list(sample(NR), NULL) # unsorted, unique
            } else if (i == 4L) {
                indices <- list(rep(1:NR, each=2L), NULL) # sorted, duplicate
            } else if (i == 5L) {
                indices <- list(sample(rep(1:NR, each=2)), sample(rep(1:NC, each=3))) # unsorted, duplicate
            } else if (i == 6L) {
                indices <- list(seq(1, NR, by=2), seq(1, NC, by=3)) # non-consecutive jumps.
            } else {
                indices <- list(integer(0), integer(0))
            }

            expect_equivalent(
                extract_array(x$mat, indices),
                extract_array(x$ref, indices)
            )
        }
    }
})

test_that("extract_sparse_array works in a wide variety of scenarios", {
    YD <- writeTileDBArray(SD, offset=c(-10L, 10L))
    NR <- nrow(YD)
    NC <- ncol(YD)

    for (i in 1:5) {
        if (i == 1L) {
            indices <- list(NULL, NULL)
        } else if (i == 2L) {
            indices <- list(NULL, 1:NC) # sorted, unique
        } else if (i == 3L) {
            indices <- list(sample(NR), NULL) # unsorted, unique
        } else if (i == 4L) {
            indices <- list(seq(1, NR, by=2), seq(1, NC, by=3)) # non-consecutive jumps.
        } else {
            indices <- list(integer(0), integer(0))
        }

        expect_equivalent(extract_sparse_array(YD, indices), extract_sparse_array(SD, indices))
    }
})
