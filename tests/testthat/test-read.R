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
