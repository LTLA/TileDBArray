# This tests the reindexing capability of reindex_sparse.
# library(testthat); library(TileDBArray); source("test-reindex.R")

library(S4Vectors)

REF <- function(df, index, dim, offset=rep(1L, length(dim))) {
    ndim <- length(index)

    # Assuming that the first 'ndim' columns of 'out' are indices.
    for (i in seq_len(ndim)) {
        cur.index <- index[[i]]
        if (is.null(cur.index)) {
            cur.index <- seq_len(dim[i])
        }
        cur.index <- cur.index + offset[i] - 1L

        # Expanding to account for duplicates in 'cur.index'.
        m <- findMatches(df[[i]], cur.index)
        df <- df[queryHits(m),]

        # Replacing with the position of each index in 'cur.index'.
        df[[i]] <- subjectHits(m)
    }

    rownames(df) <- NULL
    df 
}

TEST <- function(df, index, dim, offset=rep(1L, length(dim)))
# Derived from TileDBArray:::.extract_values
{
    index.info <- TileDBArray:::.format_indices(index, dim, offset)
    ndim <- length(index)
    output <- TileDBArray:::remap_indices(as.list(df[seq_len(ndim)]), index.info$remapping)
    list(
        indices=output$indices,
        values=rep.int(df[[ndim + 1L]], output$expand)
    )
}

SIMULATE_NONZERO <- function(indices, N, D=NULL, offset=NULL) 
# The simulator function accepts:
#
# - 'indices': list specifying the indices of the full array to obtain the desired subarray.
#    If NULL, this is assumed to take the entire dimension of the array without subsetting.
# - 'N': integer, the approximate number of non-zero values to simulate in this subarray. 
# - 'D': integer vector, the dimensions of the full array, only used if any 'indices' are NULL.
#
# This returns the locations and values of all non-zero elements in the subarray.
# Importantly, the locations are reported with respect to the full array,
# which mimics the behavior of the tiledb_array getters.
{
    options <- indices
    for (i in seq_along(options)) {
        if (is.null(options[[i]])) {
            options[[i]] <- seq_len(D[i])
        } else {
            options[[i]] <- sort(unique(options[[i]]))
        }
        if (!is.null(offset)) {
            options[[i]] <- options[[i]] + offset[i] - 1L
        }
    }

    extracted <- lapply(options, sample, size=N, replace=TRUE)
    extracted <- unique(DataFrame(extracted))
    cbind(as.data.frame(extracted), X=runif(nrow(extracted)))
}

expect_rearranged <- function(indices, values, ref) {
    obs <- DataFrame(indices, values=values) 
    ref <- DataFrame(ref)
    colnames(ref) <- colnames(obs)
    expect_identical(sort(obs), sort(ref))
}

#####################################################

set.seed(100)
dims <- c(d1=100, d2=200, d3=50) # Full dimensions of the array.

test_that("index remapping works correctly", {
    indices <- lapply(dims, sample, size=20, replace=TRUE)

    extracted <- SIMULATE_NONZERO(indices, 10)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)

    extracted <- SIMULATE_NONZERO(indices, 100)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)

    # Flooding with lots of duplicates.
    indices <- lapply(dims, sample, size=1000, replace=TRUE)

    extracted <- SIMULATE_NONZERO(indices, 10)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)

    extracted <- SIMULATE_NONZERO(indices, 100)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)

    extracted <- SIMULATE_NONZERO(indices, 1000) # More non-zero elements.
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)

    # Slightly more complex example with varying indices.
    indices <- mapply(sample, x=dims, size=c(10, 50, 20), SIMPLIFY=FALSE, MoreArgs=list(replace=TRUE))

    extracted <- SIMULATE_NONZERO(indices, 100)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)

    extracted <- SIMULATE_NONZERO(indices, 1000)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)
})

test_that("index remapping works correctly with NULLs", {
    indices <- lapply(dims, sample, size=1000, replace=TRUE)
    indices[2] <- list(NULL)

    extracted <- SIMULATE_NONZERO(indices, 100, D=dims)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)

    extracted <- SIMULATE_NONZERO(indices, 1000, D=dims)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)

    # Knocking off the first index.
    indices[1] <- list(NULL)

    extracted <- SIMULATE_NONZERO(indices, 100, D=dims)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)

    extracted <- SIMULATE_NONZERO(indices, 1000, D=dims)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)

    # Knocking off the last index, in which case everything should be returned without modification.
    indices[3] <- list(NULL)

    extracted <- SIMULATE_NONZERO(indices, 100, D=dims)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)
    expect_identical(sort(DataFrame(ref)), sort(DataFrame(extracted)))

    extracted <- SIMULATE_NONZERO(indices, 1000, D=dims)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)
    expect_identical(nrow(extracted), nrow(ref))
})

test_that("index remapping works correctly with empty inputs", {
    indices <- lapply(dims, sample, size=0, replace=TRUE)
    extracted <- SIMULATE_NONZERO(indices, 0, D=dims)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)
    expect_identical(nrow(ref), 0L)

    indices <- lapply(dims, sample, size=100, replace=TRUE)
    extracted <- SIMULATE_NONZERO(indices, 0, D=dims)
    output <- TEST(extracted, indices, dim=dims)
    ref <- REF(extracted, indices, dim=dims)
    expect_rearranged(output$indices, output$values, ref)
    expect_identical(nrow(ref), 0L)
})

test_that("index remapping works correctly with non-unity offsets", {
    indices <- lapply(dims, sample, size=20, replace=TRUE)

    O <- 1:3
    extracted <- SIMULATE_NONZERO(indices, 10, offset=O)
    output <- TEST(extracted, indices, dim=dims, offset=O)
    ref <- REF(extracted, indices, dim=dims, offset=O)
    expect_rearranged(output$indices, output$values, ref)

    O <- c(-10L, 10L, 100L)
    extracted <- SIMULATE_NONZERO(indices, 100, offset=O)
    output <- TEST(extracted, indices, dim=dims, offset=O)
    ref <- REF(extracted, indices, dim=dims, offset=O)
    expect_rearranged(output$indices, output$values, ref)
})
