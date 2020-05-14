#' Delayed TileDB arrays
#'
#' The TileDBArray class provides a \linkS4class{DelayedArray} backend for TileDB arrays (sparse and dense).
#'
#' @section Constructing a TileDBArray:
#' \code{TileDBArray(x, ...)} yields a TileDBArray object
#' given \code{x}, a URI path to the dense array (e.g., a directory).
#' Further arguments can be passed to \code{\link{tiledb_dense}} via \code{...}.
#' Alternatively, \code{x} can be a TileDBArraySeed object.
#'
#' \code{TileDBArraySeed(x, ...)} yields a TileDBArraySeed
#' with the same arguments as described for \code{TileDBArray}.
#' If \code{x} is already a TileDBArraySeed, it is returned
#' directly without further modification.
#'
#' \code{\link{DelayedArray}(x)} returns a TileDBArray object
#' given \code{x}, a TileDBArraySeed.
#'
#' In all cases, two-dimensional arrays will automatically generate a TileDBMatrix,
#' a subclass of the TileDBArray.
#'
#' @section Available operations:
#' \code{\link{extract_array}(x, index)} will return an ordinary 
#' array corresponding to the TileDBArray \code{x} subsetted
#' to the indices in \code{index}. 
#' The latter should be a list of length equal to the number of 
#' dimensions in \code{x}.
#'
#' All operations supported by \linkS4class{DelayedArray} objects are 
#' also available for TileDBArray objects.
#' 
#' @aliases
#' TileDBArraySeed
#' TileDBArraySeed-class
#' TileDBArray
#' TileDBArray-class
#' show,TileDBArray-method
#' extract_array,TileDBArray-method
#' DelayedArray,TileDBArraySeed-method
#'
#' @author Aaron Lun
#' 
#' @examples
#' data <- matrix(rpois(10000, 5), nrow=100, ncol=100)
#' B <- as(data, "TileDBArray")
#' B
#'
#' # Apply typical DelayedArray operations:
#' as.matrix(B[1:10,1:10])
#' B %*% runif(ncol(B))
#'
#' @name TileDBArray
NULL

#' @importFrom tiledb domain schema tiledb_array
TileDBArraySeed <- function(x) { 
    if (is(x, "TileDBArraySeed")) {
        return(x)
    }

    x <- tiledb_array(path)
    on.exit(tiledb_array_close(x))

    d <- dim(domain(schema(x)))
    new("TileDBArraySeed", dim=d, dimnames=vector("list", length(d)), path=path)
}

#' @export
#' @importFrom methods show
setMethod("show", "TileDBArraySeed", function(object) {
    cat(sprintf("%i x %i TileDBArraySeed object\n", nrow(object), ncol(object)))
})

#' @export
#' @importFrom DelayedArray extract_array
#' @importFrom tiledb tiledb_dense
setMethod("extract_array", "TileDBArraySeed", function(x, index) {
    d <- dim(x)
    for (i in seq_along(index)) {
        if (is.null(index[[i]])) index[[i]] <- seq_len(d[i])
    }

    # Hack to overcome zero-length indices.
    d2 <- lengths(index)
    if (any(d2==0L)) {
        return(array(numeric(0), dim=d2))
    }

    # Figuring out what type of array it is.
    x <- tiledb_dense(x@path, query_type="READ")
    on.exit(tiledb_array_close(x))

    # Hack to overcome non-contiguous subset error.
    o <- order(d2)
    least <- o[1]
    least.index <- index[[least]]

    re.o <- unique(sort(least.index))
    diff.from.last <- which(diff(re.o)!=1L)
    all.starts <- c(1L, diff.from.last+1L)
    all.ends <- c(diff.from.last, length(re.o))

    full <- lapply(d, seq_len)
    collected <- vector("list", length(all.starts))
    index[[least]] <- seq_along(re.o)

    for (i in seq_along(all.starts)) {
        cur.start <- re.o[all.starts[i]]
        cur.end <- re.o[all.ends[i]]

        full[[least]] <- cur.start:cur.end
        tmp <- do.call(`[`, c(list(x), full, list(drop=FALSE)))

        index[[least]] <- seq_len(cur.end-cur.start+1L)
        tmp <- do.call(`[`, c(list(tmp), index, list(drop=FALSE)))
        collected[[i]] <- tmp
    }

    # TODO: generalize to arrays.
    if (least==1L) {
        out <- do.call(rbind, collected)
        out <- out[match(least.index, re.o),,drop=FALSE]
    } else {
        out <- do.call(cbind, collected)
        out <- out[,match(least.index, re.o),drop=FALSE]
    }
    out
})

#' @export
#' @importFrom DelayedArray DelayedArray
TileDBArray <- function(x, ..., query_type="READ") {
    DelayedArray(TileDBArraySeed(x, ..., query_type=query_type))
}

#' @export
#' @importFrom DelayedArray DelayedArray new_DelayedArray
setMethod("DelayedArray", "TileDBArraySeed",
    function(seed) new_DelayedArray(seed, Class="TileDBMatrix")
)
