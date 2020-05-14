#' Write arrays to TileDB 
#'
#' Write array data to a TileDB backend via \pkg{DelayedArray}'s \linkS4class{RealizationSink} machinery.
#'
#' @section Writing a TileDBArray:
#' \code{writeTileDBArray(x, path=NULL, tile=100L, ctx=NULL)} writes the matrix-like object \code{x} to 
#' a TileDB resource at \code{path} (defaulting to a temporary directory if not specified).
#' \code{tile} specifies the tile extent and \code{ctx} is the TileDB context
#' (defaulting to the output of \code{\link{tiledb_ctx}()}).
#' It returns a \linkS4class{TileDBArray} object referring to \code{path}.
#'
#' \code{TileDBRealizationSink(dim, type="double", path=NULL, tile=100L, ctx=NULL)}
#' defines the realization sink for \pkg{DelayedArray} writing machinery.
#' The arguments are as defined above but with the additional \code{dim} to specify the array dimensions
#' and \code{type} to specify the data type.
#' Currently only numeric, logical and integer arrays are supported.
#'
#' @section Coercing to a TileDBArray:
#' \code{as(x, "TileDBArray")} will coerce a matrix-like object \code{x} to a TileDBArray object.
#' 
#' \code{as(x, "TileDBArraySeed")} will coerce a matrix-like object \code{x} to a TileDBArraySeed object.
#'
#' \code{as(x, "TileDBMatrix")} will coerce a matrix-like object \code{x} to a TileDBArraySeed object.
#'
#' \code{as(x, "TileDBArray")} will coerce a TileDBRealizationSink \code{x} to a TileDBArray object.
#' 
#' \code{as(x, "TileDBArraySeed")} will coerce a TileDBRealizationSink \code{x} to a TileDBArraySeed object.
#'
#' \code{as(x, "DelayedArray")} will coerce a TileDBRealizationSink \code{x} to a TileDBArray object.
#' 
#' @examples
#' X <- matrix(rnorm(100000), ncol=200)
#' path <- tempfile()
#' out <- writeTileDBArray(X, path=path)
#'
#' @aliases
#' TileDBRealizationSink
#'
#' @name TileDBRealizationSink
NULL

#' @export
#' @importFrom tiledb tiledb_domain tiledb_dim tiledb_ctx
#' tiledb_array_schema tiledb_attr tiledb_array_create
TileDBRealizationSink <- function(dim, type="double", path=NULL, tile=100L, ctx=NULL) {
    if (is.null(ctx)) {
        ctx <- tiledb_ctx()
    }
    if (is.null(path)) {
        path <- tempfile()
    }

    val <- .type.mapping[type]
    if (is.na(val)) {
        stop("'type' not supported")
    }

    collected <- vector("list", length(dim))
    tile <- rep(as.integer(tile), length(dim))
    for (i in seq_along(dim)) {
        collected[[i]] <- tiledb_dim(ctx=ctx, paste0("d", i), c(1L, dim[i]), tile[i], "INT32")
    }
    dom <- tiledb_domain(ctx=ctx, dims=collected)

    # The array will be dense with a single attribute "a" 
    # so each cell can store an integer.
    schema <- tiledb_array_schema(ctx=ctx, dom, 
        attrs=list(tiledb_attr(ctx=ctx, "x", type =val)))

    tiledb_array_create(path, schema)

    new("TileDBRealizationSink", dim=dim, type=type, path=path)
}

.type.mapping <- c(double="FLOAT64", integer="INT32", logical="INT32")

#' @export
#' @importFrom DelayedArray write_block
#' @importFrom IRanges start
#' @importFrom BiocGenerics width
#' @importFrom tiledb tiledb_array tiledb_array_close
setMethod("write_block", "TileDBRealizationSink", function(x, viewport, block) {
    obj <- tiledb_dense(x@path, query_type="WRITE")
    on.exit(tiledb_array_close(obj))

    args <- lapply(width(viewport), seq_len)
    args <- mapply(FUN="+", start(viewport) - 1L, args)

    args <- c(list(x=obj), args, list(value=block))

    # This does the assignment.
    do.call("[<-", args)

    NULL
})

#' @export
#' @importFrom DelayedArray type
setMethod("type", "TileDBRealizationSink", function(x) x@type)

#' @export
#' @importFrom DelayedArray type BLOCK_write_to_sink
writeTileDBArray <- function(x, path=NULL, ...) {
    sink <- TileDBRealizationSink(dim(x), type=type(x), path=path, ...)
    BLOCK_write_to_sink(x, sink)
    as(sink, "DenseTileDBArray")
}

#' @export
#' @importFrom DelayedArray DelayedArray
setAs("TileDBRealizationSink", "DenseTileDBArraySeed",
    function(from) DenseTileDBArraySeed(from@path)
)

#' @export
#' @importFrom DelayedArray DelayedArray
setAs("TileDBRealizationSink", "DenseTileDBArray",
    function(from) DelayedArray(as(from, "DenseTileDBArraySeed"))
)

#' @export
#' @importFrom DelayedArray DelayedArray
setAs("TileDBRealizationSink", "DelayedArray",
    function(from) DelayedArray(as(from, "DenseTileDBArraySeed"))
)

.as_TileDBArray <- function(from) writeTileDBArray(from)

#' @export
setAs("ANY", "DenseTileDBArray", .as_TileDBArray)

#' @export
setAs("DelayedArray", "DenseTileDBArray", .as_TileDBArray)

#' @export
setAs("DelayedMatrix", "DenseTileDBMatrix", .as_TileDBArray)
