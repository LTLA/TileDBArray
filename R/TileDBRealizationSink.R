#' Write arrays to TileDB 
#'
#' Write array data to a TileDB backend via \pkg{DelayedArray}'s \linkS4class{RealizationSink} machinery.
#'
#' @section Writing a TileDBArray:
#' \code{writeTileDBArray(x, ...)} writes the matrix-like object \code{x} to a TileDB backend.
#' returning a \linkS4class{TileDBArray} object referring to that resource.
#' Arguments in \code{...} are passed to \code{TileDBRealizationSink} to configure the TileDB representation
#' and include \code{path}, \code{sparse}, \code{tile} and \code{ctx}.
#'
#' \code{TileDBRealizationSink(dim, type="double", path=NULL, sparse=FALSE, tile=100L, ctx=NULL)}
#' returns a TileDBRealizationSink object that can be used to write content to a TileDB backend.
#' It accepts the following arguments:
#' \itemize{
#' \item \code{dim}, an integer vector (usually of length 2) to specify the array dimensions.
#' \item \code{type}, a string specifying the data type.
#' Currently only numeric, logical and integer arrays are supported.
#' \item \code{path}, a string containing the location of the new TileDB backend.
#' Defaults to a temporary directory if not specified.
#' \item \code{sparse} is a logical scalar indicating whether the array should be stored in sparse form.
#' \item \code{tile} is an integer scalar or vector specifying the tile extent for each dimension,
#' roughly interpreted as the size of chunks used for reading/writing the resource.
#' \item \code{ctx} is the TileDB context, defaulting to the output of \code{\link{tiledb_ctx}()}.
#' }
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
#' Y <- Matrix::rsparsematrix(1000, 1000, density=0.01)
#' path2 <- tempfile()
#' out2 <- writeTileDBArray(Y, path=path2, sparse=TRUE)
#' 
#' @aliases
#' writeTileDBArray
#' TileDBRealizationSink
#' coerce,TileDBRealizationSink,TileDBMatrix-method
#' coerce,TileDBRealizationSink,TileDBArray-method
#' coerce,TileDBRealizationSink,DelayedArray-method
#' coerce,ANY,TileDBArray-method
#' coerce,ANY,TileDBMatrix-method
#' coerce,ANY,TileDBRealizationSink-method
#'
#' @name TileDBRealizationSink
NULL

#' @export
#' @importFrom tiledb tiledb_domain tiledb_dim tiledb_ctx
#' tiledb_array_schema tiledb_attr tiledb_array_create
TileDBRealizationSink <- function(dim, type="double", sparse=FALSE, path=NULL, tile=100L, ctx=NULL) {
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
    schema <- tiledb_array_schema(ctx=ctx, dom, sparse=sparse,
        attrs=list(tiledb_attr(ctx=ctx, "x", type=val)))

    tiledb_array_create(path, schema)

    new("TileDBRealizationSink", dim=dim, type=type, path=path, sparse=sparse)
}

.type.mapping <- c(double="FLOAT64", integer="INT32", logical="INT32")

#' @export
#' @importFrom DelayedArray write_block
#' @importFrom IRanges start
#' @importFrom BiocGenerics width
#' @importFrom tiledb tiledb_dense tiledb_sparse tiledb_array_close
setMethod("write_block", "TileDBRealizationSink", function(x, viewport, block) {
    if (x@sparse) {
        obj <- tiledb_sparse(x@path, query_type="WRITE")
    } else {
        obj <- tiledb_dense(x@path, query_type="WRITE")
    }
    on.exit(tiledb_array_close(obj))

    args <- lapply(width(viewport), seq_len)
    args <- mapply(FUN="+", start(viewport) - 1L, args, SIMPLIFY=FALSE)

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
    as(sink, "TileDBArray")
}

#' @export
#' @importFrom DelayedArray DelayedArray
setAs("TileDBRealizationSink", "TileDBArraySeed",
    function(from) TileDBArraySeed(from@path)
)

#' @export
#' @importFrom DelayedArray DelayedArray
setAs("TileDBRealizationSink", "TileDBArray",
    function(from) DelayedArray(as(from, "TileDBArraySeed"))
)

#' @export
#' @importFrom DelayedArray DelayedArray
setAs("TileDBRealizationSink", "DelayedArray",
    function(from) DelayedArray(as(from, "TileDBArraySeed"))
)

.as_TileDBArray <- function(from) writeTileDBArray(from)

#' @export
setAs("ANY", "TileDBArray", .as_TileDBArray)

#' @export
setAs("DelayedArray", "TileDBArray", .as_TileDBArray)

#' @export
setAs("DelayedMatrix", "TileDBMatrix", .as_TileDBArray)
