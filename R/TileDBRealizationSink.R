#' Write arrays to TileDB 
#'
#' Write array data to a TileDB backend via \pkg{DelayedArray}'s \linkS4class{RealizationSink} machinery.
#'
#' @section Writing a TileDBArray:
#' \preformatted{TileDBRealizationSink(
#'     dim, 
#'     dimnames=NULL, 
#'     type="double", 
#'     path=getTileDBPath(), 
#'     attr=getTileDBAttr(), 
#'     sparse=FALSE,
#'     extent=getTileDBExtent(), 
#'     cellorder=getTileDBCellOrder(),
#'     tileorder=getTileDBTileOrder(),
#'     capacity=getTileDBCapacity(),
#'     context=getTileDBContext()
#' )}
#' returns a TileDBRealizationSink object that can be used to write content to a TileDB backend.
#' It accepts the following arguments:
#' \itemize{
#' \item \code{dim}, an integer vector (usually of length 2) to specify the array dimensions.
#' \item \code{dimnames}, a list of length equal to \code{dim}, containing character vectors with names for each dimension.
#' Defaults to \code{NULL}, i.e., no dimnames.
#' \item \code{type}, a string specifying the data type.
#' Currently only numeric, logical and integer arrays are supported.
#' \item \code{path}, a string containing the location of the new TileDB backend.
#' \item \code{attr}, a string specifying the name of the attribute to store.
#' \item \code{sparse}, a logical scalar indicating whether the array should be stored in sparse form.
#' \item \code{extent}, an integer scalar (or vector of length equal to \code{dim}) specifying the tile extent for each dimension.
#' Larger values improve compression at the cost of unnecessary data extraction during reads. 
#' \item \code{cellorder}, a string specifying the ordering of cells within each tile.
#' \item \code{tileorder}, a string specifying the ordering of tiles across the array.
#' \item \code{capacity}, an integer scalar specifying the size of each data tile in the sparse case.
#' \item \code{context} is the TileDB context, defaulting to the output of \code{\link{tiledb_ctx}()}.
#' }
#'
#' \code{writeTileDBArray(x, sparse=is_sparse(x), ...)} writes the matrix-like object \code{x} to a TileDB backend,
#' returning a \linkS4class{TileDBArray} object referring to that backend. 
#' Appropriate values for \code{dim}, \code{dimnames} and \code{type} are determined automatically from \code{x} itself.
#' All other arguments described for \code{TileDBRealizationSink} can be passed into \code{...} to configure the representation.
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
#' @section Sink internals:
#' \code{write_block(sink, viewport, block)} will write the subarray \code{block} to the TileDBRealizationSink \code{sink}
#' at the specified \code{viewport}, returning \code{sink} upon completion.
#' See \code{\link{write_block}} in \pkg{DelayedArray} for more details.
#'
#' \code{type(x)} will return a string specifying the type of the TileDBRealizationSink \code{x}.
#' 
#' @examples
#' X <- matrix(rnorm(100000), ncol=200)
#' path <- tempfile()
#' out <- writeTileDBArray(X, path=path)
#'
#' # Works for integer matrices.
#' Xi <- matrix(rpois(100000, 2), ncol=200)
#' pathi <- tempfile()
#' outi <- writeTileDBArray(Xi, path=pathi)
#'
#' # Works for logical matrices.
#' Xl <- matrix(rpois(100000, 0.5) > 0, ncol=200)
#' pathl <- tempfile()
#' outl <- writeTileDBArray(Xl, path=pathl)
#'
#' # Works for sparse numeric matrices.
#' Y <- Matrix::rsparsematrix(1000, 1000, density=0.01)
#' path2 <- tempfile()
#' out2 <- writeTileDBArray(Y, path=path2)
#'
#' # And for sparse logical matrices.
#' path2l <- tempfile()
#' out2l <- writeTileDBArray(Y > 0, path=path2l)
#'
#' # Works for dimnames.
#' rownames(X) <- sprintf("GENE_%i", seq_len(nrow(X)))
#' path3 <- tempfile()
#' out3 <- writeTileDBArray(X, path=path3)
#' 
#' @aliases
#' writeTileDBArray
#' TileDBRealizationSink
#' TileDBRealizationSink-class
#' write_block,TileDBRealizationSink-method
#' type,TileDBRealizationSink-method
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
TileDBRealizationSink <- function(dim, dimnames=NULL, type="double", 
    path=getTileDBPath(), 
    attr=getTileDBAttr(), 
    sparse=FALSE, 
    extent=getTileDBExtent(), 
    cellorder=getTileDBCellOrder(),
    tileorder=getTileDBTileOrder(),
    capacity=getTileDBCapacity(),
    context=getTileDBContext())
{
    collected <- vector("list", length(dim))
    extent <- rep(as.integer(extent), length(dim))
    for (i in seq_along(dim)) {
        ex <- min(extent[i], dim[i])
        collected[[i]] <- tiledb_dim(ctx=context, paste0("d", i), c(1L, dim[i]), tile=ex, type="INT32")
    }
    dom <- tiledb_domain(ctx=context, dims=collected)

    val <- r_to_tiledb_type(vector(type))
    schema <- tiledb_array_schema(ctx=context, dom, 
        sparse=sparse,
        attrs=list(tiledb_attr(ctx=context, attr, type=val)),
        cell_order = cellorder,
        tile_order = tileorder,
        capacity = capacity)

    if (is.null(path)) {
        path <- tempfile()
    }

    tiledb_array_create(path, schema)
    .edit_metadata(path, sparse=sparse, type=type, dimnames=dimnames)

    new("TileDBRealizationSink", dim=dim, type=type, path=path, sparse=sparse, attr=attr)
}

.edit_metadata <- function(path, sparse, type, dimnames) {
    has.logical <- type=="logical"
    has.dimnames <- !is.null(dimnames) && !all(vapply(dimnames, is.null, FALSE))
    if (!has.logical && !has.dimnames) {
        return(NULL)
    }

    obj <- tiledb_array(path)
    on.exit(tiledb_array_close(obj))
    obj <- tiledb_array_open(obj, "WRITE") # not sure why it doesn't work with query_type="WRITE".

    # Need to keep track of the differences between INTs and LGLs.
    if (has.logical) {
        tiledb_put_metadata(obj, "type", type)
    }

    # Adding dimnames by packing them into base-64 encoding.
    if (has.dimnames) {
        tiledb_put_metadata(obj, "dimnames", .pack_dimnames(dimnames))
    }

    NULL
} 

#' @importFrom S4Vectors setValidity2
setValidity2("TileDBRealizationSink", function(object) {
    msg <- .common_checks(object)
    if (length(msg)) {
        msg
    } else {
        TRUE
    }
})

#' @export
#' @importFrom DelayedArray start width
setMethod("write_block", "TileDBRealizationSink", function(sink, viewport, block) {
    starts <- start(viewport) - 1L

    if (sink@sparse) {
        obj <- tiledb_array(sink@path, attrs=sink@attr, query_type="WRITE")
        on.exit(tiledb_array_close(obj))

        # Need this because SparseArraySeed doesn't follow a matrix abstraction.
        if (is(block, "SparseArraySeed")) {
            store <- data.frame(
                d1=nzindex(block)[,1] + starts[1],
                d2=nzindex(block)[,2] + starts[2],
                sink=nzdata(block)
            )
        } else {
            idx <- which(block!=0, arr.ind=TRUE)
            store <- data.frame(
                d1=idx[,1] + starts[1],
                d2=idx[,2] + starts[2],
                sink=block[idx]
            )
        }

        colnames(store)[3] <- sink@attr
        obj[] <- store

    } else {
        obj <- tiledb_dense(sink@path, attrs=sink@attr, query_type="WRITE")
        on.exit(tiledb_array_close(obj))

        args <- lapply(width(viewport), seq_len)
        args <- mapply(FUN="+", starts, args, SIMPLIFY=FALSE)

        # Need to coerce the block, because it could be a SparseArraySeed.
        args <- c(list(sink=obj), args, list(value=as.array(block)))
        do.call("[<-", args)
    }

    sink
})

#' @export
setMethod("type", "TileDBRealizationSink", function(x) x@type)

#' @export
writeTileDBArray <- function(x, sparse=is_sparse(x), ...) {
    sink <- TileDBRealizationSink(dim(x), dimnames=dimnames(x), type=type(x), sparse=sparse, ...)
    sink <- BLOCK_write_to_sink(sink, x)
    as(sink, "TileDBArray")
}

setAs("TileDBRealizationSink", "TileDBArraySeed",
    function(from) TileDBArraySeed(from@path)
)

setAs("TileDBRealizationSink", "TileDBArray",
    function(from) DelayedArray(as(from, "TileDBArraySeed"))
)

setAs("TileDBRealizationSink", "DelayedArray",
    function(from) DelayedArray(as(from, "TileDBArraySeed"))
)

.as_TileDBArray <- function(from) writeTileDBArray(from)

setAs("ANY", "TileDBArray", .as_TileDBArray)

setAs("DelayedArray", "TileDBArray", .as_TileDBArray)

setAs("DelayedMatrix", "TileDBMatrix", .as_TileDBArray)
