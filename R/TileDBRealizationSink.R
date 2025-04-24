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
#'     storagetype=NULL,
#'     dimtype=getTileDBDimType(),
#'     sparse=FALSE,
#'     extent=getTileDBExtent(), 
#'     offset=1L,
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
#' \item \code{type}, a string specifying the R data type for the newly written array.
#' Currently only \code{"double"}, \code{"integer"} and \code{"logical"} arrays are supported.
#' \item \code{path}, a string containing the location of the new TileDB backend.
#' \item \code{attr}, a string specifying the name of the attribute to store.
#' \item \code{storagetype}, a string specifying the TileDB data type for the attribute, e.g., \code{"UINT8"}, \code{"FLOAT32"}.
#' If \code{NULL}, this is automatically determined from \code{type} using \code{\link{r_to_tiledb_type}}.
#' \item \code{dimtype}, a string specifying the TileDB data type for the dimension.
#' \item \code{sparse}, a logical scalar indicating whether the array should be stored in sparse form.
#' \item \code{extent}, an integer scalar (or vector of length equal to \code{dim}) specifying the tile extent for each dimension.
#' Larger values improve compression at the cost of unnecessary data extraction during reads. 
#' \item \code{offset}, an integer scalar (or vector of length equal to \code{dim}) specifying the starting offset for each dimension's domain.
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
TileDBRealizationSink <- function(
    dim,
    dimnames=NULL,
    type="double", 
    path=getTileDBPath(), 
    attr=getTileDBAttr(), 
    storagetype=NULL,
    dimtype=getTileDBDimType(),
    sparse=FALSE, 
    extent=getTileDBExtent(), 
    offset=1L,
    cellorder=getTileDBCellOrder(),
    tileorder=getTileDBTileOrder(),
    capacity=getTileDBCapacity(),
    context=getTileDBContext())
{
    ndim <- length(dim)
    collected <- vector("list", ndim)
    extent <- rep(as.integer(extent), ndim)
    offset <- rep(as.integer(offset), ndim)

    for (i in seq_along(dim)) {
        curdim <- dim[i]
        ex <- min(extent[i], curdim)
        collected[[i]] <- tiledb_dim(
            ctx=context,
            paste0("d", i),
            offset[i] - 1L + c(1L, curdim),
            tile=ex, 
            type=dimtype
        )
    }
    dom <- tiledb_domain(ctx=context, dims=collected)

    if (is.null(storagetype)) {
        storagetype <- r_to_tiledb_type(vector(type))
    }

    ncells <- if (storagetype %in% c("ASCII", "CHAR", "UTF8")) NA_integer_ else 1L

    schema <- tiledb_array_schema(
        ctx=context,
        domain=dom, 
        sparse=sparse,
        attrs=list(tiledb_attr(attr, type=storagetype, ncells=ncells, ctx=context)),
        cell_order = cellorder,
        tile_order = tileorder,
        capacity = capacity
    )

    if (is.null(path)) {
        path <- tempfile()
    }

    tiledb_array_create(path, schema)
    .edit_metadata(path, sparse=sparse, type=type, dimnames=dimnames)

    new("TileDBRealizationSink", dim=dim, type=type, path=path, sparse=sparse, attr=attr, offset=offset)
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
#' @importClassesFrom SparseArray COO_SparseArray
#' @importFrom SparseArray nzcoo nzdata nzwhich
#' @importFrom DelayedArray start width
setMethod("write_block", "TileDBRealizationSink", function(sink, viewport, block) {
    starts <- start(viewport) - 1L
    obj <- tiledb_array(sink@path, attrs=sink@attr, query_type="WRITE")
    on.exit(tiledb_array_close(obj))

    if (sink@sparse) {
        # Need this because COO_SparseArray doesn't support [.
        if (is(block, "COO_SparseArray")) {
            idx <- nzcoo(block)
            vals <- nzdata(block)
        } else {
            idx <- nzwhich(block, arr.ind=TRUE)
            vals <- block[idx]
        }

        ndim <- ncol(idx)
        store <- vector("list", ndim + 1L)
        for (i in seq_len(ndim)) {
            store[[i]] <- idx[,i] + starts[i] + sink@offset[i] - 1L
        }
        store[[ndim + 1]] <- vals

        names(store) <- c(sprintf("d%i", seq_len(ndim)), sink@attr)
        obj[] <- data.frame(store)

    } else {
        args <- lapply(width(viewport), seq_len)
        for (i in seq_along(args)) {
            args[[i]] <- args[[i]] + starts[i] + sink@offset[i] - 1L
        }

        # Need to coerce the block, because it could be a SparseArray
        # derivative.
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
