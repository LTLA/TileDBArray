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
#'     sparse=getTileDBSparse(), 
#'     extent=getTileDBExtent(), 
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
#' \item \code{sparse} is a logical scalar indicating whether the array should be stored in sparse form.
#' \item \code{extent} is an integer scalar (or vector of length equal to \code{dim})
#' specifying the tile extent for each dimension.
#' \item \code{context} is the TileDB context, defaulting to the output of \code{\link{tiledb_ctx}()}.
#' }
#'
#' \code{writeTileDBArray(x, sparse=is_sparse(x), ...)} writes the matrix-like object \code{x} to a TileDB backend,
#' returning a \linkS4class{TileDBArray} object referring to that backend. 
#' Whether a sparse array should be created is determined automatically from \code{x} itself.
#' Arguments in \code{...} are passed to \code{TileDBRealizationSink} to configure the TileDB representation;
#' all arguments listed above aside from \code{dim}, \code{dimnames} and \code{type} are applicable.
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
#' out2 <- writeTileDBArray(Y, path=path2, sparse=TRUE)
#'
#' # And for sparse logical matrices.
#' path2l <- tempfile()
#' out2l <- writeTileDBArray(Y > 0, path=path2l, sparse=TRUE)
#'
#' # Works for dimnames.
#' rownames(X) <- sprintf("GENE_%i", seq_len(nrow(X)))
#' path3 <- tempfile()
#' out3 <- writeTileDBArray(X, path=path3)
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
TileDBRealizationSink <- function(dim, dimnames=NULL, type="double", path=getTileDBPath(), 
    attr=getTileDBAttr(), sparse=getTileDBSparse(), extent=getTileDBExtent(), 
    context=getTileDBContext())
{
    val <- .type.mapping[type]
    if (is.na(val)) {
        stop("'type' not supported")
    }

    collected <- vector("list", length(dim))
    extent <- rep(as.integer(extent), length(dim))
    for (i in seq_along(dim)) {
        ex <- min(extent[i], dim[i])
        collected[[i]] <- tiledb_dim(ctx=context, paste0("d", i), c(1L, dim[i]), tile=ex, type="INT32")
    }
    dom <- tiledb_domain(ctx=context, dims=collected)

    schema <- tiledb_array_schema(ctx=context, dom, sparse=sparse,
        attrs=list(tiledb_attr(ctx=context, attr, type=val)))

    tiledb_array_create(path, schema)
    .edit_metadata(path, attr, sparse=sparse, type=type, dimnames=dimnames)

    new("TileDBRealizationSink", dim=dim, type=type, path=path, sparse=sparse, attr=attr)
}

.edit_metadata <- function(path, attr, sparse, type, dimnames) {
    has.logical <- type=="logical"
    has.dimnames <- !is.null(dimnames) && !all(vapply(dimnames, is.null, FALSE))
    if (!has.logical && !has.dimnames) {
        return(NULL)
    }

    if (sparse) {
        obj <- tiledb_sparse(path, attrs=attr)
    } else {
        obj <- tiledb_dense(path, attrs=attr)
    }
    on.exit(tiledb_array_close(obj))
    obj <- tiledb_array_open(obj, "WRITE") # not sure why it doesn't work with query_type="WRITE".

    # Need to keep track of the differences between INTs and LGLs.
    if (has.logical) {
        tiledb_put_metadata(obj, "type", type)
    }

    # Adding dimnames by , if necessary.
    if (has.dimnames) {
        tiledb_put_metadata(obj, "dimnames", .pack64(dimnames))
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

.type.mapping <- c(double="FLOAT64", integer="INT32", logical="INT32")

.rev.type.mapping <- c(
    FLOAT64="double", FLOAT32="double", UINT32="double", INT64="double", UINT64="double",
    INT32="integer", INT16="integer", INT8="integer", UINT8="integer", UINT16="integer"
)

#' @export
#' @importFrom IRanges start
#' @importFrom BiocGenerics width
setMethod("write_block", "TileDBRealizationSink", function(x, viewport, block) {
    starts <- start(viewport) - 1L

    if (x@sparse) {
        obj <- tiledb_array(x@path, attrs=x@attr, query_type="WRITE")
        on.exit(tiledb_array_close(obj))

        idx <- which(block!=0, arr.ind=TRUE)
        obj[] <- data.frame(
            d1=idx[,1] + starts[1],
            d2=idx[,2] + starts[2],
            x=block[idx]
        )

    } else {
        obj <- tiledb_dense(x@path, attrs=x@attr, query_type="WRITE")
        on.exit(tiledb_array_close(obj))

        args <- lapply(width(viewport), seq_len)
        args <- mapply(FUN="+", starts, args, SIMPLIFY=FALSE)
        args <- c(list(x=obj), args, list(value=block))
        do.call("[<-", args)
    }

    NULL
})

#' @export
setMethod("type", "TileDBRealizationSink", function(x) x@type)

#' @export
writeTileDBArray <- function(x, sparse=is_sparse(x), ...) {
    sink <- TileDBRealizationSink(dim(x), dimnames=dimnames(x), type=type(x), sparse=sparse, ...)
    BLOCK_write_to_sink(x, sink)
    as(sink, "TileDBArray")
}

#' @export
setAs("TileDBRealizationSink", "TileDBArraySeed",
    function(from) TileDBArraySeed(from@path)
)

#' @export
setAs("TileDBRealizationSink", "TileDBArray",
    function(from) DelayedArray(as(from, "TileDBArraySeed"))
)

#' @export
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
