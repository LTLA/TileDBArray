#' Delayed TileDB arrays
#'
#' The TileDBArray class provides a \linkS4class{DelayedArray} backend for TileDB arrays (sparse and dense).
#'
#' @section Constructing a TileDBArray:
#' \code{TileDBArray(x, attr)} returns a TileDBArray object given:
#' \itemize{
#' \item \code{x}, a string containing a URI to a TileDB backend, most typically a path to a directory.
#' \item \code{attr}, a string specifying the attribute to represent in the array.
#' Defaults to the first attribute.
#' }
#' Alternatively, \code{x} can be a TileDBArraySeed object, in which case \code{attr} is ignored.
#'
#' \code{TileDBArraySeed(x, attr)} returns a TileDBArraySeed
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
#' \code{\link{extract_array}(x, index)} will return an ordinary array containing values from the TileDBArraySeed \code{x},
#' subsetted to the indices specified in \code{index}. 
#' The latter should be a list of length equal to the number of dimensions in \code{x},
#' where each entry is an integer vector or \code{NULL} (in which case the entirety of the dimension is used).
#'
#' \code{\link{extract_sparse_array}(x, index)} will return a \linkS4class{COO_SparseArray}
#' representing the subset of \code{x} corresponding to the indices in \code{index}.
#' The latter should be a list of the same structure as described for \code{extract_array}.
#'
#' \code{\link{type}(x)} will return a string containing the type of the TileDBArraySeed object \code{x}.
#' Currently, only \code{"integer"}, \code{"logical"} and \code{"double"}-precision is supported.
#'
#' \code{\link{is_sparse}(x)} will return a logical scalar indicating 
#' whether the TileDBArraySeed \code{x} uses a sparse format in the TileDB backend.
#'
#' \code{\link{path}(x)} will return a string containing the path to the TileDB backend directory.
#'
#' \code{\link{chunkdim}(x)} will return an integer vector containing the tile extent in each dimension.
#' This will be used as the chunk dimensions in methods like \code{\link{chunkGrid}}.
#'
#' All of the operations described above are also equally applicable to TileDBArray objects, 
#' as their methods simply delegate to those of the TileDBArraySeed.
#'
#' All operations supported by \linkS4class{DelayedArray} objects are 
#' also available for TileDBArray objects.
#' 
#' @aliases
#' TileDBArraySeed
#' TileDBArraySeed-class
#' TileDBArray
#' TileDBArray-class
#' TileDBMatrix
#' TileDBMatrix-class
#'
#' show,TileDBArraySeed-method
#' is_sparse,TileDBArraySeed-method
#' type,TileDBArraySeed-method
#' extract_array,TileDBArraySeed-method
#' extract_sparse_array,TileDBArraySeed-method
#' DelayedArray,TileDBArraySeed-method
#' path,TileDBArraySeed-method
#' chunkdim,TileDBArraySeed-method
#' matrixClass,TileDBArray-method
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
#' # This also works for sparse arrays:
#' sdata <- Matrix::rsparsematrix(nrow=100, ncol=100, density=0.1)
#' C <- as(sdata, "TileDBArray")
#' C
#'
#' @name TileDBArray
NULL

#' @export
TileDBArraySeed <- function(x, attr) { 
    if (is(x, "TileDBArraySeed")) {
        return(x)
    }

    obj <- tiledb_array(x)
    on.exit(tiledb_array_close(obj), add=TRUE, after=FALSE)

    s <- schema(obj)
    dims <- dimensions(s)
    doms <- lapply(dims, domain)
    o <- vapply(doms, function(x) x[1L], 0L)
    d <- vapply(doms, function(x) diff(x) + 1L, 0L)
    e <- vapply(dims, tiledb::tile, 0L)

    a <- attrs(s)
    if (missing(attr)) {
        attr <- names(a)[1]
    } else if (!attr %in% names(a)) {
        stop("'attr' not in the TileDB attributes")
    }

    my.type <- tiledb_datatype_R_type(datatype(a[[attr]]))
    if (!(my.type %in% c("logical", "double", "integer", "character"))) {
        stop("'attr' refers to an unsupported type")
    }

    opened <- tiledb_array_open(obj, "READ")
    on.exit(tiledb_array_close(opened), add=TRUE, after=FALSE) # do I need to do this if we already close it above? I don't know.
    rtype <- tiledb_get_metadata(opened, "type")
    if (my.type=="integer" && identical(rtype, "logical")) {
        my.type <- rtype
    }

    dimnames <- tiledb_get_metadata(opened, "dimnames")
    if (!is.null(dimnames)) {
        dimnames <- .unpack_dimnames(dimnames)
    } else {
        dimnames <- vector("list", length(d))
    }

    new("TileDBArraySeed", 
        dim=d,
        dimnames=dimnames,
        path=x, 
        sparse=is.sparse(s),
        attr=attr,
        type=my.type,
        extent=e,
        offset=o
    )
}

#' @importFrom S4Vectors setValidity2
setValidity2("TileDBArraySeed", function(object) {
    msg <- .common_checks(object)

    d <- dim(object)
    o <- object@offset
    if (length(o) != length(d)) {
        msg <- c(msg, "'offset' must have the same length as 'dim'")
    }

    e <- object@extent
    if (length(e) != length(d)) {
        msg <- c(msg, "'extent' must have the same length as 'dim'")
    }
    if (!all(e >= 0L)) {
        msg <- c(msg, "'extent' must contain non-negative integers")
    }

    dn <- dimnames(object)
    if (length(dn)!=length(d)) {
        msg <- c(msg, "'dimnames' must the same length as 'dim'")
    }
    if (!all(d==lengths(dn) | vapply(dn, is.null, FALSE))) {
        msg <- c(msg, "each 'dimnames' must be NULL or the same length as the corresponding dimension")
    }

    if (length(msg)) {
        msg
    } else {
        TRUE
    }
})

#' @export
#' @importFrom methods show
setMethod("show", "TileDBArraySeed", function(object) {
    cat(sprintf("%i x %i TileDBArraySeed object\n", nrow(object), ncol(object)))
})

#' @export
setMethod("is_sparse", "TileDBArraySeed", function(x) x@sparse)

#' @export
setMethod("type", "TileDBArraySeed", function(x) x@type)

#' @export
setMethod("chunkdim", "TileDBArraySeed", function(x) {
    x@extent
})

#' @export
#' @importFrom DelayedArray path
setMethod("path", "TileDBArraySeed", function(object, ...) {
    object@path
})

.compact_ranges <- function(selected, delta=diff(selected)) {
    is.not.contig <- which(delta != 1L)
    cbind(
        selected[c(1L, is.not.contig + 1L)],
        selected[c(is.not.contig, length(selected))]
    )
}

#' @export
setMethod("extract_array", "TileDBArraySeed", function(x, index) {
    fill <- switch(type(x), double=0, integer=0L, logical=FALSE)
    d2 <- .get_block_dims(x, index)

    # Hack to overcome zero-length indices that cause tiledb to throw.
    if (any(d2==0L)) {
        return(array(fill, dim=d2))
    }

    raw.output.type <- if (x@sparse) "data.frame" else "array"
    obj <- tiledb_array(x@path, attrs=x@attr, query_type="READ", return_as=raw.output.type)
    on.exit(tiledb_array_close(obj))

    ndim <- length(index)
    contiguous <- remapping <- expanders <- vector("list", ndim)
    any.modified <- FALSE

    for (i in seq_len(ndim)) {
        cur.index <- index[[i]]

        if (is.null(cur.index)) {
            curd <- x@dim[i]
            contiguous[[i]] <- cbind(1L, curd) + x@offset[i] - 1L

        } else {
            original <- cur.index + x@offset[i] - 1L
            selected <- original

            # Need to account for unsorted or duplicate indices.
            modified <- FALSE
            if (is.unsorted(selected)) {
                modified <- TRUE
                selected <- sort(selected)
            }
            delta <- diff(selected)
            is.dup <- delta == 0L
            if (any(is.dup)) {
                modified <- TRUE
                selected <- selected[c(TRUE, !is.dup)]
                delta <- diff(selected)
            }

            if (modified) {
                any.modified <- TRUE
                expanders[[i]] <- match(original, selected)
            }
            if (raw.output.type == "data.frame") {
                remapping[[i]] <- selected
            }

            contiguous[[i]] <- .compact_ranges(selected, delta)
        }
    }

    selected_ranges(obj) <- contiguous 
    output <- obj[]

    if (raw.output.type == "data.frame") {
        indices <- matrix(0L, nrow(output), ndim)
        quick.dim <- integer(ndim) 
        for (i in seq_len(ndim)) {
            curremap <- remapping[[i]]
            if (!is.null(curremap)) {
                indices[,i] <- match(output[[i]], curremap)
                quick.dim[i] <- length(curremap)
            } else {
                indices[,i] <- output[[i]] - x@offset[i] + 1L
                quick.dim[i] <- x@dim[i]
            }
        }
        extracted <- array(fill, dim=quick.dim)
        extracted[indices] <- as(output[[ndim + 1L]], x@type)
    } else {
        extracted <- output[[x@attr]]
        storage.mode(extracted) <- x@type
    }

    if (any.modified) {
        for (i in seq_along(expanders)) {
            if (is.null(expanders[[i]])) {
                expanders[[i]] <- substitute()
            }
        }
        expanders$drop <- FALSE
        extracted <- do.call(`[`, c(list(extracted), expanders))
    }
    extracted
})

.get_block_dims <- function(x, index) {
    d <- dim(x)
    for (i in seq_along(index)) {
        if (!is.null(index[[i]])) {
            d[i] <- length(index[[i]])
        }
    }
    d
}

#' @export
#' @importFrom SparseArray extract_sparse_array COO_SparseArray
setMethod("extract_sparse_array", "TileDBArraySeed", function(x, index) {
    d2 <- .get_block_dims(x, index)
    if (any(d2==0L)) {
        return(COO_SparseArray(d2, nzdata=vector(type(x))))
    }

    obj <- tiledb_array(x@path, attrs=x@attr, query_type="READ", return_as="data.frame")
    on.exit(tiledb_array_close(obj))

    ndim <- length(index)
    contiguous <- remapping <- vector("list", ndim)

    for (i in seq_len(ndim)) {
        cur.index <- index[[i]]

        if (is.null(cur.index)) {
            curd <- x@dim[i]
            contiguous[[i]] <- cbind(1L, curd) + x@offset[i] - 1L

        } else {
            selected <- cur.index + x@offset[i] - 1L
            remapping[[i]] <- selected

            # No need to worry about duplicates here.
            if (is.unsorted(selected)) {
                o <- order(selected)
                selected <- selected[o]
            }

            contiguous[[i]] <- .compact_ranges(selected)
        }
    }

    selected_ranges(obj) <- contiguous
    extracted <- obj[]
    indices <- matrix(0L, nrow(extracted), ndim)
    for (i in seq_len(ndim)) {
        curremap <- remapping[[i]]
        if (!is.null(curremap)) {
            indices[,i] <- match(extracted[[i]], curremap)
        } else {
            indices[,i] <- extracted[[i]] - x@offset[i] + 1L
        }
    }

    COO_SparseArray(d2, nzcoo=indices, nzdata=as(extracted[,ndim + 1L], x@type))
})

#' @export
TileDBArray <- function(x, ...) {
    DelayedArray(TileDBArraySeed(x, ...))
}

#' @export
setMethod("DelayedArray", "TileDBArraySeed",
    function(seed) new_DelayedArray(seed, Class="TileDBArray")
)

#' @export
setMethod("matrixClass", "TileDBArray", function(x) "TileDBMatrix")
