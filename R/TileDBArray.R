#' Delayed TileDB arrays
#'
#' The TileDBArray class provides a \linkS4class{DelayedArray} backend for TileDB arrays (sparse and dense).
#'
#' @section Constructing a TileDBArray:
#' \code{TileDBArray(x, attr)} returns a TileDBArray object given:
#' \itemize{
#' \item \code{x}, a URI path to a TileDB backend, most typically a directory.
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

#' @export
#' @importFrom tiledb domain schema tiledb_array is.sparse attrs datatype
TileDBArraySeed <- function(x, attr) { 
    if (is(x, "TileDBArraySeed")) {
        return(x)
    }

    obj <- tiledb_array(x)
    on.exit(tiledb_array_close(obj))

    s <- schema(obj)
    d <- dim(domain(s))

    a <- attrs(s)
    if (missing(attr)) {
        attr <- names(a)[1]
    } else if (!attr %in% names(a)) {
        stop("'attr' not in the TileDB attributes")
    }
    
    type <- datatype(a[[attr]])
    m <- match(type, .type.mapping)
    if (is.na(m)) {
        stop("'attr' refers to an unsupported type")
    }

    new("TileDBArraySeed", dim=d, dimnames=vector("list", length(d)), path=x, 
        sparse=is.sparse(s), attr=attr, type=names(.type.mapping)[m])
}

#' @importFrom S4Vectors setValidity2
setValidity2("TileDBArraySeed", function(object) {
    msg <- .common_checks(object)

    d <- dim(object)
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
#' @importFrom DelayedArray is_sparse
setMethod("is_sparse", "TileDBArraySeed", function(x) x@sparse)

#' @export
#' @importFrom DelayedArray is_sparse
setMethod("type", "TileDBArraySeed", function(x) x@type)

#' @export
#' @importFrom DelayedArray extract_array
#' @importFrom tiledb tiledb_dense tiledb_sparse tiledb_array_close
#' @importFrom Matrix sparseMatrix
setMethod("extract_array", "TileDBArraySeed", function(x, index) {
    d <- dim(x)
    for (i in seq_along(index)) {
        if (is.null(index[[i]])) index[[i]] <- seq_len(d[i])
    }

    # Set fill to zero so that it behaves properly with sparse extraction.
    fill <- switch(type(x), double=0, integer=0L, logical=FALSE)

    # Hack to overcome zero-length indices.
    d2 <- lengths(index)
    if (any(d2==0L)) {
        return(array(rep(fill, 0L), dim=d2))
    }

    # Figuring out what type of array it is.
    if (is_sparse(x)) {
        obj <- tiledb_sparse(x@path, attrs=x@attr, query_type="READ", as.data.frame=TRUE)
    } else {
        obj <- tiledb_dense(x@path, attrs=x@attr, query_type="READ")
    }
    on.exit(tiledb_array_close(obj))

    if (is_sparse(x)) {
        .extract_noncontiguous_sparse(obj, index, fill)
    } else {
        .extract_noncontiguous_dense(obj, index, fill)
    }
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

#######################################################
# Hacks to get around tiledb's interface limitations. #
#######################################################

.get_contiguous <- function(obj, index) {
    ndim <- length(index)
    new.starts <- new.ends <- cum.width <- usdex <- vector("list", ndim)

    # Identifying all contiguous unique stretches.
    for (i in seq_len(ndim)) {
        cur.index <- index[[i]]
        re.o <- unique(sort(cur.index))
        usdex[[i]] <- re.o

        diff.from.last <- which(diff(re.o)!=1L)
        all.starts <- c(1L, diff.from.last+1L)
        all.ends <- c(diff.from.last, length(re.o))

        new.starts[[i]] <- all.starts
        new.ends[[i]] <- all.ends
    }

    # Looping across them to extract every possible combination.
    collected <- list()
    current <- rep(1L, ndim)
    totals <- lengths(new.starts)

    repeat {
        absolute <- relative <- vector("list", ndim)
        for (i in seq_len(ndim)) {
            j <- current[i]
            relative[[i]] <- new.starts[[i]][j]:new.ends[[i]][j]
            absolute[[i]] <- usdex[[i]][relative[[i]]]
        }

        # Because tiledb_sparse just errors if there are no entries.
        block <- try(do.call("[", c(list(x=obj), absolute, list(drop=FALSE))), silent=TRUE)
        if (!is(block, "try-error")) {
            collected[[length(collected)+1L]] <- list(relative=relative, block=block)
        }

        finished <- TRUE 
        for (i in seq_len(ndim)) {
            current[i] <- current[i] + 1L
            if (current[i] <= totals[i]) {
                finished <- FALSE
                break
            } else {
                current[i] <- 1L
            }
        }
        if (finished) break
    }

    list(collected=collected, usdex=usdex)
}

.extract_noncontiguous_dense <- function(obj, index, fill) {
    contig <- .get_contiguous(obj, index)
    collected <- contig$collected
    usdex <- contig$usdex

    output <- array(fill, dim=lengths(usdex))
    for (i in seq_along(collected)) { 
        current <- collected[[i]]
        output <- do.call("[<-", c(list(x=output), current$relative, list(value=current$block)))
    }

    m <- mapply(match, x=index, table=usdex, SIMPLIFY=FALSE)
    do.call("[", c(list(x=output), m, list(drop=FALSE)))
}

.extract_noncontiguous_sparse <- function(obj, index, fill) {
    contig <- .get_contiguous(obj, index)
    collected <- contig$collected
    usdex <- contig$usdex

    output <- array(fill, dim=lengths(usdex))
    for (i in seq_along(collected)) { 
        current <- collected[[i]]
        df <- current$block
        if (nrow(df)==0) next

        value <- df[,1]
        m <- mapply(match, x=as.list(df[,1L + seq_len(ncol(df)-1L)]), table=usdex)
        output[m] <- value
    }

    m <- mapply(match, x=index, table=usdex, SIMPLIFY=FALSE)
    do.call("[", c(list(x=output), m, list(drop=FALSE)))
}
