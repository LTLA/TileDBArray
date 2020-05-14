.globals <- (function () {
    current <- list(path=NULL, attr=NULL, sparse=NULL, tile=NULL, ctx=NULL)
    list(
        get=function(x) current[[x]],
        set=function(x, value) current[[x]] <<- value
    )
})()

#' TileDBArray global options
#'
#' Global options for (mostly) writing of TileDBArray backends.
#'
#' @param path String containing a path to a TileDB backend.
#' @param attr String containing the name of a TileDB attribute.
#' @param sparse Logical scalar indicating whether sparse backend should be created.
#' @param extent Integer scalar specifying the tile extent for all dimensions.
#' Alternatively, an integer vector of length equal to the number of dimensions,
#' specifying a different extent for each dimension in the array to be created.
#' @param context A TileDB context object, see \code{\link{tiledb_ctx}} for an example.
#'
#' @return
#' All of the getter functions return the current global value,
#' or a default value if the former is \code{NULL}:
#' \itemize{
#' \item \code{path} defaults to a temporary file in \code{\link{tempdir}}.
#' \item \code{attr} defaults to \code{"x"}.
#' \item \code{sparse} defaults to \code{FALSE}.
#' \item \code{extent} defaults to \code{100L}.
#' \item \code{context} defaults to the value of \code{\link{tiledb_ctx}()}.
#' }
#' 
#' All setter functions change the global value and return \code{NULL} invisibly.
#'
#' @author Aaron Lun
#' 
#' @seealso \code{\link{writeTileDBArray}}, where these functions are most often used.
#'
#' @name TileDBArray-globals
NULL

#' @export
#' @rdname TileDBArray-globals
getTileDBPath <- function() {
    if (is.null(path <- .globals$get("path"))) {
        tempfile()
    } else {
        path
    }
}

#' @export
#' @rdname TileDBArray-globals
setTileDBPath <- function(path=NULL) {
    .globals$set("path", path)
    invisible(NULL)
}

#' @export
#' @rdname TileDBArray-globals
getTileDBSparse <- function() {
    if (is.null(sparse <- .globals$get("sparse"))) {
        FALSE
    } else {
        sparse
    }
}

#' @export
#' @rdname TileDBArray-globals
setTileDBSparse <- function(sparse=NULL) {
    .globals$set("sparse", sparse)
    invisible(NULL)
}

#' @export
#' @rdname TileDBArray-globals
getTileDBAttr <- function() {
    if (is.null(attr <- .globals$get("attr"))) {
        "x" 
    } else {
        attr
    }
}

#' @export
#' @rdname TileDBArray-globals
setTileDBAttr <- function(attr=NULL) {
    .globals$set("attr", attr)
    invisible(NULL)
}

#' @export
#' @rdname TileDBArray-globals
getTileDBExtent <- function() {
    if (is.null(extent <- .globals$get("extent"))) {
        100L 
    } else {
        extent
    }
}

#' @export
#' @rdname TileDBArray-globals
setTileDBExtent <- function(extent=NULL) {
    .globals$set("extent", extent)
    invisible(NULL)
}

#' @export
getTileDBContext <- function() {
    if (is.null(context <- .globals$get("context"))) {
        tiledb_ctx()
    } else {
        context
    }
}

#' @export
#' @rdname TileDBArray-globals
setTileDBContext <- function(context=NULL) {
    .globals$set("context", context)
    invisible(NULL)
}
