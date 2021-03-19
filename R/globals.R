.globals <- (function () {
    current <- list(path=NULL, attr=NULL, sparse=NULL, tile=NULL, cellorder=NULL, ctx=NULL)
    list(
        get=function(x) current[[x]],
        set=function(x, value) current[[x]] <<- value
    )
})()

#' TileDBArray global options
#'
#' Global options for writing TileDBArray backends,
#' intended for parameters that cannot be automatically derived from the data.
#'
#' @param path String containing a path to a TileDB backend.
#' @param attr String containing the name of a TileDB attribute.
#' @param extent Integer scalar specifying the tile extent for all dimensions.
#' Alternatively, an integer vector of length equal to the number of dimensions,
#' specifying a different extent for each dimension in the array to be created.
#' @param cellorder String specifying the desired cell order.
#' @param tileorder String specifying the desired tile order.
#' @param capacity Integer scalar specifying the data tile capacity for sparse arrays.
#' @param context A TileDB context object, see \code{\link{tiledb_ctx}} for an example.
#'
#' @return
#' All of the getter functions return the current global value,
#' or a default value if the former is \code{NULL}:
#' \itemize{
#' \item \code{path} defaults to a temporary file in \code{\link{tempdir}}.
#' \item \code{attr} defaults to \code{"x"}.
#' \item \code{extent} defaults to \code{100L}.
#' \item \code{cellorder} defaults to \code{"COL_MAJOR"}.
#' \item \code{tileorder} defaults to \code{"COL_MAJOR"}.
#' \item \code{capacity} defaults to \code{10000L}.
#' \item \code{context} defaults to the value of \code{\link{tiledb_ctx}()}.
#' }
#' 
#' All setter functions change the global value and return \code{NULL} invisibly.
#'
#' @author Aaron Lun
#' @examples
#' \dontshow{old <- getTileDBPath()}
#'
#' setTileDBPath("my_local_dir")
#' getTileDBPath()
#'
#' \dontshow{setTileDBPath(old)}
#' 
#' @seealso \code{\link{writeTileDBArray}}, where these functions are most often used.
#'
#' @name TileDBArray-globals
NULL

#' @export
#' @rdname TileDBArray-globals
getTileDBPath <- function() {
    .globals$get("path")
}

#' @export
#' @rdname TileDBArray-globals
setTileDBPath <- function(path=NULL) {
    .globals$set("path", path)
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
#' @rdname TileDBArray-globals
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

#' @export
#' @rdname TileDBArray-globals
getTileDBCellOrder <- function() {
    if (is.null(cellorder <- .globals$get("cellorder"))) {
        "COL_MAJOR"
    } else {
        cellorder
    }
}

#' @export
#' @rdname TileDBArray-globals
setTileDBCellOrder <- function(cellorder=NULL) {
    .globals$set("cellorder", cellorder)
    invisible(NULL)
}

#' @export
#' @rdname TileDBArray-globals
getTileDBTileOrder <- function() {
    if (is.null(tileorder <- .globals$get("tileorder"))) {
        "COL_MAJOR"
    } else {
        tileorder
    }
}

#' @export
#' @rdname TileDBArray-globals
setTileDBTileOrder <- function(tileorder=NULL) {
    .globals$set("tileorder", tileorder)
    invisible(NULL)
}

#' @export
#' @rdname TileDBArray-globals
getTileDBCapacity <- function() {
    if (is.null(capacity <- .globals$get("capacity"))) {
        10000L  
    } else {
        capacity 
    }
}

#' @export
#' @rdname TileDBArray-globals
setTileDBCapacity <- function(capacity=NULL) {
    .globals$set("capacity", capacity)
    invisible(NULL)
}
