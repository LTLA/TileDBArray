#' @export
setClass("DenseTileDBArraySeed", slots=c(dim="integer", dimnames="list", .array="tiledb_dense"))

#' @export
#' @importClassesFrom DelayedArray DelayedMatrix
setClass("DenseTileDBMatrix", contains="DelayedMatrix", slots=c(seed="DenseTileDBArraySeed"))
