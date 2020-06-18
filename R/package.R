#' The \pkg{TileDBArray} package
#' 
#' Implements the TileDB framework as a \linkS4class{DelayedArray} backend,
#' with read and write functionality for both dense and sparse arrays.
#' Currently only integer, logical and double-precision values are supported.
#'
#' @author Aaron Lun
#' @name TileDBArray-pkg
#' @import tiledb
#' @import DelayedArray
#' @import methods
#' @importFrom Rcpp sourceCpp
#' @useDynLib TileDBArray
NULL
