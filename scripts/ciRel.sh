#!/bin/bash

## -- Env setting for no suggested packages, and unattended Debian package upgrades
export _R_CHECK_FORCE_SUGGESTS_=false
export DEBIAN_FRONTEND=noninteractive

## -- Determine current package name and version
PKG_NAME=$(awk '/Package:/ {print $2}' DESCRIPTION)
PKG_VER=$(awk '/Version:/ {print $2}' DESCRIPTION)
PKG_TGZ="${PKG_NAME}_${PKG_VER}.tar.gz"

## -- Update packages and install build dependencies
apt update -qq
apt upgrade -q -y
apt install -y --no-install-recommends r-cran-rcpp r-cran-matrix r-cran-lattice r-cran-testthat r-cran-bit64 r-cran-xts

## DelayedArray and S4Vectors in R are too old
install.r BiocManager
installBioc.r DelayedArray

## -- install tiledb (plus remaining R package dependencies from nanotime)
install.r tiledb

## -- now that everything is prepared, create a tarball and check it
R CMD build --no-manual --no-build-vignettes .
R CMD check --no-manual --no-vignettes ${PKG_TGZ}
