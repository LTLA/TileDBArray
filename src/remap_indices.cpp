#include "Rcpp.h"
#include <algorithm>

// [[Rcpp::export(rng=false)]]
Rcpp::List remap_indices(Rcpp::List extracted, Rcpp::List remapping) {
    const size_t n_dims = extracted.size();
    if (n_dims != remapping.size()) {
        throw std::runtime_error("'remapping' and 'extracted' should have the same length");
    }

    /* Setting up a stack of integer vectors for quick reference. We also
     * create the array that maps each observed index to the sequence of
     * requested indices in the extract_array() call.
     */
    std::deque<bool> do_remap(n_dims, true);
    std::vector<Rcpp::IntegerVector> matches(n_dims);
    std::vector<std::vector<std::pair<const int*, int> > > mappings(n_dims);
    std::vector<Rcpp::IntegerVector> requested(n_dims); // this MUST exist to ensure the pointers refer to a realized array.

    for (size_t d = 0; d < n_dims; ++d) {
        Rcpp::IntegerVector cur_extract = extracted[d];

        SEXP current = remapping[d];
        if (current == R_NilValue) {
            // Storing the extracted values directly, as the remapping
            // is that of 1:1 identity anyway.
            matches[d] = cur_extract; 
            do_remap[d] = false;
            continue;
        }

        Rcpp::List as_list(remapping[d]);
        requested[d] = as_list["requested"];

        Rcpp::IntegerVector ref_value = as_list["ref.value"];
        matches[d] = Rcpp::match(Rcpp::noNA(cur_extract), Rcpp::noNA(ref_value));
        for (auto mIt = matches[d].begin(); mIt!=matches[d].end(); ++mIt) {
            --(*mIt); // zero indexing.
        }

        auto& cur_mapping = mappings[d];
        Rcpp::IntegerVector ref_len = as_list["ref.length"];
        cur_mapping.reserve(ref_value.size());

        const int* rqptr=requested[d].begin();
        auto rlIt=ref_len.begin();
        for (auto rvIt=ref_value.begin(); rvIt!=ref_value.end(); ++rvIt, ++rlIt) {
            cur_mapping.push_back(std::make_pair(rqptr, *rlIt));
            rqptr += *rlIt;
        }
    }

    /* Figuring out the total required length of the output, based on the
     * number of entries after expansion of each observed set of indices.
     */
    const size_t n_values = (n_dims ? matches.front().size() : 0);
    Rcpp::IntegerVector n_copies(n_values, 1);

    for (size_t d = 0; d < n_dims; ++d) {
        if (do_remap[d]) {
            auto mIt = matches[d].begin();
            const auto& cur_mapping = mappings[d];
            for (auto ncIt = n_copies.begin(); ncIt != n_copies.end(); ++ncIt, ++mIt) {
                (*ncIt) *= cur_mapping[*mIt].second;
            }
        }
    }

    const size_t n_output = std::accumulate(n_copies.begin(), n_copies.end(), 0);
    Rcpp::IntegerMatrix out_indices(n_output, n_dims);

    /* Expanding each observed set of indices by the series of requested
     * indices. This would be naively done by looping over the index sets, but
     * we do it in a more cache-friendly manner by looping over the dimensions.
     *
     * Expansion can be considered in terms of recycling the sequence of
     * requested indices and tandem repeats of each element of the sequence. In
     * the first dimension, we recycle the entire sequence, and in subsequent
     * dimensions, we switch to recycling individual elements.
     */
    Rcpp::IntegerVector& repeat_series = n_copies;
    Rcpp::IntegerVector repeat_element(n_copies.size(), 1);
    auto outIt = out_indices.begin();

    for (size_t d = 0; d < n_dims; ++d) {
        auto& cur_mapping = mappings[d];
        auto& cur_matches = matches[d];
        auto mIt = cur_matches.begin();

        if (do_remap[d]) {
            for (size_t v = 0; v < n_values; ++v, ++mIt) {
                int& rep_seq = repeat_series[v];
                int& rep_val = repeat_element[v];

                const auto& cur_series = cur_mapping[*mIt];
                const int* ptr = cur_series.first;
                const int len = cur_series.second;
                rep_seq /= len;

                for (auto rs = 0; rs < rep_seq; ++rs) {
                    for (int i = 0; i < len; ++i, outIt += rep_val) {
                        std::fill(outIt, outIt + rep_val, ptr[i]);
                    }
                }

                rep_val *= len;
            }
        } else {
            // Equivalent to a series of length 1, as there's a 1:1 mapping
            // from each observed element back to itself.
            for (size_t v = 0; v < n_values; ++v, ++mIt) {
                const int N = repeat_series[v] * repeat_element[v];
                std::fill(outIt, outIt + N, *mIt);
                outIt += N;
            }
        }
    } 

    return Rcpp::List::create(
        Rcpp::Named("indices") = out_indices,
        Rcpp::Named("expand") = repeat_element
    );
}
