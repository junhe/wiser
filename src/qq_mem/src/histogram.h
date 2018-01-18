#ifndef HISTOGRAM_H
#define HISTOGRAM_H

#include <grpc/support/histogram.h>

// This original implementation of this class is in GRPC
// grpc/test/cpp/qps/histogram.h
//
// I think this class is not thread-safe. So, use one per thread
// and merge them later.
//
// The bucket range grows exponetially, check the following file
// for details
// https://github.com/grpc/grpc/blob/764ef8c7971e433bcea7196f0f80b0d1f83adbad/test/core/util/histogram.cc
class Histogram {
 public:
  Histogram()
      : impl_(gpr_histogram_create(default_resolution(),
                                   default_max_possible())) {}
  ~Histogram() {
    if (impl_) gpr_histogram_destroy(impl_);
  }
  Histogram(Histogram&& other) : impl_(other.impl_) { other.impl_ = nullptr; }

  void Merge(const Histogram& h) { gpr_histogram_merge(impl_, h.impl_); }
  void Add(double value) { gpr_histogram_add(impl_, value); }
  double Percentile(double pctile) const {
    return gpr_histogram_percentile(impl_, pctile);
  }
  double Count() const { return gpr_histogram_count(impl_); }
  void Swap(Histogram* other) { std::swap(impl_, other->impl_); }

  static double default_resolution() { return 0.01; }
  static double default_max_possible() { return 60e9; }

 private:
  Histogram(const Histogram&);
  Histogram& operator=(const Histogram&);

  gpr_histogram* impl_;
};

#endif

