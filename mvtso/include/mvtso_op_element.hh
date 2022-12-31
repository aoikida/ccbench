#pragma once

#include "version.hh"

template<typename T>
class ReadElement : public OpElement<T> {
public:
  using OpElement<T>::OpElement;

  Version *later_ver_, *ver_;

  ReadElement(uint64_t key, T *rcdptr, Version *later_ver, Version *ver)
          : OpElement<T>::OpElement(key, rcdptr) {
    later_ver_ = later_ver;
    ver_ = ver;
  }

  bool operator<(const ReadElement &right) const {
    return this->key_ < right.key_;
  }
};

template<typename T>
class WriteElement : public OpElement<T> {
public:
  using OpElement<T>::OpElement;

  Version *later_ver_, *new_ver_;
  bool finish_version_install_;

  WriteElement(uint64_t key, T *rcdptr, Version *later_ver, Version *new_ver)
          : OpElement<T>::OpElement(key, rcdptr) {
    later_ver_ = later_ver;
    new_ver_ = new_ver;
    finish_version_install_ = false;
  }

  bool operator<(const WriteElement &right) const {
    return this->key_ < right.key_;
  }
};