#include "concurrency/watermark.h"
#include <algorithm>
#include <exception>
#include <type_traits>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  auto iter = current_reads_.find(read_ts);
  if(iter!= current_reads_.end())
  {
    iter->second++;
  }
  else {
    current_reads_.emplace(read_ts,1);
  }
  if(!current_reads_.empty()){
    watermark_ = current_reads_.begin()->first;
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  auto iter = current_reads_.find(read_ts);
  if(iter!= current_reads_.end())
  {
    if(iter->second > 1){
      iter->second--;
    }
    else {
      current_reads_.erase(iter);
    }
  }
  else {
    throw Exception("read_ts not exist");
  }

  if(!current_reads_.empty()){
    watermark_ = current_reads_.begin()->first;
  }
}

}  // namespace bustub
