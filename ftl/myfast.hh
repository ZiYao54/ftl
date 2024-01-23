#ifndef __FTL_FAST_MAPPING__
#define __FTL_FAST_MAPPING__

#include <cinttypes>
#include <unordered_map>
#include <vector>
#include <queue>


#include "ftl/abstract_ftl.hh"
#include "ftl/common/block.hh"
#include "ftl/ftl.hh"
#include "pal/pal.hh"

namespace SimpleSSD {

namespace FTL {

class FastMapping : public AbstractFTL {
 private:
  PAL::PAL *pPAL;

  ConfigReader &conf;

  std::unordered_map<uint32_t, uint32_t> table; // block level mapping: lbn -> pbn
  std::unordered_map<uint64_t, std::pair<uint32_t, uint32_t>> RWTable; 
  std::unordered_map<uint32_t, Block> blocks;
  std::list<Block> freeBlocks;
  uint32_t nFreeBlocks;  
  uint32_t nRWBlocks = 6;
  std::unordered_map<uint32_t, Block> RWBlocks; 
  std::vector<uint32_t> lastFreeBlock;
  uint32_t lastFreeBlockIndex;

  uint32_t bitsetSize;
  
  float freeBlockRatio();
  uint32_t getFreeBlock();
  uint32_t getFreeRWBlock();
  uint32_t getLastFreeBlock();
  void doGarbageCollection(uint32_t , uint64_t &);

  void calculateTotalPages(uint64_t &, uint64_t &);

  void readInternal(Request &, uint64_t &);
  void writeInternal(Request &, uint64_t &, bool = true);
  void eraseInternal(PAL::Request &, uint64_t &);

 public:
  FastMapping(ConfigReader &, Parameter &, PAL::PAL *, DRAM::AbstractDRAM *);
  ~FastMapping();

  bool initialize() override;

  void read(Request &, uint64_t &) override;
  void write(Request &, uint64_t &) override;
  void trim(Request &, uint64_t &) override{}
  void format(LPNRange &, uint64_t &) override{}

  Status *getStatus(uint64_t, uint64_t) override;

  void getStatList(std::vector<Stats> &, std::string) override{}
};

}  // namespace FTL

}  // namespace SimpleSSD

#endif
