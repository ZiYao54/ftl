#include "ftl/myfast.hh"

#include <algorithm>
#include <limits>
#include <random>

#include "util/algorithm.hh"
#include "util/bitset.hh"

namespace SimpleSSD {

namespace FTL {

FastMapping::FastMapping(ConfigReader &c, Parameter &p, PAL::PAL *l,
                         DRAM::AbstractDRAM *d)
    : AbstractFTL(p, l, d),
      pPAL(l),
      conf(c),
      lastFreeBlock(nRWBlocks)
      {
  blocks.reserve(param.totalPhysicalBlocks);
  table.reserve(param.totalLogicalBlocks);
  RWBlocks.reserve(nRWBlocks);

  for (uint32_t i = 0; i < param.totalPhysicalBlocks; i++) {
    freeBlocks.emplace_back(Block(i, param.pagesInBlock, param.ioUnitInPage));
  }

  nFreeBlocks = param.totalPhysicalBlocks;

  status.totalLogicalPages = param.totalLogicalBlocks * param.pagesInBlock;

  lastFreeBlockIndex = 0;
  lastFreeBlock.at(lastFreeBlockIndex) =  getFreeRWBlock();


  bitsetSize = 1;
}

FastMapping::~FastMapping() {}

bool FastMapping::initialize() {
  uint64_t nPagesToWarmup;
  uint64_t nPagesToInvalidate;
  uint64_t nTotalLogicalPages;
  uint64_t tick;
  uint64_t valid;
  uint64_t invalid;
  FILLING_MODE mode;

  Request req(param.ioUnitInPage);

  debugprint(LOG_FTL_FAST_MAPPING, "Initialization started");

  nTotalLogicalPages = param.totalLogicalBlocks * param.pagesInBlock;
  nPagesToWarmup =
      nTotalLogicalPages * conf.readFloat(CONFIG_FTL, FTL_FILL_RATIO);
  nPagesToInvalidate =
      nTotalLogicalPages * conf.readFloat(CONFIG_FTL, FTL_INVALID_PAGE_RATIO);
  mode = (FILLING_MODE)conf.readUint(CONFIG_FTL, FTL_FILLING_MODE);

  debugprint(LOG_FTL_FAST_MAPPING, "Total logical pages: %" PRIu64,
             nTotalLogicalPages);
  debugprint(LOG_FTL_FAST_MAPPING,
             "Total logical pages to fill: %" PRIu64 " (%.2f %%)",
             nPagesToWarmup, nPagesToWarmup * 100.f / nTotalLogicalPages);
  debugprint(LOG_FTL_FAST_MAPPING,
             "Total invalidated pages to create: %" PRIu64 " (%.2f %%)",
             nPagesToInvalidate,
             nPagesToInvalidate * 100.f / nTotalLogicalPages);

  req.ioFlag.set();

  // Step 1. Filling
  if (mode == FILLING_MODE_0 || mode == FILLING_MODE_1) {
    // Sequential
    for (uint64_t i = 0; i < nPagesToWarmup; i++) {
      tick = 0;
      req.lpn = i;
      writeInternal(req, tick, false);
    }
  }
  else {
    // Random
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, nTotalLogicalPages - 1);

    for (uint64_t i = 0; i < nPagesToWarmup; i++) {
      tick = 0;
      req.lpn = dist(gen);
      writeInternal(req, tick, false);
    }
  }

  // Step 2. Invalidating
  if (mode == FILLING_MODE_0) {
    // Sequential
    for (uint64_t i = 0; i < nPagesToInvalidate; i++) {
      tick = 0;
      req.lpn = i;
      writeInternal(req, tick, false);
    }
  }
  else if (mode == FILLING_MODE_1) {
    // Random
    // We can successfully restrict range of LPN to create exact number of
    // invalid pages because we wrote in sequential mannor in step 1.
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, nPagesToWarmup - 1);

    for (uint64_t i = 0; i < nPagesToInvalidate; i++) {
      tick = 0;
      req.lpn = dist(gen);
      writeInternal(req, tick, false);
    }
  }
  else {
    // Random
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, nTotalLogicalPages - 1);

    for (uint64_t i = 0; i < nPagesToInvalidate; i++) {
      tick = 0;
      req.lpn = dist(gen);
      writeInternal(req, tick, false);
    }
  }

  // Report
  calculateTotalPages(valid, invalid);
  debugprint(LOG_FTL_FAST_MAPPING, "Filling finished. Page status:");
  debugprint(LOG_FTL_FAST_MAPPING,
             "  Total valid physical pages: %" PRIu64
             " (%.2f %%, target: %" PRIu64 ", error: %" PRId64 ")",
             valid, valid * 100.f / nTotalLogicalPages, nPagesToWarmup,
             (int64_t)(valid - nPagesToWarmup));
  debugprint(LOG_FTL_FAST_MAPPING,
             "  Total invalid physical pages: %" PRIu64
             " (%.2f %%, target: %" PRIu64 ", error: %" PRId64 ")",
             invalid, invalid * 100.f / nTotalLogicalPages, nPagesToInvalidate,
             (int64_t)(invalid - nPagesToInvalidate));
  debugprint(LOG_FTL_FAST_MAPPING, "Initialization finished");

  return true;
}

void FastMapping::read(Request &req, uint64_t &tick) {
  uint64_t begin = tick;

  if (req.ioFlag.count() > 0) {
    readInternal(req, tick);

    debugprint(LOG_FTL_FAST_MAPPING,
               "READ  | LPN %" PRIu64 " | %" PRIu64 " - %" PRIu64 " (%" PRIu64
               ")",
               req.lpn, begin, tick, tick - begin);
  }
  else {
    warn("FTL got empty request");
  }
}

void FastMapping::write(Request &req, uint64_t &tick) {
  uint64_t begin = tick;

  if (req.ioFlag.count() > 0) {
    writeInternal(req, tick);

    debugprint(LOG_FTL_FAST_MAPPING,
               "WRITE | LPN %" PRIu64 " | %" PRIu64 " - %" PRIu64 " (%" PRIu64
               ")",
               req.lpn, begin, tick, tick - begin);
  }
  else {
    warn("FTL got empty request");
  }
}

Status *FastMapping::getStatus(uint64_t lpnBegin, uint64_t lpnEnd) {
  status.freePhysicalBlocks = nFreeBlocks;

  if (lpnBegin == 0 && lpnEnd >= status.totalLogicalPages) {
    status.mappedLogicalPages = table.size();
  }
  else {
    status.mappedLogicalPages = 0;

    for (uint64_t lpn = lpnBegin; lpn < lpnEnd; lpn++) {
      if (table.count(lpn) > 0) {
        status.mappedLogicalPages++;
     }
    }
  }

  return &status;
}

float FastMapping::freeBlockRatio() {
  return (float)nFreeBlocks / param.totalPhysicalBlocks;
}


uint32_t FastMapping::getFreeBlock() {
  uint32_t blockIndex = 0;

  if (nFreeBlocks > 0) {
    // Search block which is blockIdx % param.pageCountToMaxPerf == idx
    auto iter = freeBlocks.begin();
    blockIndex = iter->getBlockIndex();

    // Insert found block to block list
    if (blocks.find(blockIndex) != blocks.end()) {
      panic("Corrupted");
    }

    blocks.emplace(blockIndex, std::move(*iter));

    // Remove found block from free block list
    freeBlocks.erase(iter);
    nFreeBlocks--;
  }
  else {
    panic("No free block left");
  }

  return blockIndex;
}

uint32_t FastMapping::getFreeRWBlock() {
  uint32_t blockIndex = 0;

  if (nFreeBlocks > 0) {
    auto iter = freeBlocks.begin();
    blockIndex = iter->getBlockIndex();

    // Insert found block to block list
    if (RWBlocks.find(blockIndex) != RWBlocks.end()) {
      panic("Corrupted");
    }
    
    RWBlocks.emplace(blockIndex, std::move(*iter));

    // Remove found block from free block list
    freeBlocks.erase(iter);
    nFreeBlocks--;
  }
    else {
      panic("No free block left");
    }
  
    return blockIndex;
  }

uint32_t FastMapping::getLastFreeBlock() {
  return lastFreeBlock.at(lastFreeBlockIndex);
}

void FastMapping::doGarbageCollection(uint32_t blocksToReclaim, uint64_t &tick) {
  PAL::Request req(param.ioUnitInPage);
  std::vector<PAL::Request> readRequests;
  std::vector<PAL::Request> writeRequests;
  std::vector<PAL::Request> eraseRequests;
  std::vector<uint64_t> lpns;
  Bitset bit(param.ioUnitInPage);
  uint64_t beginAt;
  uint64_t readFinishedAt = tick;
  uint64_t writeFinishedAt = tick;
  uint64_t eraseFinishedAt = tick;

  auto block = RWBlocks.find(blocksToReclaim);

  if (block == RWBlocks.end()) { panic("Invalid block");}

  for (uint32_t pageIndex = 0; pageIndex < param.pagesInBlock; pageIndex++) {
    if (block->second.getPageInfo(pageIndex, lpns, bit) && block->second.checkValidBit(pageIndex, 0)) {
      bit.set();

      uint32_t LPN = lpns.at(0);
      uint32_t LBN = LPN / param.pagesInBlock;

      auto oldMappingList = table.find(LBN);
      
        
      uint32_t newBlockIdx = getFreeBlock();
      auto newBlock = blocks.find(newBlockIdx);

      uint32_t blockLevelPageOffset = LBN * param.pagesInBlock;
      if(oldMappingList == table.end()){
        
        for(uint32_t oldBlockPageIndex = 0; oldBlockPageIndex < param.pagesInBlock; oldBlockPageIndex++){
          LPN = blockLevelPageOffset + oldBlockPageIndex;
          
          if(RWTable.find(LPN) == RWTable.end()){
            //invalidate free block
            uint32_t newPageIdx = newBlock->second.getNextWritePageIndex(0);
            newBlock->second.write(newPageIdx, LPN, 0, beginAt);
            newBlock->second.invalidate(newPageIdx, 0);
            continue;
          }
          auto tempBlock = RWBlocks.find(RWTable.find(LPN)->second.first); //pbn -> block
          auto tempPageIndex = RWTable.find(LPN)->second.second; //offset
          RWTable.erase(RWTable.find(LPN));

          tempBlock->second.getPageInfo(tempPageIndex, lpns, bit);
          
          req.blockIndex = tempBlock->first;
          req.pageIndex = tempPageIndex;
          req.ioFlag = bit;
          readRequests.push_back(req);

          tempBlock->second.invalidate(tempPageIndex, 0);

          uint32_t newPageIdx = newBlock->second.getNextWritePageIndex(0);
          if(newPageIdx != oldBlockPageIndex){
            panic("corrupted in garbage collection 2");
          }

          newBlock->second.write(newPageIdx, LPN, 0, beginAt);

          req.blockIndex = newBlockIdx;
          req.pageIndex = newPageIdx;
          req.ioFlag.set();
          writeRequests.push_back(req);
        }

        table.emplace(LBN, newBlockIdx);
        
        if(block->second.checkValidBit(pageIndex, 0)){
          panic("valid bit error in garbage collection");
        }
      }
      else{
        auto oldBlock = blocks.find(oldMappingList->second);
        for(uint32_t oldBlockPageIndex = 0; oldBlockPageIndex < param.pagesInBlock; oldBlockPageIndex++){
          LPN = blockLevelPageOffset + oldBlockPageIndex;
          auto tempBlock = oldBlock;
          auto tempPageIndex = oldBlockPageIndex;
          if(!oldBlock->second.checkValidBit(oldBlockPageIndex, 0)){

            if(RWTable.find(LPN) == RWTable.end()){ 
              uint32_t newPageIdx = newBlock->second.getNextWritePageIndex(0);
              newBlock->second.write(newPageIdx, LPN, 0, beginAt);
              newBlock->second.invalidate(newPageIdx, 0);
              continue;
            }
            tempBlock = RWBlocks.find(RWTable.find(LPN)->second.first); //pbn -> block
            tempPageIndex = RWTable.find(LPN)->second.second; //offset
            RWTable.erase(RWTable.find(LPN));
          }
          tempBlock->second.getPageInfo(tempPageIndex, lpns, bit);
          
          req.blockIndex = tempBlock->first;
          req.pageIndex = tempPageIndex;
          req.ioFlag = bit;
          readRequests.push_back(req);

          tempBlock->second.invalidate(tempPageIndex, 0);

          uint32_t newPageIdx = newBlock->second.getNextWritePageIndex(0);
          if(newPageIdx != oldBlockPageIndex){
            panic("corrupted in garbage collection 2");
          }


          newBlock->second.write(newPageIdx, LPN, 0, beginAt);
          req.blockIndex = newBlockIdx;
          req.pageIndex = newPageIdx;
          req.ioFlag.set();
          writeRequests.push_back(req);
        }

        req.blockIndex = oldBlock->first;
        req.pageIndex = 0;
        req.ioFlag.set();
        eraseRequests.push_back(req);

        auto &dataBlockIndex = table.find(LBN)->second;
        dataBlockIndex = newBlockIdx;
        
        if(block->second.checkValidBit(pageIndex, 0)){
          panic("valid bit error in garbage collection");
        }
      }
    }
  }
  req.blockIndex = block->first;
  req.pageIndex = 0;
  req.ioFlag.set();
  eraseRequests.push_back(req);
  
  for (auto &iter : readRequests) {
    beginAt = tick;
    pPAL->read(iter, beginAt);
    readFinishedAt = MAX(readFinishedAt, beginAt);
  }

  for (auto &iter : writeRequests) {
    beginAt = readFinishedAt;
    pPAL->write(iter, beginAt);
    writeFinishedAt = MAX(writeFinishedAt, beginAt);
  }

  for (auto &iter : eraseRequests) {
    beginAt = readFinishedAt;
    eraseInternal(iter, beginAt);
    eraseFinishedAt = MAX(eraseFinishedAt, beginAt);
  }

  tick = MAX(writeFinishedAt, eraseFinishedAt);
}

void FastMapping::readInternal(Request &req, uint64_t &tick) {
    PAL::Request palRequest(req);
  uint64_t beginAt;
  uint64_t finishedAt = tick;

  uint32_t LPN = req.lpn;
  uint32_t LBN = LPN / param.pagesInBlock;
  
  uint32_t OFFSET =0;
  uint32_t PBN ;
  std::unordered_map<uint32_t, SimpleSSD::FTL::Block>::iterator block ;

  auto mappingList = table.find(LBN);
  auto logMappingList = RWTable.find(LPN);

  if(mappingList == table.end() && logMappingList == RWTable.end()){
    return ;
  }
  bool found = false;
  
  if(logMappingList != RWTable.end()){
    PBN = logMappingList->second.first;
    OFFSET = logMappingList->second.second;
    block = RWBlocks.find(PBN);
    if(block->second.checkValidBit(OFFSET, 0)){
      found = true;
    }
  }
  if(mappingList != table.end() && !found){
    PBN = mappingList->second;
    OFFSET = LPN % param.pagesInBlock;
    block = blocks.find(PBN);
    if(block->second.checkValidBit(OFFSET, 0)){
      found = true;
    }
  }
  if(!found){
    return;
  }
  
  palRequest.blockIndex = PBN;
  palRequest.pageIndex = OFFSET;
  palRequest.ioFlag.set();

  beginAt = tick;

  block->second.read(palRequest.pageIndex, 0, beginAt);
  pPAL->read(palRequest, beginAt);

  finishedAt = MAX(finishedAt, beginAt);

  tick = finishedAt;
}

void FastMapping::writeInternal(Request &req, uint64_t &tick, bool sendToPAL) {
  PAL::Request palRequest(req);
  
  uint32_t LPN = req.lpn;
  uint32_t LBN = LPN / param.pagesInBlock;
  
  uint32_t OFFSET; 
  uint32_t PBN;
  std::unordered_map<uint32_t, Block>::iterator block;
  
  auto mappingList = table.find(LBN); 
  auto logMappingList = RWTable.find(LPN);
  uint64_t beginAt;
  uint64_t finishedAt = tick;

    if(logMappingList != RWTable.end()){ 
    PBN = logMappingList->second.first;
    OFFSET = logMappingList->second.second;
    block = RWBlocks.find(PBN);
    block->second.invalidate(OFFSET, 0);
  }
  else if (mappingList != table.end()) { 
    PBN = mappingList->second;
    OFFSET = LPN % param.pagesInBlock;
    block = blocks.find(PBN);
    

    block->second.invalidate(OFFSET, 0);

  }
  block = RWBlocks.find(getLastFreeBlock());

  if (block == RWBlocks.end()) {
    panic("No such block");
  }


  uint32_t pageIndex = block->second.getNextWritePageIndex(0);
  

  beginAt = tick;

  block->second.write(pageIndex, req.lpn, 0, beginAt);

  if(logMappingList == RWTable.end()){
    RWTable.emplace(LPN, std::pair<uint32_t, uint32_t>(block->first, pageIndex));
  }
  else{
    auto &mapping = logMappingList->second;
    mapping.first = block->first;
    mapping.second = pageIndex;
  }

    if (sendToPAL) {
      palRequest.blockIndex = block->first;
      palRequest.pageIndex = pageIndex;
      palRequest.ioFlag.set();
      pPAL->write(palRequest, beginAt);
    }

    finishedAt = MAX(finishedAt, beginAt);
  if (sendToPAL) {
    tick = finishedAt;
  }

  if(block->second.getNextWritePageIndex(0) == param.pagesInBlock){
    lastFreeBlockIndex = (lastFreeBlockIndex + 1) % nRWBlocks;

    if(RWBlocks.size() >= nRWBlocks){
    uint64_t beginAt = tick;
    debugprint(LOG_FTL_FAST_MAPPING,
               "GC   | On-demand | %u blocks will be reclaimed", 1);

    doGarbageCollection(lastFreeBlock.at(lastFreeBlockIndex), beginAt);

    debugprint(LOG_FTL_FAST_MAPPING,
               "GC   | Done | %" PRIu64 " - %" PRIu64 " (%" PRIu64 ")", tick,
               beginAt, beginAt - tick);
  }
  lastFreeBlock.at(lastFreeBlockIndex) = getFreeRWBlock();
  }
}

void FastMapping::eraseInternal(PAL::Request &req, uint64_t &tick) {
  static uint64_t threshold = conf.readUint(CONFIG_FTL, FTL_BAD_BLOCK_THRESHOLD);
  bool isLogBlock = false;
  auto block = blocks.find(req.blockIndex);

  // Sanity checks
  if (block == blocks.end()) {  
    block = RWBlocks.find(req.blockIndex);
    isLogBlock = true;
    if(block == RWBlocks.end()){
    panic("No such block");
    }
  }

  if (block->second.getValidPageCount() != 0) {
    panic("There are valid pages in victim block");
  }

  // Erase block
  block->second.erase();

  pPAL->erase(req, tick);

  // Check erase count
  uint32_t erasedCount = block->second.getEraseCount();

  if (erasedCount < threshold) {
    // Reverse search
    auto iter = freeBlocks.end();

    while (true) {
      iter--;

      if (iter->getEraseCount() <= erasedCount) {
        // emplace: insert before pos
        iter++;

        break;
      }

      if (iter == freeBlocks.begin()) {
        break;
      }
    }

    // Insert block to free block list
    freeBlocks.emplace(iter, std::move(block->second));
    nFreeBlocks++;
  }

  // Remove block from block list
  if(isLogBlock){
      RWBlocks.erase(block);
  }
  else{
    blocks.erase(block);
  }

}

void FastMapping::calculateTotalPages(uint64_t &valid, uint64_t &invalid) {
  valid = 0;
  invalid = 0;

  for (auto &iter : blocks) {
    valid += iter.second.getValidPageCount();
    invalid += iter.second.getDirtyPageCount();
  }
}

}  // namespace FTL

}  // namespace SimpleSSD
