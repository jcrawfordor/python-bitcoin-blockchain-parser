# Copyright (C) 2015-2016 The bitcoin-blockchain-parser developers
#
# This file is part of bitcoin-blockchain-parser.
#
# It is subject to the license terms in the LICENSE file found in the top-level
# directory of this distribution.
#
# No part of bitcoin-blockchain-parser, including this file, may be copied,
# modified, propagated, or distributed except according to the terms contained
# in the LICENSE file.

import os
import mmap
import struct
import pickle
import stat
import plyvel
import bitcoin.core

from .block import Block
from .index import DBBlockIndex
from .utils import format_hash, decode_varint


# Constant separating blocks in the .blk files
BITCOIN_CONSTANT = b"\xf9\xbe\xb4\xd9"


def get_files(path):
    """
    Given the path to the .bitcoin directory, returns the sorted list of .blk
    files contained in that directory
    """
    if not stat.S_ISDIR(os.stat(path)[stat.ST_MODE]):
        return [path]
    files = os.listdir(path)
    files = [f for f in files if f.startswith("blk") and f.endswith(".dat")]
    files = map(lambda x: os.path.join(path, x), files)
    return sorted(files)


def get_blocks(blockfile):
    """
    Given the name of a .blk file, for every block contained in the file,
    yields its raw hexadecimal value
    """
    with open(blockfile, "rb") as f:
        if os.name == 'nt':
            size = os.path.getsize(f.name)
            raw_data = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
        else:
            # Unix-only call, will not work on Windows, see python doc.
            raw_data = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        length = len(raw_data)
        offset = 0
        block_count = 0
        while offset < (length - 4):
            if raw_data[offset:offset+4] == BITCOIN_CONSTANT:
                offset += 4
                size = struct.unpack("<I", raw_data[offset:offset+4])[0]
                offset += 4 + size
                block_count += 1
                yield raw_data[offset-size:offset]
            else:
                offset += 1
        raw_data.close()


def get_block(blockfile, offset):
    """Extracts a single block from the blockfile at the given offset"""
    with open(blockfile, "rb") as f:
        f.seek(offset - 4)  # Size is present 4 bytes before the db offset
        size, = struct.unpack("<I", f.read(4))
        return f.read(size)


class Blockchain(object):
    """Represent the blockchain contained in the series of .blk files
    maintained by bitcoind.
    """

    def __init__(self, path, cache=None):
        self.path = os.path.join(path, 'blocks')
        self.blockIndexes = None
        self.cache = cache
        self.blockIndexes = self._build_block_index()
        self.blockIndexDb = plyvel.DB(os.path.join(path, 'blocks', 'index'), compression=None)
        self.txIndexDb = plyvel.DB(os.path.join(path, 'indexes', 'txindex'), compression=None)

    def get_unordered_blocks(self):
        """Yields the blocks contained in the .blk files as is,
        without ordering them according to height.
        """
        for blk_file in get_files(self.path):
            for raw_block in get_blocks(blk_file):
                yield Block(raw_block, parent=self)

    def _index_confirmed(self, chain_indexes, num_confirmations=6):
        """Check if the first block index in "chain_indexes" has at least
        "num_confirmation" (6) blocks built on top of it.
        If it doesn't it is not confirmed and is an orphan.
        """

        # chains holds a 2D list of sequential block hash chains
        # as soon as there an element of length num_confirmations,
        # we can make a decision about whether or not the block in question
        # is confirmed by checking if it's hash is in that list
        chains = []
        # this is the block in question
        first_block = None

        # loop through all future blocks
        for i, index in enumerate(chain_indexes):
            # if this block doesn't have data don't confirm it
            if index.file == -1 or index.data_pos == -1:
                return False

            # parse the block
            blkFile = os.path.join(self.path, "blk%05d.dat" % index.file)
            block = Block(get_block(blkFile, index.data_pos), parent=self)

            if i == 0:
                first_block = block

            chains.append([block.hash])

            for chain in chains:
                # if this block can be appended to an existing block in one
                # of the chains, do it
                if chain[-1] == block.header.previous_block_hash:
                    chain.append(block.hash)

                # if we've found a chain length == num_dependencies (usually 6)
                # we are ready to make a decesion on whether or not the block
                # belongs to a fork or the main chain
                if len(chain) == num_confirmations:
                    if first_block.hash in chain:
                        return True
                    else:
                        return False

    def get_transaction(self, txid):
        """Finds transaction info from LevelDB for transaction with given
        txid (as string)"""
        txInfo = self.txIndexDb.get(b't' + bitcoin.core.lx(txid))
        fileNo, offset = decode_varint(txInfo)
        blockOffs, size = decode_varint(txInfo[offset:])
        offset += size
        txOffs = decode_varint(txInfo[offset:])

        blkFile = os.path.join(self.path, "blk%05d.dat" % fileNo)
        blk = Block(get_block(blkFile, blockOffs), parent = self)
        tx = blk.get_transaction(txOffs)
        return tx


    def _build_block_index(self):
        """Gets block index data structure from leveldb"""

        if self.cache and os.path.exists(self.cache):
            # load the block index cache from a previous index
            with open(self.cache, 'rb') as f:
                blockIndexes = pickle.load(f)

        # Read the LevelDB
        self.blockIndexes = [DBBlockIndex(format_hash(k[1:]), v)
                             for k, v in self.blockIndexDb.iterator() if k[0] == ord('b')]
        db.close()
        blockIndexes.sort(key=lambda x: x.height)

        if self.cache and not os.path.exists(self.cache):
            # cache the block index for re-use next time
            with open(self.cache, 'wb') as f:
                pickle.dump(blockIndexes, f)

        # remove small forks that may have occured while the node was live.
        # Occassionally a node will receive two different solutions to a block
        # at the same time. The Leveldb index saves both, not pruning the
        # block that leads to a shorter chain once the fork is settled without
        # "-reindex"ing the bitcoind block data. This leads to at least two
        # blocks with the same height in the database.
        # We throw out blocks that don't have at least 6 other blocks on top of
        # it (6 confirmations).
        orphans = []  # hold blocks that are orphans with < 6 blocks on top
        last_height = -1
        for i, blockIdx in enumerate(blockIndexes):
            if last_height > -1:
                # if this block is the same height as the last block an orphan
                # occurred, now we have to figure out which of the two to keep
                if blockIdx.height == last_height:

                    # loop through future blocks until we find a chain 6 blocks
                    # long that includes this block. If we can't find one
                    # remove this block as it is invalid
                    if self._index_confirmed(blockIndexes[i:]):

                        # if this block is confirmed, the unconfirmed block is
                        # the previous one. Remove it.
                        orphans.append(blockIndexes[i - 1].hash)
                    else:

                        # if this block isn't confirmed, remove it.
                        orphans.append(blockIndexes[i].hash)

            last_height = blockIdx.height

        # filter out the orphan blocks, so we are left only with block indexes
        # that have been confirmed
        # (or are new enough that they haven't yet been confirmed)
        return list(filter(lambda block: block.hash not in orphans, blockIndexes))

    def get_block_by_height(self, height):
        """Returns a single block by height index number"""

        blk = self.blockIndexes[height]
        if blk.file == -1 or blk.data_pos == -1:
            raise ValueError("Block not on disk")
        blkFile = os.path.join(self.path, "blk%05d.dat" % blk.file)
        return Block(get_block(blkFile, blk.data_pos), blk.height, self)
        
    def get_ordered_blocks(self, start=0, end=None):
        """Yields the blocks contained in the .blk files as per
        the heigt extract from the leveldb index present at path
        index maintained by bitcoind.
        """

        blockIndexes = self.blockIndexes

        if end is None:
            end = len(blockIndexes)

        if end < start:
            blockIndexes = list(reversed(blockIndexes))
            start = len(blockIndexes) - start
            end = len(blockIndexes) - end

        for blkIdx in blockIndexes[start:end]:
            if blkIdx.file == -1 or blkIdx.data_pos == -1:
                break
            blkFile = os.path.join(self.path, "blk%05d.dat" % blkIdx.file)
            yield Block(get_block(blkFile, blkIdx.data_pos), blkIdx.height, self)
