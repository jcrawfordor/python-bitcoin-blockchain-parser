import os
import dateutil.parser
from .blockchain import Blockchain
import logging
import csv
logging.basicConfig(level=logging.DEBUG)

class Scanner(object):
    """Scanner is a wrapper around a Blockchain that facilitates filtering
    blocks and transactions. The intent is to parallelize this across
    multiple cores using multiprocessing, although this is not yet
    implemented.
    """
    def __init__(self, block_path, leveldb_path, leveldb_cache_path=None):
        self.blockchain = Blockchain(block_path, leveldb_path, cache=leveldb_cache_path)
        self.blocks = self.blockchain.get_ordered_blocks()
        self.selected_blocks = None
        self.selected_tx = None
    
    def load_block_list(self, list_path):
        """Load the results of a previous filter from file. The file is
        expected to be a CSV file with the first field in each line being
        a block height number."""
        self.selected_blocks = []
        with open(list_path) as f:
            rdr = csv.reader(f)
            for l in rdr:
                self.selected_blocks.append(int(l[0]))
    
    def save_block_list(self, list_path, flt=lambda x: True):
        """Save the results list from filtering to file"""
        with open(list_path, "w") as f:
            listwriter = csv.writer(f)
            if self.selected_blocks == None:
                for block in self.filter_blocks(flt):
                    listwriter.writerow([block.height])
            else:
                for block_height in self.selected_blocks:
                    listwriter.writerow([block_height])
    
    def filter_blocks(self, flt):
        """Applies previously set filter function to blocks, acts as iterator"""
        # Simple non-parallel implementation for now
        self.selected_blocks = []
        for n, block in enumerate(self.blocks):
            if n % 1000 == 0:
                logging.info("Filter processed {} blocks ({})".format(n, block.header.timestamp))
            if flt(block):
                self.selected_blocks.append(block.height)
                yield block
    
    def iterate_blocks(self, flt=lambda x: True):
        """Iterable of blocks matching filter, uses previously-loaded list
        of matching blocks if one is available, otherwise runs the filter
        now"""
        if self.selected_blocks == None:
            for block in self.filter_blocks(flt):
                yield block
        else:
            for block_height in self.selected_blocks:
                yield self.blockchain.get_block_by_height(block_height)
    
    def clear_block_filter(self):
        self.selected_blocks = None

    # Not implemented correctly yet
    def load_tx_list(self, list_path):
        """Load the results of a previous filter from file. The file is
        expected to be a CSV file with the first field in each line being
        a block height number and the second field a transaction index
        within the block."""
        self.selected_tx = []
        with open(list_path) as f:
            rdr = csv.reader(f)
            for l in rdr:
                self.selected_tx.append((int(l[0]), int(l[1])))
    
    # Not implemented correctly yet
    def save_tx_list(self, list_path, flt=lambda x: True):
        """Save the results list from filtering to file"""
        with open(list_path, "w") as f:
            listwriter = csv.writer(f)
            if self.selected_tx == None:
                for block, i, tx in self.filter_tx(flt):
                    listwriter.writerow([block.height, i])
            else:
                for block_height, i in self.selected_tx:
                    listwriter.writerow([block_height, i])
    
    def filter_tx(self, flt):
        """Applies previously set filter function to blocks, acts as iterator"""
        # Simple non-parallel implementation for now
        # First we need to get which blocks to work on
        self.selected_tx = []
        for block in self.iterate_blocks():
            for i, tx in enumerate(block.transactions):
                if flt(tx):
                    self.selected_tx.append((block.height, i))
                    yield (block, i, tx)
    
    def iterate_tx(self, flt=lambda x: True):
        """Iterable of blocks matching filter, uses previously-loaded list
        of matching blocks if one is available, otherwise runs the filter
        now"""
        if self.selected_tx == None:
            for block, i, tx in self.filter_tx(flt):
                yield block, i, tx
        else:
            for block_height, i in self.selected_tx:
                block = self.blockchain.get_block_by_height(block_height)
                tx = block.transactions[i]
                yield block, i, tx
    
    def clear_tx_filter(self):
        self.selected_tx = None