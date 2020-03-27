import sys
import os
import argparse
import hashlib
from multiprocessing import Pool, Manager

def hash_file(filename,chunk_size=2**20):
    """
    Hash a file with sha256
    param filename: file to hash
    param chunk_size: how much a file to read in at a time
    returns: the sha256 digest of the file
    """
    hash = hashlib.sha256()
    with open(filename, 'rb') as f:
        # Read an entire MB, my god the luxury
        chunk = f.read(chunk_size)
        while chunk:
            hash.update(chunk)
            chunk = f.read(chunk_size)
    return hash.digest()

def hash_directory_file(d, path, filename):
    """
    Store hash for filename at path to dict d
    param d: dict to store the hash in
    param path: path to the file
    param filename: file to hash
    """
    full_path = os.path.join(path,filename)
    d[full_path] = hash_file(full_path)

def compare_directories(dir1_files, dir2_files, threads=1):
    """
    Compare two dicts containing files and their hashes, optionally in parallel
    param dir1_files: proxy dict containing files and their hashes
    param dir2_files: proxy dict containing files and their hashes
    param threads: number of workers to start to do the comparison
    returns: a list of files that differ between the two directories
    """
    whoops = list()
    thread_pool = Pool(threads)
    def callback_function(result):
        if result is not None:
            whoops.append(result)
    for k in dir1_files.keys():
        thread_pool.apply_async(check_file,(k, dir1_files, dir2_files),callback=callback_function)
    thread_pool.close()
    thread_pool.join()
    return whoops

def check_file(f, dir1, dir2):
    """
    Check if f is a key in dir1 or dir2 and if the hashes match
    param f: a file that exists in dir1
    param dir1: a dictionary containing files and their hashes
    param dir2: a dictionary containing files and hashes to compare to
    returns: f if f is not in dir2 or has a different hash
    """
    if f not in dir2.keys():
        return f
    if dir1.get(f) != dir2.get(f):
        return f

def main():
    parser = argparse.ArgumentParser(description="Compare two directories.")
    parser.add_argument('-r', '--recursive', action="store_true", help="Recurse through given directories.")
    parser.add_argument('-t', '--threads', type=int, default=1, help="Number of threads to run concurrently")
    parser.add_argument("dir1", type=str, help="First directory to compare")
    parser.add_argument("dir2", type=str, help="Second directory to compare")
    args = parser.parse_args()
    
    if not os.path.exists(args.dir1) or not os.path.exists(args.dir2):
        raise RuntimeError("Check your file paths. One or both of them are incorrect.")
    # We'll store the hashes with the relative path+filename as the key
    with Manager() as manager:
        # Need proxies for the dicts to share them among the workers
        dir1_files = manager.dict()
        dir2_files = manager.dict()
        thread_pool = Pool(args.threads)
        if args.recursive:
            for path, dirs, files in os.walk(args.dir1):
                for f in files:
                    thread_pool.apply_async(hash_directory_file, (dir1_files, path, f))
            for path, dirs, files in os.walk(args.dir2):
                for f in files:
                    thread_pool.apply_async(hash_directory_file, (dir2_files, path, f))
            thread_pool.close()
            thread_pool.join()
        else:
            for f in os.listdir(args.dir1):
                thread_pool.apply_async(hash_directory_file, (dir1_files, args.dir1, f))
            for f in os.listdir(args.dir2):
                thread_pool.apply_async(hash_directory_file, (dir2_files, args.dir2, f))
        thread_pool.close()
        thread_pool.join()
        wrong_files = compare_directories(dir1_files, dir2_files, args.threads)
        if wrong_files:
            print("The following files were present in "+args.dir1+" and found to be missing in "+args.dir2+" or their hashes differ:")
            for f in wrong_files:
                print(f)
        else:
            print("All good. Apparently")
    
if sys.version_info[0] <= 3 and sys.version_info[1] < 6:
    raise Exception("Requires at least python 3.6")
    
if __name__ == "__main__":
    main()