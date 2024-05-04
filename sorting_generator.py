import humanfriendly
import os
import string
import random
from multiprocessing import Pool
import argparse

def run_for_config(save_dir, input_length, record_size):
    # Determine the number of records
    possible_letters = string.ascii_letters + string.digits
    input_bytes = humanfriendly.parse_size(input_length)
    num_records = int(input_bytes/record_size)
    
    # Get the file path
    save_name = f"input_{input_bytes}_{record_size}.txt"
    save_path = os.path.join(save_dir, save_name)
    print("Saving", num_records, "records of size", record_size, "to", save_name)
    if os.path.exists(save_path):
        current_file_size = os.stat(save_path).st_size
        expected_file_size = num_records * (record_size + 1)
        if current_file_size == expected_file_size:
            print("Already generated file of required size")
            return

    # Write the records
    with open(save_path, 'w+') as writer:
        for _ in range(num_records):
            record_to_write = ''.join(random.choices(possible_letters, k = record_size))
            writer.write(record_to_write + "\n")

def load_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--save_dir", type=str, help="The directory we want to save the results to")
    parser.add_argument("--num_process", type=int, help="The number of processes we want to use")
    return parser.parse_args()

def main():
    args = load_args()
    save_dir = args.save_dir
    input_lengths = ["50 MB", "125 MB", "12 GB", "120 GB"]
    record_sizes = [100, 1000, 2000]
    os.makedirs(save_dir, exist_ok = True)
    creator_pool = Pool(processes = args.num_process)

    # Launch the workers
    all_results = []
    for input_length in input_lengths:
        for record_size in record_sizes:
            result = creator_pool.apply_async(run_for_config, (save_dir, input_length,record_size,))
            all_results.append(result)
    
    print("Waiting for results")
    [result.wait() for result in all_results]
    print("Got all results")

if __name__ == "__main__":
    main()