# cs744-data-generator

Code to generate dataset for CS 764. 
You can install the requirements using:
```
$ pip install -r requirements.txt
```

Then you can create the dataset using the `sorting_generator.py` which has the arguments:
```
usage: sorting_generator.py [-h] [--save_dir SAVE_DIR] [--num_process NUM_PROCESS]

options:
  -h, --help            show this help message and exit
  --save_dir SAVE_DIR   The directory we want to save the results to (default: None)
  --num_process NUM_PROCESS
                        The number of processes we want to use (default: None)
```

Thus if you want to use a single process to generate the data to a folder called `dataset` then you can call it as such: 
```
$ python sorting_generator.py --save_dir dataset --num_process 1
```