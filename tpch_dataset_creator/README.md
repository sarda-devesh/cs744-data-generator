# TPCH Dataset Creator

# Dependecies

Ensure that you have `make` in your environment in order to build the `dbgen` code. Additionally install the required dependencies using the command:
```
$ python3 -m pip install -r requirements.txt
```


## Creating the dataset

You can use the `main.py` function in order to create the dataset which has the following arguments:
```
usage: main.py [-h] --save_dir SAVE_DIR [--overwrite] [--scale_factor SCALE_FACTOR]

options:
  -h, --help            show this help message and exit
  --save_dir SAVE_DIR   The directory to save the results to (default: None)
  --overwrite           Overwrite if results already exists (default: False)
  --scale_factor SCALE_FACTOR
                        The SF of the data to generate (default: 1)
```

For example, here is a command to create a dataset of SF = 10 and save it to a directory:
```
$ python3 main.py --scale_factor 10 --save_dir "/working_dir/shared_datasets/test_sf10" --overwrite
```

Do note that this is going to be take a while and thus we recommend running this command either in the background or in a `screen` or `tmux` sessions.