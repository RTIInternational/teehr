Install pyenv:
I clone the repo and then ran the `bash` commands from the Readme.md to add to my `~/.bashrc`.
https://github.com/pyenv/pyenv/blob/master/README.md

Install python deps:
```
sudo apt install libreadline-dev
sudo apt install libncurses-dev
sudo apt install tk-dev
sudo apt install libbz2-dev
```
Install python 3.11:
```
pyenv install 3.11
```

To upgrade poetry:
```
pyenv local 3.11.8
poetry env use 3.11.8
poetry install
```

To get back:
```
pyenv local system
poetry env use 3.10.12
poetry install
```




