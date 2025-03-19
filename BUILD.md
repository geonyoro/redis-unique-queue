# BUILD

Install the requirements.dev.txt requirements.

```bash
pip install -r requirements.dev.txt
```

Build the project

```bash
python3 -m build
```

Use twine to upload the build directory for testing first.

```bash
python3 -m twine upload --repository testpypi dist/*
```

Finally, use twine to upload the build directory.

```bash
python3 -m twine upload dist/*
```
