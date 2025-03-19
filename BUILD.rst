BUILD
====================

- Install the requirements.dev.txt requirements.
pip install -r requirements.dev.txt

Build the project
`python3 -m build`

Use twine to upload the build directory.
python3 -m twine upload --repository testpypi dist/*
