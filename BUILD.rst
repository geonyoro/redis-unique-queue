BUILD
====================

Install the requirements.dev.txt requirements.
.. code-block:: bash

  pip install -r requirements.dev.txt

Build the project
.. code-block:: bash

  python3 -m build

Use twine to upload the build directory for testing first.
.. code-block:: bash

  python3 -m twine upload --repository testpypi dist/*

Finally, use twine to upload the build directory.
.. code-block:: bash

  python3 -m twine upload dist/*
