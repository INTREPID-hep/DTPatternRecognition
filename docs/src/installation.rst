Installation
============

You can install the latest version of the package directly from GitHub using pip or Poetry.

.. note::
    Take a look at the available `tagged versions <https://github.com/DanielEstrada971102/DTPatternRecognition/tags>`_.

.. tab-set::

    .. tab-item:: Pip

        .. code-block:: shell

            pip install "git+https://github.com/DanielEstrada971102/DTPatternRecognition.git@[TAG_VERSION]"

        To check if the package was installed successfully, run:

        .. code-block:: shell

            pip show DTPatternRecognition

        Or simply run ``dtpr --help``

        If you want to work with the source and editable installs, you can clone the repo and install in editable mode:

        .. code-block:: shell

            git clone https://github.com/DanielEstrada971102/DTPatternRecognition -b [TAG_VERSION]
            cd DTPatternRecognition
            pip install -e .

    .. tab-item:: Poetry

        .. code-block:: shell

            poetry add "git+https://github.com/DanielEstrada971102/DTPatternRecognition.git@[TAG_VERSION]"

        To check if the package was installed successfully, run:

        .. code-block:: shell

            poetry show DTPatternRecognition

        Or simply run ``poetry run dtpr --help``

        If you want to work with the source and editable installs, you can clone the repository and build the environment with Poetry:

        .. code-block:: shell

            git clone https://github.com/DanielEstrada971102/DTPatternRecognition -b [TAG_VERSION]
            cd DTPatternRecognition
            poetry install
            poetry shell

.. important::
    The package requires PyROOT, so you should have it installed and available in your environment.
    If you are working in a Python virtual environment, ensure to include ROOT in it.
    For example, to create a venv that can access system-wide ROOT:

    .. code-block:: shell

        python -m venv --system-site-packages ENV_DIR
