Installation
============

To install, execute

::

    pip install datajudge

or from a conda environment

::

    conda install datajudge -c conda-forge



Snowflake
^^^^

If your backend is ``snowflake`` and you are querying large datasets,
you can additionally install ``pandas`` to make use of very fast query loading
(up to 50x speedup for large datasets).
    Note: The ``pandas`` requirement is a bug in the snowflake-python-connector
    and will not be needed in the future.
